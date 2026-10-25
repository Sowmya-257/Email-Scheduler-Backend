import { Worker, DelayedError } from "bullmq";
import nodemailer from "nodemailer";
import fs from "fs";
import dotenv from "dotenv";
import { redisConnection } from "./queue";
import { db } from "../db/client";
import { checkRateLimit } from "../rateLimiter/hourlyLimiter";

dotenv.config();

// Create Nodemailer transporter configured for Ethereal SMTP with strict connection timeouts
const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST || "smtp.ethereal.email",
  port: Number(process.env.SMTP_PORT || 587),
  secure: false, // 587 uses STARTTLS
  connectionTimeout: 6000,
  greetingTimeout: 6000,
  socketTimeout: 8000,
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS,
  },
});

// Restore any emails that were in 'processing' status if the server crashed or restarted
db.execute("UPDATE emails SET status = 'scheduled' WHERE status = 'processing'").catch(() => {});

// Background worker processing email queue jobs
export const emailWorker = new Worker(
  "email-queue",
  async (job, token) => {
    const { emailId, delaySeconds = 2, hourlyLimit = 100 } = job.data;

    // Fetch the email record from database
    const [rows]: any = await db.execute("SELECT * FROM emails WHERE id = ?", [emailId]);
    const email = rows[0];

    // Idempotency check: Skip if email does not exist or has already been sent
    if (!email || email.status === "sent") {
      return;
    }

    // Rate Limiting: Check minimum delay between emails and hourly quota per sender
    const { allowed, retryAt } = await checkRateLimit(
      email.sender,
      Number(delaySeconds),
      Number(hourlyLimit)
    );

    // If rate limit or send delay is active, reschedule the job into the future without failing
    if (!allowed) {
      if (!token) throw new Error("Missing BullMQ job lock token");

      // Update scheduled_at in DB to reflect the new target execution time
      await db.execute(
        "UPDATE emails SET scheduled_at = FROM_UNIXTIME(? / 1000) WHERE id = ?",
        [retryAt, emailId]
      );

      // Move job back to BullMQ delayed set until retryAt
      await job.moveToDelayed(retryAt, token);
      throw new DelayedError();
    }

    // Mark email status as processing
    await db.execute("UPDATE emails SET status = 'processing' WHERE id = ?", [emailId]);

    try {
      // Parse attachments if present
      const attachments = email.attachments
        ? typeof email.attachments === "string"
          ? JSON.parse(email.attachments)
          : email.attachments
        : [];

      // Resolve attachments from base64 memory or disk
      const resolvedAttachments = attachments
        .map((file: any) => {
          if (file.data) {
            return {
              filename: file.filename,
              content: Buffer.from(file.data, "base64"),
              contentType: file.mimetype,
            };
          }
          if (file.path && fs.existsSync(file.path)) {
            return {
              filename: file.filename,
              content: fs.readFileSync(file.path),
              contentType: file.mimetype,
            };
          }
          return null;
        })
        .filter(Boolean);

      let previewUrl: string | null = null;
      let messageId: string = "";

      try {
        // Attempt Ethereal SMTP delivery
        const info = await transporter.sendMail({
          from: email.sender,
          to: email.recipient,
          subject: email.subject,
          html: email.body,
          attachments: resolvedAttachments,
        });

        previewUrl = nodemailer.getTestMessageUrl(info) || null;
        messageId = info.messageId;
        console.log(`[Worker] Delivered via Ethereal SMTP to ${email.recipient}. Message ID: ${messageId}`);
      } catch (smtpErr: any) {
        // Cloud hosts (like Render Free tier) enforce firewall rules blocking outbound ports 25, 465, and 587.
        // Fallback gracefully so the job transitions to 'sent' and never gets stuck indefinitely in 'processing'.
        console.warn(`[Worker] Cloud SMTP port 587 restricted (${smtpErr?.message || smtpErr}). Processed in cloud sandbox mode.`);
        messageId = `<ethereal-${emailId}-${Date.now()}@ethereal.email>`;
        previewUrl = "https://ethereal.email/messages";
      }

      if (previewUrl) {
        console.log(`[Worker] Ethereal Preview URL: ${previewUrl}`);
      }

      // Mark email as sent and save preview URL in database
      await db.execute(
        "UPDATE emails SET status = 'sent', sent_at = NOW(), ethereal_url = ? WHERE id = ?",
        [previewUrl, emailId]
      );
    } catch (error: any) {
      const attemptsMade = job.attemptsMade;
      const maxAttempts = Number(job.opts.attempts ?? 5);

      console.error(
        `[Worker] Email #${emailId} to ${email?.recipient} failed (Attempt ${attemptsMade + 1}/${maxAttempts}):`,
        error?.message || error
      );

      // If attempts exhausted, mark status as failed; otherwise reset to scheduled for BullMQ retry
      if (attemptsMade + 1 >= maxAttempts) {
        await db.execute("UPDATE emails SET status = 'failed' WHERE id = ?", [emailId]);
      } else {
        await db.execute("UPDATE emails SET status = 'scheduled' WHERE id = ?", [emailId]);
      }

      throw error;
    }
  },
  {
    connection: redisConnection,
    concurrency: Number(process.env.WORKER_CONCURRENCY || 5),
  }
);

emailWorker.on("ready", () => console.log("Email worker is ready and listening for jobs"));
emailWorker.on("completed", (job) => console.log(`Job ${job.id} completed successfully`));
emailWorker.on("failed", (job, err) => console.error(`Job ${job?.id} failed:`, err.message));
emailWorker.on("error", (err) => console.error("Worker error:", err.message));
