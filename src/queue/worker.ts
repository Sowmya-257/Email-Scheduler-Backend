import { Worker, DelayedError } from "bullmq";
import nodemailer from "nodemailer";
import dotenv from "dotenv";
import { redisConnection } from "./queue";
import { db } from "../db/client";
import { reserveHourlySlot } from "../rateLimiter/hourlyLimiter";

dotenv.config();

const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST,
  port: Number(process.env.SMTP_PORT),
  secure: false,
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS,
  },
});

const worker = new Worker(
  "email-queue",
  async (job, token) => {
    const { emailId, delaySeconds = 5, hourlyLimit = 10 } = job.data;

    const [rows]: any = await db.execute(
      "SELECT * FROM emails WHERE id = ?",
      [emailId]
    );

    const email = rows[0];

    if (!email || email.status === "sent") return;

    // Check rate limit and send delay before processing.
    const { allowed, retryAt } = await reserveHourlySlot(
      email.sender,
      Number(emailId),
      Number(delaySeconds),
      Number(hourlyLimit)
    );

    if (!allowed) {
      if (!token) throw new Error("Missing BullMQ job lock token");

      await db.execute(
        "UPDATE emails SET scheduled_at = FROM_UNIXTIME(? / 1000) WHERE id = ?",
        [retryAt, emailId]
      );
      await job.moveToDelayed(retryAt, token);
      throw new DelayedError();
    }

    await db.execute(
      "UPDATE emails SET status = 'processing' WHERE id = ?",
      [emailId]
    );

    try {
      const attachments = email.attachments
        ? (typeof email.attachments === "string"
            ? JSON.parse(email.attachments)
            : email.attachments)
        : [];

      const info = await transporter.sendMail({
        from: email.sender,
        to: email.recipient,
        subject: email.subject,
        html: email.body,
        attachments: attachments.map(
          (file: { filename: string; path: string; mimetype: string }) => ({
            filename: file.filename,
            path: file.path,
            contentType: file.mimetype,
          })
        ),
      });

      await db.execute(
        "UPDATE emails SET status = 'sent', sent_at = NOW() WHERE id = ?",
        [emailId]
      );
      await redisConnection.del(`reservation:${emailId}`);
      console.log("Email sent:", info.messageId);
    } catch (error) {
      const attemptsMade = job.attemptsMade;
      const maxAttempts = Number(job.opts.attempts ?? 5);

      if (attemptsMade + 1 >= maxAttempts) {
        await redisConnection.del(`reservation:${emailId}`);
        await db.execute(
          "UPDATE emails SET status = 'failed' WHERE id = ?",
          [emailId]
        );
      } else {
        await db.execute(
          "UPDATE emails SET status = 'scheduled' WHERE id = ?",
          [emailId]
        );
      }

      throw error;
    }
  },
  {
    connection: redisConnection,
    concurrency: Number(process.env.WORKER_CONCURRENCY || 5),
  }
);

worker.on("ready", () => console.log("Email worker started"));
worker.on("completed", (job) => console.log(`Job ${job.id} completed`));
worker.on("failed", (job, err) =>
  console.error(`Job ${job?.id} failed:`, err.message)
);
worker.on("error", (err) => console.error("Worker error:", err.message));
