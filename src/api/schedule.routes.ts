import { Router } from "express";
import { db } from "../db/client";
import { emailQueue } from "../queue/queue";
import multer from "multer";
import path from "path";
import fs from "fs";

const router = Router();

// Ensure local uploads directory exists
const uploadDir = path.resolve(process.cwd(), "uploads");
fs.mkdirSync(uploadDir, { recursive: true });

// Configure multer for file attachments (up to 10 files, 10MB each)
const upload = multer({
  dest: uploadDir,
  limits: {
    files: 10,
    fileSize: 10 * 1024 * 1024,
  },
});

/**
 * GET /api/emails/scheduled
 * Retrieves all pending or currently processing scheduled emails.
 * Supports optional ?sender=... filter for user dashboard scoping.
 */
router.get("/scheduled", async (req, res) => {
  try {
    const sender = typeof req.query.sender === "string" ? req.query.sender.trim() : null;

    let query = `
      SELECT id, recipient, subject, body, sender, scheduled_at, status, created_at, ethereal_url
      FROM emails
      WHERE status IN ('scheduled', 'processing')
    `;
    const params: any[] = [];

    if (sender) {
      query += " AND sender = ?";
      params.push(sender);
    }

    query += " ORDER BY scheduled_at ASC";

    const [rows] = await db.execute(query, params);
    res.json(rows);
  } catch (error) {
    console.error("Failed to fetch scheduled emails:", error);
    res.status(500).json({ error: "Failed to fetch scheduled emails" });
  }
});

/**
 * GET /api/emails/sent
 * Retrieves all sent and failed emails with execution timestamps and Ethereal preview URLs.
 * Supports optional ?sender=... filter for user dashboard scoping.
 */
router.get("/sent", async (req, res) => {
  try {
    const sender = typeof req.query.sender === "string" ? req.query.sender.trim() : null;

    let query = `
      SELECT id, recipient, subject, body, sender, scheduled_at, status, sent_at, ethereal_url
      FROM emails
      WHERE status IN ('sent', 'failed')
    `;
    const params: any[] = [];

    if (sender) {
      query += " AND sender = ?";
      params.push(sender);
    }

    query += " ORDER BY id DESC";

    const [rows] = await db.execute(query, params);
    res.json(rows);
  } catch (error) {
    console.error("Failed to fetch sent emails:", error);
    res.status(500).json({ error: "Failed to fetch sent emails" });
  }
});

/**
 * POST /api/emails/schedule
 * Schedules single or bulk emails using BullMQ delayed jobs and persists records in MySQL.
 */
router.post("/schedule", upload.array("attachments", 10), async (req, res) => {
  const files = (req.files as Express.Multer.File[]) || [];
  const createdIds: number[] = [];
  const queuedJobs: any[] = [];

  try {
    const { subject, body, sender, scheduledAt, delaySeconds, hourlyLimit } = req.body;

    // Parse recipients list from JSON array or single recipient string
    let recipients: string[] = [];
    if (req.body.recipients) {
      const rawRecipients =
        typeof req.body.recipients === "string"
          ? JSON.parse(req.body.recipients)
          : req.body.recipients;

      if (!Array.isArray(rawRecipients)) {
        throw new Error("Recipients must be an array of email addresses.");
      }
      recipients = rawRecipients;
    } else if (req.body.recipient) {
      recipients = [req.body.recipient];
    }

    // Clean and deduplicate recipient emails
    recipients = [
      ...new Set(
        recipients
          .filter((item): item is string => typeof item === "string")
          .map((item) => item.trim().toLowerCase())
          .filter(Boolean)
      ),
    ];

    if (!recipients.length) {
      res.status(400).json({ error: "At least one recipient email address is required." });
      return;
    }

    if (recipients.length > 500) {
      res.status(400).json({ error: "A maximum of 500 recipients is allowed per request." });
      return;
    }

    // Basic email format validation
    const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    const invalidEmail = recipients.find((item) => !emailPattern.test(item));
    if (invalidEmail) {
      res.status(400).json({ error: `Invalid email address detected: ${invalidEmail}` });
      return;
    }

    if (!subject?.trim() || !body?.trim() || !sender?.trim() || !scheduledAt) {
      res.status(400).json({
        error: "Subject, body, sender, and scheduledAt are required fields.",
      });
      return;
    }

    const sendTime = new Date(scheduledAt);
    if (Number.isNaN(sendTime.getTime())) {
      res.status(400).json({ error: "Invalid scheduled date format." });
      return;
    }

    // Persist attachments with metadata and Base64 fallback
    const attachmentData = JSON.stringify(
      await Promise.all(
        files.map(async (file) => {
          let base64Data: string | undefined;
          try {
            const buffer = await fs.promises.readFile(file.path);
            base64Data = buffer.toString("base64");
          } catch (err) {
            console.warn("Could not read file for Base64 cache:", err);
          }
          return {
            filename: file.originalname,
            path: path.resolve(file.path),
            mimetype: file.mimetype,
            data: base64Data,
          };
        })
      )
    );

    const parsedDelay = Math.max(0, Number(delaySeconds) || 2);
    const delayMs = parsedDelay * 1000;
    const parsedHourlyLimit = Math.max(1, Number(hourlyLimit) || 100);
    const baseTime = sendTime.getTime();

    // Iterate through recipients and schedule with staggered delays
    for (let i = 0; i < recipients.length; i++) {
      const recipient = recipients[i];

      // Stagger send timestamps across hour windows and send delays
      const hourIndex = Math.floor(i / parsedHourlyLimit);
      const offsetWithinHour = i % parsedHourlyLimit;
      const emailSendTimeMs = baseTime + hourIndex * 3_600_000 + offsetWithinHour * delayMs;
      const emailSendTime = new Date(emailSendTimeMs);

      // Insert record into MySQL
      const [result]: any = await db.execute(
        `INSERT INTO emails (recipient, subject, body, sender, scheduled_at, attachments)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [recipient, subject.trim(), body, sender.trim(), emailSendTime, attachmentData]
      );

      const emailId = Number(result.insertId);
      createdIds.push(emailId);

      // Add delayed job to BullMQ queue
      const job = await emailQueue.add(
        "send-email",
        {
          emailId,
          delaySeconds: parsedDelay,
          hourlyLimit: parsedHourlyLimit,
        },
        {
          delay: Math.max(0, emailSendTimeMs - Date.now()),
          jobId: `email-${emailId}`, // Deterministic job ID prevents duplicate queuing
          attempts: 5,
          backoff: {
            type: "exponential",
            delay: 5000,
          },
        }
      );

      queuedJobs.push(job);
    }

    res.status(201).json({
      message: "Emails scheduled successfully.",
      recipientCount: recipients.length,
      emailIds: createdIds,
    });
  } catch (error) {
    console.error("Scheduling failed:", error);

    // Rollback queued jobs if an error occurs mid-operation
    for (const job of queuedJobs) {
      try {
        await job.remove();
      } catch (err) {
        console.error("Could not remove job during rollback:", err);
      }
    }

    if (createdIds.length > 0) {
      try {
        const placeholders = createdIds.map(() => "?").join(",");
        await db.execute(`DELETE FROM emails WHERE id IN (${placeholders})`, createdIds);
      } catch (err) {
        console.error("Could not delete emails during rollback:", err);
      }
    }

    res.status(500).json({ error: "Failed to schedule emails. Please check inputs and retry." });
  }
});

/**
 * DELETE /api/emails/:id
 * Cancels and deletes a scheduled email before it is dispatched.
 */
router.delete("/:id", async (req, res) => {
  try {
    const emailId = Number(req.params.id);
    if (!Number.isInteger(emailId) || emailId <= 0) {
      res.status(400).json({ error: "Invalid email ID" });
      return;
    }

    // Check if email is scheduled
    const [rows]: any = await db.execute("SELECT * FROM emails WHERE id = ?", [emailId]);
    if (!rows.length) {
      res.status(404).json({ error: "Email not found" });
      return;
    }

    const email = rows[0];
    if (email.status === "sent") {
      res.status(400).json({ error: "Cannot cancel an email that has already been sent." });
      return;
    }

    // Remove job from BullMQ queue
    try {
      const job = await emailQueue.getJob(`email-${emailId}`);
      if (job) {
        await job.remove();
      }
    } catch (jobErr) {
      console.warn("Could not find or remove BullMQ job:", jobErr);
    }

    // Delete email record from MySQL
    await db.execute("DELETE FROM emails WHERE id = ?", [emailId]);

    res.json({ success: true, message: "Scheduled email cancelled and removed." });
  } catch (error) {
    console.error("Failed to cancel email:", error);
    res.status(500).json({ error: "Could not cancel scheduled email." });
  }
});

/**
 * GET /api/emails/:id/attachments/:index
 * Downloads or previews an uploaded email attachment.
 */
router.get("/:id/attachments/:index", async (req, res) => {
  try {
    const emailId = Number(req.params.id);
    const index = Number(req.params.index);

    if (!Number.isInteger(emailId) || emailId <= 0 || !Number.isInteger(index) || index < 0) {
      res.status(400).json({ error: "Invalid attachment request parameters." });
      return;
    }

    const [rows]: any = await db.execute("SELECT attachments FROM emails WHERE id = ?", [emailId]);
    if (!rows.length || !rows[0].attachments) {
      res.status(404).json({ error: "Attachment not found" });
      return;
    }

    const attachments =
      typeof rows[0].attachments === "string"
        ? JSON.parse(rows[0].attachments)
        : rows[0].attachments;

    const file = attachments[index];
    if (!file) {
      res.status(404).json({ error: "Attachment index not found" });
      return;
    }

    res.setHeader("Content-Type", file.mimetype || "application/octet-stream");
    res.setHeader(
      "Content-Disposition",
      `inline; filename="${encodeURIComponent(file.filename || "attachment")}"`
    );
    res.setHeader("X-Content-Type-Options", "nosniff");

    const filePath = file.path ? path.resolve(file.path) : null;
    const resolvedUploadDir = path.resolve(uploadDir);

    if (filePath && filePath.startsWith(resolvedUploadDir + path.sep) && fs.existsSync(filePath)) {
      res.sendFile(filePath);
      return;
    }

    if (file.data) {
      res.send(Buffer.from(file.data, "base64"));
      return;
    }

    res.status(404).json({ error: "Attachment file content is no longer available" });
  } catch (error) {
    console.error("Attachment retrieval error:", error);
    res.status(500).json({ error: "Could not load attachment file" });
  }
});

/**
 * GET /api/emails/:id
 * Fetches single email details.
 */
router.get("/:id", async (req, res) => {
  try {
    const [rows]: any = await db.execute("SELECT * FROM emails WHERE id = ?", [req.params.id]);
    if (!rows.length) {
      res.status(404).json({ message: "Email not found" });
      return;
    }
    res.json(rows[0]);
  } catch (error) {
    console.error("Failed to fetch email details:", error);
    res.status(500).json({ message: "Failed to fetch email details" });
  }
});

export default router;
