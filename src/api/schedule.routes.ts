import { Router } from "express";
import { db } from "../db/client";
import { emailQueue } from "../queue/queue";

import multer from "multer";
import path from "path";
import fs from "fs";

const router = Router();

const uploadDir = path.resolve(process.cwd(), "uploads");
fs.mkdirSync(uploadDir, { recursive: true });

const upload = multer({
  dest: uploadDir,
  limits: {
    files: 10,
    fileSize: 10 * 1024 * 1024,
  },
});

// GET scheduled emails
router.get("/scheduled", async (_req, res) => {
  try {
    const [rows] = await db.execute(
      `SELECT id, recipient, subject, body, sender, scheduled_at, status, created_at
       FROM emails
       WHERE status IN ('scheduled', 'processing')
       ORDER BY scheduled_at ASC`
    );

    res.json(rows);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Failed to fetch scheduled emails" });
  }
});

// GET sent and failed emails
router.get("/sent", async (_req, res) => {
  try {
    const [rows] = await db.execute(
      `SELECT id, recipient, subject, body, sender, scheduled_at, status, sent_at
       FROM emails
       WHERE status IN ('sent', 'failed')
       ORDER BY id DESC`
    );

    res.json(rows);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Failed to fetch sent emails" });
  }
});

router.post(
  "/schedule",
  upload.array("attachments", 10),
  async (req, res) => {
    const files = (req.files as Express.Multer.File[]) || [];
    const createdIds: number[] = [];
    const queuedJobs: any[] = [];

    try {
      const {
        subject,
        body,
        sender,
        scheduledAt,
        delaySeconds,
        hourlyLimit,
      } = req.body;

      // Supports both bulk recipients and the existing single-recipient API.
      let recipients: string[];

      if (req.body.recipients) {
        const rawRecipients =
          typeof req.body.recipients === "string"
            ? JSON.parse(req.body.recipients)
            : req.body.recipients;

        if (!Array.isArray(rawRecipients)) {
          throw new Error("Recipients must be an array.");
        }

        recipients = rawRecipients;
      } else if (req.body.recipient) {
        recipients = [req.body.recipient];
      } else {
        recipients = [];
      }

      recipients = [
        ...new Set(
          recipients
            .filter((value): value is string => typeof value === "string")
            .map((value) => value.trim().toLowerCase())
            .filter(Boolean)
        ),
      ];

      if (!recipients.length) {
        res.status(400).json({ error: "At least one recipient is required." });
        return;
      }

      if (recipients.length > 500) {
        res.status(400).json({ error: "Maximum 500 recipients per request." });
        return;
      }

      const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      const invalidRecipient = recipients.find(
        (recipient) => !emailPattern.test(recipient)
      );

      if (invalidRecipient) {
        res.status(400).json({
          error: `Invalid email address: ${invalidRecipient}`,
        });
        return;
      }

      if (!subject?.trim() || !body?.trim() || !sender?.trim() || !scheduledAt) {
        res.status(400).json({
          error: "Subject, body, sender, and scheduledAt are required.",
        });
        return;
      }

      const sendTime = new Date(scheduledAt);

      if (
        Number.isNaN(sendTime.getTime()) ||
        sendTime.getTime() <= Date.now()
      ) {
        res.status(400).json({
          error: "scheduledAt must be a future date.",
        });
        return;
      }

      const attachmentData = JSON.stringify(
        files.map((file) => ({
          filename: file.originalname,
          path: path.resolve(file.path),
          mimetype: file.mimetype,
        }))
      );

      const parsedDelay = Math.max(0, Number(delaySeconds) || 0);
      const delayMs = parsedDelay * 1000;
      const parsedHourlyLimit = Math.max(
        1,
        Number(hourlyLimit) || Number(process.env.MAX_EMAILS_PER_HOUR) || 100
      );
      const baseTime = sendTime.getTime();

      // Create DB records and queue jobs with staggered delay & hourly limit
      for (let i = 0; i < recipients.length; i++) {
        const recipient = recipients[i];

        // Stagger recipients by delay and hourly limit
        const hourIndex = Math.floor(i / parsedHourlyLimit);
        const offsetWithinHour = i % parsedHourlyLimit;
        const emailSendTimeMs =
          baseTime + hourIndex * 3_600_000 + offsetWithinHour * delayMs;
        const emailSendTime = new Date(emailSendTimeMs);

        const [result]: any = await db.execute(
          `INSERT INTO emails
            (recipient, subject, body, sender, scheduled_at, attachments)
           VALUES (?, ?, ?, ?, ?, ?)`,
          [
            recipient,
            subject.trim(),
            body,
            sender.trim(),
            emailSendTime,
            attachmentData,
          ]
        );

        const emailId = Number(result.insertId);
        createdIds.push(emailId);

        const job = await emailQueue.add(
          "send-email",
          {
            emailId,
            delaySeconds: parsedDelay,
            hourlyLimit: parsedHourlyLimit,
          },
          {
            delay: Math.max(0, emailSendTimeMs - Date.now()),
            jobId: `email-${emailId}`,
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
      console.error("Bulk scheduling failed:", error);

      // Best-effort cleanup if the operation fails partway through.
      for (const job of queuedJobs) {
        try {
          await job.remove();
        } catch (cleanupError) {
          console.error("Could not remove queued job:", cleanupError);
        }
      }

      if (createdIds.length > 0) {
        try {
          const placeholders = createdIds.map(() => "?").join(",");
          await db.execute(
            `DELETE FROM emails WHERE id IN (${placeholders})`,
            createdIds
          );
        } catch (cleanupError) {
          console.error("Could not clean up email records:", cleanupError);
        }
      }

      for (const file of files) {
        try {
          await fs.promises.unlink(file.path);
        } catch {
          // File may already have been removed.
        }
      }

      res.status(500).json({
        error: "Failed to schedule emails. Please try again.",
      });
    }
  }
);

// GET an email attachment for preview
router.get("/:id/attachments/:index", async (req, res) => {
  try {
    const emailId = Number(req.params.id);
    const index = Number(req.params.index);

    if (
      !Number.isInteger(emailId) ||
      emailId <= 0 ||
      !Number.isInteger(index) ||
      index < 0
    ) {
      res.status(400).json({ error: "Invalid attachment request" });
      return;
    }

    const [rows]: any = await db.execute(
      "SELECT attachments FROM emails WHERE id = ?",
      [emailId]
    );

    if (!rows.length || !rows[0].attachments) {
      res.status(404).json({ error: "Attachments not found" });
      return;
    }

    const attachments =
      typeof rows[0].attachments === "string"
        ? JSON.parse(rows[0].attachments)
        : rows[0].attachments;

    const file = attachments[index];

    if (!file?.path || !file?.mimetype) {
      res.status(404).json({ error: "Attachment not found" });
      return;
    }

    const filePath = path.resolve(file.path);
    const resolvedUploadDir = path.resolve(uploadDir);

    // Only serve files stored inside the uploads directory.
    if (!filePath.startsWith(resolvedUploadDir + path.sep)) {
      res.status(403).json({ error: "Invalid attachment path" });
      return;
    }

    if (!fs.existsSync(filePath)) {
      res.status(404).json({ error: "Attachment file no longer exists" });
      return;
    }

    res.setHeader("Content-Type", file.mimetype);
    res.setHeader("Content-Disposition", "inline");
    res.setHeader("X-Content-Type-Options", "nosniff");

    res.sendFile(filePath);
  } catch (error) {
    console.error("Attachment preview failed:", error);
    res.status(500).json({ error: "Could not load attachment" });
  }
});

router.get("/:id", async (req, res) => {
  try {
    const [rows]: any = await db.execute(
      "SELECT * FROM emails WHERE id = ?",
      [req.params.id]
    );

    if (!rows.length) {
      return res.status(404).json({ message: "Email not found" });
    }

    return res.json(rows[0]);
  } catch (error) {
    console.error("Failed to fetch email:", error);
    return res.status(500).json({ message: "Failed to fetch email" });
  }
});

export default router;
