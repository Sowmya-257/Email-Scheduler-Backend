import mysql from "mysql2/promise";
import dotenv from "dotenv";

dotenv.config();

// Create connection pool for MySQL
export const db = process.env.DATABASE_URL
  ? mysql.createPool({
      uri: process.env.DATABASE_URL,
      ssl: {
        rejectUnauthorized: false,
      },
      connectionLimit: 10,
    })
  : mysql.createPool({
      host: process.env.DB_HOST || "localhost",
      port: Number(process.env.DB_PORT || 3306),
      user: process.env.DB_USER || "root",
      password: process.env.DB_PASSWORD || "root123",
      database: process.env.DB_NAME || "reachinbox",
      ssl: process.env.DB_SSL === "true" ? { rejectUnauthorized: false } : undefined,
      connectionLimit: 10,
    });

// Initializes database table and runs automatic schema adjustments
export async function initDb() {
  try {
    await db.query(`
      CREATE TABLE IF NOT EXISTS emails (
        id INT AUTO_INCREMENT PRIMARY KEY,
        recipient VARCHAR(255) NOT NULL,
        subject VARCHAR(255) NOT NULL,
        body LONGTEXT NOT NULL,
        attachments LONGTEXT NULL,
        sender VARCHAR(255) NOT NULL,
        scheduled_at DATETIME NOT NULL,
        status ENUM('scheduled', 'processing', 'sent', 'failed') NOT NULL DEFAULT 'scheduled',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        sent_at DATETIME NULL,
        ethereal_url VARCHAR(500) NULL,
        INDEX idx_sender_status (sender, status),
        INDEX idx_scheduled_at (scheduled_at)
      )
    `);

    // Safely add ethereal_url column if it doesn't already exist in older deployments
    const [cols]: any = await db.query(
      `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'emails' AND COLUMN_NAME = 'ethereal_url'`
    );
    if (!cols || cols.length === 0) {
      await db.query("ALTER TABLE emails ADD COLUMN ethereal_url VARCHAR(500) NULL");
    }

    // Safely remove restrictive unique key from older schema if it exists
    const [indexes]: any = await db.query(
      `SHOW INDEX FROM emails WHERE Key_name = 'unique_email_schedule'`
    );
    if (indexes && indexes.length > 0) {
      await db.query("ALTER TABLE emails DROP INDEX unique_email_schedule");
    }

    // Reset any stale processing jobs from unexpected shutdowns or timeouts
    await db.query("UPDATE emails SET status = 'scheduled' WHERE status = 'processing'");
  } catch (err: any) {
    console.warn("Database initialization notice:", err?.message || err);
  }
}
