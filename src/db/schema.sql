-- ReachInbox Email Scheduler Database Schema
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
);
