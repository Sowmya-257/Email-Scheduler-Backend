CREATE TABLE IF NOT EXISTS emails (
    id INT AUTO_INCREMENT PRIMARY KEY,
    recipient VARCHAR(255) NOT NULL,
    subject VARCHAR(255) NOT NULL,
    body TEXT NOT NULL,
    attachments JSON NULL,
    sender VARCHAR(255) NOT NULL,
    scheduled_at DATETIME NOT NULL,
    status ENUM('scheduled', 'processing', 'sent', 'failed')
        NOT NULL DEFAULT 'scheduled',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sent_at DATETIME NULL,
    UNIQUE KEY unique_email_schedule (
        recipient, subject, scheduled_at
    )
);
