import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import { db, initDb } from "./db/client";
import { redisConnection } from "./queue/queue";
import scheduleRoutes from "./api/schedule.routes";
import "./queue/worker";

dotenv.config();

const app = express();
app.use(express.json());

// Enable CORS for frontend applications
const frontendOrigin = process.env.FRONTEND_URL?.replace(/\/+$/, "");
app.use(
  cors({
    origin: frontendOrigin
      ? [frontendOrigin, `${frontendOrigin}/`, "http://localhost:5173", "http://localhost:3000"]
      : true,
    credentials: true,
  })
);

// Root informational endpoint
app.get("/", (_req, res) => {
  res.json({ message: "ReachInbox Email Scheduler Backend API is running", status: "ok" });
});

// Health check endpoint validating database and Redis connectivity
app.get("/health", async (_req, res) => {
  let dbStatus = "unknown";
  let redisStatus = "unknown";
  let dbError = null;
  let redisError = null;

  try {
    await db.query("SELECT 1");
    dbStatus = "connected";
  } catch (err: any) {
    dbStatus = "disconnected";
    dbError = err?.message || String(err);
  }

  try {
    await redisConnection.ping();
    redisStatus = "connected";
  } catch (err: any) {
    redisStatus = "disconnected";
    redisError = err?.message || String(err);
  }

  const ok = dbStatus === "connected" && redisStatus === "connected";
  res.status(ok ? 200 : 500).json({
    status: ok ? "ok" : "error",
    database: dbStatus,
    redis: redisStatus,
    dbError,
    redisError,
  });
});

// Email scheduling routes
app.use("/api/emails", scheduleRoutes);

const PORT = Number(process.env.PORT || 5000);

// Initialize database schema and start Express server
initDb().finally(() => {
  app.listen(PORT, () => {
    console.log(`Backend running on http://localhost:${PORT}`);
  });
});
