import express from "express";
import dotenv from "dotenv";
import { db } from "./db/client";
import { redisConnection } from "./queue/queue";
import scheduleRoutes from "./api/schedule.routes";
import cors from "cors";
import "./queue/worker";

dotenv.config();

const app = express();
app.use(express.json());

const frontendOrigin = process.env.FRONTEND_URL?.replace(/\/+$/, "");
app.use(
  cors({
    origin: frontendOrigin
      ? [frontendOrigin, `${frontendOrigin}/`, "http://localhost:5173"]
      : true,
    credentials: true,
  })
);

app.get("/", (_req, res) => {
  res.json({ message: "ReachInbox Email Scheduler Backend API is running", status: "ok" });
});

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

const PORT = Number(process.env.PORT || 5000);

app.use("/api/emails", scheduleRoutes);

app.listen(PORT, () => {
  console.log(`Backend running on http://localhost:${PORT}`);
});
