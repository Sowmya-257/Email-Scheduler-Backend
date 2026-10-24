import express from "express";
import dotenv from "dotenv";
import { db } from "./db/client";
import { redisConnection } from "./queue/queue";
import scheduleRoutes from "./api/schedule.routes";
import cors from "cors";

dotenv.config();

const app = express();
app.use(express.json());

app.use(cors({ origin: process.env.FRONTEND_URL || true, credentials: true }));


app.get("/health", async (_req, res) => {
  try {
    await db.query("SELECT 1");
    await redisConnection.ping();

    res.json({ status: "ok", database: "connected", redis: "connected" });
  } catch {
    res.status(500).json({ status: "error" });
  }
});

const PORT = Number(process.env.PORT || 5000);

app.use("/api/emails", scheduleRoutes);

app.listen(PORT, () => {
  console.log(`Backend running on http://localhost:${PORT}`);
});
