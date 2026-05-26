/**
 * Standalone Socket.IO realtime server for the Kenya Airways frontend.
 *
 * Run:
 *   npm install socket.io
 *   node server.js
 *
 * Useful env:
 *   SOCKET_SERVER_PORT=3001
 *   SOCKET_PORT=3001
 *   PORT=3001
 *   SOCKET_CORS_ORIGIN=http://localhost:3000
 *
 * The Next app should point to this server with:
 *   SOCKET_SERVER_URL=http://localhost:3001
 *   NEXT_PUBLIC_SOCKET_URL=http://localhost:3001
 */

const http = require("http");
const { Server } = require("socket.io");
const dotenv = require("dotenv");
dotenv.config();

const PORT = Number(
  process.env.SOCKET_SERVER_PORT ||
    process.env.SOCKET_PORT ||
    process.env.PORT ||
    3001,
);

const CORS_ORIGIN = process.env.SOCKET_CORS_ORIGIN || "*";
const HEARTBEAT_MS = Number(process.env.SOCKET_HEARTBEAT_MS || 30000);

const onlineUsers = new Map();
const liveSessions = new Map();

function json(res, status, payload) {
  res.writeHead(status, {
    "content-type": "application/json",
    "access-control-allow-origin": CORS_ORIGIN,
  });
  res.end(JSON.stringify(payload));
}

function addUserSocket(userId, socketId) {
  const key = String(userId);
  const sockets = onlineUsers.get(key) || new Set();
  sockets.add(socketId);
  onlineUsers.set(key, sockets);
  return sockets.size;
}

function removeUserSocket(userId, socketId) {
  if (!userId) return 0;
  const key = String(userId);
  const sockets = onlineUsers.get(key);
  if (!sockets) return 0;
  sockets.delete(socketId);
  if (sockets.size === 0) onlineUsers.delete(key);
  return sockets.size;
}

function userRoom(userId) {
  return `user:${userId}`;
}

function presenceRoom(userId) {
  return `presence:${userId}`;
}

function flightRoom(flightId) {
  return `flight:${flightId}`;
}

function bookingRoom(bookingId) {
  return `booking:${bookingId}`;
}

function liveRoom(sessionId) {
  return `live:${sessionId}`;
}

const server = http.createServer((req, res) => {
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "access-control-allow-origin": CORS_ORIGIN,
      "access-control-allow-methods": "GET,POST,OPTIONS",
      "access-control-allow-headers": "content-type,authorization",
    });
    res.end();
    return;
  }

  if (req.url === "/health" || req.url === "/") {
    json(res, 200, {
      ok: true,
      service: "kenya-airways-socket",
      port: PORT,
      clients: io.engine.clientsCount,
      onlineUsers: onlineUsers.size,
      liveSessions: liveSessions.size,
      uptime: Math.round(process.uptime()),
    });
    return;
  }

  json(res, 404, { ok: false, error: "not_found" });
});

const io = new Server(server, {
  cors: {
    origin: CORS_ORIGIN,
    methods: ["GET", "POST"],
    credentials: CORS_ORIGIN !== "*",
  },
  transports: ["websocket", "polling"],
  pingInterval: HEARTBEAT_MS,
  pingTimeout: HEARTBEAT_MS,
});

io.on("connection", (socket) => {
  console.log("[socket] connected", socket.id);

  socket.emit("server:ready", {
    socketId: socket.id,
    connectedAt: new Date().toISOString(),
  });

  socket.on("presence:join", (payload = {}) => {
    const userId = payload.userId || payload.id;
    if (!userId) return;

    socket.data.userId = String(userId);
    socket.join(userRoom(userId));
    socket.join(presenceRoom(userId));

    const connectionCount = addUserSocket(userId, socket.id);
    if (connectionCount === 1) {
      socket.broadcast.emit("presence", {
        userId: String(userId),
        status: "online",
        at: new Date().toISOString(),
      });
    }

    socket.emit("presence:joined", {
      userId: String(userId),
      status: "online",
      connections: connectionCount,
    });
  });

  socket.on("presence:leave", () => {
    const userId = socket.data.userId;
    const remaining = removeUserSocket(userId, socket.id);
    if (userId && remaining === 0) {
      socket.broadcast.emit("presence", {
        userId,
        status: "offline",
        at: new Date().toISOString(),
      });
    }
    socket.data.userId = null;
  });

  socket.on("presence", (payload = {}) => {
    const userId = payload.userId || socket.data.userId;
    if (!userId) return;
    socket.broadcast.emit("presence", {
      userId: String(userId),
      status: payload.status || "online",
      at: new Date().toISOString(),
    });
  });

  socket.on("flight:join", (payload = {}) => {
    if (!payload.flightId) return;
    socket.join(flightRoom(payload.flightId));
    socket.emit("flight:joined", { flightId: payload.flightId });
  });

  socket.on("flight:leave", (payload = {}) => {
    if (!payload.flightId) return;
    socket.leave(flightRoom(payload.flightId));
  });

  socket.on("booking:join", (payload = {}) => {
    if (!payload.bookingId) return;
    socket.join(bookingRoom(payload.bookingId));
    socket.emit("booking:joined", { bookingId: payload.bookingId });
  });

  socket.on("booking:leave", (payload = {}) => {
    if (!payload.bookingId) return;
    socket.leave(bookingRoom(payload.bookingId));
  });

  socket.on("support:join", (payload = {}) => {
    const sessionId = payload.sessionId || payload.id;
    if (!sessionId) return;

    socket.data.liveSessionId = String(sessionId);
    socket.join(liveRoom(sessionId));

    const sockets = liveSessions.get(String(sessionId)) || new Set();
    sockets.add(socket.id);
    liveSessions.set(String(sessionId), sockets);

    io.to(liveRoom(sessionId)).emit("support:presence", {
      sessionId: String(sessionId),
      count: sockets.size,
      at: new Date().toISOString(),
    });
  });

  socket.on("support:leave", (payload = {}) => {
    const sessionId = payload.sessionId || socket.data.liveSessionId;
    if (!sessionId) return;
    socket.leave(liveRoom(sessionId));
    const sockets = liveSessions.get(String(sessionId));
    if (sockets) {
      sockets.delete(socket.id);
      if (sockets.size === 0) liveSessions.delete(String(sessionId));
      else liveSessions.set(String(sessionId), sockets);
    }
  });

  socket.on("support:message", (payload = {}) => {
    const sessionId = payload.sessionId || socket.data.liveSessionId;
    if (!sessionId) return;
    const message = {
      id: payload.id || `${Date.now()}-${socket.id}`,
      sessionId: String(sessionId),
      sender: payload.sender || socket.data.userId || "visitor",
      message: payload.message || "",
      metadata: payload.metadata || {},
      createdAt: payload.createdAt || new Date().toISOString(),
    };
    io.to(liveRoom(sessionId)).emit("support:message", message);
    io.emit("support:activity", {
      sessionId: String(sessionId),
      type: "message",
      createdAt: message.createdAt,
    });
  });

  socket.on("seat_update", (payload = {}) => {
    if (payload.flightId)
      io.to(flightRoom(payload.flightId)).emit("seat_update", payload);
    io.emit("seat_update", payload);
  });

  socket.on("booking_change", (payload = {}) => {
    const bookingId = payload.bookingId || payload.id;
    if (bookingId)
      io.to(bookingRoom(bookingId)).emit("booking_change", payload);
    io.emit("booking_change", payload);
  });

  socket.on("notification", (payload = {}) => {
    if (payload.userId) {
      io.to(userRoom(payload.userId)).emit("notification", payload);
      io.to(presenceRoom(payload.userId)).emit("notification", payload);
    } else {
      io.emit("notification", payload);
    }
  });

  socket.on("flight_status", (payload = {}) => {
    if (payload.flightId)
      io.to(flightRoom(payload.flightId)).emit("flight_status", payload);
    io.emit("flight_status", payload);
  });

  socket.on("assignment_update", (payload = {}) => {
    io.emit("assignment_update", payload);
  });

  socket.on("admin:broadcast", (payload = {}) => {
    io.emit("notification", {
      type: "admin_broadcast",
      payload,
      createdAt: new Date().toISOString(),
    });
  });

  socket.on("disconnect", (reason) => {
    const userId = socket.data.userId;
    const remaining = removeUserSocket(userId, socket.id);
    if (userId && remaining === 0) {
      socket.broadcast.emit("presence", {
        userId,
        status: "offline",
        at: new Date().toISOString(),
      });
    }

    const sessionId = socket.data.liveSessionId;
    if (sessionId) {
      const sockets = liveSessions.get(String(sessionId));
      if (sockets) {
        sockets.delete(socket.id);
        if (sockets.size === 0) liveSessions.delete(String(sessionId));
      }
    }

    console.log("[socket] disconnected", socket.id, reason);
  });
});

server.listen(PORT, () => {
  console.log(
    `[socket] Socket.IO server listening on http://localhost:${PORT}`,
  );
  console.log(`[socket] CORS origin: ${CORS_ORIGIN}`);
});

function shutdown(signal) {
  console.log(`[socket] ${signal} received, closing server`);
  io.close(() => {
    server.close(() => process.exit(0));
  });
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
