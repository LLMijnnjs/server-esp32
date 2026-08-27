// server.js — WebSocket relay + small HTTP API for logs
// - In-memory log buffer + SSE streaming (/logs and /logs/stream)
// - POST /client-logs accepts forwarded console logs from browsers
// - WebSocket relay: phones <-> esp32 devices with simple id/password auth
// Notes: keep this file small and synchronous-friendly. For heavy load, move logs to a persistent store.

const express = require('express');
const WebSocket = require('ws');
const http = require('http');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// --- Config
const LOG_MAX = 1000; // keep last N log entries in memory
const CLIENT_LOG_RATE_LIMIT = 300; // max forwarded logs per minute per IP
const CLIENT_LOG_WINDOW_MS = 60 * 1000;

// --- In-memory state
const logs = [];
const sseClients = new Set();
const rateMap = new Map(); // ip -> { count, resetAt }

function pushLog(level, message) {
  const entry = { ts: new Date().toISOString(), level, message };
  logs.push(entry);
  if (logs.length > LOG_MAX) logs.shift();

  // console output
  if (level === 'error') console.error(message);
  else if (level === 'warn') console.warn(message);
  else console.log(message);

  // broadcast to SSE clients; remove clients that error
  const payload = `data: ${JSON.stringify(entry)}\n\n`;
  for (const res of Array.from(sseClients)) {
    try {
      res.write(payload);
    } catch (e) {
      try { sseClients.delete(res); res.end(); } catch (_) {}
    }
  }
}

// convenience wrappers
const log = {
  info: (m) => pushLog('info', m),
  warn: (m) => pushLog('warn', m),
  error: (m) => pushLog('error', m),
};

// ⚠️ Les passwords viennent des variables d'env!
const PASSWORDS = {
  'PHONE_1': process.env.PHONE_1_PASSWORD,
  'PHONE_2': process.env.PHONE_2_PASSWORD,
  'ESP32_A': process.env.ESP32_A_PASSWORD,
  'ESP32_B': process.env.ESP32_B_PASSWORD
};

let esp32_A = null;
let esp32_B = null;
let phones = [];

// Basic security headers (lightweight, no extra deps)
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  // CORS: keep permissive for now but note the risk in README
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  next();
});

// accept JSON bodies (for client log forwarding)
app.use(express.json({ limit: '32kb' }));

// --- Helper: get client IP (trusting x-forwarded-for if behind a proxy)
function clientIp(req) {
  const xf = req.headers['x-forwarded-for'];
  if (xf) return xf.split(',')[0].trim();
  return req.connection && req.connection.remoteAddress ? req.connection.remoteAddress : 'unknown';
}

// --- Simple in-memory rate limiter for /client-logs
function checkRateLimit(ip) {
  const now = Date.now();
  const item = rateMap.get(ip);
  if (!item || item.resetAt <= now) {
    rateMap.set(ip, { count: 1, resetAt: now + CLIENT_LOG_WINDOW_MS });
    return true;
  }
  if (item.count >= CLIENT_LOG_RATE_LIMIT) return false;
  item.count += 1;
  return true;
}

// Root
app.get('/', (req, res) => {
  res.send('Serveur WebSocket Sécurisé OK');
});

// POST /client-logs (forwarded from browser console via sendBeacon/fetch)
app.post('/client-logs', (req, res) => {
  try {
    const ip = clientIp(req);
    if (!checkRateLimit(ip)) {
      return res.status(429).json({ error: 'rate_limited' });
    }

    const { level, msg, ts } = req.body || {};
    // basic validation
    const allowed = new Set(['info', 'warn', 'error', 'log', 'debug']);
    const lvl = (level || 'info').toLowerCase();
    if (!allowed.has(lvl)) return res.status(400).json({ error: 'invalid_level' });

    const message = typeof msg === 'string' ? msg : JSON.stringify(msg || '');
    if (message.length > 4000) return res.status(400).json({ error: 'msg_too_long' });

    const entry = {
      ts: ts || new Date().toISOString(),
      level: lvl === 'log' ? 'info' : lvl,
      message: `[client] ${message}`
    };
    pushLog(entry.level, entry.message);
    // no content
    return res.status(204).end();
  } catch (err) {
    console.error('client-logs error', err);
    return res.status(500).json({ error: 'internal' });
  }
});

// expose last logs as JSON
app.get('/logs', (req, res) => {
  res.json(logs.slice(-200));
});

// SSE stream for live logs: GET /logs/stream
app.get('/logs/stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive'
  });
  // send backlog
  for (const entry of logs.slice(-200)) {
    res.write(`data: ${JSON.stringify(entry)}\n\n`);
  }
  sseClients.add(res);
  req.on('close', () => {
    sseClients.delete(res);
  });
});

function normalizeId(id) {
  if (!id || typeof id !== 'string') return id;
  // accept both PHONE1 or PHONE_1 formats
  if (id.match(/^PHONE[ _]?1$/i)) return 'PHONE_1';
  if (id.match(/^PHONE[ _]?2$/i)) return 'PHONE_2';
  if (id.toUpperCase() === 'ESP32A' || id.toUpperCase() === 'ESP32_A') return 'ESP32_A';
  if (id.toUpperCase() === 'ESP32B' || id.toUpperCase() === 'ESP32_B') return 'ESP32_B';
  return id.toUpperCase();
}

// --- WebSocket handling with basic heartbeat
wss.on('connection', (ws, req) => {
  log.info('📡 Nouvelle connexion');

  // attach simple aliveness flag
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);

      // If socket already authenticated earlier, prefer that identity
      if (ws.clientId) {
        const id = ws.clientId;
        // If this is an application message
        if (msg.action === 'message' || msg.type === 'message') {
          log.info(`📩 Reçu message de ${id}: ${JSON.stringify(msg)}`);

          // forward to ESP32(s) if available
          if ((id === 'PHONE_1' || id === 'PHONE_2') && esp32_A) {
            try { esp32_A.send(JSON.stringify(msg)); } catch (e) { log.error('send to esp32_A failed ' + e.message); }
          }
          if ((id === 'PHONE_1' || id === 'PHONE_2') && esp32_B) {
            try { esp32_B.send(JSON.stringify(msg)); } catch (e) { log.error('send to esp32_B failed ' + e.message); }
          }

          // ACK back to sender if msgId present
          if (msg.msgId) {
            try { ws.send(JSON.stringify({ type: 'ack', msgId: msg.msgId, status: 'ok' })); } catch (e) { log.error('ack send failed ' + e.message); }
          }
          return;
        }

        // Other messages (non-auth) from authenticated socket: attach origin and forward as earlier
        msg.from = id;
        if (id === 'ESP32_A' && esp32_B) {
          try { esp32_B.send(JSON.stringify(msg)); } catch (e) { log.error('send to esp32_B failed ' + e.message); }
        }
        if (id === 'ESP32_B' && esp32_A) {
          try { esp32_A.send(JSON.stringify(msg)); } catch (e) { log.error('send to esp32_A failed ' + e.message); }
        }

        return;
      }

      // Socket not authenticated yet: try to treat this message as an auth attempt
      const rawId = msg.id || msg.rawId;
      const id = normalizeId(rawId);
      const password = msg.password;

      log.info(`📩 Reçu de client: ${JSON.stringify({ rawId, id, passwordPresent: !!password })}`);

      // verify id exists
      if (!PASSWORDS[id]) {
        log.warn(`❌ ID invalide: ${rawId} -> normalized=${id}`);
        try { ws.send(JSON.stringify({ type: 'auth', status: 'denied', reason: 'invalid id' })); } catch (e) { log.error('send failed: ' + e.message); }
        return;
      }

      // verify password
      if (password !== PASSWORDS[id]) {
        log.warn(`❌ Password incorrect pour ${id}`);
        try { ws.send(JSON.stringify({ type: 'auth', status: 'denied', reason: 'password incorrect' })); } catch (e) { log.error('send failed: ' + e.message); }
        return;
      }

      // authentication successful
      ws.clientId = id;
      log.info(`✓ ${id} authentifié!`);
      try { ws.send(JSON.stringify({ type: 'auth', status: 'ok' })); } catch (e) { log.error('send failed: ' + e.message); }

      // store socket references
      if (id === 'ESP32_A') {
        esp32_A = ws;
        log.info('✓ ESP32_A connecté');
      } else if (id === 'ESP32_B') {
        esp32_B = ws;
        log.info('✓ ESP32_B connecté');
      } else if (id === 'PHONE_1' || id === 'PHONE_2') {
        if (!phones.includes(ws)) phones.push(ws);
        log.info(`📱 ${id} connecté`);
      }

      // don't forward the auth message as a payload to other peers

    } catch (e) {
      log.error('❌ Erreur lors du parsing ou du traitement: ' + (e && e.stack ? e.stack : e));
      try { ws.send(JSON.stringify({ type: 'error', message: 'invalid message' })); } catch (e) {}
    }
  });

  ws.on('close', (code, reason) => {
  if (ws === esp32_A) esp32_A = null;
  if (ws === esp32_B) esp32_B = null;
  phones = phones.filter(p => p !== ws);

  log.info(`⚠️ Connexion fermée — code=${code}, reason=${reason}`);
  });
});

// heartbeat to detect dead clients
const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(() => {}); } catch (e) { /* ignore */ }
  });
}, 30 * 1000);

// graceful shutdown handlers
function shutdown() {
  clearInterval(heartbeatInterval);
  // close SSE clients
  for (const res of sseClients) {
    try { res.end(); } catch (e) {}
  }
  server.close(() => {
    log.info('Server closed');
    process.exit(0);
  });
  // force exit after timeout
  setTimeout(() => process.exit(0), 5000);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  log.info(`🚀 Serveur sur port ${PORT}`);
});
