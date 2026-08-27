const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const url = require('url');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Small in-memory log buffer + SSE streaming for console-like log follow
const LOG_MAX = 1000;
const logs = [];
const sseClients = new Set();

function pushLog(level, message) {
  const entry = { ts: new Date().toISOString(), level, message };
  logs.push(entry);
  if (logs.length > LOG_MAX) logs.shift();
  // console output
  if (level === 'error') console.error(message);
  else if (level === 'warn') console.warn(message);
  else console.log(message);
  // broadcast to SSE clients
  const payload = `data: ${JSON.stringify(entry)}\n\n`;
  for (const res of sseClients) {
    try { res.write(payload); } catch (e) { /* ignore write errors */ }
  }
}

// convenience wrappers
const log = { info: (m) => pushLog('info', m), warn: (m) => pushLog('warn', m), error: (m) => pushLog('error', m) };

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

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    next();
});

// accept JSON bodies (for client log forwarding)
app.use(express.json({ limit: '32kb' }));

app.get('/', (req, res) => {
    res.send('Serveur WebSocket Sécurisé OK');
});

// POST /client-logs (forwarded from browser console via sendBeacon/fetch)
app.post('/client-logs', (req, res) => {
  try {
    const { level, msg, ts } = req.body || {};
    const entry = {
      ts: ts || new Date().toISOString(),
      level: level || 'info',
      message: '[client] ' + (msg || '')
    };
    pushLog(entry.level, entry.message);
    res.status(204).end();
  } catch (err) {
    console.error('client-logs error', err);
    res.status(500).json({ error: 'internal' });
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

wss.on('connection', (ws, req) => {
    log.info('📡 Nouvelle connexion');
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            const rawId = msg.id;
            const id = normalizeId(rawId);
            const password = msg.password;

            log.info(`📩 Reçu de client: ${JSON.stringify({ rawId, id, passwordPresent: !!password })}`);

            // ✅ Vérifier que l'ID existe
            if (!PASSWORDS[id]) {
                log.warn(`❌ ID invalide: ${rawId} -> normalized=${id}`);
                // respond explicitly so the client can react
                try { ws.send(JSON.stringify({ type: 'auth', status: 'denied', reason: 'invalid id' })); } catch(e){ log.error('send failed: '+e.message); }
                return;
            }
            
            // ✅ Vérifier le password
            if (password !== PASSWORDS[id]) {
                log.warn(`❌ Password incorrect pour ${id}`);
                try { ws.send(JSON.stringify({ type: 'auth', status: 'denied', reason: 'password incorrect' })); } catch(e){ log.error('send failed: '+e.message); }
                return;
            }
            
            log.info(`✓ ${id} authentifié!`);

            // send explicit auth ok to the client
            try { ws.send(JSON.stringify({ type: 'auth', status: 'ok' })); } catch(e){ log.error('send failed: '+e.message); }
            
            // ✅ Identifier (store the socket)
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
            
            // ✅ Relayer: attach origin and forward to other endpoints
            msg.from = id;

            // forward to ESP32s if phones sent message
            if ((id === 'PHONE_1' || id === 'PHONE_2') && esp32_A) {
                try { esp32_A.send(JSON.stringify(msg)); } catch(e) { log.error('send to esp32_A failed '+e.message); }
            }
            if ((id === 'PHONE_1' || id === 'PHONE_2') && esp32_B) {
                try { esp32_B.send(JSON.stringify(msg)); } catch(e) { log.error('send to esp32_B failed '+e.message); }
            }
            if (id === 'ESP32_A' && esp32_B) {
                try { esp32_B.send(JSON.stringify(msg)); } catch(e) { log.error('send to esp32_B failed '+e.message); }
            }
            if (id === 'ESP32_B' && esp32_A) {
                try { esp32_A.send(JSON.stringify(msg)); } catch(e) { log.error('send to esp32_A failed '+e.message); }
            }
            
        } catch(e) {
            log.error('❌ Erreur lors du parsing ou du traitement: '+(e && e.stack ? e.stack : e));
            try { ws.send(JSON.stringify({ type: 'error', message: 'invalid message' })); } catch(e){}
        }
    });
    
    ws.on('close', () => {
        if (ws === esp32_A) esp32_A = null;
        if (ws === esp32_B) esp32_B = null;
        phones = phones.filter(p => p !== ws);
        log.info('⚠️ Connexion fermée');
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    log.info(`🚀 Serveur sur port ${PORT}`);
});
