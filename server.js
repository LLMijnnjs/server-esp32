const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const url = require('url');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

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

app.get('/', (req, res) => {
    res.send('Serveur WebSocket Sécurisé OK');
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

wss.on('connection', (ws) => {
    console.log('📡 Nouvelle connexion');
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            const rawId = msg.id;
            const id = normalizeId(rawId);
            const password = msg.password;

            // ✅ Vérifier que l'ID existe
            if (!PASSWORDS[id]) {
                console.log(`❌ ID invalide: ${rawId} -> normalized=${id}`);
                // respond explicitly so the client can react
                try { ws.send(JSON.stringify({ type: 'auth', status: 'denied', reason: 'invalid id' })); } catch(e){}
                return;
            }
            
            // ✅ Vérifier le password
            if (password !== PASSWORDS[id]) {
                console.log(`❌ Password incorrect pour ${id}`);
                try { ws.send(JSON.stringify({ type: 'auth', status: 'denied', reason: 'password incorrect' })); } catch(e){}
                return;
            }
            
            console.log(`✓ ${id} authentifié!`);

            // send explicit auth ok to the client
            try { ws.send(JSON.stringify({ type: 'auth', status: 'ok' })); } catch(e){}
            
            // ✅ Identifier (store the socket)
            if (id === 'ESP32_A') {
                esp32_A = ws;
                console.log('✓ ESP32_A connecté');
            } else if (id === 'ESP32_B') {
                esp32_B = ws;
                console.log('✓ ESP32_B connecté');
            } else if (id === 'PHONE_1' || id === 'PHONE_2') {
                if (!phones.includes(ws)) phones.push(ws);
                console.log(`📱 ${id} connecté`);
            }
            
            // ✅ Relayer: attach origin and forward to other endpoints
            msg.from = id;

            // forward to ESP32s if phones sent message
            if ((id === 'PHONE_1' || id === 'PHONE_2') && esp32_A) {
                try { esp32_A.send(JSON.stringify(msg)); } catch(e) { console.error('send to esp32_A failed', e); }
            }
            if ((id === 'PHONE_1' || id === 'PHONE_2') && esp32_B) {
                try { esp32_B.send(JSON.stringify(msg)); } catch(e) { console.error('send to esp32_B failed', e); }
            }
            if (id === 'ESP32_A' && esp32_B) {
                try { esp32_B.send(JSON.stringify(msg)); } catch(e) { console.error('send to esp32_B failed', e); }
            }
            if (id === 'ESP32_B' && esp32_A) {
                try { esp32_A.send(JSON.stringify(msg)); } catch(e) { console.error('send to esp32_A failed', e); }
            }
            
        } catch(e) {
            console.error('❌ Erreur lors du parsing ou du traitement:', e);
            try { ws.send(JSON.stringify({ type: 'error', message: 'invalid message' })); } catch(e){}
        }
    });
    
    ws.on('close', () => {
        if (ws === esp32_A) esp32_A = null;
        if (ws === esp32_B) esp32_B = null;
        phones = phones.filter(p => p !== ws);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Serveur sur port ${PORT}`);
});
