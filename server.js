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

wss.on('connection', (ws) => {
    console.log('📡 Nouvelle connexion');
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            
            const id = msg.id;
            const password = msg.password;
            
            // ✅ Vérifier que l'ID existe
            if (!PASSWORDS[id]) {
                console.log(`❌ ID invalide: ${id}`);
                return;
            }
            
            // ✅ Vérifier le password
            if (password !== PASSWORDS[id]) {
                console.log(`❌ Password incorrect pour ${id}`);
                ws.send(JSON.stringify({error: 'Password incorrect'}));
                return;
            }
            
            console.log(`✓ ${id} authentifié!`);
            
            // ✅ Identifier
            if (id === 'ESP32_A') {
                esp32_A = ws;
                console.log('✓ ESP32_A connecté');
            } else if (id === 'ESP32_B') {
                esp32_B = ws;
                console.log('✓ ESP32_B connecté');
            } else if (id === 'PHONE_1' || id === 'PHONE_2') {
                phones.push(ws);
                console.log(`📱 ${id} connecté`);
            }
            
            // ✅ Relayer
            msg.from = id;
            
            if ((id === 'PHONE_1' || id === 'PHONE_2') && esp32_A) {
                esp32_A.send(JSON.stringify(msg));
            }
            if ((id === 'PHONE_1' || id === 'PHONE_2') && esp32_B) {
                esp32_B.send(JSON.stringify(msg));
            }
            if (id === 'ESP32_A' && esp32_B) {
                esp32_B.send(JSON.stringify(msg));
            }
            if (id === 'ESP32_B' && esp32_A) {
                esp32_A.send(JSON.stringify(msg));
            }
            
        } catch(e) {
            console.error('❌ Erreur:', e);
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
