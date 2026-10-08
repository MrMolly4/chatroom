/* ============================================================
 * MollyWorld — server.js
 * Signaling + static file server for the MollyWorld client.
 * Built by mm4 productions.
 *
 * - Serves index.html and static assets
 * - WebSocket signaling for WebRTC (offer / answer / ICE)
 * - Per-room channels and message history (last 200 per channel)
 * - Optional JSON persistence to disk (PERSIST=1)
 * - Per-peer rate limiting
 * - Health check endpoint at /health
 * - Auto-cleanup of empty rooms
 * - Ping/pong keepalive
 * - Graceful shutdown
 * ============================================================ */

'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

/* ============================================================
 * CONFIG
 * ============================================================ */
const APP_NAME = 'MollyWorld';
const APP_VENDOR = 'mm4 productions';
const APP_VERSION = '2.0.0';

const PORT = process.env.PORT || 8080;
const HOST = process.env.HOST || '0.0.0.0';

// Persistence: set PERSIST=1 to save rooms to disk between restarts
const PERSIST = process.env.PERSIST === '1' || process.env.PERSIST === 'true';
const DB_FILE = process.env.DB_FILE || path.join(__dirname, 'rooms.json');

// How long an empty room sticks around before being deleted (ms)
const EMPTY_ROOM_TTL = 60 * 60 * 1000; // 1 hour

// Max messages retained per channel
const MAX_HISTORY = 200;

// Max size of a single incoming WebSocket frame
const MAX_PAYLOAD = 256 * 1024;

// Rate limits (per peer, per minute)
const RATE = {
    chat: 30,
    edit: 20,
    delete: 20,
    reaction: 60,
    typing: 120,
    state: 60,
    signal: 300,
    channel: 10
};

/* ============================================================
 * STATIC FILE SERVING
 * ============================================================ */
const MIME = {
    '.html': 'text/html; charset=utf-8',
    '.htm': 'text/html; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.mjs': 'text/javascript; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif',
    '.webp': 'image/webp',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon',
    '.woff': 'font/woff',
    '.woff2': 'font/woff2',
    '.ttf': 'font/ttf',
    '.otf': 'font/otf',
    '.txt': 'text/plain; charset=utf-8',
    '.map': 'application/json; charset=utf-8'
};

function serveStatic(req, res) {
    let url;
    try {
        url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    } catch {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('Bad request');
        return;
    }

    // ---- Health check ----
    if (url.pathname === '/health') {
        res.writeHead(200, {
            'Content-Type': 'application/json; charset=utf-8',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'no-store'
        });
        res.end(JSON.stringify({
            ok: true,
            app: APP_NAME,
            vendor: APP_VENDOR,
            version: APP_VERSION,
            uptime: Math.floor(process.uptime()),
                               rooms: rooms.size,
                               peers: Array.from(rooms.values()).reduce((sum, r) => sum + r.peers.size, 0),
                               memory: Math.round(process.memoryUsage().rss / 1024 / 1024) + 'MB',
                               persist: PERSIST
        }));
        return;
    }

    // ---- Normalize and prevent traversal ----
    let rel = decodeURIComponent(url.pathname);
    if (rel === '/' || rel === '') rel = '/index.html';
    rel = path.normalize(rel).replace(/^(\.\.[/\\])+/, '').replace(/^[/\\]+/, '');
    const full = path.join(__dirname, rel);

    // Ensure resolved path is still inside __dirname
    if (!full.startsWith(__dirname)) {
        res.writeHead(403, { 'Content-Type': 'text/plain' });
        res.end('Forbidden');
        return;
    }

    fs.stat(full, (err, stat) => {
        if (err || !stat.isFile()) {
            // SPA fallback: serve index.html for any unknown path
            const idx = path.join(__dirname, 'index.html');
            fs.readFile(idx, (e2, data) => {
                if (e2) {
                    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
                    res.end('Not found');
                    return;
                }
                res.writeHead(200, {
                    'Content-Type': 'text/html; charset=utf-8',
                    'Cache-Control': 'no-cache'
                });
                res.end(data);
            });
            return;
        }

        const ext = path.extname(full).toLowerCase();
        const type = MIME[ext] || 'application/octet-stream';
        const isHtml = ext === '.html' || ext === '.htm';

        res.writeHead(200, {
            'Content-Type': type,
            'Content-Length': stat.size,
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': isHtml ? 'no-cache' : 'public, max-age=3600'
        });

        const stream = fs.createReadStream(full);
        stream.on('error', () => { try { res.end(); } catch {} });
        stream.pipe(res);
    });
}

/* ============================================================
 * ROOM STORE
 * ============================================================ */
/*
 r *ooms: Map<roomId, {
 id, name, createdAt,
peers: Map<ws, Peer>,
channels: Map<channelId, {id, name, type}>,
messages: Map<channelId, Message[]>,
emptySince: number|null,
cleanupTimer: Timeout|null
}>

Peer: {
id, name, avatar, tier, role, status,
micOn, camOn, sharing, voiceChannelId,
rate: { [event]: number[] },
connectedAt
}

Message: {
id, channelId, peerId, name, avatar, text, time,
replyTo, edited, deleted, reactions
}
*/
const rooms = new Map();

function makeId(len = 8) {
    return crypto.randomBytes(16).toString('base64url').slice(0, len);
}

function getRoom(roomId) {
    if (!rooms.has(roomId)) {
        rooms.set(roomId, {
            id: roomId,
            name: null,
            peers: new Map(),
                  channels: new Map(),
                  messages: new Map(),
                  emptySince: null,
                  cleanupTimer: null,
                  createdAt: Date.now()
        });
    }
    const room = rooms.get(roomId);
    room.emptySince = null;
    if (room.cleanupTimer) {
        clearTimeout(room.cleanupTimer);
        room.cleanupTimer = null;
    }
    return room;
}

function ensureDefaultChannels(room) {
    if (room.channels.size > 0) return;
    const defaults = [
        { id: 'general-' + room.id, name: 'general', type: 'text' },
        { id: 'random-' + room.id, name: 'random', type: 'text' },
        { id: 'voice-' + room.id, name: 'General', type: 'voice' }
    ];
    defaults.forEach(c => room.channels.set(c.id, c));
}

function pushHistory(room, channelId, msg) {
    if (!room.messages.has(channelId)) room.messages.set(channelId, []);
    const arr = room.messages.get(channelId);
    arr.push(msg);
    if (arr.length > MAX_HISTORY) arr.splice(0, arr.length - MAX_HISTORY);
}

/* ============================================================
 * PERSISTENCE
 * ============================================================ */
let saveTimer = null;

function scheduleSave() {
    if (!PERSIST) return;
    if (saveTimer) return;
    saveTimer = setTimeout(() => {
        saveTimer = null;
        saveToDisk();
    }, 2000);
}

function saveToDisk() {
    if (!PERSIST) return;
    try {
        const out = {};
        for (const [roomId, room] of rooms.entries()) {
            const messages = {};
            for (const [channelId, msgs] of room.messages.entries()) {
                messages[channelId] = msgs.slice(-MAX_HISTORY);
            }
            out[roomId] = {
                id: room.id,
                name: room.name,
                createdAt: room.createdAt,
                channels: Array.from(room.channels.values()),
                messages
            };
        }
        fs.writeFileSync(DB_FILE, JSON.stringify(out, null, 2));
    } catch (err) {
        console.error('[' + APP_NAME + '] Save failed:', err.message);
    }
}

function loadFromDisk() {
    if (!PERSIST) return;
    try {
        if (!fs.existsSync(DB_FILE)) return;
        const raw = fs.readFileSync(DB_FILE, 'utf8');
        const data = JSON.parse(raw);
        for (const [roomId, r] of Object.entries(data)) {
            const room = {
                id: roomId,
                name: r.name || null,
                peers: new Map(),
                channels: new Map((r.channels || []).map(c => [c.id, c])),
                messages: new Map(),
                emptySince: Date.now(),
                cleanupTimer: null,
                createdAt: r.createdAt || Date.now()
            };
            for (const [chId, msgs] of Object.entries(r.messages || {})) {
                room.messages.set(chId, msgs.slice(-MAX_HISTORY));
            }
            room.cleanupTimer = setTimeout(() => {
                if (room.peers.size === 0) {
                    rooms.delete(roomId);
                    scheduleSave();
                }
            }, EMPTY_ROOM_TTL);
            rooms.set(roomId, room);
        }
        console.log(`[${APP_NAME}] Restored ${rooms.size} room(s) from ${DB_FILE}`);
    } catch (err) {
        console.error(`[${APP_NAME}] Load failed:`, err.message);
    }
}

/* ============================================================
 * RATE LIMITING
 * ============================================================ */
function rateLimit(peer, event) {
    const max = RATE[event];
    if (!max) return true;
    const now = Date.now();
    const windowStart = now - 60000;
    peer.rate[event] = peer.rate[event] || [];
    peer.rate[event] = peer.rate[event].filter(t => t > windowStart);
    if (peer.rate[event].length >= max) return false;
    peer.rate[event].push(now);
    return true;
}

/* ============================================================
 * MESSAGING HELPERS
 * ============================================================ */
function send(ws, obj) {
    if (ws.readyState === ws.OPEN) {
        try { ws.send(JSON.stringify(obj)); } catch {}
    }
}

function broadcast(room, obj, exceptWs) {
    const data = JSON.stringify(obj);
    for (const ws of room.peers.keys()) {
        if (ws === exceptWs) continue;
        if (ws.readyState === ws.OPEN) {
            try { ws.send(data); } catch {}
        }
    }
}

function findPeerWs(room, peerId) {
    for (const [ws, peer] of room.peers.entries()) {
        if (peer.id === peerId) return ws;
    }
    return null;
}

function publicPeer(p) {
    return {
        id: p.id,
        name: p.name,
        avatar: p.avatar,
        tier: p.tier,
        role: p.role,
        status: p.status,
        micOn: p.micOn,
        camOn: p.camOn,
        sharing: p.sharing,
        voiceChannelId: p.voiceChannelId
    };
}

/* ============================================================
 * HTTP SERVER
 * ============================================================ */
const server = http.createServer((req, res) => {
    // CORS preflight
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
    }
    serveStatic(req, res);
});

/* ============================================================
 * WEBSOCKET SERVER
 * ============================================================ */
const wss = new WebSocketServer({ server, maxPayload: MAX_PAYLOAD });

wss.on('connection', (ws, req) => {
    let url;
    try {
        url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    } catch {
        try { ws.close(1008, 'bad request'); } catch {}
        return;
    }

    const roomId = url.searchParams.get('room');
    if (!roomId || roomId.length > 64) {
        try { ws.close(1008, 'room required'); } catch {}
        return;
    }

    const room = getRoom(roomId);
    const peer = {
        id: null,
        name: null,
        avatar: '',
        tier: 'free',
        role: 'member',
        status: 'online',
        micOn: true,
        camOn: false,
        sharing: false,
        voiceChannelId: null,
        rate: {},
        connectedAt: Date.now()
    };
    room.peers.set(ws, peer);

    // Keepalive
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw); } catch { return; }
        if (!msg || typeof msg.type !== 'string') return;

        /* ---------- JOIN ---------- */
        if (msg.type === 'join') {
            if (!msg.peer || typeof msg.peer.id !== 'string') return;
            peer.id = String(msg.peer.id).slice(0, 64);
            peer.name = String(msg.peer.name || 'Guest').slice(0, 64);
            peer.avatar = typeof msg.peer.avatar === 'string' ? msg.peer.avatar.slice(0, 200000) : '';
            peer.tier = String(msg.peer.tier || 'free').slice(0, 16);
            peer.role = ['owner', 'admin', 'mod', 'member'].includes(msg.role) ? msg.role : 'member';
            peer.status = String(msg.peer.status || 'online').slice(0, 16);

            // First joiner becomes owner
            if (room.peers.size === 1) peer.role = 'owner';

            ensureDefaultChannels(room);

            // Send existing peers
            const existing = [];
            for (const [otherWs, otherPeer] of room.peers.entries()) {
                if (otherWs === ws) continue;
                if (!otherPeer.id) continue;
                existing.push(publicPeer(otherPeer));
            }
            send(ws, { type: 'peers', peers: existing });

            // Send channel list
            send(ws, { type: 'channel-list', channels: Array.from(room.channels.values()) });

            // Send history for every channel
            for (const [channelId, msgs] of room.messages.entries()) {
                send(ws, { type: 'history', channelId, messages: msgs.slice(-MAX_HISTORY) });
            }

            // Notify everyone else
            broadcast(room, { type: 'peer-joined', peer: publicPeer(peer) }, ws);

            console.log(`[${roomId}] ${peer.name} joined (${peer.id}) — ${room.peers.size} peer(s)`);
            return;
        }

        /* All other events require an identified peer */
        if (!peer.id) return;

        /* ---------- HISTORY REQUEST ---------- */
        if (msg.type === 'history-request') {
            const channelId = String(msg.channelId || '');
            const msgs = room.messages.get(channelId) || [];
            send(ws, { type: 'history', channelId, messages: msgs.slice(-MAX_HISTORY) });
            return;
        }

        /* ---------- CHAT ---------- */
        if (msg.type === 'chat') {
            if (!rateLimit(peer, 'chat')) return;
            const channelId = String(msg.channelId || '');
            if (!channelId || channelId.length > 128) return;
            const text = String(msg.text || '').slice(0, 4000);
            if (!text) return;
            const message = {
                id: String(msg.id || makeId(14)),
          channelId,
          peerId: peer.id,
          name: peer.name,
          avatar: peer.avatar,
          text,
          time: typeof msg.time === 'number' ? msg.time : Date.now(),
          replyTo: msg.replyTo ? String(msg.replyTo) : null,
          edited: false,
          deleted: false,
          reactions: {}
            };
            pushHistory(room, channelId, message);
            scheduleSave();
            broadcast(room, { type: 'chat', ...message, from: peer.id }, ws);
            return;
        }

        /* ---------- EDIT ---------- */
        if (msg.type === 'edit') {
            if (!rateLimit(peer, 'edit')) return;
            const arr = room.messages.get(String(msg.channelId || '')) || [];
            const m = arr.find(x => x.id === msg.msgId);
            if (!m || m.peerId !== peer.id || m.deleted) return;
            m.text = String(msg.text || '').slice(0, 4000);
            m.edited = true;
            scheduleSave();
            broadcast(room, {
                type: 'edit',
                channelId: m.channelId,
                msgId: m.id,
                text: m.text
            });
            return;
        }

        /* ---------- DELETE ---------- */
        if (msg.type === 'delete') {
            if (!rateLimit(peer, 'delete')) return;
            const arr = room.messages.get(String(msg.channelId || '')) || [];
            const m = arr.find(x => x.id === msg.msgId);
            if (!m) return;
            const canDelete =
            m.peerId === peer.id ||
            peer.role === 'owner' ||
            peer.role === 'admin' ||
            peer.role === 'mod';
            if (!canDelete) return;
            m.deleted = true;
            m.text = '';
            scheduleSave();
            broadcast(room, { type: 'delete', channelId: m.channelId, msgId: m.id });
            return;
        }

        /* ---------- REACTION ---------- */
        if (msg.type === 'reaction') {
            if (!rateLimit(peer, 'reaction')) return;
            const arr = room.messages.get(String(msg.channelId || '')) || [];
            const m = arr.find(x => x.id === msg.msgId);
            if (!m) return;
            const emoji = String(msg.emoji || '').slice(0, 8);
            if (!emoji) return;
            m.reactions = m.reactions || {};
            const set = new Set(m.reactions[emoji] || []);
            if (msg.add) set.add(peer.id);
            else set.delete(peer.id);
            m.reactions[emoji] = Array.from(set);
            if (!m.reactions[emoji].length) delete m.reactions[emoji];
            scheduleSave();
            broadcast(room, {
                type: 'reaction',
                channelId: m.channelId,
                msgId: m.id,
                emoji,
                add: !!msg.add,
                peerId: peer.id
            }, ws);
            return;
        }

        /* ---------- TYPING ---------- */
        if (msg.type === 'typing') {
            if (!rateLimit(peer, 'typing')) return;
            broadcast(room, {
                type: 'typing',
                channelId: String(msg.channelId || ''),
                      peerId: peer.id,
                      typing: !!msg.typing
            }, ws);
            return;
        }

        /* ---------- STATE ---------- */
        if (msg.type === 'state') {
            if (!rateLimit(peer, 'state')) return;
            if (typeof msg.micOn === 'boolean') peer.micOn = msg.micOn;
            if (typeof msg.camOn === 'boolean') peer.camOn = msg.camOn;
            if (typeof msg.sharing === 'boolean') peer.sharing = msg.sharing;
            if (typeof msg.status === 'string') peer.status = msg.status.slice(0, 16);
            broadcast(room, {
                type: 'state',
                peerId: peer.id,
                micOn: peer.micOn,
                camOn: peer.camOn,
                sharing: peer.sharing,
                status: peer.status
            }, ws);
            return;
        }

        /* ---------- VOICE CHANNEL ---------- */
        if (msg.type === 'join-voice') {
            peer.voiceChannelId = String(msg.channelId || '');
            broadcast(room, { type: 'join-voice', peerId: peer.id, channelId: peer.voiceChannelId }, ws);
            return;
        }

        if (msg.type === 'leave-voice') {
            peer.voiceChannelId = null;
            broadcast(room, { type: 'leave-voice', peerId: peer.id }, ws);
            return;
        }

        /* ---------- ROLE ---------- */
        if (msg.type === 'role') {
            if (peer.role !== 'owner' && peer.role !== 'admin') return;
            const targetId = String(msg.peerId || '');
            const newRole = ['owner', 'admin', 'mod', 'member'].includes(msg.role) ? msg.role : null;
            if (!targetId || !newRole) return;
            const targetWs = findPeerWs(room, targetId);
            if (!targetWs) return;
            const target = room.peers.get(targetWs);
            if (!target) return;
            // Admins cannot change owners
            if (peer.role === 'admin' && target.role === 'owner') return;
            target.role = newRole;
            send(targetWs, { type: 'role', peerId: targetId, role: newRole });
            broadcast(room, { type: 'role', peerId: targetId, role: newRole }, targetWs);
            return;
        }

        /* ---------- KICK ---------- */
        if (msg.type === 'kick') {
            const rank = { owner: 3, admin: 2, mod: 1, member: 0 };
            if (rank[peer.role] < 1) return;
            const targetId = String(msg.peerId || '');
            const targetWs = findPeerWs(room, targetId);
            if (!targetWs) return;
            const target = room.peers.get(targetWs);
            if (!target) return;
            // Can't kick equal or higher rank
            if (rank[target.role] >= rank[peer.role]) return;
            send(targetWs, { type: 'kick', peerId: targetId });
            return;
        }

        /* ---------- CHANNEL CREATE ---------- */
        if (msg.type === 'channel-create') {
            if (!rateLimit(peer, 'channel')) return;
            if (peer.role !== 'owner' && peer.role !== 'admin' && peer.role !== 'mod') return;
            if (!msg.channel || !msg.channel.id) return;
            const ch = {
                id: String(msg.channel.id).slice(0, 128),
          name: String(msg.channel.name || 'channel').slice(0, 64),
          type: msg.channel.type === 'voice' ? 'voice' : 'text'
            };
            room.channels.set(ch.id, ch);
            scheduleSave();
            broadcast(room, { type: 'channel-create', channel: ch }, ws);
            return;
        }

        /* ---------- CHANNEL DELETE ---------- */
        if (msg.type === 'channel-delete') {
            if (!rateLimit(peer, 'channel')) return;
            if (peer.role !== 'owner' && peer.role !== 'admin') return;
            const channelId = String(msg.channelId || '');
            room.channels.delete(channelId);
            room.messages.delete(channelId);
            scheduleSave();
            broadcast(room, { type: 'channel-delete', channelId }, ws);
            return;
        }

        /* ---------- WEBRTC SIGNALING ---------- */
        if (msg.type === 'offer' || msg.type === 'answer' || msg.type === 'ice') {
            if (!rateLimit(peer, 'signal')) return;
            const to = String(msg.to || '');
            if (!to) return;
            const targetWs = findPeerWs(room, to);
            if (!targetWs) return;
            const payload = { type: msg.type, from: peer.id };
            if (msg.sdp) payload.sdp = msg.sdp;
            if (msg.candidate) payload.candidate = msg.candidate;
            send(targetWs, payload);
            return;
        }
    });

    ws.on('close', () => {
        const me = room.peers.get(ws);
        room.peers.delete(ws);
        if (me && me.id) {
            broadcast(room, { type: 'peer-left', peerId: me.id });
            console.log(`[${roomId}] ${me.name} left — ${room.peers.size} peer(s)`);
        }

        if (room.peers.size === 0) {
            room.emptySince = Date.now();
            room.cleanupTimer = setTimeout(() => {
                if (room.peers.size === 0) {
                    rooms.delete(roomId);
                    scheduleSave();
                    console.log(`[${roomId}] cleaned up`);
                }
            }, EMPTY_ROOM_TTL);
        }
    });

    ws.on('error', () => {
        try { ws.close(); } catch {}
    });
});

/* ============================================================
 * KEEPALIVE
 * ============================================================ */
const heartbeat = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            try { ws.terminate(); } catch {}
            return;
        }
        ws.isAlive = false;
        try { ws.ping(); } catch {}
    });
}, 30000);

wss.on('close', () => clearInterval(heartbeat));

/* ============================================================
 * GRACEFUL SHUTDOWN
 * ============================================================ */
function shutdown(signal) {
    console.log(`\n[${APP_NAME}] ${signal} received — shutting down…`);
    clearInterval(heartbeat);
    if (PERSIST) saveToDisk();
    wss.clients.forEach((ws) => { try { ws.close(1001, 'server shutting down'); } catch {} });
    wss.close(() => {
        server.close(() => {
            console.log(`[${APP_NAME}] Goodbye.`);
            process.exit(0);
        });
    });
    setTimeout(() => process.exit(1), 5000).unref();
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

/* ============================================================
 * START
 * ============================================================ */
loadFromDisk();

server.listen(PORT, HOST, () => {
    console.log('');
    console.log('  ' + APP_NAME);
    console.log('  by ' + APP_VENDOR);
    console.log('  ──────────────────────────────────────────');
    console.log(`  Version          ${APP_VERSION}`);
    console.log(`  Listening on     http://${HOST}:${PORT}`);
    console.log(`  Persistence      ${PERSIST ? 'ON  (' + DB_FILE + ')' : 'off'}`);
    console.log(`  Max history      ${MAX_HISTORY} messages per channel`);
    console.log(`  Empty room TTL   ${Math.round(EMPTY_ROOM_TTL / 60000)} min`);
    console.log(`  Max payload      ${Math.round(MAX_PAYLOAD / 1024)} KB`);
    console.log('');
});
