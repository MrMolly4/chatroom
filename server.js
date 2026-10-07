'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const DATA_DIR = path.join(__dirname, '.data');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const ROOMS_FILE = path.join(DATA_DIR, 'rooms.json');

/* ============================================================
 * PERSISTENCE
 * ============================================================ */

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

function loadJSON(file, fallback) {
    try {
        if (!fs.existsSync(file)) return fallback;
        return JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch (err) {
        console.error('[store] failed to load', file, err.message);
        return fallback;
    }
}

function saveJSON(file, data) {
    try {
        fs.writeFileSync(file, JSON.stringify(data, null, 2));
    } catch (err) {
        console.error('[store] failed to save', file, err.message);
    }
}

/** users: { userId: { id, name, profile, tier, createdAt, lastSeen } } */
const users = loadJSON(USERS_FILE, {});

/** rooms: { roomId: { id, name, hostId, createdAt, persistent } } */
const rooms = loadJSON(ROOMS_FILE, {});

let saveTimer = null;
function scheduleSave() {
    if (saveTimer) return;
    saveTimer = setTimeout(() => {
        saveTimer = null;
        saveJSON(USERS_FILE, users);
        saveJSON(ROOMS_FILE, rooms);
    }, 800);
}

/* ============================================================
 * HTTP SERVER
 * ============================================================ */

const MIME = {
    '.html': 'text/html; charset=utf-8',
    '.js':   'application/javascript; charset=utf-8',
    '.css':  'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.svg':  'image/svg+xml',
    '.png':  'image/png',
    '.jpg':  'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.webp': 'image/webp',
    '.ico':  'image/x-icon',
    '.woff2':'font/woff2',
};

function serveStatic(req, res) {
    let urlPath = decodeURIComponent(req.url.split('?')[0]);
    if (urlPath === '/') urlPath = '/index.html';

    const filePath = path.join(__dirname, urlPath);
    if (!filePath.startsWith(__dirname)) {
        res.writeHead(403).end('Forbidden');
        return;
    }

    fs.readFile(filePath, (err, data) => {
        if (err) {
            // SPA fallback for unknown non-asset routes
            if (!path.extname(urlPath)) {
                return fs.readFile(path.join(__dirname, 'index.html'), (e2, d2) => {
                    if (e2) return res.writeHead(404).end('Not found');
                    res.writeHead(200, { 'Content-Type': MIME['.html'] }).end(d2);
                });
            }
            res.writeHead(404).end('Not found');
            return;
        }
        const ext = path.extname(filePath).toLowerCase();
        res.writeHead(200, {
            'Content-Type': MIME[ext] || 'application/octet-stream',
            'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=3600',
        }).end(data);
    });
}

const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') return res.writeHead(204).end();

    // REST: profile lookup
    if (req.url.startsWith('/api/user/') && req.method === 'GET') {
        const id = req.url.slice('/api/user/'.length).split('?')[0];
        const u = users[id];
        if (!u) return res.writeHead(404, { 'Content-Type': MIME['.json'] }).end('{}');
        return res.writeHead(200, { 'Content-Type': MIME['.json'] }).end(JSON.stringify(u));
    }

    // REST: room lookup
    if (req.url.startsWith('/api/room/') && req.method === 'GET') {
        const id = req.url.slice('/api/room/'.length).split('?')[0];
        const r = rooms[id];
        if (!r) return res.writeHead(404, { 'Content-Type': MIME['.json'] }).end('{}');
        return res.writeHead(200, { 'Content-Type': MIME['.json'] }).end(JSON.stringify(r));
    }

    serveStatic(req, res);
});

/* ============================================================
 * WEBSOCKET SIGNALING
 * ============================================================ */

const wss = new WebSocketServer({ server, maxPayload: 1024 * 512 });

/**
 * Live rooms map: roomId -> Map<ws, member>
 * member = { id, name, profile, tier, micOn, camOn, sharing, handRaised, role, joinedAt }
 */
const liveRooms = new Map();

function getLiveRoom(id) {
    if (!liveRooms.has(id)) liveRooms.set(id, new Map());
    return liveRooms.get(id);
}

function safeSend(ws, payload) {
    if (ws.readyState === ws.OPEN) {
        try { ws.send(JSON.stringify(payload)); } catch (_) {}
    }
}

function broadcast(room, payload, except) {
    const data = JSON.stringify(payload);
    for (const ws of room.keys()) {
        if (ws === except) continue;
        if (ws.readyState === ws.OPEN) {
            try { ws.send(data); } catch (_) {}
        }
    }
}

function findWsByPeerId(room, peerId) {
    for (const [ws, m] of room.entries()) if (m.id === peerId) return ws;
    return null;
}

function publicMember(m) {
    return {
        id: m.id,
        name: m.name,
        profile: m.profile,
        tier: m.tier,
        micOn: m.micOn,
        camOn: m.camOn,
        sharing: m.sharing,
        handRaised: m.handRaised,
        role: m.role,
        joinedAt: m.joinedAt,
    };
}

function requireTier(member, minTier) {
    const rank = { free: 0, 'mm4+': 1, 'mm4++': 2, 'mm5-ultra': 3 };
    return (rank[member.tier] || 0) >= (rank[minTier] || 0);
}

wss.on('connection', (ws, req) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const roomId = url.searchParams.get('room');
    const userId = url.searchParams.get('user') || crypto.randomBytes(8).toString('hex');

    if (!roomId) { try { ws.close(1008, 'room required'); } catch (_) {} return; }

    const room = getLiveRoom(roomId);
    const existingUser = users[userId] || {};
    const member = {
        id: userId,
        name: existingUser.name || 'Guest',
        profile: existingUser.profile || defaultProfile(),
       tier: existingUser.tier || 'free',
       micOn: true,
       camOn: false,
       sharing: false,
       handRaised: false,
       role: 'member',
       joinedAt: Date.now(),
       ws,
    };
    room.set(ws, member);

    if (!users[userId]) {
        users[userId] = {
            id: userId,
            name: member.name,
            profile: member.profile,
            tier: member.tier,
            createdAt: Date.now(),
       lastSeen: Date.now(),
        };
        scheduleSave();
    } else {
        users[userId].lastSeen = Date.now();
        scheduleSave();
    }

    ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw.toString()); } catch { return; }
        const me = room.get(ws);
        if (!me) return;

        switch (msg.type) {
            /* ---------- JOIN / HELLO ---------- */
            case 'join': {
                if (msg.peer) {
                    if (msg.peer.name) me.name = String(msg.peer.name).slice(0, 32);
                    if (msg.peer.profile) me.profile = sanitizeProfile(msg.peer.profile);
                    users[me.id] = users[me.id] || {};
                    users[me.id].name = me.name;
                    users[me.id].profile = me.profile;
                    users[me.id].lastSeen = Date.now();
                    scheduleSave();
                }

                // Promote first joiner to host
                const isEmptyOfHost = ![...room.values()].some(m => m.role === 'host' && m !== me);
                if (isEmptyOfHost) me.role = 'host';

                const peers = [...room.values()]
                .filter(m => m !== me)
                .map(publicMember);

                safeSend(ws, {
                    type: 'joined',
                    room: rooms[roomId] || { id: roomId, name: msg.roomName || 'Meeting' },
                    self: publicMember(me),
                         peers,
                });

                broadcast(room, { type: 'peer-joined', peer: publicMember(me) }, ws);

                if (!rooms[roomId]) {
                    rooms[roomId] = {
                        id: roomId,
                        name: msg.roomName || 'Meeting',
                        hostId: me.id,
                        createdAt: Date.now(),
          persistent: false,
                    };
                    scheduleSave();
                }
                break;
            }

            /* ---------- PROFILE / TIER ---------- */
            case 'profile': {
                me.profile = sanitizeProfile(msg.profile || me.profile);
                users[me.id] = users[me.id] || { id: me.id };
                users[me.id].profile = me.profile;
                users[me.id].name = me.name;
                users[me.id].lastSeen = Date.now();
                scheduleSave();
                broadcast(room, { type: 'peer-updated', peer: publicMember(me) });
                break;
            }

            case 'tier': {
                const allowed = ['free', 'mm4+', 'mm4++', 'mm5-ultra'];
                if (allowed.includes(msg.tier)) {
                    me.tier = msg.tier;
                    users[me.id] = users[me.id] || { id: me.id };
                    users[me.id].tier = me.tier;
                    scheduleSave();
                    broadcast(room, { type: 'peer-updated', peer: publicMember(me) });
                }
                break;
            }

            case 'rename': {
                if (msg.name) {
                    me.name = String(msg.name).slice(0, 32);
                    users[me.id] = users[me.id] || { id: me.id };
                    users[me.id].name = me.name;
                    scheduleSave();
                    broadcast(room, { type: 'peer-updated', peer: publicMember(me) });
                }
                break;
            }

            /* ---------- STATE BROADCAST ---------- */
            case 'state': {
                if (typeof msg.micOn === 'boolean') me.micOn = msg.micOn;
                if (typeof msg.camOn === 'boolean') me.camOn = msg.camOn;
                if (typeof msg.sharing === 'boolean') me.sharing = msg.sharing;
                if (typeof msg.handRaised === 'boolean') me.handRaised = msg.handRaised;
                broadcast(room, { type: 'peer-updated', peer: publicMember(me) }, ws);
                break;
            }

            /* ---------- WEBRTC RELAY ---------- */
            case 'offer':
            case 'answer':
            case 'ice': {
                if (msg.to) {
                    const target = findWsByPeerId(room, msg.to);
                    if (target) safeSend(target, { ...msg, from: me.id });
                }
                break;
            }

            /* ---------- CHAT ---------- */
            case 'chat': {
                const text = String(msg.text || '').slice(0, 2000);
                if (!text) break;
                const payload = {
                    type: 'chat',
                    from: me.id,
                    name: me.name,
                    profile: me.profile,
                    text,
                    ts: Date.now(),
                };
                broadcast(room, payload, ws);
                safeSend(ws, { ...payload, self: true });
                break;
            }

            /* ---------- REACTIONS ---------- */
            case 'reaction': {
                const emoji = String(msg.emoji || '👍').slice(0, 4);
                broadcast(room, {
                    type: 'reaction',
                    from: me.id,
                    name: me.name,
                    emoji,
                    ts: Date.now(),
                }, ws);
                break;
            }

            /* ---------- MODERATION ---------- */
            case 'kick': {
                if (me.role !== 'host') break;
                const target = findWsByPeerId(room, msg.peerId);
                if (target) {
                    safeSend(target, { type: 'kicked', by: me.name });
                    try { target.close(1000, 'kicked'); } catch (_) {}
                }
                break;
            }

            case 'mute-peer': {
                if (me.role !== 'host') break;
                const target = findWsByPeerId(room, msg.peerId);
                if (target) safeSend(target, { type: 'force-mute', by: me.name });
                break;
            }

            case 'role': {
                if (me.role !== 'host') break;
                const allowed = ['host', 'cohost', 'member'];
                if (!allowed.includes(msg.role)) break;
                const targetWs = findWsByPeerId(room, msg.peerId);
                if (targetWs) {
                    const target = room.get(targetWs);
                    target.role = msg.role;
                    broadcast(room, { type: 'peer-updated', peer: publicMember(target) });
                }
                break;
            }

            /* ---------- NITRO / MM4+ PERKS ---------- */
            case 'mm4-perk': {
                if (!requireTier(me, 'mm4+')) {
                    safeSend(ws, { type: 'error', message: 'MM4+ required' });
                    break;
                }
                const perk = msg.perk;
                const duration = Math.min(Number(msg.duration) || 0, 60 * 60 * 1000);
                if (!['screen-4k', 'custom-layout', 'stream-mode'].includes(perk)) break;
                broadcast(room, {
                    type: 'perk-activated',
                    from: me.id,
                    name: me.name,
                    perk,
                    duration,
                }, ws);
                break;
            }

            /* ---------- HEARTBEAT ---------- */
            case 'ping': {
                safeSend(ws, { type: 'pong', ts: Date.now() });
                break;
            }
        }
    });

    ws.on('close', () => {
        const me = room.get(ws);
        room.delete(ws);
        if (me) {
            broadcast(room, { type: 'peer-left', peerId: me.id, name: me.name });
        }
        if (room.size === 0) {
            liveRooms.delete(roomId);
            // keep room metadata for a while; don't delete yet
        }
    });

    ws.on('error', () => { try { ws.close(); } catch (_) {} });
});

/* ============================================================
 * HELPERS
 * ============================================================ */

function defaultProfile() {
    return {
        avatar: '',
        banner: '',
        bio: '',
        pronouns: '',
        accent: '#ffffff',
        status: 'online',
        customStatus: '',
    };
}

function sanitizeProfile(p) {
    const out = defaultProfile();
    if (!p || typeof p !== 'object') return out;
    if (typeof p.avatar === 'string' && p.avatar.length < 500000) out.avatar = p.avatar;
    if (typeof p.banner === 'string' && p.banner.length < 2000000) out.banner = p.banner;
    if (typeof p.bio === 'string') out.bio = p.bio.slice(0, 300);
    if (typeof p.pronouns === 'string') out.pronouns = p.pronouns.slice(0, 40);
    if (typeof p.accent === 'string' && /^#[0-9a-f]{3,8}$/i.test(p.accent)) out.accent = p.accent;
    if (['online','idle','dnd','invisible'].includes(p.status)) out.status = p.status;
    if (typeof p.customStatus === 'string') out.customStatus = p.customStatus.slice(0, 128);
    return out;
}

/* ============================================================
 * HEARTBEAT SWEEP — close dead sockets
 * ============================================================ */

setInterval(() => {
    for (const [roomId, room] of liveRooms.entries()) {
        for (const [ws] of room.entries()) {
            if (ws.readyState !== ws.OPEN) {
                room.delete(ws);
            }
        }
        if (room.size === 0) liveRooms.delete(roomId);
    }
}, 30000);

server.listen(PORT, () => {
    console.log(`MM Meet signaling server listening on http://localhost:${PORT}`);
});
