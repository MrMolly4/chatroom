// server.js — mm4 chatroom signaling + static server
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;

const MIME = {
    '.html': 'text/html; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon',
    '.woff2': 'font/woff2'
};

const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

    const url = new URL(req.url, `http://${req.headers.host}`);
    let filePath = url.pathname === '/' ? '/index.html' : url.pathname;

    // Prevent traversal
    filePath = path.normalize(filePath).replace(/^(\.\.[/\\])+/, '');
    const full = path.join(__dirname, filePath);

    fs.readFile(full, (err, data) => {
        if (err) {
            // Fallback to index.html for SPA routing
            fs.readFile(path.join(__dirname, 'index.html'), (e2, idx) => {
                if (e2) { res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('Not found'); return; }
                res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
                res.end(idx);
            });
            return;
        }
        const ext = path.extname(full).toLowerCase();
        res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
        res.end(data);
    });
});

const wss = new WebSocketServer({ server });

// roomId -> { peers: Map<ws, {id,name,avatar,tier,role,status,voiceChannelId}>, channels: Map<channelId, {id,name,type}>, messages: Map<channelId, msg[]> }
const rooms = new Map();

function getRoom(id) {
    if (!rooms.has(id)) {
        rooms.set(id, {
            peers: new Map(),
                  channels: new Map(),
                  messages: new Map(),
                  emptySince: null,
                  cleanupTimer: null
        });
    }
    const r = rooms.get(id);
    r.emptySince = null;
    if (r.cleanupTimer) { clearTimeout(r.cleanupTimer); r.cleanupTimer = null; }
    return r;
}

function send(ws, msg) {
    if (ws.readyState === ws.OPEN) {
        try { ws.send(JSON.stringify(msg)); } catch {}
    }
}

function broadcast(room, msg, except) {
    for (const ws of room.peers.keys()) {
        if (ws === except) continue;
        send(ws, msg);
    }
}

function findPeerWs(room, peerId) {
    for (const [ws, p] of room.peers.entries()) {
        if (p.id === peerId) return ws;
    }
    return null;
}

function pushHistory(room, channelId, msg) {
    if (!room.messages.has(channelId)) room.messages.set(channelId, []);
    const arr = room.messages.get(channelId);
    arr.push(msg);
    if (arr.length > 200) arr.splice(0, arr.length - 200);
}

wss.on('connection', (ws, req) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const roomId = url.searchParams.get('room');
    if (!roomId) { try { ws.close(1008, 'room required'); } catch {} return; }

    const room = getRoom(roomId);
    room.peers.set(ws, {
        id: null, name: null, avatar: '', tier: 'free', role: 'member',
        status: 'online', micOn: true, camOn: false, sharing: false,
        voiceChannelId: null
    });

    ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw); } catch { return; }
        const me = room.peers.get(ws);
        if (!me) return;

        switch (msg.type) {
            case 'join': {
                me.id = msg.peer.id;
                me.name = msg.peer.name;
                me.avatar = msg.peer.avatar || '';
                me.tier = msg.peer.tier || 'free';
                me.role = msg.role || 'member';
                me.status = msg.peer.status || 'online';

                // Send existing peers to new joiner
                const existing = [];
                for (const [otherWs, otherPeer] of room.peers.entries()) {
                    if (otherWs !== ws && otherPeer.id) existing.push(otherPeer);
                }
                send(ws, { type: 'peers', peers: existing });

                // Send channel list
                if (room.channels.size === 0) {
                    // Default channels
                    const defaults = [
                        { id: 'general-' + roomId, name: 'general', type: 'text' },
                        { id: 'random-' + roomId, name: 'random', type: 'text' },
                        { id: 'voice-' + roomId, name: 'General', type: 'voice' }
                    ];
                    defaults.forEach(c => room.channels.set(c.id, c));
                }
                send(ws, { type: 'channel-list', channels: Array.from(room.channels.values()) });

                // Send recent history for all channels
                for (const [channelId, msgs] of room.messages.entries()) {
                    send(ws, { type: 'history', channelId, messages: msgs.slice(-100) });
                }

                broadcast(room, { type: 'peer-joined', peer: me }, ws);
                break;
            }

            case 'history-request': {
                const msgs = room.messages.get(msg.channelId) || [];
                send(ws, { type: 'history', channelId: msg.channelId, messages: msgs.slice(-100) });
                break;
            }

            case 'chat': {
                const message = {
                    id: msg.id,
                    channelId: msg.channelId,
                    peerId: me.id,
                    name: msg.name || me.name,
                    avatar: msg.avatar || me.avatar,
                    text: msg.text,
                    time: msg.time || Date.now(),
          replyTo: msg.replyTo || null,
          reactions: {}
                };
                pushHistory(room, msg.channelId, message);
                broadcast(room, { ...message, type: 'chat', from: me.id }, ws);
                break;
            }

            case 'edit': {
                const arr = room.messages.get(msg.channelId) || [];
                const m = arr.find(x => x.id === msg.msgId);
                if (m && m.peerId === me.id) {
                    m.text = msg.text;
                    m.edited = true;
                    broadcast(room, { type: 'edit', channelId: msg.channelId, msgId: msg.msgId, text: msg.text }, null);
                }
                break;
            }

            case 'delete': {
                const arr = room.messages.get(msg.channelId) || [];
                const m = arr.find(x => x.id === msg.msgId);
                if (!m) break;
                const canDelete = m.peerId === me.id ||
                ['owner', 'admin', 'mod'].includes(me.role);
                if (canDelete) {
                    m.deleted = true;
                    m.text = '';
                    broadcast(room, { type: 'delete', channelId: msg.channelId, msgId: msg.msgId }, null);
                }
                break;
            }

            case 'reaction': {
                const arr = room.messages.get(msg.channelId) || [];
                const m = arr.find(x => x.id === msg.msgId);
                if (!m) break;
                m.reactions = m.reactions || {};
                const set = new Set(m.reactions[msg.emoji] || []);
                if (msg.add) set.add(me.id);
                else set.delete(me.id);
                m.reactions[msg.emoji] = Array.from(set);
                if (!m.reactions[msg.emoji].length) delete m.reactions[msg.emoji];
                broadcast(room, { type: 'reaction', channelId: msg.channelId, msgId: msg.msgId, emoji: msg.emoji, add: msg.add, peerId: me.id }, ws);
                break;
            }

            case 'typing': {
                broadcast(room, {
                    type: 'typing',
                    channelId: msg.channelId,
                    peerId: me.id,
                    typing: !!msg.typing
                }, ws);
                break;
            }

            case 'state': {
                if ('micOn' in msg) me.micOn = msg.micOn;
                if ('camOn' in msg) me.camOn = msg.camOn;
                if ('sharing' in msg) me.sharing = msg.sharing;
                if ('status' in msg) me.status = msg.status;
                broadcast(room, {
                    type: 'state',
                    peerId: me.id,
                    micOn: me.micOn,
                    camOn: me.camOn,
                    sharing: me.sharing,
                    status: me.status
                }, ws);
                break;
            }

            case 'join-voice': {
                me.voiceChannelId = msg.channelId;
                broadcast(room, { type: 'join-voice', peerId: me.id, channelId: msg.channelId }, ws);
                break;
            }

            case 'leave-voice': {
                me.voiceChannelId = null;
                broadcast(room, { type: 'leave-voice', peerId: me.id }, ws);
                break;
            }

            case 'role': {
                const targetWs = findPeerWs(room, msg.peerId);
                if (!targetWs) break;
                const target = room.peers.get(targetWs);
                if (!target) break;
                // Only admins/owners can promote
                if (!['owner', 'admin'].includes(me.role)) break;
                target.role = msg.role;
                send(targetWs, { type: 'role', peerId: msg.peerId, role: msg.role });
                broadcast(room, { type: 'role', peerId: msg.peerId, role: msg.role }, targetWs);
                break;
            }

            case 'kick': {
                if (!['owner', 'admin', 'mod'].includes(me.role)) break;
                const targetWs = findPeerWs(room, msg.peerId);
                if (!targetWs) break;
                send(targetWs, { type: 'kick', peerId: msg.peerId });
                break;
            }

            case 'channel-create': {
                room.channels.set(msg.channel.id, msg.channel);
                broadcast(room, { type: 'channel-create', channel: msg.channel }, ws);
                break;
            }

            case 'channel-delete': {
                room.channels.delete(msg.channelId);
                room.messages.delete(msg.channelId);
                broadcast(room, { type: 'channel-delete', channelId: msg.channelId }, ws);
                break;
            }

            case 'offer':
            case 'answer':
            case 'ice': {
                if (msg.to) {
                    const targetWs = findPeerWs(room, msg.to);
                    if (targetWs) send(targetWs, { ...msg, from: me.id });
                }
                break;
            }
        }
    });

    ws.on('close', () => {
        const me = room.peers.get(ws);
        room.peers.delete(ws);
        if (me && me.id) {
            broadcast(room, { type: 'peer-left', peerId: me.id });
        }
        if (room.peers.size === 0) {
            // Schedule cleanup after 1 hour
            room.emptySince = Date.now();
            room.cleanupTimer = setTimeout(() => {
                if (room.peers.size === 0) rooms.delete(roomId);
            }, 60 * 60 * 1000);
        }
    });

    ws.on('error', () => { try { ws.close(); } catch {} });
});

server.listen(PORT, () => {
    console.log(`mm4 chatroom running on http://localhost:${PORT}`);
});
