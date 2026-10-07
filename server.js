const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
    res.end('Signaling server running.\n');
});

const wss = new WebSocketServer({ server });

// roomId -> Map<ws, { id, name, role }>
const rooms = new Map();

function getRoom(id) {
    if (!rooms.has(id)) rooms.set(id, new Map());
    return rooms.get(id);
}

function send(ws, msg) {
    if (ws.readyState === ws.OPEN) {
        try { ws.send(JSON.stringify(msg)); } catch (_) {}
    }
}

function broadcast(room, msg, exceptWs) {
    for (const ws of room.keys()) {
        if (ws === exceptWs) continue;
        send(ws, msg);
    }
}

wss.on('connection', (ws, req) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const roomId = url.searchParams.get('room');
    if (!roomId) { try { ws.close(1008, 'room required'); } catch (_) {} return; }

    const room = getRoom(roomId);
    room.set(ws, { id: null, name: null, role: 'member' });

    ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw); } catch { return; }
        const me = room.get(ws);
        if (!me) return;

        switch (msg.type) {
            case 'join': {
                me.id = msg.peer.id;
                me.name = msg.peer.name;
                me.role = msg.role || 'member';

                // Send existing peers to the new joiner
                const existing = [];
                for (const [otherWs, otherPeer] of room.entries()) {
                    if (otherWs !== ws && otherPeer.id) existing.push(otherPeer);
                }
                send(ws, { type: 'peers', peers: existing });

                // Tell everyone else about the new peer
                broadcast(room, { type: 'peer-joined', peer: me }, ws);
                break;
            }

            case 'role': {
                // Update the target's stored role on the server
                for (const [otherWs, otherPeer] of room.entries()) {
                    if (otherPeer.id === msg.peerId) {
                        otherPeer.role = msg.role;
                        // Forward the role change to the target AND to everyone else
                        send(otherWs, { type: 'role', peerId: msg.peerId, role: msg.role, from: me.id });
                        broadcast(room, { type: 'role', peerId: msg.peerId, role: msg.role, from: me.id }, otherWs);
                        break;
                    }
                }
                break;
            }

            case 'kick': {
                // Find the target and tell them to leave
                for (const [otherWs, otherPeer] of room.entries()) {
                    if (otherPeer.id === msg.peerId) {
                        send(otherWs, { type: 'kick', peerId: msg.peerId, from: me.id });
                        // Also notify others
                        broadcast(room, { type: 'peer-left', peerId: msg.peerId }, otherWs);
                        break;
                    }
                }
                break;
            }

            case 'offer':
            case 'answer':
            case 'ice':
            case 'chat':
            case 'delete':
            case 'state': {
                if (msg.to) {
                    for (const [otherWs, otherPeer] of room.entries()) {
                        if (otherPeer.id === msg.to) {
                            send(otherWs, { ...msg, from: me.id });
                            break;
                        }
                    }
                } else {
                    broadcast(room, { ...msg, from: me.id }, ws);
                }
                break;
            }
        }
    });

    ws.on('close', () => {
        const me = room.get(ws);
        room.delete(ws);
        if (me && me.id) {
            broadcast(room, { type: 'peer-left', peerId: me.id });
        }
        if (room.size === 0) rooms.delete(roomId);
    });

        ws.on('error', () => { try { ws.close(); } catch (_) {} });
});

server.listen(PORT, () => {
    console.log('Signaling server listening on ' + PORT);
});
