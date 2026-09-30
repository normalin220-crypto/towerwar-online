const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || '0.0.0.0';
const ROOT = __dirname;
const GAME_FILE = path.join(ROOT, 'TowerWar_online.html');
const MAX_MESSAGE = 1024 * 1024 * 2;

const rooms = new Map();
const clients = new Set();

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
};

function cleanName(value) {
  return String(value || 'Игрок').replace(/[<>\"'\\]/g, '').trim().slice(0, 18) || 'Игрок';
}

function cleanColor(value) {
  const allowed = new Set(['blue','red','pink','lgreen','black','orange','purple','teal']);
  return allowed.has(value) ? value : 'blue';
}

function makeRoomCode() {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  for (;;) {
    let code = '';
    for (let i = 0; i < 5; i++) code += alphabet[crypto.randomInt(alphabet.length)];
    if (!rooms.has(code)) return code;
  }
}

function publicRooms() {
  return [...rooms.values()]
    .filter(r => r.host && r.socket && r.socket.readyState === WebSocket.OPEN)
    .map(r => ({
      code: r.code,
      hostName: r.hostName,
      mode: r.mode,
      players: (r.host ? 1 : 0) + (r.guest ? 1 : 0),
      createdAt: r.createdAt,
    }))
    .sort((a,b) => a.createdAt - b.createdAt);
}

function send(ws, kind, msg) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  try { ws.send(JSON.stringify({ kind, ...msg })); } catch {}
}

function sendLobby(ws, msg) { send(ws, 'lobby', msg); }
function sendGame(ws, msg) { send(ws, 'game', { msg }); }

function broadcastRoomList() {
  const payload = { t: 'list', rooms: publicRooms() };
  for (const c of clients) {
    if (c.ws.readyState === WebSocket.OPEN) sendLobby(c.ws, payload);
  }
}

function detachClient(client, reason = 'disconnect') {
  if (!client || !client.room) return;
  const room = rooms.get(client.room);
  if (!room) return;

  if (room.host === client) {
    if (room.guest) {
      sendLobby(room.guest.ws, { t: 'removed', reason: 'host_left' });
      try { room.guest.ws.close(4001, 'Host left'); } catch {}
      room.guest.room = null;
    }
    rooms.delete(room.code);
  } else if (room.guest === client) {
    room.guest = null;
    sendLobby(room.host.ws, { t: 'room_left', reason });
  }

  client.room = null;
  client.role = null;
  broadcastRoomList();
}

function roomOf(client) {
  return client.room ? rooms.get(client.room) : null;
}

function handleList(client) {
  sendLobby(client.ws, { t: 'list', rooms: publicRooms() });
}

function handleCreate(client, msg) {
  if (client.room) {
    sendLobby(client.ws, { t: 'error', message: 'Ты уже находишься в комнате.' });
    return;
  }

  const room = {
    code: makeRoomCode(),
    host: client,
    guest: null,
    hostName: cleanName(msg.name),
    hostColor: cleanColor(msg.color),
    guestName: '',
    guestColor: '',
    mode: 'pvp',
    createdAt: Date.now(),
  };

  rooms.set(room.code, room);
  client.room = room.code;
  client.role = 'host';
  client.name = room.hostName;
  client.color = room.hostColor;

  sendLobby(client.ws, {
    t: 'created',
    room: room.code,
    role: 'host',
    color: room.hostColor,
  });
  broadcastRoomList();
}

function handleJoin(client, msg) {
  if (client.room) {
    sendLobby(client.ws, { t: 'error', message: 'Ты уже находишься в комнате.' });
    return;
  }

  const code = String(msg.room || '').trim().toUpperCase();
  const room = rooms.get(code);
  if (!room || !room.host || room.host.ws.readyState !== WebSocket.OPEN) {
    sendLobby(client.ws, { t: 'error', message: 'Комната не найдена.' });
    if (room) rooms.delete(code);
    broadcastRoomList();
    return;
  }
  if (room.guest) {
    sendLobby(client.ws, { t: 'error', message: 'Комната уже заполнена.' });
    return;
  }

  const color = cleanColor(msg.color);
  if (color === room.hostColor) {
    sendLobby(client.ws, { t: 'error', message: 'Этот цвет уже занят хозяином комнаты. Выбери другой.' });
    return;
  }

  room.guest = client;
  room.guestName = cleanName(msg.name);
  room.guestColor = color;
  client.room = room.code;
  client.role = 'guest';
  client.name = room.guestName;
  client.color = color;

  sendLobby(client.ws, {
    t: 'joined',
    room: room.code,
    role: 'guest',
    hostName: room.hostName,
    hostColor: room.hostColor,
    guestColor: room.guestColor,
  });
  sendLobby(room.host.ws, {
    t: 'peer_joined',
    room: room.code,
    guestName: room.guestName,
    guestColor: room.guestColor,
    players: 2,
  });

  broadcastRoomList();
}

function handleLeave(client) {
  detachClient(client, 'leave');
  sendLobby(client.ws, { t: 'left' });
}

function handleLobby(client, msg) {
  if (!msg || !msg.t) return;
  switch (msg.t) {
    case 'list': handleList(client); break;
    case 'create': handleCreate(client, msg); break;
    case 'join': handleJoin(client, msg); break;
    case 'leave': handleLeave(client); break;
    default: break;
  }
}

function handleGame(client, msg) {
  const room = roomOf(client);
  if (!room || !msg || !msg.t) return;

  const other = client.role === 'host' ? room.guest : room.host;
  if (!other || other.ws.readyState !== WebSocket.OPEN) {
    if (msg.t === 'end') return;
    return;
  }

  // The authoritative game is still the host, exactly like the old two-phone mode.
  // The difference is that all traffic now goes through this public relay server,
  // so players do not need to share a LAN/Wi-Fi or know each other's IP addresses.
  sendGame(other.ws, msg);

  // End of match closes the room for new players. Existing clients may return to menu/reload.
  if (msg.t === 'end') {
    setTimeout(() => {
      const current = rooms.get(room.code);
      if (current === room) {
        if (room.host) { room.host.room = null; room.host.role = null; }
        if (room.guest) { room.guest.room = null; room.guest.role = null; }
        rooms.delete(room.code);
        broadcastRoomList();
      }
    }, 3000);
  }
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  if (url.pathname === '/health') {
    res.writeHead(200, {'Content-Type': 'application/json; charset=utf-8'});
    res.end(JSON.stringify({ok:true, rooms:rooms.size, players:clients.size}));
    return;
  }

  if (url.pathname === '/api/rooms') {
    res.writeHead(200, {'Content-Type': 'application/json; charset=utf-8', 'Cache-Control':'no-store'});
    res.end(JSON.stringify(publicRooms()));
    return;
  }

  let file = GAME_FILE;
  if (url.pathname === '/TowerWar_online.html') file = GAME_FILE;
  if (url.pathname !== '/' && url.pathname !== '/TowerWar_online.html') {
    res.writeHead(404, {'Content-Type':'text/plain; charset=utf-8'});
    res.end('Not found');
    return;
  }
  fs.readFile(file, (err, data) => {
    if (err) {
      res.writeHead(500, {'Content-Type':'text/plain; charset=utf-8'});
      res.end('Game file unavailable');
      return;
    }
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(file)] || 'application/octet-stream',
      'Cache-Control': 'no-store',
    });
    res.end(data);
  });
});

const wss = new WebSocketServer({server, path:'/ws', maxPayload:MAX_MESSAGE});

wss.on('connection', ws => {
  const client = {ws, room:null, role:null, name:'', color:''};
  clients.add(client);
  ws._twClient = client;

  sendLobby(ws, {t:'hello', rooms:publicRooms()});

  ws.on('message', raw => {
    let data;
    try {
      data = JSON.parse(raw.toString('utf8'));
    } catch {
      sendLobby(ws, {t:'error', message:'Некорректное сообщение.'});
      return;
    }
    if (!data || typeof data !== 'object') return;
    try {
      if (data.kind === 'lobby') handleLobby(client, data.msg);
      else if (data.kind === 'game') handleGame(client, data.msg);
    } catch (err) {
      console.error('message error', err);
      sendLobby(ws, {t:'error', message:'Внутренняя ошибка сервера.'});
    }
  });

  ws.on('close', () => {
    detachClient(client, 'disconnect');
    clients.delete(client);
  });
  ws.on('error', () => {
    detachClient(client, 'error');
    clients.delete(client);
  });
});

// Heartbeat for dead mobile/browser connections.
const heartbeat = setInterval(() => {
  for (const client of clients) {
    if (client.ws.readyState === WebSocket.OPEN) {
      try { client.ws.ping(); } catch {}
    }
  }
}, 30000);

server.listen(PORT, HOST, () => {
  console.log(`Tower War online server listening on http://${HOST}:${PORT}`);
});

function shutdown(){
  clearInterval(heartbeat);
  for (const client of clients) {
    try { client.ws.close(1001, 'Server shutdown'); } catch {}
  }
  server.close(() => process.exit(0));
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
