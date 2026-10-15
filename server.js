const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
// اختیاری: آدرس سایت GitHub Pages، مثل https://username.github.io
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '';

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('walkie-talkie server ok');
});

const wss = new WebSocketServer({
  server,
  maxPayload: 64 * 1024,
  verifyClient: (info) => {
    if (!ALLOWED_ORIGIN) return true;
    const origin = info.origin || '';
    return origin === ALLOWED_ORIGIN || origin.startsWith('http://localhost');
  },
});

// room name -> { clients: Set<ws>, talker: ws | null }
const rooms = new Map();

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function broadcast(room, obj, except) {
  for (const c of room.clients) if (c !== except) send(c, obj);
}

function sendPresence(room) {
  broadcast(room, { type: 'presence', count: room.clients.size });
}

function releaseTalker(room, ws) {
  if (room.talker === ws) {
    room.talker = null;
    broadcast(room, { type: 'idle' }, ws);
  }
}

wss.on('connection', (ws, req) => {
  const params = new URL(req.url, 'http://localhost').searchParams;
  const roomName = (params.get('room') || 'general').trim().slice(0, 40) || 'general';
  const name = (params.get('name') || 'ناشناس').trim().slice(0, 30) || 'ناشناس';

  let room = rooms.get(roomName);
  if (!room) {
    room = { clients: new Set(), talker: null };
    rooms.set(roomName, room);
  }
  room.clients.add(ws);
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));

  send(ws, room.talker
    ? { type: 'talking', name: room.talker.userName }
    : { type: 'idle' });
  ws.userName = name;
  sendPresence(room);

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      // فقط صدای گوینده‌ی فعلی پخش می‌شود
      if (room.talker !== ws) return;
      for (const c of room.clients) {
        if (c !== ws && c.readyState === 1) c.send(data, { binary: true });
      }
      return;
    }
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.type === 'start') {
      if (room.talker && room.talker !== ws) {
        send(ws, { type: 'busy', name: room.talker.userName });
      } else {
        room.talker = ws;
        broadcast(room, { type: 'talking', name }, ws);
      }
    } else if (msg.type === 'stop') {
      releaseTalker(room, ws);
    }
  });

  ws.on('close', () => {
    releaseTalker(room, ws);
    room.clients.delete(ws);
    if (room.clients.size === 0) rooms.delete(roomName);
    else sendPresence(room);
  });

  ws.on('error', () => {});
});

// قطع اتصال‌های مرده و زنده نگه داشتن اتصال پشت پروکسی
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log('listening on', PORT));
