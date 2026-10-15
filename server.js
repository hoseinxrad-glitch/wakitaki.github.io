const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const ADMIN = process.env.ADMIN_PASSWORD || '';
const ORIGIN = process.env.ALLOWED_ORIGIN || '';

const rooms = new Map(); // name -> { clients:Set, talker, msgs:[] }
const banned = new Set();
let mid = 0;

const send = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));
const bc = (r, o, ex) => { for (const c of r.clients) if (c !== ex) send(c, o); };
const presence = (r) => bc(r, { type: 'presence', count: r.clients.size, users: [...r.clients].map((c) => c.userName) });
const release = (r, ws) => { if (r.talker === ws) { r.talker = null; bc(r, { type: 'idle' }, ws); } };
function kick(name, roomName) {
  for (const [rn, r] of rooms)
    if (!roomName || rn === roomName)
      for (const c of [...r.clients]) if (c.userName === name) { send(c, { type: 'kicked' }); c.close(); }
}

const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'x-admin,content-type');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  if (!req.url.startsWith('/admin/')) { res.writeHead(200); return res.end('walkie-talkie server ok'); }
  if (!ADMIN || req.headers['x-admin'] !== ADMIN) { res.writeHead(401); return res.end('unauthorized'); }
  let body = ''; for await (const c of req) body += c;
  let d = {}; try { d = JSON.parse(body || '{}'); } catch {}
  const act = req.url.slice(7), r = rooms.get(d.room);
  if (act === 'kick') kick(d.name, d.room);
  else if (act === 'ban') { banned.add(d.name); kick(d.name); }
  else if (act === 'unban') banned.delete(d.name);
  else if (act === 'delete' && r) { r.msgs = r.msgs.filter((m) => m.id !== d.id); bc(r, { type: 'deleted', id: d.id }); }
  else if (act === 'clear' && r) { for (const m of r.msgs) bc(r, { type: 'deleted', id: m.id }); r.msgs = []; }
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    banned: [...banned],
    rooms: [...rooms].map(([name, x]) => ({ name, users: [...x.clients].map((c) => c.userName), msgs: x.msgs })),
  }));
});

const wss = new WebSocketServer({
  server, maxPayload: 64 * 1024,
  verifyClient: (i) => !ORIGIN || i.origin === ORIGIN || (i.origin || '').startsWith('http://localhost'),
});

wss.on('connection', (ws, req) => {
  const q = new URL(req.url, 'http://x').searchParams;
  const roomName = (q.get('room') || 'general').trim().slice(0, 40) || 'general';
  const name = (q.get('name') || 'ناشناس').trim().slice(0, 30) || 'ناشناس';
  ws.userName = name;
  if (banned.has(name)) { send(ws, { type: 'kicked' }); return ws.close(); }

  let room = rooms.get(roomName);
  if (!room) rooms.set(roomName, (room = { clients: new Set(), talker: null, msgs: [] }));
  room.clients.add(ws);
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
  send(ws, { type: 'history', msgs: room.msgs });
  send(ws, room.talker ? { type: 'talking', name: room.talker.userName, video: room.talker.video } : { type: 'idle' });
  presence(room);

  ws.on('message', (data, isBinary) => {
    if (isBinary) { // صدا یا تصویر گوینده‌ی فعلی
      if (room.talker !== ws) return;
      for (const c of room.clients) if (c !== ws && c.readyState === 1) c.send(data, { binary: true });
      return;
    }
    let m; try { m = JSON.parse(data.toString()); } catch { return; }
    if (m.type === 'start') {
      if (room.talker && room.talker !== ws) return send(ws, { type: 'busy', name: room.talker.userName });
      room.talker = ws; ws.video = !!m.video;
      bc(room, { type: 'talking', name, video: ws.video }, ws);
    } else if (m.type === 'stop') release(room, ws);
    else if (m.type === 'chat' && typeof m.text === 'string' && m.text.trim()) {
      const msg = { id: ++mid, name, text: m.text.trim().slice(0, 500), t: Date.now() };
      room.msgs.push(msg);
      if (room.msgs.length > 100) room.msgs.shift();
      bc(room, { type: 'chat', ...msg });
    }
  });

  ws.on('close', () => {
    release(room, ws);
    room.clients.delete(ws);
    if (!room.clients.size && !room.msgs.length) rooms.delete(roomName);
    else presence(room);
  }); 
  ws.on('error', () => {});
});

setInterval(() => {
  for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; ws.ping(); }
}, 25000);

server.listen(PORT, () => console.log('listening on', PORT));
