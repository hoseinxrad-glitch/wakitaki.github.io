const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const ADMIN = process.env.ADMIN_PASSWORD || '';
const ORIGIN = process.env.ALLOWED_ORIGIN || '';
// اگر DATABASE_URL تنظیم باشد، ورود فقط با حساب پیام‌رسان ممکن است و نام کاربر از دیتابیس خوانده می‌شود
let pool = null;
if (process.env.DATABASE_URL) {
  const { Pool } = require('pg');
  pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 3 });
  pool.on('error', (e) => console.error('db error', e.message));
}

const rooms = new Map(); // key -> room  (تماس تصویری با پیشوند meet:)
const banned = new Set(), muted = new Set();
const settings = { maxUsers: 0, maxLen: 500, badWords: [] };
const started = Date.now();
let mid = 0, pidSeq = 0, totalMsgs = 0;

const send = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));
const bc = (r, o, ex) => { for (const c of r.clients) if (c !== ex) send(c, o); };
const sys = (r, text) => bc(r, { type: 'system', text });
const presence = (r) => bc(r, { type: 'presence', count: r.clients.size, users: [...r.clients].map((c) => c.userName) });
const peersMsg = (r) => bc(r, { type: 'peers', peers: [...r.clients].map((c) => ({ id: c.pid, name: c.userName })) });
const release = (r, ws) => { if (r.talker === ws) { r.talker = null; bc(r, { type: 'idle' }, ws); } };
const newRoom = (key) => ({ key, clients: new Set(), talker: null, msgs: [], locked: false, password: '', persist: false, bytes: 0 });

function kick(name, key) {
  for (const [k, r] of rooms)
    if (!key || k === key)
      for (const c of [...r.clients]) if (c.userName === name) { send(c, { type: 'kicked' }); c.close(); }
}

const snapshot = () => {
  const rs = [...rooms.values()];
  return {
    stats: {
      online: rs.reduce((n, r) => n + r.clients.size, 0), rooms: rs.length, totalMsgs,
      bytes: rs.reduce((n, r) => n + r.bytes, 0), uptime: Math.floor((Date.now() - started) / 1000),
    },
    settings, banned: [...banned], muted: [...muted],
    rooms: rs.map((r) => ({
      name: r.key, users: [...r.clients].map((c) => c.userName), msgs: r.msgs,
      locked: r.locked, hasPassword: !!r.password, bytes: r.bytes,
    })),
  };
};

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
  else if (act === 'mute') muted.add(d.name);
  else if (act === 'unmute') muted.delete(d.name);
  else if (act === 'delete' && r) { r.msgs = r.msgs.filter((m) => m.id !== d.id); bc(r, { type: 'deleted', id: d.id }); }
  else if (act === 'clear' && r) { for (const m of r.msgs) bc(r, { type: 'deleted', id: m.id }); r.msgs = []; }
  else if (act === 'announce' && d.text) {
    const t = String(d.text).slice(0, 500);
    if (d.room) { if (r) sys(r, t); } else for (const x of rooms.values()) sys(x, t);
  }
  else if (act === 'lock' && r) r.locked = !!d.locked;
  else if (act === 'setpw' && r) r.password = String(d.password || '').slice(0, 40);
  else if (act === 'create' && d.room) {
    const key = (d.meet ? 'meet:' : '') + String(d.room).trim().slice(0, 40);
    if (!rooms.has(key)) { const x = newRoom(key); x.persist = true; rooms.set(key, x); }
  }
  else if (act === 'delroom' && r) {
    for (const c of [...r.clients]) { send(c, { type: 'kicked' }); c.close(); }
    rooms.delete(d.room);
  }
  else if (act === 'rename' && r && d.to) {
    const to = (d.room.startsWith('meet:') ? 'meet:' : '') + String(d.to).trim().replace(/^meet:/, '').slice(0, 40);
    if (to && !rooms.has(to)) { rooms.delete(d.room); r.key = to; rooms.set(to, r); bc(r, { type: 'renamed', room: to }); }
  }
  else if (act === 'settings') {
    settings.maxUsers = Math.max(0, parseInt(d.maxUsers, 10) || 0);
    settings.maxLen = Math.min(2000, Math.max(20, parseInt(d.maxLen, 10) || 500));
    settings.badWords = (Array.isArray(d.badWords) ? d.badWords : []).map((w) => String(w).trim().toLowerCase()).filter(Boolean).slice(0, 200);
  }
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(snapshot()));
});

const wss = new WebSocketServer({
  server, maxPayload: 64 * 1024,
  verifyClient: (i) => !ORIGIN || i.origin === ORIGIN || (i.origin || '').startsWith('http://localhost'),
});

wss.on('connection', async (ws, req) => {
  const q = new URL(req.url, 'http://x').searchParams;
  const roomName = (q.get('room') || 'general').trim().slice(0, 40) || 'general';
  let name = (q.get('name') || 'ناشناس').trim().slice(0, 30) || 'ناشناس';
  const meet = q.get('mode') === 'meet';
  const key = (meet ? 'meet:' : '') + roomName;
  if (pool) {
    let row = null;
    try {
      row = (await pool.query('select u.username, u.banned from sessions s join users u on u.id = s.user_id where s.token = $1', [String(q.get('token') || '')])).rows[0];
    } catch (e) { console.error('auth error', e.message); }
    if (!row || row.banned) { send(ws, { type: 'denied', reason: 'auth' }); return ws.close(); }
    name = row.username;
    if (ws.readyState !== 1) return;
  }
  ws.userName = name;
  ws.pid = ++pidSeq & 0xffff;
  const deny = (reason) => { send(ws, { type: 'denied', reason }); ws.close(); };

  if (banned.has(name)) { send(ws, { type: 'kicked' }); return ws.close(); }
  let room = rooms.get(key);
  if (!room) rooms.set(key, (room = newRoom(key)));
  if (room.locked) return deny('locked');
  if (room.password && q.get('pw') !== room.password) return deny('password');
  if (settings.maxUsers && room.clients.size >= settings.maxUsers) return deny('full');

  room.clients.add(ws);
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
  send(ws, { type: 'history', msgs: room.msgs });
  send(ws, room.talker ? { type: 'talking', name: room.talker.userName, video: room.talker.video } : { type: 'idle' });
  presence(room);
  if (meet) { send(ws, { type: 'you', id: ws.pid }); peersMsg(room); }

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      if (muted.has(name)) return;
      room.bytes += data.length;
      if (meet) { // تماس تصویری: همه با شناسه‌ی فرستنده پخش می‌شوند
        const out = Buffer.concat([Buffer.from([ws.pid >> 8, ws.pid & 255]), data]);
        for (const c of room.clients) if (c !== ws && c.readyState === 1 && c.bufferedAmount < (data[0] === 2 ? 65536 : 524288)) c.send(out, { binary: true });
        return;
      }
      if (room.talker !== ws) return;
      for (const c of room.clients) if (c !== ws && c.readyState === 1 && c.bufferedAmount < (data[0] === 2 ? 65536 : 524288)) c.send(data, { binary: true });
      return;
    }
    let m; try { m = JSON.parse(data.toString()); } catch { return; }

    if (m.type === 'start') {
      if (muted.has(name)) return send(ws, { type: 'system', text: 'شما بی‌صدا شده‌اید و نمی‌توانید صحبت کنید.' });
      if (room.talker && room.talker !== ws) return send(ws, { type: 'busy', name: room.talker.userName });
      room.talker = ws; ws.video = !!m.video;
      bc(room, { type: 'talking', name, video: ws.video }, ws);
    } else if (m.type === 'stop') release(room, ws);
    else if (m.type === 'chat' && typeof m.text === 'string' && m.text.trim()) {
      if (muted.has(name)) return send(ws, { type: 'system', text: 'شما بی‌صدا شده‌اید و نمی‌توانید پیام بفرستید.' });
      const text = m.text.trim().slice(0, settings.maxLen);
      const low = text.toLowerCase();
      if (settings.badWords.some((w) => low.includes(w))) return send(ws, { type: 'system', text: 'پیام شما به دلیل کلمات ممنوعه ارسال نشد.' });
      const msg = { id: ++mid, name, text, t: Date.now() };
      totalMsgs++;
      room.msgs.push(msg);
      if (room.msgs.length > 100) room.msgs.shift();
      bc(room, { type: 'chat', ...msg });
    }
  });

  ws.on('close', () => {
    release(room, ws);
    room.clients.delete(ws);
    if (!room.clients.size && !room.msgs.length && !room.persist) {
      if (rooms.get(room.key) === room) rooms.delete(room.key);
    } else { presence(room); if (meet) peersMsg(room); }
  });
  ws.on('error', () => {});
});

setInterval(() => {
  for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; ws.ping(); }
}, 25000);

server.listen(PORT, () => console.log('listening on', PORT));
