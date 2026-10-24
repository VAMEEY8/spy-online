const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const FILES = { '/': 'index.html', '/manifest.webmanifest': 'manifest.webmanifest', '/sw.js': 'sw.js',
  '/icon-192.png': 'icon-192.png', '/icon-512.png': 'icon-512.png', '/.well-known/assetlinks.json': 'assetlinks.json' };
const TYPES = { html: 'text/html; charset=utf-8', webmanifest: 'application/manifest+json', js: 'text/javascript', png: 'image/png', json: 'application/json' };
const server = http.createServer((req, res) => {
  const f = FILES[req.url.split('?')[0]], p = f && path.join(__dirname, f);
  if (!p || !fs.existsSync(p)) { res.writeHead(404); return res.end('Not found'); }
  res.writeHead(200, { 'Content-Type': TYPES[f.split('.').pop()], 'Cache-Control': f === 'sw.js' ? 'no-cache' : 'public, max-age=300' });
  fs.createReadStream(p).pipe(res);
});
const wss = new WebSocketServer({ server, maxPayload: 3e6 });

const rooms = new Map();
const DEF = ['Мектеп', 'Аурухана', 'Ұшақ', 'Базар', 'Кітапхана', 'Теңіз', 'Цирк', 'Банк'].map(place => ({ game: '', place, roles: [] }));
const rnd = n => Math.random() * n | 0;
const shuffle = a => { a = a.slice(); for (let i = a.length - 1; i > 0; i--) { const j = rnd(i + 1); [a[i], a[j]] = [a[j], a[i]]; } return a; };
const send = (ws, o) => ws && ws.readyState === 1 && ws.send(JSON.stringify(o));
const clean = (s, n) => String(s || '').trim().slice(0, n);
const broadcast = (r, o) => r.players.forEach(p => send(p.ws, o));
const active = r => [...r.players.values()].filter(p => p.ws);
const newCode = () => { let c; do c = Array.from({ length: 4 }, () => 'ABCDEFGHJKLMNPQRSTUVWXYZ'[rnd(24)]).join(''); while (rooms.has(c)); return c; };

function state(r, me) {
  return { t: 'state', code: r.code, you: me, host: r.host, phase: r.phase, minutes: r.minutes, spies: r.spies, hint: r.hint,
    count: r.items.length, now: Date.now(), endAt: r.endAt || 0, result: r.result || null,
    caught: r.phase === 'guess' ? r.caught : null,
    players: [...r.players.values()].map(p => ({ id: p.id, nick: p.nick, ava: p.ava, score: p.score, on: !!p.ws, voice: p.voice, voted: p.id in r.votes })) };
}
const push = r => r.players.forEach(p => send(p.ws, state(r, p.id)));

function join(ws, m) {
  let r = m.room && rooms.get(clean(m.room, 4).toUpperCase()), created = false;
  if (m.room && !r) return send(ws, { t: 'err', m: 'Рум табылмады' });
  if (!r) {
    r = { code: newCode(), host: null, players: new Map(), items: [], minutes: 5, spies: 1, hint: false, phase: 'lobby', votes: {} };
    rooms.set(r.code, r); created = true;
  }
  clearTimeout(r.del);
  let p = m.id && r.players.get(m.id);
  if (p) p.ws = ws;
  else {
    if (r.players.size >= 12) return send(ws, { t: 'err', m: 'Рум толы' });
    if (r.phase !== 'lobby') return send(ws, { t: 'err', m: 'Ойын басталып кетті' });
    const ava = String(m.ava || '🙂');
    p = { id: Math.random().toString(36).slice(2, 10), nick: clean(m.nick, 14) || 'Ойыншы', ava: ava.length > 9000 ? '🙂' : ava, score: 0, ws, voice: false };
    r.players.set(p.id, p);
  }
  if (!r.host || !r.players.has(r.host)) r.host = p.id;
  ws.pid = p.id; ws.room = r.code;
  send(ws, { t: 'joined', id: p.id, code: r.code, created });
  if (p.card) send(ws, p.card);
  push(r);
}

function leave(r, p, force) {
  if (force || r.phase === 'lobby') r.players.delete(p.id);
  if (r.host === p.id) { const n = active(r)[0]; r.host = n ? n.id : null; }
  clearTimeout(r.del);
  if (!active(r).length) r.del = setTimeout(() => { clearTimeout(r.timer); rooms.delete(r.code); }, 6e5);
  else push(r);
}

function start(r) {
  const ps = [...r.players.values()].filter(p => p.ws);
  if (ps.length < 3) return;
  const pool = r.items.length ? r.items : DEF, e = pool[rnd(pool.length)];
  const k = Math.max(1, Math.min(r.spies, Math.floor((ps.length - 1) / 2)));
  r.pool = pool; r.entry = e; r.spyIds = shuffle(ps.map(p => p.id)).slice(0, k);
  const others = shuffle([...new Set(pool.map(x => x.place))].filter(x => x !== e.place)).slice(0, 7);
  const opts = shuffle([e.place, ...others]), roles = shuffle(e.roles);
  let i = 0;
  r.players.forEach(p => {
    const spy = r.spyIds.includes(p.id);
    p.card = { t: 'role', spy, hasRoles: e.roles.length > 0, game: spy && !r.hint ? '' : e.game, place: spy ? '' : e.place,
      role: spy || !roles.length ? '' : roles[i++ % roles.length], opts: spy ? opts : null };
    send(p.ws, p.card);
  });
  r.votes = {}; r.caught = null; r.result = null; r.phase = 'play'; r.endAt = Date.now() + r.minutes * 6e4;
  clearTimeout(r.timer); r.timer = setTimeout(() => r.phase === 'play' && toVote(r), r.minutes * 6e4);
  push(r);
}
function toVote(r) { clearTimeout(r.timer); r.phase = 'vote'; r.votes = {}; r.endAt = 0; push(r); }
function tally(r) {
  const c = {}; Object.values(r.votes).forEach(t => c[t] = (c[t] || 0) + 1);
  const max = Math.max(0, ...Object.values(c)), top = Object.keys(c).filter(t => c[t] === max);
  if (top.length === 1 && r.spyIds.includes(top[0])) { r.caught = top[0]; r.phase = 'guess'; push(r); }
  else finish(r, 'spy', top.length === 1 ? 'Қарапайым ойыншы шығарылды' : 'Дауыс тең болды немесе берілмеді');
}
function finish(r, win, why) {
  clearTimeout(r.timer); r.phase = 'result'; r.endAt = 0;
  r.players.forEach(p => { const spy = r.spyIds.includes(p.id); if (win === 'spy' && spy) p.score += 3; if (win === 'civ' && !spy) p.score += 2; });
  r.result = { win, why, spies: r.spyIds, game: r.entry.game, place: r.entry.place };
  push(r);
}

function handle(r, me, m) {
  const host = r.host === me.id;
  switch (m.t) {
    case 'items':
      if (host && r.phase === 'lobby' && Array.isArray(m.items)) {
        r.items = m.items.slice(0, 6000).map(x => ({ game: clean(x.game, 80), place: clean(x.place, 80),
          roles: Array.isArray(x.roles) ? x.roles.slice(0, 20).map(s => clean(s, 60)).filter(Boolean) : [] })).filter(x => x.place);
        push(r);
      } break;
    case 'set':
      if (host && r.phase === 'lobby') {
        r.minutes = Math.min(60, Math.max(1, m.minutes | 0 || 5)); r.spies = Math.min(3, Math.max(1, m.spies | 0 || 1)); r.hint = !!m.hint; push(r);
      } break;
    case 'start': if (host && r.phase === 'lobby') start(r); break;
    case 'chat': { const text = clean(m.text, 200); if (text) broadcast(r, { t: 'chat', from: me.id, nick: me.nick, text }); break; }
    case 'react': broadcast(r, { t: 'react', from: me.id, e: clean(m.e, 4) }); break;
    case 'voice': me.voice = !!m.on; push(r); break;
    case 'sig': { const to = r.players.get(m.to); if (to) send(to.ws, { t: 'sig', from: me.id, data: m.data }); break; }
    case 'vote_start': if (host && r.phase === 'play') toVote(r); break;
    case 'vote':
      if (r.phase === 'vote' && r.players.has(m.target) && m.target !== me.id) {
        r.votes[me.id] = m.target; push(r);
        if (active(r).every(p => r.votes[p.id])) tally(r);
      } break;
    case 'tally': if (host && r.phase === 'vote') tally(r); break;
    case 'guess':
      if (r.spyIds && r.spyIds.includes(me.id) && (r.phase === 'play' || (r.phase === 'guess' && r.caught === me.id))) {
        const ok = m.place === r.entry.place;
        finish(r, ok ? 'spy' : 'civ', ok ? 'Шпион орынды тапты' : 'Шпион орынды таба алмады');
      } break;
    case 'next':
      if (host && r.phase === 'result') { r.phase = 'lobby'; r.result = null; r.endAt = 0; r.players.forEach(p => p.card = null); push(r); }
      break;
    case 'leave': { const w = me.ws; me.ws = null; if (w) w.room = null; leave(r, me, true); break; }
  }
}

wss.on('connection', ws => {
  ws.alive = true; ws.on('pong', () => ws.alive = true);
  ws.on('message', d => {
    let m; try { m = JSON.parse(d); } catch { return; }
    if (m.t === 'join') return join(ws, m);
    const r = rooms.get(ws.room), me = r && r.players.get(ws.pid);
    if (me) handle(r, me, m);
  });
  ws.on('close', () => {
    const r = rooms.get(ws.room), p = r && r.players.get(ws.pid);
    if (!p || p.ws !== ws) return;
    p.ws = null; p.voice = false; leave(r, p, false);
  });
});
setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); }), 25000);
server.listen(process.env.PORT || 3000, () => console.log('Spy server on', process.env.PORT || 3000));
