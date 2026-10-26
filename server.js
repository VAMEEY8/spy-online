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
  return { t: 'state', code: r.code, you: me, host: r.host, phase: r.phase, minutes: r.minutes, spies: r.spies, hint: r.hint, lost: !!r.lost, game: r.game, g: r.phase === 'g' && r.g ? G[r.game].view(r, r.players.get(me)) : null,
    count: r.items.length, custom: !!r.custom, src: r.src || '', photos: r.items.filter(x => x.img).length, now: Date.now(), endAt: r.endAt || 0, result: r.result || null,
    caught: r.phase === 'guess' ? r.caught : null,
    players: [...r.players.values()].map(p => ({ id: p.id, nick: p.nick, ava: p.ava, score: p.score, on: !!p.ws, voice: p.voice, voted: p.id in r.votes, ready: !!p.ready })) };
}
const push = r => r.players.forEach(p => send(p.ws, state(r, p.id)));

function join(ws, m) {
  let r = m.room && rooms.get(clean(m.room, 4).toUpperCase()), created = false;
  if (m.room && !r) return send(ws, { t: 'err', m: 'Рум табылмады' });
  if (!r) {
    r = { code: newCode(), host: null, players: new Map(), items: [], minutes: 5, spies: 1, hint: false, lost: false, game: 'spy', pw: clean(m.pw, 20), phase: 'lobby', votes: {} };
    rooms.set(r.code, r); created = true;
  }
  clearTimeout(r.del);
  let p = m.id && r.players.get(m.id);
  if (r.pw && !p && clean(m.pw, 20) !== r.pw) return send(ws, { t: 'err', m: 'Құпиясөз қате' });
  const cid = clean(m.cid, 40);
  if (!p && cid) p = [...r.players.values()].find(x => x.cid === cid);  // бір құрылғы екі рет кірмейді
  if (p) { p.ws = ws; clearTimeout(p.rm); }
  else {
    if (r.players.size >= 12) return send(ws, { t: 'err', m: 'Рум толы' });
    if (r.phase !== 'lobby') return send(ws, { t: 'err', m: 'Ойын басталып кетті' });
    const ava = String(m.ava || '🙂');
    p = { id: Math.random().toString(36).slice(2, 10), nick: clean(m.nick, 14) || 'Ойыншы', ava: ava.length > 9000 ? '🙂' : ava, score: 0, ws, voice: false, ready: false, cid };
    r.players.set(p.id, p);
  }
  if (!r.host || !r.players.has(r.host)) r.host = p.id;
  ws.pid = p.id; ws.room = r.code;
  send(ws, { t: 'joined', id: p.id, code: r.code, created });
  if (p.card) send(ws, p.card);
  if (r.phase === 'g' && r.game === 'draw' && r.g) send(ws, { t: 'strokes', list: r.g.strokes });
  if (r.phase === 'g' && r.imgMap) send(ws, { t: 'imgs', map: r.imgMap });
  push(r);
}

const fixHost = r => { const h = r.players.get(r.host); if (!h || !h.ws) { const n = active(r)[0]; if (n) r.host = n.id; } };
function idle(r) {
  if (active(r).length) return;
  clearTimeout(r.del);
  r.del = setTimeout(() => { clearTimeout(r.timer); rooms.delete(r.code); }, 6e5);
}
// Байланыс үзілсе, ойыншы бірден өшірілмейді: 20 сек ішінде қайта кіре алады (файл таңдау, қолданба ауысу)
function leave(r, p, force) {
  if (force) { r.players.delete(p.id); fixHost(r); }
  else p.rm = setTimeout(() => {
    if (p.ws || r.players.get(p.id) !== p) return;
    if (r.phase === 'lobby') r.players.delete(p.id);
    fixHost(r); push(r); idle(r);
  }, 20000);
  push(r); idle(r);
}

function start(r) {
  const ps = [...r.players.values()].filter(p => p.ws);
  if (ps.length < 3) return;
  if (!ps.every(p => p.ready || p.id === r.host)) return send(r.players.get(r.host).ws, { t: 'msg', m: 'Барлығы дайын емес' });
  const pool = r.items.length ? r.items : DEF, e = pool[rnd(pool.length)];
  const k = Math.max(1, Math.min(r.spies, Math.floor((ps.length - 1) / 2)));
  const order = shuffle(ps.map(p => p.id));
  r.pool = pool; r.entry = e; r.spyIds = order.slice(0, k);
  r.lostId = r.lost && ps.length >= 4 ? order[k] : null;  // «Адасқан»: басқа орынды алған қарапайым ойыншы
  const alt0 = pool.filter(x => x.game === e.game && x.place !== e.place), alt = alt0.length ? alt0 : pool.filter(x => x.place !== e.place);
  const dp = r.lostId && alt.length ? alt[rnd(alt.length)] : e; r.lostPlace = r.lostId ? dp.place : '';
  const others = shuffle([...new Set(pool.map(x => x.place))].filter(x => x !== e.place)).slice(0, 7);
  const opts = shuffle([e.place, ...others]), roles = shuffle(e.roles);
  let i = 0;
  r.players.forEach(p => {
    const spy = r.spyIds.includes(p.id);
    p.card = { t: 'role', spy, hasRoles: e.roles.length > 0, game: spy && !r.hint ? '' : e.game, place: spy ? '' : (p.id === r.lostId ? dp.place : e.place), img: spy ? '' : ((p.id === r.lostId ? dp.img : e.img) || ''),
      role: spy || !roles.length ? '' : roles[i++ % roles.length], opts: spy ? opts : null };
    send(p.ws, p.card);
  });
  r.players.forEach(p => p.ready = false);
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
  r.result = { win, why, spies: r.spyIds, game: r.entry.game, place: r.entry.place, lost: r.lostId, lostPlace: r.lostPlace };
  push(r);
}

// ===================== қосымша ойындар =====================
const GAMES = ['spy', 'mafia', 'who', 'code', 'draw', 'truth'];
const EXTRA = 'Домбыра,Киіз үй,Бәйге,Шай,Қымыз,Бауырсақ,Алматы,Астана,Ұшақ,Телефон,Қарындаш,Тоңазытқыш,Футбол,Мектеп,Кітап,Дәрігер,Мұғалім,Көпір,Өзен,Ай,Күн,Жұлдыз,Қар,Жаңбыр,Ат,Түйе,Қасқыр,Бүркіт,Балық,Орман,Шатыр,Сағат,Айна,Кілем,Шәйнек,Құлып,Қолшатыр,Велосипед,Пойыз,Кеме,Ракета,Банк,Базар,Цирк,Театр,Аурухана,Тау,Көл,Жол,Қала'.split(',');
const CHARS = 'Абай,Алдар Көсе,Қорқыт ата,Шыңғыс хан,Әл-Фараби,Мұхтар Әуезов,Месси,Роналду,Супермен,Бэтмен,Золушка,Шрек,Микки Маус,Гарри Поттер,Наполеон,Эйнштейн,Ньютон,Колумб,Гагарин,Моцарт,Леонардо да Винчи,Пушкин,Шекспир,Тарзан,Робин Гуд,Спанч Боб,Пикачу,Марио,Человек-паук,Железный человек'.split(',');
const RN = { mafia: 'Мафия', doc: 'Дәрігер', com: 'Комиссар', civ: 'Тұрғын' };
const norm = s => String(s).toLowerCase().replace(/\(.*?\)/g, '').replace(/[^\p{L}\p{N}]/gu, '');
const hit = (g, w) => { const a = norm(g); return !!a && [w, ...(w.match(/\((.*?)\)/g) || []).map(x => x.slice(1, -1))].some(x => norm(x) === a); };
const pool = r => { const w = [...new Set((r.items.length ? r.items : DEF).map(x => x.place))]; return shuffle(w.length >= 30 ? w : [...new Set([...w, ...EXTRA])]); };
const tm = (r, s, fn) => { clearTimeout(r.timer); r.g.endAt = Date.now() + s * 1000; r.timer = setTimeout(fn, s * 1000); };
const nick = (r, id) => (r.players.get(id) || {}).nick || '?';
const pts = (r, ids, n) => ids.forEach(id => { const p = r.players.get(id); if (p) p.score += n; });
const over = (r, o) => { clearTimeout(r.timer); r.g.over = o; r.g.endAt = 0; };
const G = {};

G.mafia = {
  min: 5,
  start(r) {
    const ps = shuffle(active(r).map(p => p.id)), m = ps.length >= 10 ? 3 : ps.length >= 7 ? 2 : 1, role = {};
    ps.forEach((id, i) => role[id] = i < m ? 'mafia' : i === m ? 'doc' : i === m + 1 ? 'com' : 'civ');
    r.g = { role, alive: ps.slice(), day: 0, ph: 'night', acts: {}, votes: {}, checks: [], log: ['Ойын басталды. Түн түсті.'], over: null };
    G.mafia.night(r);
  },
  night(r) { const g = r.g; g.ph = 'night'; g.day++; g.acts = {}; tm(r, 75, () => { G.mafia.dawn(r); push(r); }); },
  dawn(r) {
    const g = r.g, ent = Object.entries(g.acts), by = k => ent.filter(([id]) => g.role[id] === k).map(([, t]) => t);
    const picks = by('mafia'), cnt = t => picks.filter(x => x === t).length;
    const victim = picks.sort((a, b) => cnt(b) - cnt(a))[0], saved = by('doc')[0], chk = ent.find(([id]) => g.role[id] === 'com');
    if (chk) g.checks.push({ target: chk[1], mafia: g.role[chk[1]] === 'mafia' });
    if (victim && victim !== saved) { g.alive = g.alive.filter(x => x !== victim); g.log.push(`${g.day}-түн: ${nick(r, victim)} өлтірілді`); }
    else g.log.push(`${g.day}-түн: ешкім өлген жоқ`);
    if (!G.mafia.win(r)) { g.ph = 'day'; tm(r, 180, () => { G.mafia.toVote(r); push(r); }); }
  },
  toVote(r) { r.g.ph = 'vote'; r.g.votes = {}; tm(r, 60, () => { G.mafia.tally(r); push(r); }); },
  tally(r) {
    const g = r.g, c = {}; Object.values(g.votes).forEach(t => c[t] = (c[t] || 0) + 1);
    const mx = Math.max(0, ...Object.values(c)), top = Object.keys(c).filter(t => c[t] === mx);
    if (top.length === 1) { g.alive = g.alive.filter(x => x !== top[0]); g.log.push(`Күндіз: ${nick(r, top[0])} шығарылды (${RN[g.role[top[0]]]})`); }
    else g.log.push('Күндіз: дауыс тең, ешкім шығарылған жоқ');
    if (!G.mafia.win(r)) G.mafia.night(r);
  },
  win(r) {
    const g = r.g, m = g.alive.filter(id => g.role[id] === 'mafia').length, o = g.alive.length - m, w = !m ? 'civ' : m >= o ? 'mafia' : null;
    if (!w) return false;
    over(r, { win: w }); pts(r, Object.keys(g.role).filter(id => (g.role[id] === 'mafia') === (w === 'mafia')), 3); return true;
  },
  act(r, me, m) {
    const g = r.g, role = g.role[me.id], alive = g.alive.includes(me.id), t = m.target, nr = ['mafia', 'doc', 'com'];
    if (g.over) return false;
    if (m.a === 'pick' && g.ph === 'night' && alive && nr.includes(role) && g.alive.includes(t) && !g.acts[me.id]) {
      g.acts[me.id] = t;
      if (g.alive.filter(id => nr.includes(g.role[id])).every(id => g.acts[id])) G.mafia.dawn(r);
    } else if (m.a === 'vote' && g.ph === 'vote' && alive && g.alive.includes(t) && t !== me.id) {
      g.votes[me.id] = t;
      if (g.alive.filter(id => (r.players.get(id) || {}).ws).every(id => g.votes[id])) G.mafia.tally(r);
    } else if (m.a === 'force' && r.host === me.id) {
      if (g.ph === 'night') G.mafia.dawn(r); else if (g.ph === 'day') G.mafia.toVote(r); else G.mafia.tally(r);
    } else return false;
  },
  view(r, me) {
    const g = r.g, mine = g.role[me.id];
    return { ph: g.ph, day: g.day, endAt: g.endAt, role: mine, alive: g.alive, log: g.log.slice(-8), over: g.over, roles: g.over ? g.role : null,
      mates: mine === 'mafia' ? Object.keys(g.role).filter(id => g.role[id] === 'mafia') : null, acted: !!g.acts[me.id],
      checks: mine === 'com' ? g.checks : null, voted: g.votes[me.id] || null, nVoted: Object.keys(g.votes).length };
  },
};

G.who = {
  min: 2,
  start(r) {
    const own = [...new Set(r.items.map(x => x.place))], ids = active(r).map(p => p.id), src = r.custom ? (own.length >= ids.length ? shuffle(own) : pool(r)) : shuffle(CHARS), word = {};
    ids.forEach((id, i) => word[id] = src[i % src.length]);
    r.g = { word, ids, found: [], over: null };
    tm(r, 600, () => { over(r, { done: 1 }); push(r); });
  },
  act(r, me, m) {
    const g = r.g, w = g.word[me.id];
    if (g.over) return false;
    if (m.a === 'guess' && w && !g.found.includes(me.id)) {
      if (hit(m.text, w)) {
        pts(r, [me.id], Math.max(1, 4 - g.found.length)); g.found.push(me.id);
        broadcast(r, { t: 'chat', from: me.id, nick: me.nick, text: '✓ ' + w });
        if (g.found.length === g.ids.length) over(r, { done: 1 });
      } else broadcast(r, { t: 'chat', from: me.id, nick: me.nick, text: '✗ ' + clean(m.text, 40) });
    } else if (m.a === 'force' && r.host === me.id) over(r, { done: 1 }); else return false;
  },
  view(r, me) {
    const g = r.g;
    return { endAt: g.endAt, over: g.over, players: g.ids.map(id => ({ id, found: g.found.includes(id), word: id === me.id && !g.found.includes(id) && !g.over ? null : g.word[id] })) };
  },
};

G.code = {
  min: 4,
  start(r) {
    const ps = shuffle(active(r).map(p => p.id)), team = {}; ps.forEach((id, i) => team[id] = i % 2 ? 'blue' : 'red');
    const first = rnd(2) ? 'red' : 'blue', other = first === 'red' ? 'blue' : 'red';
    r.g = { team, caps: { red: ps.find(id => team[id] === 'red'), blue: ps.find(id => team[id] === 'blue') }, words: pool(r).slice(0, 25),
      key: shuffle([...Array(9).fill(first), ...Array(8).fill(other), ...Array(7).fill('n'), 'x']), shown: Array(25).fill(false), turn: first, clue: null, left: 0, over: null };
  },
  swap(g) { g.turn = g.turn === 'red' ? 'blue' : 'red'; g.clue = null; g.left = 0; },
  end(r, win) { over(r, { win }); pts(r, Object.keys(r.g.team).filter(id => r.g.team[id] === win), 3); },
  rem(g, c) { return g.key.filter((k, i) => k === c && !g.shown[i]).length; },
  act(r, me, m) {
    const g = r.g, t = g.team[me.id], cap = g.caps[t] === me.id;
    if (g.over) return false;
    if (m.a === 'force' && r.host === me.id) return over(r, { win: null });
    if (t !== g.turn) return false;
    if (m.a === 'clue' && cap && !g.clue) { const n = Math.max(0, Math.min(9, m.n | 0)); g.clue = { w: clean(m.w, 30) || '?', n }; g.left = n + 1; }
    else if (m.a === 'pick' && !cap && g.clue && g.shown[m.i] === false) {
      g.shown[m.i] = true; const c = g.key[m.i];
      if (c === 'x') return G.code.end(r, t === 'red' ? 'blue' : 'red');
      if (!G.code.rem(g, 'red')) return G.code.end(r, 'red');
      if (!G.code.rem(g, 'blue')) return G.code.end(r, 'blue');
      if (c === t && --g.left > 0) return;
      G.code.swap(g);
    } else if (m.a === 'end' && !cap && g.clue) G.code.swap(g); else return false;
  },
  view(r, me) {
    const g = r.g, t = g.team[me.id], cap = g.caps[t] === me.id, show = cap || g.over;
    return { cells: g.words.map((w, i) => ({ w, s: g.shown[i], c: g.shown[i] || show ? g.key[i] : null })), team: g.team, caps: g.caps, turn: g.turn, clue: g.clue, left: g.left,
      mine: t, cap, rem: { red: G.code.rem(g, 'red'), blue: G.code.rem(g, 'blue') }, over: g.over };
  },
};

G.draw = {
  min: 3,
  start(r) { r.g = { order: shuffle(active(r).map(p => p.id)), i: -1, strokes: [], got: [], reveal: false, over: null, words: pool(r) }; G.draw.next(r); },
  next(r) {
    const g = r.g; g.i++;
    if (g.i >= g.order.length) return over(r, { done: 1 });
    g.drawer = g.order[g.i]; g.word = g.words[g.i % g.words.length]; g.strokes = []; g.got = []; g.reveal = false;
    broadcast(r, { t: 'clear' }); tm(r, 80, () => { G.draw.endRound(r); push(r); });
  },
  endRound(r) {
    const g = r.g; if (g.reveal || g.over) return;
    g.reveal = true; clearTimeout(r.timer); g.endAt = 0;
    setTimeout(() => { if (r.g === g && !g.over) { G.draw.next(r); push(r); } }, 4000);
  },
  chat(r, me, text) {
    const g = r.g;
    if (g.over || g.reveal || me.id === g.drawer || g.got.includes(me.id) || !hit(text, g.word)) return false;
    g.got.push(me.id); pts(r, [me.id], 2); pts(r, [g.drawer], 1);
    broadcast(r, { t: 'chat', from: me.id, nick: me.nick, text: '✓ тапты!' });
    if (active(r).filter(p => p.id !== g.drawer).every(p => g.got.includes(p.id))) G.draw.endRound(r);
    push(r); return true;
  },
  act(r, me, m) {
    const g = r.g;
    if (g.over) return false;
    if (m.a === 'force' && r.host === me.id) return over(r, { done: 1 });
    if (me.id !== g.drawer || g.reveal) return false;
    if (m.a === 'stroke' && Array.isArray(m.s) && g.strokes.length < 6000) {
      const s = m.s.slice(0, 80).map(a => a.slice(0, 4).map(Number)), c = clean(m.c, 9);
      g.strokes.push(...s.map(x => [...x, c]));
      r.players.forEach(p => p.id !== me.id && send(p.ws, { t: 'stroke', s, c }));
    } else if (m.a === 'clear') { g.strokes = []; r.players.forEach(p => p.id !== me.id && send(p.ws, { t: 'clear' })); }
    return false;
  },
  view(r, me) {
    const g = r.g;
    return { drawer: g.drawer, word: me.id === g.drawer || g.reveal || g.over ? g.word : null, len: (g.word || '').length, endAt: g.endAt, got: g.got, reveal: g.reveal, round: g.i + 1, total: g.order.length, over: g.over };
  },
};

G.truth = {
  min: 3,
  start(r) { r.g = { order: shuffle(active(r).map(p => p.id)), i: 0, ph: 'write', st: null, lie: 0, votes: {}, over: null }; },
  reveal(r) {
    const g = r.g, v = Object.entries(g.votes); g.ph = 'reveal';
    pts(r, v.filter(([, i]) => i === g.lie).map(([id]) => id), 1); pts(r, [g.order[g.i]], v.filter(([, i]) => i !== g.lie).length);
  },
  act(r, me, m) {
    const g = r.g, tl = g.order[g.i], host = r.host === me.id;
    if (g.over) return false;
    if (m.a === 'submit' && g.ph === 'write' && me.id === tl) {
      const st = (Array.isArray(m.st) ? m.st : []).slice(0, 3).map(x => clean(x, 120));
      if (st.length < 3 || !st.every(Boolean)) return false;
      g.st = st; g.lie = Math.max(0, Math.min(2, m.lie | 0)); g.ph = 'vote'; g.votes = {};
    } else if (m.a === 'vote' && g.ph === 'vote' && me.id !== tl && g.order.includes(me.id)) {
      g.votes[me.id] = Math.max(0, Math.min(2, m.i | 0));
      if (active(r).filter(p => p.id !== tl && g.order.includes(p.id)).every(p => p.id in g.votes)) G.truth.reveal(r);
    } else if (m.a === 'force' && host && g.ph === 'vote') G.truth.reveal(r);
    else if (m.a === 'next' && host && g.ph !== 'vote') {
      g.i++;
      if (g.i >= g.order.length) over(r, { done: 1 }); else { g.ph = 'write'; g.st = null; g.votes = {}; }
    } else return false;
  },
  view(r, me) {
    const g = r.g, ph = g.ph;
    return { ph, teller: g.order[g.i], round: g.i + 1, total: g.order.length, st: ph !== 'write' ? g.st : null, lie: ph === 'reveal' ? g.lie : null,
      votes: ph === 'reveal' ? g.votes : null, voted: me.id in g.votes, nVoted: Object.keys(g.votes).length, over: g.over };
  },
};

function startG(r) {
  const gm = G[r.game], ps = active(r), hw = r.players.get(r.host).ws;
  if (ps.length < gm.min) return send(hw, { t: 'msg', m: 'Кемінде ' + gm.min + ' ойыншы керек' });
  if (!ps.every(p => p.ready || p.id === r.host)) return send(hw, { t: 'msg', m: 'Барлығы дайын емес' });
  r.players.forEach(p => p.ready = false); r.phase = 'g'; r.result = null; gm.start(r); sendImgs(r); push(r);
}
// Фото-карталар: тек осы ойында қолданылатын суреттер бір рет жіберіледі
function sendImgs(r) {
  const used = r.game === 'who' ? Object.values(r.g.word) : r.game === 'code' ? r.g.words : [], map = {};
  r.items.forEach(x => { if (x.img && used.includes(x.place)) map[x.place] = x.img; });
  r.imgMap = Object.keys(map).length ? map : null;
  if (r.imgMap) broadcast(r, { t: 'imgs', map });
}

function handle(r, me, m) {
  const host = r.host === me.id;
  switch (m.t) {
    case 'items':
      if (host && r.phase === 'lobby' && Array.isArray(m.items)) {
        const keep = r.items.filter(x => x.img); r.items = m.items.slice(0, 6000).map(x => ({ game: clean(x.game, 80), place: clean(x.place, 80),
          roles: Array.isArray(x.roles) ? x.roles.slice(0, 20).map(s => clean(s, 60)).filter(Boolean) : [] })).filter(x => x.place).concat(keep);
        r.custom = !!m.custom || keep.length > 0; r.src = clean(m.name, 60);
        push(r);
      } break;
    case 'set':
      if (host && r.phase === 'lobby') {
        r.minutes = Math.min(60, Math.max(1, m.minutes | 0 || 5)); r.spies = Math.min(3, Math.max(1, m.spies | 0 || 1)); r.hint = !!m.hint; r.lost = !!m.lost; r.game = GAMES.includes(m.game) ? m.game : 'spy'; push(r);
      } break;
    case 'start': if (host && r.phase === 'lobby') (r.game === 'spy' ? start(r) : startG(r)); break;
    case 'chat': {
      const text = clean(m.text, 200);
      if (!text || (r.phase === 'g' && r.g && G[r.game].chat && G[r.game].chat(r, me, text))) break;
      broadcast(r, { t: 'chat', from: me.id, nick: me.nick, text }); break;
    }
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
    case 'act': if (r.phase === 'g' && r.g && G[r.game].act(r, me, m) !== false) push(r); break;
    case 'photo': {
      const img = String(m.img || '');
      if (r.phase !== 'lobby' || !img.startsWith('data:image/') || img.length > 60000 || r.items.filter(x => x.img).length >= 60) break;
      if (!r.custom) r.items = [];  // әдепкі сөздер өз фотоларың келгенде алынады
      const base = clean(m.name, 30) || 'Сурет'; let name = base, n = 2;
      while (r.items.some(x => x.place === name)) name = base + ' ' + n++;
      r.items.push({ game: 'Фото', place: name, roles: [], img }); r.custom = true; push(r); break;
    }
    case 'photos_clear':
      if (host && r.phase === 'lobby') {
        r.items = r.items.filter(x => !x.img); r.custom = !!r.src;
        if (!r.items.length) { r.items = DEF.slice(); r.custom = false; }
        push(r);
      } break;
    case 'ready': if (r.phase === 'lobby') { me.ready = !!m.on; push(r); } break;
    case 'kick': {
      const k = host && r.phase === 'lobby' && m.id !== me.id && r.players.get(m.id);
      if (k) { send(k.ws, { t: 'err', m: 'Хост сізді румнан шығарды' }); if (k.ws) k.ws.room = null; r.players.delete(k.id); push(r); }
      break;
    }
    case 'skip': if (host && r.phase === 'guess') finish(r, 'civ', 'Шпион жауап бермеді'); break;
    case 'next':
      if (host && (r.phase === 'result' || r.phase === 'g')) { r.g = null; clearTimeout(r.timer); r.phase = 'lobby'; r.result = null; r.endAt = 0; r.players.forEach(p => { p.card = null; p.ready = false; }); push(r); }
      break;
    case 'leave': {
      const w = me.ws; me.ws = null; if (w) w.room = null; leave(r, me, true);
      if (r.spyIds && ['play', 'vote', 'guess'].includes(r.phase) && !r.spyIds.some(id => r.players.has(id))) finish(r, 'civ', 'Шпион ойыннан шықты');
      break;
    }
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
