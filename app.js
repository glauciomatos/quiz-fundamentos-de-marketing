/* Quiz Chão de Fábrica — jogo ao vivo
 * Sem "?sala=" na URL: tela do professor (projetor).
 * Com "?sala=CODIGO": tela do aluno (celular).
 * Comunicação em tempo real por MQTT sobre WebSocket seguro, num servidor público gratuito.
 * O professor (navegador do projetor) é quem guarda o jogo e calcula as pontuações. */

const params = new URLSearchParams(location.search);
// Servidores públicos gratuitos. Os de porta 443 vêm primeiro: redes de escola costumam bloquear as outras portas.
const BROKERS = [
  {url: "wss://mqtt.eclipseprojects.io:443/mqtt"},
  {url: "wss://public.cloud.shiftr.io:443", username: "public", password: "public"},
  {url: "wss://broker.emqx.io:8084/mqtt"},
  {url: "wss://broker.hivemq.com:8884/mqtt"},
  {url: "wss://test.mosquitto.org:8081/mqtt"}
];
const BROKER_OVERRIDE = params.get("broker"); // só para testes
/* Textos e identidade de cada sala. Uma página pode definir window.QUIZ antes de carregar este arquivo. */
const T = Object.assign({
  key: "qcf",
  topic: "quiz-chao-de-fabrica/v1/",
  name: "Quiz Chão de Fábrica",
  eyebrow: "Fundamentos dos Processos de Produção · Revisão ao vivo",
  lobbyTitle: "Aguardando a equipe do turno",
  joinTitle: "Bater o ponto e entrar no turno",
  endEyebrow: "Fim do turno",
  champion: "Campeão do turno!",
  logo: null
}, window.QUIZ || {});
const TOPIC_ROOT = T.topic;
const $app = document.getElementById("app");

const esc = s => String(s ?? "").replace(/[&<>"']/g, m => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[m]));
const store = {
  get(k){ try{ return JSON.parse(localStorage.getItem(k)); }catch(e){ return null; } },
  set(k,v){ try{ localStorage.setItem(k, JSON.stringify(v)); }catch(e){} },
  del(k){ try{ localStorage.removeItem(k); }catch(e){} }
};
const rid = n => Array.from(crypto.getRandomValues(new Uint8Array(n)), b => "abcdefghijkmnpqrstuvwxyz23456789"[b % 32]).join("");
const fmt = n => Number(n || 0).toLocaleString("pt-BR");
const GEAR_SVG = '<svg width="32" height="32" viewBox="0 0 24 24" aria-hidden="true"><path fill="#ffc61a" d="M10.3 2h3.4l.5 2.6 1.9.8 2.2-1.5 2.4 2.4-1.5 2.2.8 1.9 2.6.5v3.4l-2.6.5-.8 1.9 1.5 2.2-2.4 2.4-2.2-1.5-1.9.8-.5 2.6h-3.4l-.5-2.6-1.9-.8-2.2 1.5-2.4-2.4 1.5-2.2-.8-1.9L2 13.7v-3.4l2.6-.5.8-1.9-1.5-2.2 2.4-2.4 2.2 1.5 1.9-.8zM12 8.5a3.5 3.5 0 1 0 0 7 3.5 3.5 0 0 0 0-7z"/></svg>';
const GEAR = T.logo || GEAR_SVG;

function toast(msg){
  const host = document.getElementById("toastHost");
  host.innerHTML = `<div class="toast" role="status">${esc(msg)}</div>`;
  clearTimeout(toast.t); toast.t = setTimeout(()=>host.innerHTML="", 2600);
}

/* ---------- som ---------- */
let audioCtx = null;
function beep(freq, dur=.12, type="square", vol=.05, when=0){
  try{
    audioCtx = audioCtx || new (window.AudioContext || window.webkitAudioContext)();
    const t = audioCtx.currentTime + when, o = audioCtx.createOscillator(), g = audioCtx.createGain();
    o.type = type; o.frequency.value = freq; g.gain.setValueAtTime(vol, t); g.gain.exponentialRampToValueAtTime(.0001, t + dur);
    o.connect(g).connect(audioCtx.destination); o.start(t); o.stop(t + dur);
  }catch(e){}
}
const sfx = {
  join(){ beep(880,.07,"square",.03); },
  reveal(){ beep(660,.1); beep(990,.18,"square",.05,.1); },
  tick(){ beep(1200,.03,"square",.02); },
  end(){ [523,659,784,1046].forEach((f,i)=>beep(f,.18,"square",.05,i*.14)); },
  ok(){ beep(660,.1); beep(990,.18,"square",.05,.1); },
  bad(){ beep(220,.25,"sawtooth",.04); }
};

/* ---------- conexão ---------- */
function connectTo(broker, timeoutMs){
  const cfg = typeof broker === "string" ? {url: broker} : broker;
  return new Promise((resolve, reject) => {
    const opts = {clientId: "qcf_" + rid(10), clean: true, keepalive: 30, connectTimeout: timeoutMs, reconnectPeriod: 0, protocolVersion: 4};
    if(cfg.username){ opts.username = cfg.username; opts.password = cfg.password; }
    const c = mqtt.connect(cfg.url, opts);
    let done = false;
    const fail = err => { if(done) return; done = true; clearTimeout(t); try{ c.end(true); }catch(e){} reject(err || new Error("falhou")); };
    const t = setTimeout(() => fail(new Error("timeout")), timeoutMs + 500);
    c.once("connect", () => { if(done){ try{ c.end(true); }catch(e){} return; } done = true; clearTimeout(t); c.options.reconnectPeriod = 2000; resolve(c); });
    c.once("error", fail);
    c.once("close", () => fail(new Error("fechou")));
  });
}
/* Tenta todos os servidores ao mesmo tempo. Depois do primeiro sucesso espera um instante
   e fica com o preferido (o já usado pela sala) ou o primeiro da lista que conectou. */
function connectAny(preferred){
  if(BROKER_OVERRIDE) return connectTo(BROKER_OVERRIDE, 6000).then(client => ({client, b: 0}));
  return new Promise((resolve, reject) => {
    const ok = {}; let pending = BROKERS.length, chosen = false, timer = null;
    const choose = () => {
      if(chosen) return; chosen = true;
      const keys = Object.keys(ok).map(Number).sort((x,y) => x - y);
      if(!keys.length){ reject(new Error("sem conexão")); return; }
      const b = keys.includes(preferred) ? preferred : keys[0];
      keys.forEach(k => { if(k !== b) try{ ok[k].end(true); }catch(e){} });
      resolve({client: ok[b], b});
    };
    BROKERS.forEach((cfg, i) => {
      connectTo(cfg, 9000).then(c => {
        if(chosen){ try{ c.end(true); }catch(e){} return; }
        ok[i] = c;
        if(i === preferred || i === 0) choose();
        else if(!timer) timer = setTimeout(choose, 1500);
      }).catch(() => {}).finally(() => { if(--pending === 0) choose(); });
    });
  });
}
function questionFor(i){ return QUESTIONS[i]; }

/* =====================================================================
   PROFESSOR
   ===================================================================== */
const HOST_KEY = T.key + "-host-v1";
let H = null, hc = null, hostConnected = false, lastSecond = null, lastStateSent = 0;

function newGameId(){ return rid(6); }
function freshHost(){ /* estado salvo no navegador do professor */
  return { room: String(10000 + (crypto.getRandomValues(new Uint32Array(1))[0] % 90000)), b: 0,
    game: newGameId(), phase: "lobby", scope: 0, limit: 20, qlist: [], qi: -1, qStart: 0,
    players: {}, kicked: [], answers: {}, seq: 0 };
}
function saveHost(){ store.set(HOST_KEY, H); }
function topic(name){ return TOPIC_ROOT + H.room + "/" + name; }
function curQ(){ return H.qi >= 0 && H.qi < H.qlist.length ? questionFor(H.qlist[H.qi]) : null; }
function deadline(){ return H.qStart + H.limit * 1000; }
function joinUrl(){
  const u = new URL(location.origin + location.pathname);
  u.searchParams.set("sala", H.room);
  if(BROKER_OVERRIDE) u.searchParams.set("broker", BROKER_OVERRIDE); else u.searchParams.set("b", H.b);
  return u.toString();
}
function ranking(){
  return Object.entries(H.players)
    .filter(([id]) => !H.kicked.includes(id))
    .map(([id,p]) => ({id, ...p}))
    .sort((a,b) => b.score - a.score || b.correct - a.correct || a.name.localeCompare(b.name, "pt-BR"));
}
function activeIds(){ return Object.keys(H.players).filter(id => !H.kicked.includes(id)); }

function pub(name, obj, retain){
  if(!hc || !hostConnected) return;
  hc.publish(topic(name), JSON.stringify(obj), {qos: 1, retain: !!retain});
}
function publishState(){
  const q = curQ();
  const msg = {seq: ++H.seq, game: H.game, phase: H.phase, qi: H.qi, total: H.qlist.length};
  if(q && H.phase !== "lobby" && H.phase !== "end"){
    msg.text = q.q; msg.opts = q.o; msg.tf = !!q.tf; msg.aula = q.a; msg.al = AULAS[q.a].split("·")[0].trim();
    if(H.phase === "question"){ msg.rem = Math.max(0, deadline() - Date.now()); msg.lim = H.limit; }
    else msg.correct = q.c;
  }
  lastStateSent = Date.now();
  pub("state", msg, true);
}
function publishScores(){
  const r = ranking(), players = {};
  r.forEach((p,i) => { players[p.id] = {n: p.name, s: p.score, r: i+1, c: p.correct, lq: p.lq ?? -1, lo: p.lo ? 1 : 0, lp: p.lp || 0}; });
  pub("scores", {game: H.game, qi: H.qi, count: r.length, players, out: H.kicked}, true);
}

async function startHost(){
  H = store.get(HOST_KEY) || freshHost();
  H.connFailed = false;
  if(!H.game) H.game = newGameId();
  saveHost();
  document.title = T.name + " · Professor";
  renderHost();
  try{
    const {client, b} = await connectAny(H.b);
    hc = client;
    if(H.b !== b){ H.b = b; saveHost(); }
    hostConnected = true; H.connFailed = false;
    wireHostClient();
    onHostConnect();
  }catch(e){
    hostConnected = false; H.connFailed = true; renderHost();
  }
  setInterval(hostTick, 250);
}
function wireHostClient(){
  hc.on("connect", () => { hostConnected = true; onHostConnect(); });
  hc.on("close", () => { if(hostConnected){ hostConnected = false; updateNet(); } });
  hc.on("message", (t, buf) => {
    let m; try{ m = JSON.parse(buf.toString()); }catch(e){ return; }
    if(!m || typeof m !== "object") return;
    if(t === topic("join")) onJoin(m);
    else if(t === topic("ans")) onAnswer(m);
  });
}
function onHostConnect(){
  hc.subscribe([topic("join"), topic("ans")], {qos: 1});
  publishState(); publishScores(); renderHost();
}
function cleanName(s){
  return String(s || "").replace(/[\u0000-\u001f\u007f-\u009f​-‏‪-‮⁠-⁩]/g, "").replace(/\s+/g, " ").trim().slice(0, 18);
}
function onJoin(m){
  const id = String(m.id || "").slice(0, 24), name = cleanName(m.name);
  if(!/^[a-z0-9]{6,24}$/.test(id) || !name) return;
  if(H.kicked.includes(id)){ publishScores(); return; }
  const isNew = !H.players[id];
  if(!isNew && H.players[id].name === name){ publishScores(); return; }
  let final = name, k = 2;
  const taken = new Set(Object.entries(H.players).filter(([pid]) => pid !== id && !H.kicked.includes(pid)).map(([,p]) => p.name.toLowerCase()));
  while(taken.has(final.toLowerCase())) final = name.slice(0, 15) + " " + (k++);
  if(isNew){
    if(activeIds().length >= 150) return;
    H.players[id] = {name: final, score: 0, correct: 0, streak: 0, answered: 0, joinedAt: Date.now()};
    sfx.join();
  } else H.players[id].name = final;
  saveHost(); publishScores();
  if(H.phase === "lobby") renderNames(); else updateAnswered();
}
function onAnswer(m){
  if(H.phase !== "question" || m.game !== H.game || m.qi !== H.qi) return;
  const id = String(m.id || ""), c = m.c;
  if(!H.players[id] || H.kicked.includes(id) || H.answers[id]) return;
  const q = curQ(); if(!Number.isInteger(c) || c < 0 || c >= q.o.length) return;
  const t = Date.now() - H.qStart;
  if(t > H.limit * 1000 + 1500) return;
  H.answers[id] = {c, t: Math.min(t, H.limit * 1000)};
  saveHost(); updateAnswered();
  if(activeIds().every(pid => H.answers[pid])) setTimeout(() => { if(H.phase === "question") doReveal(); }, 600);
}
function hostTick(){
  if(H.phase !== "question") return;
  const left = deadline() - Date.now();
  const sec = Math.max(0, Math.ceil(left / 1000));
  const el = document.getElementById("hclock");
  if(el && sec !== lastSecond){
    lastSecond = sec; el.textContent = sec; el.classList.toggle("low", sec <= 5);
    if(sec <= 5 && sec > 0) sfx.tick();
  }
  if(left <= 0){ doReveal(); return; }
  if(Date.now() - lastStateSent > 2000) publishState();
}

/* --- ações do professor --- */
function startGame(){
  const scope = H.scope;
  H.qlist = QUESTIONS.map((q,i) => i).filter(i => !scope || QUESTIONS[i].a === scope);
  H.qi = -1; H.game = newGameId();
  Object.values(H.players).forEach(p => Object.assign(p, {score: 0, correct: 0, streak: 0, answered: 0, lq: -1, lo: 0, lp: 0, prevRank: null}));
  nextQuestion();
}
function nextQuestion(){
  if(H.qi + 1 >= H.qlist.length){ endGame(); return; }
  const before = ranking();
  before.forEach((p,i) => H.players[p.id].prevRank = i + 1);
  H.qi++; H.answers = {}; H.qStart = Date.now() + 400; H.phase = "question"; lastSecond = null;
  saveHost(); publishState(); publishScores(); renderHost();
}
function doReveal(){
  if(H.phase !== "question") return;
  const q = curQ();
  for(const id of activeIds()){
    const p = H.players[id], a = H.answers[id];
    p.lq = H.qi; p.lp = 0; p.lo = 0;
    if(a){ p.answered = (p.answered || 0) + 1; }
    if(a && a.c === q.c){
      p.streak = (p.streak || 0) + 1; p.correct++;
      const base = Math.round(1000 * (1 - a.t / (H.limit * 1000) / 2));
      const bonus = p.streak >= 2 ? Math.min(500, (p.streak - 1) * 100) : 0;
      p.lp = base + bonus; p.lo = 1; p.score += p.lp;
    } else p.streak = 0;
  }
  H.phase = "reveal"; sfx.reveal();
  saveHost(); publishState(); publishScores(); renderHost();
}
function showBoard(){ H.phase = "board"; saveHost(); publishState(); renderHost(); }
function endGame(){ H.phase = "end"; sfx.end(); saveHost(); publishState(); publishScores(); renderHost(); }
function backToLobby(){
  H.phase = "lobby"; H.qi = -1; H.qlist = []; H.answers = {}; H.game = newGameId();
  Object.values(H.players).forEach(p => Object.assign(p, {score: 0, correct: 0, streak: 0, answered: 0, lq: -1, lo: 0, lp: 0}));
  saveHost(); publishState(); publishScores(); renderHost();
}
function newRoom(){
  if(hc && hostConnected){ hc.publish(topic("state"), "", {qos:1, retain:true}); hc.publish(topic("scores"), "", {qos:1, retain:true}); hc.unsubscribe([topic("join"), topic("ans")]); }
  const b = H.b; H = freshHost(); H.b = b; saveHost();
  if(hc && hostConnected) onHostConnect(); else renderHost();
}
function kick(id){
  if(!H.kicked.includes(id)) H.kicked.push(id);
  saveHost(); publishScores(); renderHost();
}
function resultsCsv(){
  const rows = [["Posição","Nome","Pontos","Acertos","Respondidas","Questões"]];
  ranking().forEach((p,i) => rows.push([i+1, p.name, p.score, p.correct, p.answered || 0, H.qlist.length]));
  return rows.map(r => r.map(v => /[",;\n]/.test(String(v)) ? '"' + String(v).replace(/"/g,'""') + '"' : v).join(";")).join("\n");
}

/* --- telas do professor --- */
function hostTop(){
  return `<div class="topbar">
    <div class="brand">${GEAR}<span>${esc(T.name)}</span></div>
    <div class="row">
      <span class="chip">Sala <b>${esc(H.room)}</b></span>
      <span class="chip">Jogadores <b id="pcount">${activeIds().length}</b></span>
      <span class="chip net ${hostConnected?"on":""}" id="net">${hostConnected?"Online":"Conectando…"}</span>
    </div>
  </div><div class="hazard"></div>`;
}
function updateNet(){
  const n = document.getElementById("net"); if(!n) return;
  n.classList.toggle("on", hostConnected); n.textContent = hostConnected ? "Online" : "Reconectando…";
}
function renderHost(){
  const ph = H.phase;
  if(ph === "lobby") return renderLobby();
  if(ph === "question" || ph === "reveal") return renderHostQuestion();
  if(ph === "board") return renderBoard();
  if(ph === "end") return renderEnd();
}
function renderLobby(){
  const url = joinUrl();
  $app.className = "app";
  $app.innerHTML = hostTop() + `
  ${H.connFailed ? `<div class="explain" role="alert" style="border-color:var(--bad)"><b style="color:var(--bad)">Este computador não conseguiu se conectar ao servidor do jogo.</b><br>A rede pode estar bloqueando a conexão. Tente: (1) clicar em Tentar de novo; (2) conectar o computador a outra rede, como o roteador do seu celular. Os alunos podem continuar usando a rede deles.<div style="margin-top:10px"><button class="btn small" onclick="location.reload()">Tentar de novo</button></div></div>` : (hostConnected ? "" : `<div class="explain"><span class="dot"></span> Conectando ao servidor do jogo… Aguarde o indicador Online antes de mostrar o QR Code.</div>`)}
  <div class="lobby pop">
    <div class="joincard">
      <div class="step">Aponte a câmera do celular para o QR Code</div>
      <div id="qr" role="img" aria-label="QR Code para entrar na sala ${esc(H.room)}"></div>
      <div class="step" style="font-size:15px;color:#555">ou acesse</div>
      <div class="url">${esc(url)}</div>
      <div class="step" style="font-size:15px;color:#555;margin-top:4px">Sala</div>
      <div class="pin">${esc(H.room)}</div>
    </div>
    <div style="display:flex;flex-direction:column;gap:16px;min-width:0">
      <div>
        <span class="eyebrow">${esc(T.eyebrow)}</span>
        <h1 style="font-size:clamp(30px,4.4vw,52px);line-height:1;margin-top:8px">${esc(T.lobbyTitle)}</h1>
      </div>
      <div class="players">
        <div class="row"><span class="count" id="bigcount">${activeIds().length}</span><span class="muted">alunos na sala. Clique no × para remover um nome.</span></div>
        <div class="names" id="names"></div>
      </div>
      <div class="settings">
        <label for="scope">Conteúdo
          <select id="scope">
            <option value="0" ${H.scope===0?"selected":""}>Todas as aulas · ${QUESTIONS.length} questões</option>
            ${Object.keys(AULAS).map(Number).map(a => `<option value="${a}" ${H.scope===a?"selected":""}>Só ${esc(AULAS[a].split("·")[0].trim())} · ${QUESTIONS.filter(q=>q.a===a).length} questões</option>`).join("")}
          </select></label>
        <label for="limit">Tempo por questão
          <select id="limit">
            ${[10,15,20,30,45].map(s=>`<option value="${s}" ${H.limit===s?"selected":""}>${s} segundos</option>`).join("")}
          </select></label>
        <span class="spacer"></span>
        <button class="btn" id="start" ${activeIds().length && hostConnected ? "" : "disabled"}>Começar o jogo</button>
      </div>
      <div class="row">
        <button class="btn ghost small" id="copy">Copiar link</button>
        <button class="btn ghost small" id="newroom">Nova sala</button>
      </div>
    </div>
  </div>`;
  try{
    const qr = qrcode(0, "M"); qr.addData(url); qr.make();
    document.getElementById("qr").innerHTML = qr.createSvgTag({cellSize: 8, margin: 2, scalable: true});
  }catch(e){ document.getElementById("qr").innerHTML = '<p>Não foi possível gerar o QR Code. Use o endereço abaixo.</p>'; }
  renderNames();
  document.getElementById("scope").onchange = e => { H.scope = +e.target.value; saveHost(); };
  document.getElementById("limit").onchange = e => { H.limit = +e.target.value; saveHost(); };
  document.getElementById("start").onclick = startGame;
  document.getElementById("copy").onclick = () => copyText(url, "Link copiado");
  const nr = document.getElementById("newroom");
  nr.onclick = () => {
    if(nr.dataset.armed){ newRoom(); return; }
    nr.dataset.armed = "1"; nr.textContent = "Confirmar: apagar a lista e criar outra sala";
    setTimeout(() => { if(document.body.contains(nr)){ delete nr.dataset.armed; nr.textContent = "Nova sala"; } }, 4000);
  };
}
function renderNames(){
  const box = document.getElementById("names"); if(!box) return;
  const list = activeIds().map(id => ({id, ...H.players[id]})).sort((a,b) => a.joinedAt - b.joinedAt);
  box.innerHTML = list.length ? list.map(p => `<span class="name pop">${esc(p.name)}<button data-k="${esc(p.id)}" aria-label="Remover ${esc(p.name)}" title="Remover">×</button></span>`).join("")
    : `<div class="empty" style="width:100%">Os nomes aparecem aqui assim que os alunos entrarem.</div>`;
  box.querySelectorAll("button[data-k]").forEach(b => b.onclick = () => kick(b.dataset.k));
  const n = activeIds().length;
  const bc = document.getElementById("bigcount"); if(bc) bc.textContent = n;
  const pc = document.getElementById("pcount"); if(pc) pc.textContent = n;
  const st = document.getElementById("start"); if(st) st.disabled = !(n && hostConnected);
}
function updateAnswered(){
  const el = document.getElementById("answered"); const total = activeIds().length;
  if(el) el.innerHTML = `<b>${Object.keys(H.answers).filter(id => !H.kicked.includes(id)).length}</b>de ${total} responderam`;
  const pc = document.getElementById("pcount"); if(pc) pc.textContent = total;
}
function renderHostQuestion(){
  const q = curQ(), rev = H.phase === "reveal";
  const counts = q.o.map((_,i) => Object.entries(H.answers).filter(([id,a]) => a.c === i && !H.kicked.includes(id)).length);
  const left = Math.max(0, Math.ceil((deadline() - Date.now()) / 1000));
  lastSecond = left;
  $app.className = "app";
  $app.innerHTML = hostTop() + `
  <div class="row"><span class="chip">Questão <b>${H.qi+1}</b>/${H.qlist.length}</span><span class="chip"><b>${esc(AULAS[q.a].split("·")[0].trim())}</b></span>${q.tf?'<span class="chip">Verdadeiro ou falso</span>':""}</div>
  <div class="qhead">
    <div class="bigclock ${!rev && left<=5?"low":""}" id="hclock">${rev ? "0" : left}</div>
    <div class="qcard pop"><span class="tag">${esc(AULAS[q.a].split("·")[1].trim())}</span><h2>${esc(q.q)}</h2></div>
    <div class="answered" id="answered"></div>
  </div>
  <div class="answers ${rev?"reveal":""}">
    ${q.o.map((o,i) => `<div class="ans c${q.tf ? (i===0?2:0) : i} ${rev ? (i===q.c?"right":"wrong") : ""}">
      <span class="ico">${q.tf ? TF_ICONS[i] : ICONS[i]}</span><span class="txt">${esc(o)}</span>
      ${rev ? `<span class="cnt" title="respostas">${counts[i]}</span>` : ""}
    </div>`).join("")}
  </div>
  ${rev ? `<div class="explain pop">${q.e}</div>` : ""}
  <div class="bar-actions">
    <button class="btn ghost small" id="endnow">Encerrar partida</button>
    ${rev ? `<button class="btn" id="board">Ver ranking</button>` : `<button class="btn ghost" id="stop">Encerrar tempo</button>`}
  </div>`;
  updateAnswered();
  const e = document.getElementById("endnow");
  e.onclick = () => { if(e.dataset.armed) endGame(); else { e.dataset.armed = 1; e.textContent = "Confirmar encerramento"; } };
  if(rev) document.getElementById("board").onclick = showBoard;
  else document.getElementById("stop").onclick = doReveal;
}
function renderBoard(){
  const r = ranking().slice(0, 10), last = H.qi + 1 >= H.qlist.length;
  $app.className = "app";
  $app.innerHTML = hostTop() + `
  <div class="board pop">
    <span class="eyebrow" style="text-align:center">Após a questão ${H.qi+1} de ${H.qlist.length}</span>
    <h2>Ranking</h2>
    ${r.length ? r.map((p,i) => {
      const moved = p.prevRank ? p.prevRank - (i+1) : 0;
      return `<div class="rank ${i<3?"top":""}"><span class="pos">${i+1}º</span><span class="nm">${esc(p.name)}${moved>0?` <span style="color:var(--ok);font-size:.7em">▲${moved}</span>`:""}</span><span class="gain">${p.lp?"+"+fmt(p.lp):""}</span><span class="pt">${fmt(p.score)}</span></div>`;
    }).join("") : `<div class="empty">Ninguém na sala.</div>`}
    <div class="bar-actions" style="margin-top:8px">
      <button class="btn ghost small" id="endnow">Encerrar partida</button>
      <button class="btn" id="next">${last ? "Resultado final" : "Próxima questão"}</button>
    </div>
  </div>`;
  document.getElementById("next").onclick = nextQuestion;
  const e = document.getElementById("endnow");
  e.onclick = () => { if(e.dataset.armed) endGame(); else { e.dataset.armed = 1; e.textContent = "Confirmar encerramento"; } };
}
function renderEnd(){
  const r = ranking(), [g, s, b] = r;
  const pod = (p, cls, pos) => p ? `<div class="pod ${cls}"><span class="p">${pos}º</span><span class="n">${esc(p.name)}</span><span class="s">${fmt(p.score)} pts · ${p.correct} acertos</span></div>` : `<div></div>`;
  $app.className = "app";
  $app.innerHTML = hostTop() + `
  <div class="board pop">
    <span class="eyebrow" style="text-align:center">${esc(T.endEyebrow)} · ${H.qlist.length} questões</span>
    <h2>Pódio</h2>
    <div class="podium">${pod(s,"s2",2)}${pod(g,"g",1)}${pod(b,"s3",3)}</div>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr><th class="num">#</th><th>Nome</th><th class="num">Pontos</th><th class="num">Acertos</th><th class="num">Aproveitamento</th></tr></thead>
      <tbody>${r.map((p,i) => `<tr><td class="num">${i+1}</td><td>${esc(p.name)}</td><td class="num">${fmt(p.score)}</td><td class="num">${p.correct}/${H.qlist.length}</td><td class="num">${H.qlist.length ? Math.round(100*p.correct/H.qlist.length) : 0}%</td></tr>`).join("")}</tbody>
    </table>
  </div>
  <div class="bar-actions" style="max-width:900px;margin:0 auto;width:100%">
    <button class="btn ghost small" id="copy">Copiar resultados</button>
    <button class="btn ghost small" id="csv">Baixar planilha (CSV)</button>
    <button class="btn" id="again">Nova partida com a mesma turma</button>
  </div>`;
  document.getElementById("copy").onclick = () => copyText(resultsCsv().replace(/;/g, "\t"), "Resultados copiados. Cole na planilha.");
  document.getElementById("csv").onclick = () => {
    const blob = new Blob(["﻿" + resultsCsv()], {type: "text/csv;charset=utf-8"});
    const a = document.createElement("a"); a.href = URL.createObjectURL(blob);
    a.download = `ranking-sala-${H.room}-${new Date().toISOString().slice(0,10)}.csv`;
    document.body.appendChild(a); a.click(); a.remove();
  };
  document.getElementById("again").onclick = backToLobby;
}
function copyText(text, ok){
  (navigator.clipboard ? navigator.clipboard.writeText(text) : Promise.reject())
    .then(() => toast(ok), () => toast("Não foi possível copiar automaticamente."));
}

/* =====================================================================
   ALUNO
   ===================================================================== */
const ROOM = (params.get("sala") || "").replace(/\D/g, "").slice(0, 8);
let P = null, pc = null, pConnected = false, pTimer = null, joinRetry = null;

function pKey(k){ return T.key + "-" + k + "-" + ROOM; }
function ptopic(n){ return TOPIC_ROOT + ROOM + "/" + n; }

async function startPlayer(){
  document.title = T.name + " · Sala " + ROOM;
  $app.className = "app phone";
  let id = store.get(pKey("id"));
  if(!id){ id = rid(12); store.set(pKey("id"), id); }
  P = {id, name: store.get(pKey("name")) || "", state: null, scores: null, recvAt: 0, view: ""};
  if(!P.name) renderPJoin(); else renderPlayer(true);
  const b = parseInt(params.get("b"), 10);
  try{
    pc = BROKER_OVERRIDE ? await connectTo(BROKER_OVERRIDE, 7000) : await connectTo(BROKERS[Number.isInteger(b) && BROKERS[b] ? b : 0], 9000);
    pConnected = true;
    pc.on("connect", () => { pConnected = true; onPConnect(); });
    pc.on("close", () => { pConnected = false; renderPlayer(true); });
    pc.on("message", (t, buf) => {
      const s = buf.toString();
      if(!s){ if(t === ptopic("state")){ P.state = null; renderPlayer(true); } return; }
      let m; try{ m = JSON.parse(s); }catch(e){ return; }
      if(t === ptopic("state")) onPState(m);
      else if(t === ptopic("scores")) { P.scores = m; renderPlayer(); }
    });
    onPConnect();
  }catch(e){
    pConnected = false;
    $app.innerHTML = `<div class="center"><div class="box"><h2>Não conseguimos conectar</h2><p class="muted">Confira se o celular está com internet (Wi-Fi ou dados móveis) e tente de novo.</p><button class="btn" onclick="location.reload()">Tentar de novo</button></div></div>`;
  }
}
function onPConnect(){
  pc.subscribe([ptopic("state"), ptopic("scores")], {qos: 1});
  sendJoin();
  renderPlayer(true);
}
function sendJoin(){
  if(!P.name || !pc || !pConnected) return;
  pc.publish(ptopic("join"), JSON.stringify({id: P.id, name: P.name}), {qos: 1});
  clearTimeout(joinRetry);
  joinRetry = setTimeout(() => { if(!me()) sendJoin(); }, 4000);
}
function me(){ return P.scores && P.scores.players && P.scores.players[P.id]; }
function kicked(){ return P.scores && Array.isArray(P.scores.out) && P.scores.out.includes(P.id); }
function onPState(m){
  const prev = P.state;
  P.state = m; P.recvAt = Date.now();
  if(m.phase === "question") P.deadline = Date.now() + Math.max(0, (m.rem || 0) - 150);
  if(prev && prev.game !== m.game) sendJoin();
  renderPlayer();
}
function answeredKey(){ return pKey("ans"); }
function myAnswer(){ const a = store.get(answeredKey()); const s = P.state; return a && s && a.game === s.game && a.qi === s.qi ? a : null; }

function renderPJoin(){
  P.view = "join";
  $app.innerHTML = `
  <div class="topbar"><div class="brand">${GEAR}<span>${esc(T.name)}</span></div><span class="chip">Sala <b>${esc(ROOM)}</b></span></div>
  <div class="hazard"></div>
  <div class="center">
    <form class="box pop" id="jf" autocomplete="off">
      <span class="eyebrow">Revisão ao vivo</span>
      <h2>${esc(T.joinTitle)}</h2>
      <label class="label" for="nick">Seu nome (como o professor vai ver no ranking)</label>
      <input class="field" id="nick" maxlength="18" required placeholder="Ex.: Ana Paula" value="${esc(store.get(T.key + "-lastname") || "")}">
      <button class="btn" type="submit">Entrar</button>
      <p class="muted" style="font-size:13px">Quanto mais rápido você acertar, mais pontos ganha. Acertos seguidos valem bônus.</p>
    </form>
  </div>`;
  document.getElementById("jf").addEventListener("submit", e => {
    e.preventDefault();
    const n = cleanName(document.getElementById("nick").value);
    if(!n){ document.getElementById("nick").focus(); return; }
    P.name = n; store.set(pKey("name"), n); store.set(T.key + "-lastname", n);
    beep(880, .06, "square", .03);
    sendJoin(); renderPlayer(true);
  });
}
function pTop(){
  const m = me();
  return `<div class="topbar">
    <div class="brand" style="font-size:16px">${GEAR}<span>${esc(m ? m.n : P.name)}</span></div>
    <div class="row">${m ? `<span class="chip"><b>${fmt(m.s)}</b> pts</span>` : ""}<span class="chip net ${pConnected?"on":""}">${pConnected ? "Sala " + esc(ROOM) : "Reconectando…"}</span></div>
  </div>`;
}
function renderPlayer(force){
  if(!P.name){ if(P.view !== "join") renderPJoin(); return; }
  const s = P.state, m = me();
  let view;
  if(kicked()) view = "kicked";
  else if(!s) view = "connecting";
  else if(!m) view = "entering";
  else view = s.phase + ":" + s.qi + ":" + s.game + (s.phase === "question" && myAnswer() ? ":sent" : "");
  const sig = view + "|" + (m ? m.s + ":" + m.r + ":" + m.lq : "") + "|" + pConnected + "|" + (P.scores ? P.scores.count : 0);
  if(!force && sig === P.view) return;
  P.view = sig;
  clearInterval(pTimer);

  if(view === "kicked"){
    $app.innerHTML = pTop() + `<div class="center"><div class="result wait"><h2>Você saiu da sala</h2><p>O professor removeu este nome. Se foi engano, avise o professor.</p></div></div>`; return;
  }
  if(view === "connecting"){
    $app.innerHTML = pTop() + `<div class="center"><div class="result wait"><span class="dot"></span><h2>Conectando à sala ${esc(ROOM)}…</h2><p class="muted">Se demorar, leia de novo o QR Code que está no projetor.</p></div></div>`; return;
  }
  if(view === "entering"){
    $app.innerHTML = pTop() + `<div class="center"><div class="result wait"><span class="dot"></span><h2>Entrando…</h2><p class="muted">Aguardando a tela do professor confirmar seu nome.</p><button class="btn ghost small" id="rename">Trocar nome</button></div></div>`;
    document.getElementById("rename").onclick = () => { P.name = ""; store.del(pKey("name")); renderPJoin(); };
    return;
  }
  const ph = s.phase;
  if(ph === "lobby"){
    $app.innerHTML = pTop() + `<div class="center"><div class="result wait pop">
      <span class="eyebrow">Você está dentro!</span>
      <h2>${esc(m.n)}</h2>
      <p>Procure seu nome na tela do projetor. O jogo começa quando o professor iniciar.</p>
      <p class="muted" style="font-size:14px">${P.scores.count} ${P.scores.count===1?"aluno":"alunos"} na sala</p>
      <span class="dot"></span>
    </div></div>`; return;
  }
  if(ph === "question"){
    const sent = myAnswer();
    if(sent){
      $app.innerHTML = pTop() + `<div class="center"><div class="result wait pop">
        <span class="eyebrow">Questão ${s.qi+1} de ${s.total}</span>
        <div class="ans c${s.tf ? (sent.c===0?2:0) : sent.c}" style="min-height:0;width:120px;height:120px;justify-content:center"><span class="ico" style="width:64px;height:64px">${s.tf ? TF_ICONS[sent.c] : ICONS[sent.c]}</span></div>
        <h2>Resposta enviada!</h2><p class="muted">Aguarde o tempo acabar e olhe para o projetor.</p></div></div>`;
      return;
    }
    $app.innerHTML = pTop() + `
      <div class="row"><span class="chip">Questão <b>${s.qi+1}</b>/${s.total}</span><span class="chip"><b>${esc(s.al || "Aula " + s.aula)}</b></span></div>
      <div class="ptimer"><i id="ptf"></i></div>
      <p class="pq">${esc(s.text)}</p>
      <div class="ptiles ${s.tf?"tf":""}">
        ${s.opts.map((o,i) => `<button class="ans c${s.tf ? (i===0?2:0) : i}" data-c="${i}"><span class="ico">${s.tf ? TF_ICONS[i] : ICONS[i]}</span><span class="txt">${esc(o)}</span></button>`).join("")}
      </div>`;
    const total = (s.lim || 20) * 1000;
    const fill = document.getElementById("ptf");
    const tick = () => { const left = Math.max(0, P.deadline - Date.now()); if(fill) fill.style.transform = `scaleX(${Math.min(1, left / total)})`; };
    tick(); pTimer = setInterval(tick, 200);
    $app.querySelectorAll("[data-c]").forEach(b => b.onclick = () => {
      if(myAnswer()) return;
      const c = +b.dataset.c;
      store.set(answeredKey(), {game: s.game, qi: s.qi, c});
      if(pc && pConnected) pc.publish(ptopic("ans"), JSON.stringify({id: P.id, game: s.game, qi: s.qi, c}), {qos: 1});
      if(navigator.vibrate) try{ navigator.vibrate(30); }catch(e){}
      renderPlayer(true);
    });
    return;
  }
  if(ph === "reveal" || ph === "board"){
    const answeredThis = m.lq === s.qi;
    const ok = answeredThis && m.lo;
    const sent = store.get(answeredKey());
    const didSend = sent && sent.game === s.game && sent.qi === s.qi;
    if(!answeredThis){
      $app.innerHTML = pTop() + `<div class="center"><div class="result wait"><span class="dot"></span><h2>Calculando…</h2></div></div>`; return;
    }
    if(P.lastFx !== s.game + s.qi){ P.lastFx = s.game + s.qi; ok ? sfx.ok() : sfx.bad(); }
    $app.innerHTML = pTop() + `
      <div class="result ${ok?"ok":"bad"} pop">
        <span class="eyebrow" style="color:inherit;opacity:.75">Questão ${s.qi+1} de ${s.total}</span>
        <h2>${ok ? "Correto!" : (didSend ? "Não foi dessa vez" : "Tempo esgotado")}</h2>
        <span class="big">${ok ? "+" + fmt(m.lp) : "+0"}</span>
        ${!ok ? `<p>Resposta certa: <b>${esc(s.opts[s.correct])}</b></p>` : ""}
      </div>
      <div class="mini">
        <div><b>${m.r}º</b><span>de ${P.scores.count} na sala</span></div>
        <div><b>${fmt(m.s)}</b><span>pontos</span></div>
      </div>
      <p class="muted" style="text-align:center">Olhe o projetor para ver a explicação e o ranking.</p>`;
    return;
  }
  if(ph === "end"){
    const msg = m.r === 1 ? T.champion : m.r <= 3 ? "Pódio!" : "Fim do jogo";
    $app.innerHTML = pTop() + `
      <div class="result ${m.r<=3?"ok":"wait"} pop">
        <span class="eyebrow" style="color:inherit">${esc(m.n)}</span>
        <h2>${msg}</h2>
        <span class="big">${m.r}º lugar</span>
        <p>de ${P.scores.count} alunos</p>
      </div>
      <div class="mini">
        <div><b>${fmt(m.s)}</b><span>pontos</span></div>
        <div><b>${m.c}/${s.total}</b><span>acertos</span></div>
      </div>`;
    if(P.lastFx !== s.game + "end"){ P.lastFx = s.game + "end"; sfx.end(); }
    return;
  }
}

/* ---------- início ---------- */
if(ROOM) startPlayer(); else startHost();
