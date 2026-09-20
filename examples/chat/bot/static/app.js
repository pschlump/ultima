// Chat demo page (design doc §11.4 #2): chat:lobby is a pub/sub topic,
// chat:lobby:hist a capped list of the last 100 messages, and presence is
// chat:presence:<user> keys with a 15s PX (refreshed on a timer) plus the
// __keyevent@0__:expired notification stream. Everything runs on one §9.4
// resumable WS session. The vendored ESM bundle (static/vendor/
// ultima-client.js) is a copy of clients/javascript/dist/esm.
import { UltimaWS } from "./vendor/ultima-client.js";

const CHANNEL = "chat:lobby";
const HIST_KEY = "chat:lobby:hist";
const HIST_CAP = 100;
const PRESENCE_PREFIX = "chat:presence:";
const PRESENCE_CHANNEL = "chat:presence";
const PRESENCE_PX = 15000;
const PRESENCE_BEAT_MS = 10000;
const EXPIRED_CHANNEL = "__keyevent@0__:expired";

const statusEl = document.getElementById("status");
const noteEl = document.getElementById("note");
const logEl = document.getElementById("log");
const onlineEl = document.getElementById("online");
const textEl = document.getElementById("text");
const sendBtn = document.getElementById("send");

const params = new URLSearchParams(location.search);
const addr = params.get("server") || "127.0.0.1:6381";
const user =
  params.get("user") ||
  localStorage.getItem("ultima.chat.user") ||
  "user-" + Math.random().toString(36).slice(2, 8);
localStorage.setItem("ultima.chat.user", user);

const online = new Set();
let beatTimer = null;

function note(text) {
  noteEl.textContent = text;
}

function addLine(who, text, cls) {
  const div = document.createElement("div");
  div.className = "line" + (cls ? " " + cls : "");
  const whoEl = document.createElement("span");
  whoEl.className = "who";
  whoEl.textContent = who ? who + ": " : "";
  div.appendChild(whoEl);
  div.appendChild(document.createTextNode(text));
  logEl.appendChild(div);
  logEl.scrollTop = logEl.scrollHeight;
}

function renderOnline() {
  onlineEl.innerHTML = "";
  for (const u of [...online].sort()) {
    const li = document.createElement("li");
    li.textContent = u;
    onlineEl.appendChild(li);
  }
}

function onChat(msg) {
  try {
    const m = JSON.parse(msg.text);
    addLine(m.user, m.text);
  } catch {
    addLine(null, msg.text);
  }
}

function onPresence(msg) {
  try {
    const beat = JSON.parse(msg.text);
    if (typeof beat.user === "string" && !online.has(beat.user)) {
      online.add(beat.user);
      renderOnline();
    }
  } catch {
    // ignore malformed beats
  }
}

function onExpired(msg) {
  // Keyevent payload is the expired key itself.
  if (msg.text.startsWith(PRESENCE_PREFIX)) {
    const gone = msg.text.slice(PRESENCE_PREFIX.length);
    if (online.delete(gone)) {
      renderOnline();
      addLine(null, gone + " went away", "sys");
    }
  }
}

async function post(text) {
  const payload = JSON.stringify({ user, text, ts: Date.now() });
  // Same sequence the Go bot's botcore.Post uses: RPUSH, LTRIM, PUBLISH.
  await ws.rpush(HIST_KEY, payload);
  await ws.exec("LTRIM", HIST_KEY, String(-HIST_CAP), "-1");
  await ws.exec("PUBLISH", CHANNEL, payload);
}

async function beat() {
  try {
    await ws.set(PRESENCE_PREFIX + user, String(Date.now()), { px: PRESENCE_PX });
    await ws.exec("PUBLISH", PRESENCE_CHANNEL, JSON.stringify({ user }));
  } catch {
    // connection down; the next beat retries
  }
}

const ws = new UltimaWS({
  url: `ws://${addr}/ws/v1`,
  onStateChange: (s) => {
    statusEl.textContent = s;
    statusEl.className = s;
  },
  onResumed: () => note("session resumed — no messages lost"),
  onGap: () => note("session expired — reconnected fresh; messages during the drop were lost"),
  onError: (err) => note("client: " + err),
});

async function join() {
  await ws.subscribe(CHANNEL, onChat);
  await ws.subscribe(PRESENCE_CHANNEL, onPresence);
  await ws.subscribe(EXPIRED_CHANNEL, onExpired);

  // Load the capped history (the current tail, after a gap).
  const hist = await ws.lrange(HIST_KEY, 0, -1);
  for (const raw of hist) {
    try {
      const m = JSON.parse(raw);
      addLine(m.user, m.text);
    } catch {
      addLine(null, raw);
    }
  }
  addLine(null, "joined as " + user, "sys");

  await beat();
  beatTimer = setInterval(beat, PRESENCE_BEAT_MS);
}

sendBtn.addEventListener("click", () => {
  const text = textEl.value.trim();
  if (!text) return;
  textEl.value = "";
  post(text).catch((err) => note("send failed: " + err));
});
textEl.addEventListener("keydown", (ev) => {
  if (ev.key === "Enter") sendBtn.click();
});
window.addEventListener("beforeunload", () => {
  if (beatTimer) clearInterval(beatTimer);
});

ws.connect();
join().catch((err) => note("join failed: " + err));
