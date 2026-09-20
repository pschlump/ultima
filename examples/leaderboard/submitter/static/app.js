// Leaderboard demo page (design doc §11.4 #1): subscribes to lb:update over
// /ws/v1 on a §9.4 resumable session and re-reads the top 10 with
// ZRANGE … WITHSCORES whenever an update arrives. The vendored ESM bundle
// (static/vendor/ultima-client.js) is a copy of clients/javascript/dist/esm.
import { UltimaWS } from "./vendor/ultima-client.js";

const SCORES_KEY = "lb:scores";
const UPDATE_CHANNEL = "lb:update";
const TOP = 10;

const statusEl = document.getElementById("status");
const noteEl = document.getElementById("note");
const boardEl = document.getElementById("board");
const addrEl = document.getElementById("addr");
const dropBtn = document.getElementById("drop");
const reconnectBtn = document.getElementById("reconnect");

addrEl.value = new URLSearchParams(location.search).get("server") || addrEl.value;

let ws = null;

function setStatus(s) {
  statusEl.textContent = s;
  statusEl.className = s;
}

function note(text) {
  noteEl.textContent = text;
}

async function refresh(flashPlayer) {
  if (!ws) return;
  try {
    // ZRANGE lb:scores -10 -1 WITHSCORES = the top 10, ascending; flip for display.
    const rows = (await ws.zrangeWithScores(SCORES_KEY, -TOP, -1)).reverse();
    boardEl.innerHTML = "";
    rows.forEach((r, i) => {
      const tr = document.createElement("tr");
      if (r.member === flashPlayer) tr.className = "flash";
      for (const cell of [String(i + 1), r.member, String(r.score)]) {
        const td = document.createElement("td");
        td.textContent = cell;
        tr.appendChild(td);
      }
      boardEl.appendChild(tr);
    });
  } catch (err) {
    note("refresh failed: " + err);
  }
}

function connect() {
  if (ws) ws.close();
  const addr = addrEl.value.trim();
  ws = new UltimaWS({
    url: `ws://${addr}/ws/v1`,
    onStateChange: setStatus,
    onResumed: () => note("session resumed — replay caught up, no updates lost"),
    onGap: () => {
      note("session expired on the server — reconnected fresh; board re-read");
      refresh();
    },
    onError: (err) => note("client: " + err),
  });
  ws.connect();
  ws.subscribe(UPDATE_CHANNEL, (msg) => {
    let player = null;
    try {
      player = JSON.parse(msg.text).player;
    } catch {
      // ignore malformed demo payloads
    }
    refresh(player);
  }).then(
    () => refresh(),
    (err) => note("subscribe failed: " + err),
  );
}

dropBtn.addEventListener("click", () => {
  // Kill the socket without closing the client: the §9.4 resume path runs on
  // the automatic reconnect (watch the status pill flip and the note line).
  if (ws) ws.forceReconnect();
});
reconnectBtn.addEventListener("click", connect);

connect();
