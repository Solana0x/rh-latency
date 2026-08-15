// Robinhood Chain latency harness — raw persistent TLS sockets vs ordinary
// https.request, measured against every sequencer load-balancer IP.
// Networking layer only; no keys, no strategy. Private endpoints via EXTRA_RPCS.
import net from "node:net";
import tls from "node:tls";
import dns from "node:dns/promises";
import https from "node:https";
import http from "node:http";
import { performance } from "node:perf_hooks";

const SEQUENCER_HOST = "sequencer.mainnet.chain.robinhood.com";
const PUBLIC_RPC = "https://rpc.mainnet.chain.robinhood.com";
const EXTRA = (process.env.EXTRA_RPCS || "").split(",").map((s) => s.trim()).filter(Boolean);

const jsonBody = (method, params = []) => JSON.stringify({ jsonrpc: "2.0", method, params, id: 1 });
const SEND_BODY = jsonBody("eth_sendRawTransaction", ["0x02"]); // parsed then rejected — nothing broadcast

function frame(host, path, body) {
  return Buffer.from(
    `POST ${path} HTTP/1.1\r\nHost: ${host}\r\nContent-Type: application/json\r\n` +
    `Content-Length: ${Buffer.byteLength(body)}\r\nConnection: keep-alive\r\n\r\n${body}`
  );
}

const CRLF2 = Buffer.from("\r\n\r\n");
const CONTENT_LENGTH = Buffer.from("content-length:");
function findHeader(buf, name, limit) {
  const n = name.length;
  outer: for (let i = 0; i <= limit - n; i++) {
    for (let j = 0; j < n; j++) if ((buf[i + j] | 0x20) !== name[j]) continue outer;
    return i + n;
  }
  return -1;
}

class HotSocket {
  constructor({ label, host, port = 443, path = "/", ip = null }) {
    Object.assign(this, { label, host, port, path, ip });
    this.sock = null; this.queue = []; this.buf = Buffer.alloc(0);
  }
  connect() {
    return new Promise((resolve) => {
      this.sock = tls.connect({
        host: this.ip || this.host, servername: this.host, port: this.port,
        ALPNProtocols: ["http/1.1"],
      });
      this.sock.once("secureConnect", () => {
        this.sock.setNoDelay(true);
        this.sock.on("data", (c) => this._onData(c));
        this.sock.on("error", () => this._drop());
        this.sock.on("close", () => this._drop());
        resolve(true);
      });
      this.sock.once("error", () => resolve(false));
      this.sock.setTimeout(15000, () => this.sock.destroy());
    });
  }
  _drop() { for (const r of this.queue.splice(0)) r({ ok: false }); this.sock = null; }
  _onData(chunk) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    for (;;) {
      const headEnd = this.buf.indexOf(CRLF2);
      if (headEnd === -1) return;
      const clAt = findHeader(this.buf, CONTENT_LENGTH, headEnd);
      if (clAt === -1) return;
      let len = 0;
      for (let p = clAt; p < headEnd; p++) {
        const c = this.buf[p];
        if (c === 0x20) continue;
        if (c < 0x30 || c > 0x39) break;
        len = len * 10 + (c - 0x30);
      }
      const start = headEnd + 4;
      if (this.buf.length < start + len) return;
      const body = this.buf.toString("latin1", start, start + len);
      this.buf = this.buf.subarray(start + len);
      const r = this.queue.shift();
      if (r) r({ ok: true, body });
    }
  }
  fire(buffer) {
    if (!this.sock || this.sock.destroyed) return Promise.resolve({ ok: false });
    const p = new Promise((res) => this.queue.push(res));
    this.sock.write(buffer);
    return p;
  }
  fireMany(buffer, count) {
    if (!this.sock || this.sock.destroyed) return Promise.resolve([]);
    const ps = new Array(count);
    for (let i = 0; i < count; i++) ps[i] = new Promise((res) => this.queue.push(res));
    this.sock.write(buffer);
    return Promise.all(ps);
  }
  destroy() { this.sock?.destroy(); }
}

const pct = (a, p) => [...a].sort((x, y) => x - y)[Math.floor(a.length * p)];
const f = (n) => n.toFixed(2).padStart(7);

async function run() {
  const L = [];
  const out = (s) => { console.log(s); L.push(s); };

  out(`===== Robinhood Chain — Ohio box @ ${new Date().toISOString()} =====\n`);

  // ── 1. Raw persistent TLS socket to every sequencer LB IP ──
  let ips = [];
  try { ips = await dns.resolve4(SEQUENCER_HOST); } catch {}
  out(`sequencer DNS → ${ips.join(", ") || "resolution failed"}\n`);
  out(`--- RAW PERSISTENT TLS SOCKET (the bot's fire path) ---`);

  for (const ip of ips) {
    const hs = new HotSocket({ label: ip, host: SEQUENCER_HOST, ip });
    const t0 = performance.now();
    const ok = await hs.connect();
    const hand = performance.now() - t0;
    if (!ok) { out(`  ${ip.padEnd(16)} CONNECT FAILED`); continue; }
    const buf = frame(SEQUENCER_HOST, "/", SEND_BODY);
    const warm = [], disp = [];
    let reply = "";
    for (let i = 0; i < 500; i++) {
      const s = performance.now();
      const p = hs.fire(buf);
      disp.push(performance.now() - s);
      const r = await p;
      warm.push(performance.now() - s);
      if (i === 0) reply = (r.body || "").slice(0, 60).replace(/\s+/g, " ");
    }
    warm.shift(); disp.shift();
    out(`  ${ip.padEnd(16)} handshake ${f(hand)}ms | send min ${f(Math.min(...warm))}ms p50 ${f(pct(warm,0.5))}ms p90 ${f(pct(warm,0.90))}ms p99 ${f(pct(warm,0.99))}ms max ${f(Math.max(...warm))}ms  (n=${warm.length})`);
    out(`  ${" ".repeat(16)} dispatch p50 ${pct(disp,0.5).toFixed(3)}ms p99 ${pct(disp,0.99).toFixed(3)}ms max ${Math.max(...disp).toFixed(3)}ms`);
    out(`  ${" ".repeat(16)} over 5ms: ${warm.filter(x=>x>5).length}/${warm.length} | over 10ms: ${warm.filter(x=>x>10).length}/${warm.length}`);
    out(`  ${" ".repeat(16)} reply: ${reply}`);
    hs.destroy();
  }

  // ── 1b. PIPELINING: N requests in ONE write vs N separate writes ──
  out(`\n--- PIPELINED SALVO (simulating N wallets firing at once) ---`);
  if (ips.length) {
    const hs = new HotSocket({ label: "pipe", host: SEQUENCER_HOST, ip: ips[0] });
    if (await hs.connect()) {
      const one = frame(SEQUENCER_HOST, "/", SEND_BODY);
      for (const N of [1, 3, 5, 10]) {
        const salvo = Buffer.concat(Array(N).fill(one));
        // (a) pipelined: single write
        const pipeT = [];
        let good = 0;
        for (let i = 0; i < 40; i++) {
          const s = performance.now();
          const rs = await hs.fireMany(salvo, N);
          pipeT.push(performance.now() - s);
          if (rs.length === N && rs.every((r) => r.ok && r.body.includes("jsonrpc"))) good++;
        }
        // (b) sequential writes, all in flight before awaiting
        const seqT = [];
        for (let i = 0; i < 40; i++) {
          const s = performance.now();
          const ps = [];
          for (let k = 0; k < N; k++) ps.push(hs.fire(one));
          const dispatched = performance.now() - s;
          await Promise.all(ps);
          seqT.push(dispatched);
        }
        out(`  N=${String(N).padStart(2)}  pipelined: ${good}/40 fully answered | last-byte-out p50 ${f(pct(pipeT,0.5))}ms`);
        out(`        separate writes dispatch p50 ${f(pct(seqT,0.5))}ms  (delay imposed on the LAST wallet)`);
      }
      hs.destroy();
    }
  }

  // ── 2. Same thing through ordinary https.request, for comparison ──
  out(`\n--- ORDINARY https.request + keep-alive agent (for comparison) ---`);
  for (const url of [`https://${SEQUENCER_HOST}`, PUBLIC_RPC, ...EXTRA]) {
    const u = new URL(url);
    const agent = new https.Agent({ keepAlive: true, maxSockets: 1 });
    const once = (body) => new Promise((res, rej) => {
      const req = https.request({ agent, host: u.hostname, path: u.pathname + u.search, method: "POST",
        headers: { "content-type": "application/json", "content-length": Buffer.byteLength(body) } },
        (r) => { const c = []; r.on("data", (d) => c.push(d)); r.on("end", () => res(Buffer.concat(c).toString())); });
      req.setTimeout(15000, () => req.destroy(new Error("timeout")));
      req.on("error", rej); req.end(body);
    });
    try {
      await once(SEND_BODY);
      const t = [];
      for (let i = 0; i < 15; i++) { const s = performance.now(); await once(SEND_BODY); t.push(performance.now() - s); }
      out(`  ${u.hostname.padEnd(38)} send min ${f(Math.min(...t))}ms p50 ${f(pct(t,0.5))}ms`);
    } catch (e) {
      out(`  ${u.hostname.padEnd(38)} FAILED ${e.message}`);
    } finally { agent.destroy(); }
  }

  // ── 3. Read endpoints ──
  out(`\n--- READ path (eth_blockNumber) ---`);
  for (const url of [PUBLIC_RPC, ...EXTRA]) {
    const u = new URL(url);
    const agent = new https.Agent({ keepAlive: true, maxSockets: 1 });
    const body = jsonBody("eth_blockNumber");
    const once = () => new Promise((res, rej) => {
      const req = https.request({ agent, host: u.hostname, path: u.pathname + u.search, method: "POST",
        headers: { "content-type": "application/json", "content-length": Buffer.byteLength(body) } },
        (r) => { const c = []; r.on("data", (d) => c.push(d)); r.on("end", () => res(Buffer.concat(c).toString())); });
      req.setTimeout(15000, () => req.destroy(new Error("timeout")));
      req.on("error", rej); req.end(body);
    });
    try {
      await once();
      const t = [];
      for (let i = 0; i < 15; i++) { const s = performance.now(); await once(); t.push(performance.now() - s); }
      out(`  ${u.hostname.padEnd(38)} read min ${f(Math.min(...t))}ms p50 ${f(pct(t,0.5))}ms p99 ${f(pct(t,0.99))}ms`);
    } catch (e) {
      out(`  ${u.hostname.padEnd(38)} FAILED ${e.message}`);
    } finally { agent.destroy(); }
  }

  out(`\ndispatch p50 = pure software cost. send p50 = what a mint tx actually pays.`);
  return L.join("\n");
}

let report = "running...";
run().then((r) => { report = r; }).catch((e) => { report = "error: " + (e.stack || e.message); });

http.createServer((_q, s) => { s.writeHead(200, { "content-type": "text/plain" }); s.end(report); })
  .listen(process.env.PORT || 10000);
