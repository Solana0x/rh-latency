// Two remaining software levers against the sequencer's ~1.25ms RTT floor:
//   1. plaintext HTTP on port 80 — skips TLS record crypto entirely
//   2. HTTP/2 multiplexing — N wallets on ONE socket without head-of-line blocking
// Compared against the current design (raw TLS socket per wallet).
import net from "node:net";
import tls from "node:tls";
import http2 from "node:http2";
import dns from "node:dns/promises";
import http from "node:http";
import os from "node:os";
import { performance } from "node:perf_hooks";

const HOST = "sequencer.mainnet.chain.robinhood.com";
const BODY = JSON.stringify({ jsonrpc: "2.0", method: "eth_sendRawTransaction", params: ["0x02"], id: 1 });

const pct = (a, p) => [...a].sort((x, y) => x - y)[Math.floor(a.length * p)];
const f = (n) => (Number.isFinite(n) ? n.toFixed(3) : "n/a").padStart(7);

const frame = (host, body) => Buffer.from(
  `POST / HTTP/1.1\r\nHost: ${host}\r\nContent-Type: application/json\r\n` +
  `Content-Length: ${Buffer.byteLength(body)}\r\nConnection: keep-alive\r\n\r\n${body}`
);

function tcpRtt(ip, port = 443) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const s = net.connect({ host: ip, port });
    s.once("connect", () => { const ms = performance.now() - t0; s.destroy(); resolve(ms); });
    s.once("error", () => resolve(NaN));
    s.setTimeout(5000, () => { s.destroy(); resolve(NaN); });
  });
}

function roundTrip(sock, buf) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    sock.once("data", (d) => resolve({ ms: performance.now() - t0, text: d.toString().slice(-120) }));
    sock.write(buf);
  });
}

function openTls(ip) {
  return new Promise((resolve) => {
    const s = tls.connect({ host: ip, servername: HOST, port: 443, ALPNProtocols: ["http/1.1"] });
    s.once("secureConnect", () => { s.setNoDelay(true); resolve(s); });
    s.once("error", () => resolve(null));
    s.setTimeout(8000, () => { s.destroy(); resolve(null); });
  });
}

function openPlain(ip, port = 80) {
  return new Promise((resolve) => {
    const s = net.connect({ host: ip, port });
    s.once("connect", () => { s.setNoDelay(true); resolve(s); });
    s.once("error", () => resolve(null));
    s.setTimeout(8000, () => { s.destroy(); resolve(null); });
  });
}

async function run() {
  const L = [];
  const out = (s) => { console.log(s); L.push(s); };
  out(`===== Protocol shoot-out @ ${new Date().toISOString()} =====`);
  out(`host ${os.hostname()} | ${os.cpus().length} cpu\n`);

  let ips = [];
  try { ips = await dns.resolve4(HOST); } catch {}

  // Pick the closest IP by TCP RTT — the others are a different AZ.
  const ranked = [];
  for (const ip of ips) {
    const t = [];
    for (let i = 0; i < 15; i++) { const v = await tcpRtt(ip); if (Number.isFinite(v)) t.push(v); }
    ranked.push({ ip, rtt: t.length ? Math.min(...t) : Infinity });
  }
  ranked.sort((a, b) => a.rtt - b.rtt);
  out(`TCP RTT floor by IP: ${ranked.map((r) => `${r.ip} ${r.rtt.toFixed(2)}ms`).join(" | ")}`);
  const IP = ranked[0].ip;
  out(`using fastest: ${IP} (RTT floor ${ranked[0].rtt.toFixed(3)}ms)\n`);

  const buf = frame(HOST, BODY);

  // ── 1. TLS (current design) ──
  out(`── 1. TLS 1.3 raw socket (current design) ──`);
  const tlsSock = await openTls(IP);
  let tlsP50 = NaN;
  if (tlsSock) {
    await roundTrip(tlsSock, buf);
    const t = [];
    for (let i = 0; i < 200; i++) t.push((await roundTrip(tlsSock, buf)).ms);
    tlsP50 = pct(t, 0.5);
    out(`   min ${f(Math.min(...t))}ms  p50 ${f(tlsP50)}ms  p90 ${f(pct(t,0.9))}ms  p99 ${f(pct(t,0.99))}ms`);
    tlsSock.destroy();
  } else out(`   failed`);

  // ── 2. Plaintext HTTP on port 80 ──
  out(`\n── 2. Plaintext HTTP, port 80 (no TLS crypto) ──`);
  const plain = await openPlain(IP, 80);
  if (plain) {
    const first = await roundTrip(plain, buf);
    const works = first.text.includes("jsonrpc") || first.text.includes("error");
    out(`   first reply: ${first.text.replace(/\s+/g, " ").slice(0, 90)}`);
    if (works && !/301|302|400 Bad Request/i.test(first.text)) {
      const t = [];
      for (let i = 0; i < 200; i++) t.push((await roundTrip(plain, buf)).ms);
      const p50 = pct(t, 0.5);
      out(`   min ${f(Math.min(...t))}ms  p50 ${f(p50)}ms  p90 ${f(pct(t,0.9))}ms`);
      out(`   → vs TLS: ${(tlsP50 - p50).toFixed(3)}ms ${p50 < tlsP50 ? "FASTER" : "slower"}`);
    } else {
      out(`   port 80 does not serve JSON-RPC (redirect or rejection) — TLS is mandatory`);
    }
    plain.destroy();
  } else out(`   port 80 unreachable`);

  // ── 3. HTTP/2 multiplexing: N wallets, ONE socket ──
  out(`\n── 3. HTTP/2 multiplexed streams (N wallets, ONE socket) ──`);
  await new Promise((resolve) => {
    const client = http2.connect(`https://${HOST}`, {
      createConnection: () => {
        const s = tls.connect({ host: IP, servername: HOST, port: 443, ALPNProtocols: ["h2"] });
        s.once("secureConnect", () => s.setNoDelay(true));
        return s;
      },
    });
    client.on("error", (e) => { out(`   h2 error: ${e.message}`); resolve(); });
    client.on("connect", async () => {
      const send = () => new Promise((res) => {
        const t0 = performance.now();
        const req = client.request({ ":method": "POST", ":path": "/", "content-type": "application/json" });
        let body = "";
        req.on("data", (d) => { body += d; });
        req.on("end", () => res({ ms: performance.now() - t0, body }));
        req.on("error", () => res({ ms: NaN, body: "" }));
        req.end(BODY);
      });
      const warm = await send();
      out(`   reply: ${warm.body.replace(/\s+/g, " ").slice(0, 80)}`);

      const single = [];
      for (let i = 0; i < 100; i++) { const r = await send(); if (Number.isFinite(r.ms)) single.push(r.ms); }
      out(`   single stream    min ${f(Math.min(...single))}ms  p50 ${f(pct(single,0.5))}ms`);

      for (const N of [3, 5]) {
        const all = [];
        for (let i = 0; i < 40; i++) {
          const t0 = performance.now();
          await Promise.all(Array.from({ length: N }, () => send()));
          all.push(performance.now() - t0);
        }
        out(`   ${N} streams / 1 socket  all-answered p50 ${f(pct(all,0.5))}ms`);
      }

      // Compare: N separate TLS sockets, one request each.
      for (const N of [3, 5]) {
        const pool = [];
        for (let k = 0; k < N; k++) { const s = await openTls(IP); if (s) { await roundTrip(s, buf); pool.push(s); } }
        const all = [];
        for (let i = 0; i < 40; i++) {
          const t0 = performance.now();
          await Promise.all(pool.map((s) => roundTrip(s, buf)));
          all.push(performance.now() - t0);
        }
        out(`   ${N} separate TLS sockets all-answered p50 ${f(pct(all,0.5))}ms`);
        pool.forEach((s) => s.destroy());
      }
      client.close();
      resolve();
    });
  });

  out(`\nRTT floor is the physical limit — protocol choices only remove overhead above it.`);
  return L.join("\n");
}

let report = "running...";
run().then((r) => { report = r; }).catch((e) => { report = "error: " + (e.stack || e.message); });
http.createServer((_q, s) => { s.writeHead(200, { "content-type": "text/plain" }); s.end(report); })
  .listen(process.env.PORT || 10000);
