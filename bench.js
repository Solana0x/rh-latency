// Decompose the send latency to the Robinhood Chain sequencer:
//   pure TCP RTT  vs  TLS handshake  vs  full request round trip
// so we know whether the remaining milliseconds are network distance
// (fixable only by moving the box) or protocol/server overhead.
import net from "node:net";
import tls from "node:tls";
import dns from "node:dns/promises";
import http from "node:http";
import os from "node:os";
import { performance } from "node:perf_hooks";

const HOST = "sequencer.mainnet.chain.robinhood.com";
const SEND_BODY = JSON.stringify({ jsonrpc: "2.0", method: "eth_sendRawTransaction", params: ["0x02"], id: 1 });

const pct = (a, p) => [...a].sort((x, y) => x - y)[Math.floor(a.length * p)];
const f = (n) => (Number.isFinite(n) ? n.toFixed(3) : "n/a").padStart(7);

function frame(host, body) {
  return Buffer.from(
    `POST / HTTP/1.1\r\nHost: ${host}\r\nContent-Type: application/json\r\n` +
    `Content-Length: ${Buffer.byteLength(body)}\r\nConnection: keep-alive\r\n\r\n${body}`
  );
}

// One TCP handshake = exactly one network round trip. This is the physical floor.
function tcpRtt(ip, port = 443) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const s = net.connect({ host: ip, port });
    s.once("connect", () => { const ms = performance.now() - t0; s.destroy(); resolve(ms); });
    s.once("error", () => resolve(NaN));
    s.setTimeout(5000, () => { s.destroy(); resolve(NaN); });
  });
}

function tlsHandshake(ip, host) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const s = tls.connect({ host: ip, servername: host, port: 443, ALPNProtocols: ["http/1.1"] });
    s.once("secureConnect", () => {
      const info = { ms: performance.now() - t0, proto: s.getProtocol(), alpn: s.alpnProtocol, cipher: s.getCipher()?.name };
      s.destroy();
      resolve(info);
    });
    s.once("error", () => resolve({ ms: NaN }));
    s.setTimeout(8000, () => { s.destroy(); resolve({ ms: NaN }); });
  });
}

function openHot(ip, host) {
  return new Promise((resolve) => {
    const s = tls.connect({ host: ip, servername: host, port: 443, ALPNProtocols: ["http/1.1"] });
    s.once("secureConnect", () => { s.setNoDelay(true); resolve(s); });
    s.once("error", () => resolve(null));
    s.setTimeout(8000, () => { s.destroy(); resolve(null); });
  });
}

function roundTrip(sock, buf) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    sock.once("data", () => resolve(performance.now() - t0));
    sock.write(buf);
  });
}

async function run() {
  const L = [];
  const out = (s) => { console.log(s); L.push(s); };
  out(`===== Latency decomposition @ ${new Date().toISOString()} =====`);
  out(`host ${os.hostname()} | ${os.cpus().length} cpu | ${os.platform()} ${os.release()}\n`);

  let ips = [];
  try { ips = await dns.resolve4(HOST); } catch {}
  out(`sequencer IPs: ${ips.join(", ")}\n`);

  const buf = frame(HOST, SEND_BODY);

  for (const ip of ips) {
    out(`── ${ip} ──`);

    const tcp = [];
    for (let i = 0; i < 25; i++) { const v = await tcpRtt(ip); if (Number.isFinite(v)) tcp.push(v); }
    if (tcp.length === 0) { out("   TCP unreachable\n"); continue; }
    const tcpMin = Math.min(...tcp), tcpP50 = pct(tcp, 0.5);
    out(`   TCP handshake (1 RTT)  min ${f(tcpMin)}ms  p50 ${f(tcpP50)}ms  p90 ${f(pct(tcp,0.9))}ms`);

    const h = await tlsHandshake(ip, HOST);
    out(`   TLS handshake          ${f(h.ms)}ms  (${h.proto || "?"}, alpn=${h.alpn || "none"}, ${h.cipher || "?"})`);

    const sock = await openHot(ip, HOST);
    if (!sock) { out(`   hot socket failed\n`); continue; }
    await roundTrip(sock, buf);
    const rt = [];
    for (let i = 0; i < 200; i++) rt.push(await roundTrip(sock, buf));
    const rtMin = Math.min(...rt), rtP50 = pct(rt, 0.5);
    out(`   Warm request           min ${f(rtMin)}ms  p50 ${f(rtP50)}ms  p90 ${f(pct(rt,0.9))}ms  p99 ${f(pct(rt,0.99))}ms`);
    out(`   → server+TLS cost      min ${f(rtMin - tcpMin)}ms  p50 ${f(rtP50 - tcpP50)}ms`);
    out(`   → network share        ${((tcpMin / rtMin) * 100).toFixed(0)}% of the min round trip`);
    sock.destroy();
    out("");
  }

  out(`── protocol probes ──`);
  for (const port of [80, 8545, 8547]) {
    const v = await tcpRtt(ips[0], port);
    out(`   port ${String(port).padEnd(5)} ${Number.isFinite(v) ? `OPEN (TCP ${f(v)}ms)` : "closed/filtered"}`);
  }
  const h2 = await new Promise((resolve) => {
    const s = tls.connect({ host: ips[0], servername: HOST, port: 443, ALPNProtocols: ["h2", "http/1.1"] });
    s.once("secureConnect", () => { const a = s.alpnProtocol; s.destroy(); resolve(a); });
    s.once("error", () => resolve("error"));
    s.setTimeout(8000, () => { s.destroy(); resolve("timeout"); });
  });
  out(`   ALPN h2 offered → server chose: ${h2}${h2 === "h2" ? "  (HTTP/2: parallel streams on ONE socket)" : "  (HTTP/1.1 only: parallel sockets required)"}`);

  out(`\nIf network share is ~90%+, only moving the box closer helps.`);
  return L.join("\n");
}

let report = "running...";
run().then((r) => { report = r; }).catch((e) => { report = "error: " + (e.stack || e.message); });
http.createServer((_q, s) => { s.writeHead(200, { "content-type": "text/plain" }); s.end(report); })
  .listen(process.env.PORT || 10000);
