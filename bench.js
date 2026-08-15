// Robinhood Chain endpoint latency benchmark.
// Public endpoints are hardcoded; private ones come from EXTRA_RPCS (never committed).
import https from "node:https";
import http from "node:http";
import { performance } from "node:perf_hooks";

const PUBLIC = [
  "https://sequencer.mainnet.chain.robinhood.com",
  "https://rpc.mainnet.chain.robinhood.com",
];
const EXTRA = (process.env.EXTRA_RPCS || "").split(",").map((s) => s.trim()).filter(Boolean);
const URLS = [...PUBLIC, ...EXTRA];

const BODY = JSON.stringify({ jsonrpc: "2.0", method: "eth_chainId", params: [], id: 1 });
const SEND = JSON.stringify({ jsonrpc: "2.0", method: "eth_sendRawTransaction", params: ["0x02"], id: 1 });

function hit(url, agent, body) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const t0 = performance.now();
    let connectMs = null;
    const req = https.request(
      { agent, host: u.hostname, path: u.pathname + u.search, method: "POST",
        headers: { "content-type": "application/json", "content-length": Buffer.byteLength(body) } },
      (res) => {
        const ip = res.socket?.remoteAddress ?? "?";
        const c = [];
        res.on("data", (d) => c.push(d));
        res.on("end", () => resolve({ ms: performance.now() - t0, connectMs, ip, text: Buffer.concat(c).toString() }));
      }
    );
    req.on("socket", (s) => { s.once("connect", () => { connectMs = performance.now() - t0; }); });
    req.setTimeout(10000, () => req.destroy(new Error("timeout")));
    req.on("error", reject);
    req.end(body);
  });
}

const med = (a) => [...a].sort((x, y) => x - y)[Math.floor(a.length / 2)];
const f = (n) => (n === null || n === undefined ? "  n/a" : n.toFixed(1).padStart(6));

async function bench() {
  const lines = [];
  const out = (s) => { console.log(s); lines.push(s); };
  out(`\n===== Robinhood Chain latency @ ${new Date().toISOString()} =====`);
  out(`host region hint: ${process.env.RENDER_REGION || process.env.REGION || "unknown"}\n`);

  for (const url of URLS) {
    const host = new URL(url).hostname;
    const agent = new https.Agent({ keepAlive: true });
    try {
      const cold = await hit(url, agent, BODY);
      const reads = [], sends = [];
      for (let i = 0; i < 8; i++) reads.push((await hit(url, agent, BODY)).ms);
      for (let i = 0; i < 5; i++) sends.push((await hit(url, agent, SEND)).ms);
      out(`${host}`);
      out(`   ip ${cold.ip}`);
      out(`   cold(TCP connect) ${f(cold.connectMs)}ms | cold total ${f(cold.ms)}ms`);
      out(`   WARM read  min ${f(Math.min(...reads))}ms  med ${f(med(reads))}ms`);
      out(`   WARM send  min ${f(Math.min(...sends))}ms  med ${f(med(sends))}ms`);
    } catch (e) {
      out(`${host}  FAILED: ${e.message}`);
    } finally {
      agent.destroy();
    }
  }
  out(`\nWARM send min = what a pre-signed mint tx actually costs from this box.`);
  out(`In AWS us-east-2 the sequencer should read ~1-3ms.\n`);
  return lines.join("\n");
}

let report = "running...";
bench().then((r) => { report = r; }).catch((e) => { report = "error: " + e.message; });

// Free-tier web services must bind a port; serve the report so it's readable via URL too.
http.createServer((_q, s) => {
  s.writeHead(200, { "content-type": "text/plain" });
  s.end(report);
}).listen(process.env.PORT || 10000);
