// Is Render's networking adding fixed overhead, or is the sequencer simply
// further away? Compare TCP RTT from this box to several AWS us-east-2
// endpoints. If everything sits at ~1.1ms, the overhead is local (container
// overlay / NAT egress) and a plain EC2 box removes it. If AWS services answer
// in ~0.3ms while the sequencer takes 1.15ms, the distance is real and only
// AZ placement helps.
import net from "node:net";
import dns from "node:dns/promises";
import http from "node:http";
import os from "node:os";
import { performance } from "node:perf_hooks";

const TARGETS = [
  ["sequencer (robinhood)", "sequencer.mainnet.chain.robinhood.com", 443],
  ["s3.us-east-2", "s3.us-east-2.amazonaws.com", 443],
  ["dynamodb.us-east-2", "dynamodb.us-east-2.amazonaws.com", 443],
  ["ec2.us-east-2", "ec2.us-east-2.amazonaws.com", 443],
  ["sqs.us-east-2", "sqs.us-east-2.amazonaws.com", 443],
  ["kinesis.us-east-2", "kinesis.us-east-2.amazonaws.com", 443],
  ["ec2.us-east-1 (Virginia)", "ec2.us-east-1.amazonaws.com", 443],
  ["ec2.us-west-2 (Oregon)", "ec2.us-west-2.amazonaws.com", 443],
];

const pct = (a, p) => [...a].sort((x, y) => x - y)[Math.floor(a.length * p)];
const f = (n) => (Number.isFinite(n) ? n.toFixed(3) : "n/a").padStart(7);

function tcpRtt(host, port) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const s = net.connect({ host, port });
    s.once("connect", () => { const ms = performance.now() - t0; s.destroy(); resolve(ms); });
    s.once("error", () => resolve(NaN));
    s.setTimeout(5000, () => { s.destroy(); resolve(NaN); });
  });
}

async function run() {
  const L = [];
  const out = (s) => { console.log(s); L.push(s); };
  out(`===== Is the box or the distance to blame? @ ${new Date().toISOString()} =====`);
  out(`host ${os.hostname()} | ${os.cpus().length} cpu | ${os.platform()} ${os.release()}`);
  out(`load ${os.loadavg().map((n) => n.toFixed(2)).join(" ")} | mem ${(os.totalmem() / 1e9).toFixed(1)}GB\n`);

  out(`target                          resolved IP        TCP RTT (25 samples)`);
  out(`${"-".repeat(78)}`);

  for (const [label, host, port] of TARGETS) {
    let ip = host;
    try { const a = await dns.resolve4(host); ip = a[0]; } catch {}
    const t = [];
    for (let i = 0; i < 25; i++) { const v = await tcpRtt(ip, port); if (Number.isFinite(v)) t.push(v); }
    if (t.length === 0) { out(`${label.padEnd(31)} ${ip.padEnd(18)} unreachable`); continue; }
    out(`${label.padEnd(31)} ${ip.padEnd(18)} min ${f(Math.min(...t))}ms  p50 ${f(pct(t,0.5))}ms`);
  }

  out(`\nReading it:`);
  out(`  AWS us-east-2 services at ~1.1ms  → the box adds fixed overhead; EC2 will be faster.`);
  out(`  AWS us-east-2 services at ~0.3ms  → the sequencer is genuinely further; pick the right AZ.`);
  out(`  us-east-1/us-west-2 give a distance yardstick (Virginia ~12ms, Oregon ~50ms).`);
  return L.join("\n");
}

let report = "running...";
run().then((r) => { report = r; }).catch((e) => { report = "error: " + (e.stack || e.message); });
http.createServer((_q, s) => { s.writeHead(200, { "content-type": "text/plain" }); s.end(report); })
  .listen(process.env.PORT || 10000);
