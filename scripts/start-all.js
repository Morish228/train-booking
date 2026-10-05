#!/usr/bin/env node
/**
 * Start the entire local stack in one command.
 *
 *   npm run start:all          # infra must already be up (docker compose up -d)
 *   npm run start:all -- --frontend
 *
 * Runs the 8 backend services as child processes, prefixing each line of output
 * with the service name so one terminal is enough. Ctrl+C stops everything.
 *
 * Start order matters: Kafka consumers first, then producers, then the gateway.
 */

const { spawn } = require('child_process');
const path = require('path');
const net = require('net');

const ROOT = path.join(__dirname, '..');
const WITH_FRONTEND = process.argv.includes('--frontend');

// Order is deliberate — consumers before producers, gateway last.
const SERVICES = [
  { name: 'admin-service',        dir: 'admin-service' },
  { name: 'inventory-service',    dir: 'inventory-service' },
  { name: 'search-service',       dir: 'search-service' },
  { name: 'user-service',         dir: 'user-service' },
  { name: 'payment-service',      dir: 'payment-service' },
  { name: 'booking-service',      dir: 'booking-service' },
  { name: 'notification-service', dir: 'notification-service' },
  { name: 'api-gateway',          dir: 'api-gateway' },
];

const COLORS = ['36', '35', '33', '32', '34', '31', '95', '96'];
const INFRA = [
  { name: 'postgres',      host: '127.0.0.1', port: 5432 },
  { name: 'redis',         host: '127.0.0.1', port: 6379 },
  { name: 'kafka',         host: '127.0.0.1', port: 9093 },
  { name: 'elasticsearch', host: '127.0.0.1', port: 9200 },
];

function probe({ host, port }, timeout = 1500) {
  return new Promise((resolve) => {
    const sock = new net.Socket();
    const done = (ok) => { sock.destroy(); resolve(ok); };
    sock.setTimeout(timeout);
    sock.once('connect', () => done(true));
    sock.once('timeout', () => done(false));
    sock.once('error', () => done(false));
    sock.connect(port, host);
  });
}

async function checkInfra() {
  const results = await Promise.all(INFRA.map(async (i) => ({ ...i, up: await probe(i) })));
  const down = results.filter((r) => !r.up);
  if (down.length) {
    console.log('\n  Infrastructure is not fully up:\n');
    for (const r of results) {
      console.log(`    ${r.up ? ' OK ' : 'DOWN'}  ${r.name.padEnd(14)} ${r.host}:${r.port}`);
    }
    console.log('\n  Start it first:\n');
    console.log('    docker compose up -d\n');
    if (down.length === results.length) process.exit(1);
    console.log('  Continuing anyway — some services may fail to connect.\n');
  }
}

const children = [];

function launch({ name, dir }, colorIndex) {
  const color = COLORS[colorIndex % COLORS.length];
  const child = spawn('npm', ['start'], {
    cwd: path.join(ROOT, dir),
    shell: true,
    env: process.env,
  });
  children.push(child);

  const prefix = `\x1b[${color}m[${name.padEnd(20)}]\x1b[0m`;
  const pipe = (stream) => {
    let buf = '';
    stream.on('data', (chunk) => {
      buf += chunk.toString();
      const lines = buf.split('\n');
      buf = lines.pop();
      for (const line of lines) {
        if (line.trim()) console.log(`${prefix} ${line}`);
      }
    });
  };
  pipe(child.stdout);
  pipe(child.stderr);

  child.on('exit', (code) => {
    if (code !== 0 && code !== null) {
      console.log(`${prefix} \x1b[31mexited with code ${code}\x1b[0m`);
    }
  });
}

function shutdown() {
  console.log('\n  Stopping all services...');
  for (const c of children) { try { c.kill(); } catch { /* already gone */ } }
  setTimeout(() => process.exit(0), 500);
}

async function main() {
  console.log('\n=== RailBook — starting local stack ===\n');
  await checkInfra();

  console.log('  Starting services (Ctrl+C to stop everything):\n');
  SERVICES.forEach((s, i) => { launch(s, i); });

  if (WITH_FRONTEND) {
    setTimeout(() => {
      console.log('  Starting frontend on http://localhost:3000\n');
      launch({ name: 'frontend', dir: 'frontend' }, SERVICES.length);
    }, 4000);
  } else {
    console.log('\n  Frontend not started. Add --frontend, or run it separately:\n');
    console.log('    cd frontend && npm run dev\n');
  }

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main();
