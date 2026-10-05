#!/usr/bin/env node
/**
 * Seed script — populates the local demo with stations, trains, routes and
 * schedules so there is something to search and book.
 *
 * Everything goes through the API Gateway (port 4000), never the database
 * directly, so the Kafka pipeline (admin -> inventory -> search) fires exactly
 * as it does in normal use. That means Elasticsearch ends up populated too.
 *
 * Usage:
 *   node scripts/seed.js
 *   node scripts/seed.js --clean      # wipe existing demo data first
 *
 * Auth: it logs in with SEED_EMAIL / SEED_PASSWORD (defaults below). If that
 * user does not exist yet it registers it, reading the OTP off Kafka the same
 * way user-service publishes it.
 */

const path = require('path');

// kafkajs lives in notification-service/node_modules; this script sits at the
// repo root, so resolve it explicitly rather than pulling in a second copy.
const kafkajsPath = path.join(__dirname, '..', 'notification-service', 'node_modules', 'kafkajs');
let Kafka = null;
try { ({ Kafka } = require(kafkajsPath)); } catch { /* OTP tap unavailable */ }

const GATEWAY = process.env.GATEWAY_URL || 'http://localhost:4000/api';
const KAFKA_BROKER = process.env.KAFKA_BROKER || '127.0.0.1:9093';

const SEED_EMAIL = process.env.SEED_EMAIL || 'demo.admin@example.com';
const SEED_PASSWORD = process.env.SEED_PASSWORD || 'DemoPass123!';

const CLEAN = process.argv.includes('--clean');

// ---------------------------------------------------------------------------
// tiny cookie-aware fetch
// ---------------------------------------------------------------------------
let cookieJar = {};

function cookieHeader() {
  return Object.entries(cookieJar).map(([k, v]) => `${k}=${v}`).join('; ');
}

function absorbCookies(res) {
  const raw = res.headers.getSetCookie ? res.headers.getSetCookie() : [];
  for (const c of raw) {
    const [pair] = c.split(';');
    const idx = pair.indexOf('=');
    if (idx > 0) cookieJar[pair.slice(0, idx).trim()] = pair.slice(idx + 1).trim();
  }
}

async function api(method, route, body) {
  const res = await fetch(`${GATEWAY}${route}`, {
    method,
    headers: {
      'Content-Type': 'application/json',
      ...(Object.keys(cookieJar).length ? { Cookie: cookieHeader() } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  absorbCookies(res);
  const text = await res.text();
  let json;
  try { json = text ? JSON.parse(text) : {}; } catch { json = { raw: text }; }
  return { status: res.status, ok: res.ok, body: json };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// auth
// ---------------------------------------------------------------------------
async function tryLogin() {
  const r = await api('POST', '/users/auth/login', { email: SEED_EMAIL, password: SEED_PASSWORD });
  return r.ok;
}

/**
 * Find the newest OTP published for `email` on the topic.
 *
 * Reads from the BEGINNING of the topic with a throwaway group, so it does not
 * matter whether the message was published before or after we started
 * listening — replaying history removes the race. Takes the LAST match, since
 * that is the most recent send.
 */
async function tapOtp(email, windowMs = 10000) {
  if (!Kafka) throw new Error('kafkajs not found — cannot auto-register');
  const kafka = new Kafka({ clientId: 'seed-otp-tap', brokers: [KAFKA_BROKER], logLevel: 0 });
  const consumer = kafka.consumer({ groupId: `seed-otp-${Date.now()}-${process.pid}` });
  await consumer.connect();
  await consumer.subscribe({ topic: 'notification.otp-email', fromBeginning: true });

  let found = null;
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const payload = JSON.parse(message.value.toString());
        if (payload.email === email && payload.otp) found = payload.otp;
      } catch { /* ignore malformed */ }
    },
  });

  const deadline = Date.now() + windowMs;
  while (!found && Date.now() < deadline) await sleep(200);
  await sleep(400); // settle, in case a newer send lands right behind
  await consumer.disconnect().catch(() => {});

  if (!found) throw new Error(`no OTP for ${email} found on topic`);
  return found;
}

async function registerViaOtp() {
  console.log(`  registering ${SEED_EMAIL}`);
  // Send first, then read — the tap replays from the beginning so ordering
  // does not matter, and we avoid the consumer-group join race.
  const sent = await api('POST', '/users/auth/send-otp', {
    firstName: 'Demo', lastName: 'Admin',
    email: SEED_EMAIL, password: SEED_PASSWORD, confirmPassword: SEED_PASSWORD,
  });
  if (!sent.ok) throw new Error(`send-otp failed: ${JSON.stringify(sent.body)}`);

  const otp = await tapOtp(SEED_EMAIL);
  const verified = await api('POST', '/users/auth/verify-otp', { otp });
  if (!verified.ok) throw new Error(`verify-otp failed: ${JSON.stringify(verified.body)}`);
}

async function authenticate() {
  console.log('>> auth');
  if (await tryLogin()) { console.log('  logged in as existing user'); return; }
  await registerViaOtp();
  if (!(await tryLogin())) throw new Error('registered but login still failed');
  console.log('  registered + logged in');
}

// ---------------------------------------------------------------------------
// demo data
// ---------------------------------------------------------------------------
const STATIONS = [
  { name: 'New Delhi',       code: 'NDLS', city: 'Delhi',     state: 'Delhi' },
  { name: 'Mumbai Central',  code: 'MMCT', city: 'Mumbai',    state: 'Maharashtra' },
  { name: 'Howrah Junction', code: 'HWH',  city: 'Kolkata',   state: 'West Bengal' },
  { name: 'Chennai Central', code: 'MAS',  city: 'Chennai',   state: 'Tamil Nadu' },
  { name: 'KSR Bengaluru',   code: 'SBC',  city: 'Bengaluru', state: 'Karnataka' },
  { name: 'Pune Junction',   code: 'PUNE', city: 'Pune',      state: 'Maharashtra' },
];

const SEAT_TYPES = ['LOWER', 'MIDDLE', 'UPPER', 'SIDE_LOWER', 'SIDE_UPPER'];

function buildSeats(count, basePrice) {
  return Array.from({ length: count }, (_, i) => ({
    seatNumber: i + 1,
    seatType: SEAT_TYPES[i % SEAT_TYPES.length],
    price: basePrice + (i % SEAT_TYPES.length) * 50,
  }));
}

const TRAINS = [
  {
    trainNumber: '12301', trainName: 'Rajdhani Express', coachName: 'AC',
    seats: buildSeats(12, 1500),
    route: [
      { code: 'NDLS', sequenceNumber: 1, arrivalTime: null,    departureTime: '16:00', distanceFromOrigin: 0 },
      { code: 'PUNE', sequenceNumber: 2, arrivalTime: '04:00', departureTime: '04:10', distanceFromOrigin: 1400 },
      { code: 'MMCT', sequenceNumber: 3, arrivalTime: '08:00', departureTime: null,    distanceFromOrigin: 1600 },
    ],
  },
  {
    trainNumber: '12009', trainName: 'Shatabdi Express', coachName: 'CC',
    seats: buildSeats(10, 900),
    route: [
      { code: 'SBC', sequenceNumber: 1, arrivalTime: null,    departureTime: '06:00', distanceFromOrigin: 0 },
      { code: 'MAS', sequenceNumber: 2, arrivalTime: '11:30', departureTime: null,    distanceFromOrigin: 360 },
    ],
  },
  {
    trainNumber: '12841', trainName: 'Coromandel Express', coachName: 'SL',
    seats: buildSeats(14, 700),
    route: [
      { code: 'HWH',  sequenceNumber: 1, arrivalTime: null,    departureTime: '14:50', distanceFromOrigin: 0 },
      { code: 'MAS',  sequenceNumber: 2, arrivalTime: '09:00', departureTime: '09:15', distanceFromOrigin: 1660 },
      { code: 'SBC',  sequenceNumber: 3, arrivalTime: '14:00', departureTime: null,    distanceFromOrigin: 1960 },
    ],
  },
];

function futureDates(days) {
  const out = [];
  const now = new Date();
  for (let d = 1; d <= days; d++) {
    const dt = new Date(now.getTime() + d * 86400000);
    out.push(dt.toISOString().slice(0, 10));
  }
  return out;
}

// ---------------------------------------------------------------------------
// seeding
// ---------------------------------------------------------------------------
async function seedStations() {
  console.log('>> stations');
  const byCode = {};
  const byName = {};
  const existing = await api('GET', '/admins/stations/station');
  for (const s of existing.body?.data?.stations || existing.body?.data || []) {
    if (s?.code) byCode[s.code] = s.id;
    if (s?.name) byName[s.name] = s.id;
  }

  for (const st of STATIONS) {
    // Match on code OR name. Stations may pre-exist under a different code
    // (e.g. Mumbai Central is MMCT, not the older BCT), and the service returns
    // a 500 rather than a 409 on a duplicate, so we must avoid the collision.
    const hit = byCode[st.code] || byName[st.name];
    if (hit) { byCode[st.code] = hit; console.log(`   = ${st.code} exists (as "${st.name}")`); continue; }
    const r = await api('POST', '/admins/stations/station', st);
    if (r.ok) {
      byCode[st.code] = r.body.data.id;
      console.log(`   + ${st.code} ${st.name}`);
    } else {
      console.log(`   ! ${st.code} — ${JSON.stringify(r.body)}`);
    }
  }
  return byCode;
}

async function seedTrain(t, stationIds) {
  const r = await api('POST', '/admins/trains/train', {
    trainNumber: t.trainNumber, trainName: t.trainName,
    coachName: t.coachName, seats: t.seats,
  });
  if (!r.ok) {
    if (r.status === 409) { console.log(`   = ${t.trainNumber} exists`); return null; }
    console.log(`   ! ${t.trainNumber} — ${JSON.stringify(r.body)}`);
    return null;
  }
  const trainId = r.body.data.id;
  console.log(`   + ${t.trainNumber} ${t.trainName} (${t.seats.length} seats)`);

  const stations = t.route.map((leg) => ({
    stationId: stationIds[leg.code],
    sequenceNumber: leg.sequenceNumber,
    arrivalTime: leg.arrivalTime,
    departureTime: leg.departureTime,
    distanceFromOrigin: leg.distanceFromOrigin,
  })).filter((s) => s.stationId);

  const route = await api('POST', '/admins/trains/route', { trainId, stations });
  console.log(route.ok
    ? `     route: ${stations.map((s) => Object.keys(stationIds).find((k) => stationIds[k] === s.stationId)).join(' -> ')}`
    : `     ! route failed: ${JSON.stringify(route.body)}`);

  return trainId;
}

async function seedSchedules(trainId) {
  for (const departureDate of futureDates(7)) {
    const r = await api('POST', '/admins/schedules/schedule', { trainId, departureDate });
    if (r.status === 409) continue;              // already scheduled
    if (r.status === 429) {                      // gateway burst limit
      console.log('     ~ rate limited — stopping schedule creation here');
      return;
    }
    if (!r.ok) console.log(`     ! schedule ${departureDate}: ${JSON.stringify(r.body)}`);
    await sleep(250);                            // pace requests under the burst limit
  }
}

async function main() {
  console.log('\n=== IRCTC demo seed ===\n');
  await authenticate();

  if (CLEAN) console.log('\n(--clean is not wired to a delete API; skipping wipe)\n');

  const stationIds = await seedStations();

  console.log('\n>> trains + routes');
  const trainIds = [];
  for (const t of TRAINS) {
    const id = await seedTrain(t, stationIds);
    if (id) trainIds.push(id);
  }

  console.log('\n>> schedules (next 7 days)');
  for (const id of trainIds) await seedSchedules(id);
  console.log(`   + schedules created for ${trainIds.length} train(s)`);

  console.log('\n>> waiting for Kafka pipeline (admin -> inventory -> search)...');
  await sleep(6000);

  const search = await api('GET', '/search/trains?from=NDLS&to=MMCT');
  const found = search.body?.data?.count ?? 0;
  console.log(search.ok
    ? `   search NDLS->MMCT returned ${found} result(s)`
    : `   ! search check failed: ${JSON.stringify(search.body)}`);

  console.log('\n=== done ===');
  console.log(`Login: ${SEED_EMAIL} / ${SEED_PASSWORD}`);
  console.log('Frontend: http://localhost:3000\n');
}

main().catch((e) => { console.error('\nSEED FAILED:', e.message); process.exit(1); });
