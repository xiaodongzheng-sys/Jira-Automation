const fs = require('fs');
const path = require('path');

const EXCLUDED_MESSAGE_TYPES = new Set([
  'c.g.c.i',
  'c.g.m',
  'c.g.i.j',
  'history',
  'sys.c.g.u.p',
  'sys.c.g.u.sr',
  'sys.c.b.s',
  'c.b.n',
  'c.g.r',
  'c.g.u.n',
]);

function parseArgs(argv) {
  const args = { dataDir: '', days: 7, now: new Date().toISOString() };
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    const value = argv[index + 1];
    if (token === '--data-dir') {
      args.dataDir = value || '';
      index += 1;
    } else if (token === '--days') {
      args.days = Number(value || '7');
      index += 1;
    } else if (token === '--now') {
      args.now = value || args.now;
      index += 1;
    }
  }
  return args;
}

function loadLocalConfig(dataDir) {
  const configPath = path.join(dataDir, 'config.json');
  if (!fs.existsSync(configPath)) {
    throw new Error(`SeaTalk desktop config was not found at ${configPath}.`);
  }
  const payload = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  const uid = String(payload.LAST_LOGIN_USER_ID || '').trim();
  if (!uid) {
    throw new Error('SeaTalk desktop config does not include LAST_LOGIN_USER_ID.');
  }
  return { configPath, uid };
}

function buildSeries(periodStart, days, sparseCounts) {
  const rows = [];
  for (let offset = 0; offset < days; offset += 1) {
    const current = new Date(periodStart.getTime());
    current.setDate(periodStart.getDate() + offset);
    const isoDate = formatLocalIsoDate(current);
    rows.push({
      date: isoDate,
      label: current.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      count: Number(sparseCounts.get(isoDate) || 0),
    });
  }
  return rows;
}

function formatLocalIsoDate(value) {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, '0');
  const day = String(value.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function createLocalDateRange(nowIso, days) {
  const now = new Date(nowIso);
  if (Number.isNaN(now.getTime())) {
    throw new Error('Invalid --now timestamp for SeaTalk local metrics.');
  }
  const periodStart = new Date(now.getTime());
  periodStart.setHours(0, 0, 0, 0);
  periodStart.setDate(periodStart.getDate() - (days - 1));
  const periodEnd = new Date(periodStart.getTime());
  periodEnd.setDate(periodEnd.getDate() + days);
  const todayStart = new Date(now.getTime());
  todayStart.setHours(0, 0, 0, 0);
  return {
    now,
    periodStart,
    periodEnd,
    todayStart,
    periodStartEpoch: Math.floor(periodStart.getTime() / 1000),
    periodEndEpoch: Math.floor(periodEnd.getTime() / 1000),
    todayStartEpoch: Math.floor(todayStart.getTime() / 1000),
  };
}

function loadDatabase(dataDir, uid) {
  const appResources = '/Applications/SeaTalk.app/Contents/Resources';
  const Database = require(path.join(appResources, '2_9_3_bundle.asar/node_modules/better-sqlite3-multiple-ciphers/lib/database'));
  const nativeBinding = path.join(
    appResources,
    '2_9_3_bundle.asar.unpacked/node_modules/better-sqlite3-multiple-ciphers/build/Release/better_sqlite3.node',
  );
  const dbPath = path.join(dataDir, `main_${uid}.sqlite`);
  if (!fs.existsSync(dbPath)) {
    throw new Error(`SeaTalk desktop database was not found at ${dbPath}.`);
  }
  const db = new Database(dbPath, { readonly: true, fileMustExist: true, nativeBinding });
  db.pragma(`key='40a3884b8b032e6f${uid}'`);
  db.pragma('journal_mode=WAL');
  return { db, dbPath };
}

function createMessageFilterSql() {
  const placeholders = Array.from(EXCLUDED_MESSAGE_TYPES).map(() => '?').join(', ');
  return `
    (sid LIKE 'group-%' OR sid LIKE 'buddy-%')
    AND t NOT IN (${placeholders})
  `;
}

function collectCounts(db, selfUid, ranges) {
  const filterSql = createMessageFilterSql();
  const excludedTypes = Array.from(EXCLUDED_MESSAGE_TYPES);
  const totalReceived = db.prepare(`
    SELECT COUNT(*) AS count
    FROM chat_message
    WHERE ts >= ? AND ts < ?
      AND u != ?
      AND ${filterSql}
  `).get(ranges.periodStartEpoch, ranges.periodEndEpoch, selfUid, ...excludedTypes).count;
  const totalSent = db.prepare(`
    SELECT COUNT(*) AS count
    FROM chat_message
    WHERE ts >= ? AND ts < ?
      AND u = ?
      AND ${filterSql}
  `).get(ranges.periodStartEpoch, ranges.periodEndEpoch, selfUid, ...excludedTypes).count;
  const receivedToday = db.prepare(`
    SELECT COUNT(*) AS count
    FROM chat_message
    WHERE ts >= ? AND ts < ?
      AND u != ?
      AND ${filterSql}
  `).get(ranges.todayStartEpoch, ranges.periodEndEpoch, selfUid, ...excludedTypes).count;
  const currentUnread = db.prepare(`
    SELECT COALESCE(SUM(unreadCount), 0) AS count
    FROM session_info
    WHERE sid LIKE 'group-%' OR sid LIKE 'buddy-%'
  `).get().count;
  const inboundRows = db.prepare(`
    SELECT strftime('%Y-%m-%d', ts, 'unixepoch', 'localtime') AS day, COUNT(*) AS count
    FROM chat_message
    WHERE ts >= ? AND ts < ?
      AND u != ?
      AND ${filterSql}
    GROUP BY day
    ORDER BY day
  `).all(ranges.periodStartEpoch, ranges.periodEndEpoch, selfUid, ...excludedTypes);
  const outboundRows = db.prepare(`
    SELECT strftime('%Y-%m-%d', ts, 'unixepoch', 'localtime') AS day, COUNT(*) AS count
    FROM chat_message
    WHERE ts >= ? AND ts < ?
      AND u = ?
      AND ${filterSql}
    GROUP BY day
    ORDER BY day
  `).all(ranges.periodStartEpoch, ranges.periodEndEpoch, selfUid, ...excludedTypes);
  return {
    receivedToday: Number(receivedToday || 0),
    currentUnread: Number(currentUnread || 0),
    totalReceived: Number(totalReceived || 0),
    totalSent: Number(totalSent || 0),
    inboundRows,
    outboundRows,
  };
}

function computeEstimatedReadRate(receivedToday, currentUnread) {
  const received = Number(receivedToday || 0);
  const unread = Number(currentUnread || 0);
  if (received <= 0) return null;
  const estimatedRead = Math.max(0, received - unread);
  return Math.round((estimatedRead / received) * 100);
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.dataDir) {
    throw new Error('SeaTalk local metrics requires --data-dir.');
  }
  const ranges = createLocalDateRange(args.now, args.days);
  const { uid } = loadLocalConfig(args.dataDir);
  const { db } = loadDatabase(args.dataDir, uid);
  try {
    const counts = collectCounts(db, Number(uid), ranges);
    const inboundMap = new Map(counts.inboundRows.map((row) => [row.day, Number(row.count || 0)]));
    const outboundMap = new Map(counts.outboundRows.map((row) => [row.day, Number(row.count || 0)]));
    const estimatedReadRate = computeEstimatedReadRate(counts.receivedToday, counts.currentUnread);
    const payload = {
      summary: {
        received_today: counts.receivedToday,
        current_unread: counts.currentUnread,
        read_rate_percent: estimatedReadRate,
        received_period_total: counts.totalReceived,
        sent_period_total: counts.totalSent,
      },
      trends: {
        received: buildSeries(ranges.periodStart, args.days, inboundMap),
        sent: buildSeries(ranges.periodStart, args.days, outboundMap),
      },
      metric_availability: {
        current_unread: { available: true, reason: '' },
        read_rate_percent: estimatedReadRate === null
          ? { available: false, reason: 'No inbound SeaTalk messages were received today.' }
          : { available: true, reason: 'Estimated as (Received Today - Current Unread) / Received Today.' },
      },
      generated_at: args.now,
      period_days: args.days,
      data_quality: {
        used_fallback_cache: false,
        partial_data: false,
        status_note: 'Live SeaTalk metrics loaded from local SeaTalk desktop data on this Mac. Read Rate is an estimate based on today received volume and the current unread snapshot.',
        source_scope: 'Local SeaTalk desktop direct and group conversations for the signed-in desktop account.',
        current_account_uid: uid,
      },
    };
    process.stdout.write(JSON.stringify(payload));
  } finally {
    db.close();
  }
}

const UNAVAILABLE_REASON = 'Not available from local SeaTalk desktop data for this scope.';

try {
  main();
} catch (error) {
  process.stderr.write(`${error.message || String(error)}\n`);
  process.exit(1);
}
