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

const KNOWN_BOT_BUDDY_IDS = new Set([
  '1001647',
  '976217',
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
  return { uid };
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
  return { db };
}

function createLocalDateRange(nowIso, days) {
  const now = new Date(nowIso);
  if (Number.isNaN(now.getTime())) {
    throw new Error('Invalid --now timestamp for SeaTalk local export.');
  }
  const periodStart = new Date(now.getTime());
  periodStart.setHours(0, 0, 0, 0);
  periodStart.setDate(periodStart.getDate() - (days - 1));
  const periodEnd = new Date(periodStart.getTime());
  periodEnd.setDate(periodEnd.getDate() + days);
  return {
    now,
    periodStart,
    periodEnd,
    periodStartEpoch: Math.floor(periodStart.getTime() / 1000),
    periodEndEpoch: Math.floor(periodEnd.getTime() / 1000),
  };
}

function safeParseJson(value) {
  if (!value || typeof value !== 'string') return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function visitNames(obj, rememberUid, rememberSid, sid) {
  if (!obj || typeof obj !== 'object') return;
  if (Array.isArray(obj)) {
    obj.forEach((item) => visitNames(item, rememberUid, rememberSid, sid));
    return;
  }
  if ((obj.uid || obj.u) && typeof obj.n === 'string') {
    rememberUid(String(obj.uid || obj.u), obj.n);
  }
  if (obj.ni && typeof obj.ni.n === 'string') rememberSid(sid, obj.ni.n);
  if (obj.oi && typeof obj.oi.n === 'string') rememberSid(sid, obj.oi.n);
  Object.values(obj).forEach((value) => visitNames(value, rememberUid, rememberSid, sid));
}

function flattenRichText(parsed) {
  const lines = [];
  if (!parsed || !parsed.f || !Array.isArray(parsed.f.e)) return lines;
  for (const block of parsed.f.e) {
    if (!block || !Array.isArray(block.e)) continue;
    const text = block.e
      .map((part) => {
        if (!part) return '';
        if (typeof part.tx === 'string') return part.tx;
        return '';
      })
      .join('');
    if (text) lines.push(text);
  }
  return lines;
}

function extractText(row, parsed) {
  if (row.t === 'image') return '[image]';
  if (row.t === 'video') return '[video]';
  if (row.t === 'file') return '[file]';
  if (row.t === 'sticker.c') return '[sticker]';
  if (row.t && row.t !== 'text') return `[${row.t}]`;
  if (parsed && typeof parsed.c === 'string' && parsed.c.trim()) return parsed.c.trim();
  const richLines = flattenRichText(parsed);
  if (richLines.length) return richLines.join('\n').trim();
  return '[empty message]';
}

function buildNameMaps(rows) {
  const uidNames = new Map();
  const sidNames = new Map();
  const rememberUid = (uid, name) => {
    if (!uid || !name) return;
    const trimmed = String(name).trim();
    if (!trimmed) return;
    const current = uidNames.get(uid);
    if (!current || trimmed.length > current.length) uidNames.set(uid, trimmed);
  };
  const rememberSid = (sid, name) => {
    if (!sid || !name) return;
    const trimmed = String(name).trim();
    if (!trimmed) return;
    const current = sidNames.get(sid);
    if (!current || trimmed.length > current.length) sidNames.set(sid, trimmed);
  };

  for (const row of rows) {
    const parsed = safeParseJson(row.c);
    const quoted = safeParseJson(row.q);
    visitNames(parsed, rememberUid, rememberSid, row.sid);
    visitNames(quoted, rememberUid, rememberSid, row.sid);
  }

  for (const row of rows) {
    if (row.sid.startsWith('buddy-')) {
      const buddyUid = row.sid.slice('buddy-'.length);
      if (uidNames.has(buddyUid)) sidNames.set(row.sid, uidNames.get(buddyUid));
    }
  }
  return { uidNames, sidNames };
}

function formatTimestamp(epochSeconds) {
  const date = new Date(Number(epochSeconds) * 1000);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hour = String(date.getHours()).padStart(2, '0');
  const minute = String(date.getMinutes()).padStart(2, '0');
  const second = String(date.getSeconds()).padStart(2, '0');
  return `${year}-${month}-${day} ${hour}:${minute}:${second}`;
}

function buildHistoryText(rows, selfUid, days, nowIso) {
  const { uidNames, sidNames } = buildNameMaps(rows);
  const filteredRows = rows.filter((row) => !isBotConversationRow(row, sidNames));
  const lines = [
    'SeaTalk Chat History Export',
    `Window: last ${days} days`,
    `Generated at: ${nowIso}`,
    `Includes thread replies when they are stored as regular message rows in the local SeaTalk database.`,
    'Private SeaTalk bot conversations are excluded from this export.',
    '',
  ];
  let currentConversation = '';
  for (const row of filteredRows) {
    const parsed = safeParseJson(row.c);
    const conversationName = sidNames.get(row.sid) || row.sid;
    const senderName = uidNames.get(String(row.u)) || (String(row.u) === String(selfUid) ? 'Zheng Xiaodong' : `UID ${row.u}`);
    const text = extractText(row, parsed);
    if (conversationName !== currentConversation) {
      currentConversation = conversationName;
      lines.push(`=== ${conversationName} (${row.sid}) ===`);
    }
    const normalizedText = text.split('\n').map((part, index) => (index === 0 ? part : `    ${part}`)).join('\n');
    lines.push(`[${formatTimestamp(row.ts)}] ${senderName}: ${normalizedText}`);
  }
  return `${lines.join('\n')}\n`;
}

function isBotConversationRow(row, sidNames) {
  if (!row.sid || !row.sid.startsWith('buddy-')) return false;
  const buddyUid = row.sid.slice('buddy-'.length);
  if (KNOWN_BOT_BUDDY_IDS.has(buddyUid)) return true;
  const conversationName = String(sidNames.get(row.sid) || '').toLowerCase();
  if (!conversationName) return false;
  return ['bot', 'assistant', 'chatbot', 'copilot', 'ai '].some((keyword) => conversationName.includes(keyword));
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.dataDir) {
    throw new Error('SeaTalk local export requires --data-dir.');
  }
  const { uid } = loadLocalConfig(args.dataDir);
  const { db } = loadDatabase(args.dataDir, uid);
  const ranges = createLocalDateRange(args.now, args.days);
  try {
    const excludedTypes = Array.from(EXCLUDED_MESSAGE_TYPES);
    const placeholders = excludedTypes.map(() => '?').join(', ');
    const rows = db.prepare(`
      SELECT sid, ts, t, u, c, q
      FROM chat_message_view
      WHERE ts >= ? AND ts < ?
        AND (sid LIKE 'group-%' OR sid LIKE 'buddy-%')
        AND t NOT IN (${placeholders})
      ORDER BY sid ASC, ts ASC, mid ASC
    `).all(ranges.periodStartEpoch, ranges.periodEndEpoch, ...excludedTypes);
    process.stdout.write(buildHistoryText(rows, uid, args.days, args.now));
  } finally {
    db.close();
  }
}

try {
  main();
} catch (error) {
  process.stderr.write(`${error.message || String(error)}\n`);
  process.exit(1);
}
