const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const Database = require('better-sqlite3');

// ===== CONFIG =====
const PORT = process.env.PORT || 3000;
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';
const INIT_DATA_MAX_AGE = 86400; // initData valid for 24 hours
const MAX_BODY_SIZE = 4096; // max request body in bytes

// ===== SQLite DATABASE =====
const DB_PATH = path.join(__dirname, 'tapcoin.db');
const db = new Database(DB_PATH);

// Enable WAL mode for better performance
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

// ===== CREATE TABLES =====
db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    user_name TEXT DEFAULT 'Player',
    coins INTEGER DEFAULT 0,
    total_taps INTEGER DEFAULT 0,
    level INTEGER DEFAULT 1,
    tap_power INTEGER DEFAULT 1,
    max_energy INTEGER DEFAULT 1000,
    energy_regen INTEGER DEFAULT 3,
    last_tap INTEGER DEFAULT 0,
    created_at INTEGER DEFAULT (strftime('%s','now') * 1000),
    last_energy_update INTEGER DEFAULT (strftime('%s','now') * 1000),
    energy REAL DEFAULT 1000
  );

  CREATE TABLE IF NOT EXISTS boosts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    boost_id TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    type TEXT NOT NULL,
    effect_value INTEGER NOT NULL,
    cost INTEGER NOT NULL,
    max_level INTEGER DEFAULT 10,
    cost_multiplier REAL DEFAULT 1.8,
    icon TEXT DEFAULT '⚡'
  );

  CREATE TABLE IF NOT EXISTS user_boosts (
    user_id TEXT NOT NULL,
    boost_id TEXT NOT NULL,
    level INTEGER DEFAULT 0,
    purchased_at INTEGER DEFAULT (strftime('%s','now') * 1000),
    PRIMARY KEY (user_id, boost_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (boost_id) REFERENCES boosts(boost_id)
  );

  CREATE TABLE IF NOT EXISTS quests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    quest_id TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    type TEXT NOT NULL,
    target INTEGER NOT NULL,
    reward_coins INTEGER DEFAULT 0,
    reward_tap_power INTEGER DEFAULT 0,
    icon TEXT DEFAULT '🎯',
    sort_order INTEGER DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS user_quests (
    user_id TEXT NOT NULL,
    quest_id TEXT NOT NULL,
    progress INTEGER DEFAULT 0,
    completed INTEGER DEFAULT 0,
    claimed INTEGER DEFAULT 0,
    completed_at INTEGER DEFAULT 0,
    PRIMARY KEY (user_id, quest_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (quest_id) REFERENCES quests(quest_id)
  );

  CREATE TABLE IF NOT EXISTS anti_cheat_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    details TEXT,
    timestamp INTEGER DEFAULT (strftime('%s','now') * 1000)
  );
`);

// ===== SEED DEFAULT BOOSTS =====
const defaultBoosts = [
  { boost_id: 'tap_power', name: 'Tap Power', description: 'Увеличивает силу тапа на +1', type: 'tap_power', effect_value: 1, cost: 100, max_level: 20, cost_multiplier: 1.8, icon: '👆' },
  { boost_id: 'energy_max', name: 'Energy Tank', description: 'Увеличивает максимум энергии на +500', type: 'max_energy', effect_value: 500, cost: 200, max_level: 10, cost_multiplier: 2.0, icon: '🔋' },
  { boost_id: 'energy_regen', name: 'Energy Regen', description: 'Ускоряет восстановление энергии на +1/сек', type: 'energy_regen', effect_value: 1, cost: 500, max_level: 10, cost_multiplier: 2.2, icon: '⚡' },
  { boost_id: 'multi_tap', name: 'Multi Tap', description: 'Дополнительная сила тапа +2', type: 'tap_power', effect_value: 2, cost: 1000, max_level: 10, cost_multiplier: 2.5, icon: '✌️' },
];

const insertBoost = db.prepare(`
  INSERT OR IGNORE INTO boosts (boost_id, name, description, type, effect_value, cost, max_level, cost_multiplier, icon)
  VALUES (@boost_id, @name, @description, @type, @effect_value, @cost, @max_level, @cost_multiplier, @icon)
`);

for (const b of defaultBoosts) {
  insertBoost.run(b);
}

// ===== SEED DEFAULT QUESTS =====
const defaultQuests = [
  { quest_id: 'taps_100', name: 'Новичок', description: 'Сделай 100 тапов', type: 'total_taps', target: 100, reward_coins: 50, reward_tap_power: 0, icon: '🐣', sort_order: 1 },
  { quest_id: 'taps_1000', name: 'Тапер', description: 'Сделай 1 000 тапов', type: 'total_taps', target: 1000, reward_coins: 200, reward_tap_power: 0, icon: '👆', sort_order: 2 },
  { quest_id: 'taps_10000', name: 'Маньяк кликов', description: 'Сделай 10 000 тапов', type: 'total_taps', target: 10000, reward_coins: 1000, reward_tap_power: 1, icon: '🔥', sort_order: 3 },
  { quest_id: 'taps_50000', name: 'Легенда', description: 'Сделай 50 000 тапов', type: 'total_taps', target: 50000, reward_coins: 5000, reward_tap_power: 2, icon: '🏆', sort_order: 4 },
  { quest_id: 'coins_1000', name: 'Копилка', description: 'Накопи 1 000 монет', type: 'coins', target: 1000, reward_coins: 100, reward_tap_power: 0, icon: '💰', sort_order: 5 },
  { quest_id: 'coins_10000', name: 'Богач', description: 'Накопи 10 000 монет', type: 'coins', target: 10000, reward_coins: 500, reward_tap_power: 1, icon: '💎', sort_order: 6 },
  { quest_id: 'coins_100000', name: 'Миллионер', description: 'Накопи 100 000 монет', type: 'coins', target: 100000, reward_coins: 5000, reward_tap_power: 3, icon: '👑', sort_order: 7 },
  { quest_id: 'level_3', name: 'Карьерист', description: 'Достигни 3 уровня', type: 'level', target: 3, reward_coins: 300, reward_tap_power: 0, icon: '📈', sort_order: 8 },
  { quest_id: 'level_5', name: 'Элита', description: 'Достигни 5 уровня', type: 'level', target: 5, reward_coins: 2000, reward_tap_power: 2, icon: '⭐', sort_order: 9 },
  { quest_id: 'boost_1', name: 'Первый буст', description: 'Купи любой буст', type: 'boosts_bought', target: 1, reward_coins: 150, reward_tap_power: 0, icon: '🚀', sort_order: 10 },
];

const insertQuest = db.prepare(`
  INSERT OR IGNORE INTO quests (quest_id, name, description, type, target, reward_coins, reward_tap_power, icon, sort_order)
  VALUES (@quest_id, @name, @description, @type, @target, @reward_coins, @reward_tap_power, @icon, @sort_order)
`);

for (const q of defaultQuests) {
  insertQuest.run(q);
}

// ===== PREPARED STATEMENTS =====
const stmts = {
  getUser: db.prepare('SELECT * FROM users WHERE user_id = ?'),
  createUser: db.prepare(`
    INSERT OR IGNORE INTO users (user_id, user_name, coins, total_taps, level, tap_power, max_energy, energy_regen, last_tap, energy, last_energy_update)
    VALUES (?, ?, 0, 0, 1, 1, 1000, 3, ?, 1000, ?)
  `),
  updateUserTap: db.prepare(`
    UPDATE users SET coins = ?, total_taps = ?, level = ?, tap_power = ?, user_name = ?, last_tap = ?, energy = ?, last_energy_update = ?, max_energy = ?, energy_regen = ?
    WHERE user_id = ?
  `),
  getLeaderboard: db.prepare('SELECT user_id, user_name, coins, level FROM users ORDER BY coins DESC LIMIT 100'),
  getUserBoosts: db.prepare('SELECT ub.boost_id, ub.level, b.* FROM user_boosts ub JOIN boosts b ON ub.boost_id = b.boost_id WHERE ub.user_id = ?'),
  getBoost: db.prepare('SELECT * FROM boosts WHERE boost_id = ?'),
  getUserBoost: db.prepare('SELECT * FROM user_boosts WHERE user_id = ? AND boost_id = ?'),
  upsertUserBoost: db.prepare(`
    INSERT INTO user_boosts (user_id, boost_id, level) VALUES (?, ?, 1)
    ON CONFLICT(user_id, boost_id) DO UPDATE SET level = level + 1, purchased_at = strftime('%s','now') * 1000
  `),
  getAllBoosts: db.prepare('SELECT * FROM boosts ORDER BY cost'),
  getAllQuests: db.prepare('SELECT * FROM quests ORDER BY sort_order'),
  getUserQuests: db.prepare('SELECT uq.*, q.* FROM quests q LEFT JOIN user_quests uq ON q.quest_id = uq.quest_id AND uq.user_id = ? ORDER BY q.sort_order'),
  upsertUserQuest: db.prepare(`
    INSERT INTO user_quests (user_id, quest_id, progress, completed, claimed)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(user_id, quest_id) DO UPDATE SET progress = ?, completed = ?, claimed = ?
  `),
  claimQuest: db.prepare(`
    UPDATE user_quests SET claimed = 1, completed_at = ? WHERE user_id = ? AND quest_id = ? AND completed = 1 AND claimed = 0
  `),
  updateUserCoins: db.prepare('UPDATE users SET coins = ?, tap_power = ? WHERE user_id = ?'),
  logAntiCheat: db.prepare('INSERT INTO anti_cheat_log (user_id, event_type, details) VALUES (?, ?, ?)'),
  countUserBoosts: db.prepare('SELECT COUNT(*) as cnt FROM user_boosts WHERE user_id = ?'),
};

// ===== HELPERS =====
function getOrCreateUser(userId, userName) {
  let user = stmts.getUser.get(userId);
  if (!user) {
    const now = Date.now();
    stmts.createUser.run(userId, userName || 'Player', now, now);
    user = stmts.getUser.get(userId);
  }
  return user;
}

function calculateLevel(coins) {
  if (coins >= 1000000) return 7;
  if (coins >= 500000) return 6;
  if (coins >= 100000) return 5;
  if (coins >= 50000) return 4;
  if (coins >= 10000) return 3;
  if (coins >= 1000) return 2;
  return 1;
}

function calculateEnergy(user) {
  const now = Date.now();
  const elapsed = (now - user.last_energy_update) / 1000;
  const regen = user.energy_regen || 3;
  const maxEnergy = user.max_energy || 1000;
  const newEnergy = Math.min(user.energy + elapsed * regen, maxEnergy);
  return { energy: newEnergy, last_energy_update: now };
}

function getUserTapPower(userId) {
  const boosts = stmts.getUserBoosts.all(userId);
  let extraTap = 0;
  let extraMaxEnergy = 0;
  let extraRegen = 0;

  for (const b of boosts) {
    const level = b.level || 0;
    if (b.type === 'tap_power') extraTap += b.effect_value * level;
    if (b.type === 'max_energy') extraMaxEnergy += b.effect_value * level;
    if (b.type === 'energy_regen') extraRegen += b.effect_value * level;
  }

  return {
    tapPower: 1 + extraTap,
    maxEnergy: 1000 + extraMaxEnergy,
    energyRegen: 3 + extraRegen
  };
}

// ===== ANTI-CHEAT SYSTEM =====
const antiCheat = {
  tapHistory: new Map(), // userId -> array of { time, coins }
  syncHistory: new Map(), // userId -> { count, windowStart }
  suspiciousUsers: new Set(),
  // Anti-autoclick: track intervals between syncs for pattern detection
  intervalHistory: new Map(), // userId -> array of intervals (ms)

  // Check for impossibly fast tapping
  checkTapSpeed(userId, coinsClaimed, tapPower) {
    const now = Date.now();
    if (!this.tapHistory.has(userId)) {
      this.tapHistory.set(userId, []);
    }
    const history = this.tapHistory.get(userId);
    history.push({ time: now, coins: coinsClaimed });

    // Keep only last 60 seconds of history
    const cutoff = now - 60000;
    while (history.length > 0 && history[0].time < cutoff) {
      history.shift();
    }

    // Max batch = 15 taps/sec * 2 sec sync interval * tapPower (no hard cap)
    const maxCoinsPerBatch = 30 * tapPower;
    if (coinsClaimed > maxCoinsPerBatch) {
      stmts.logAntiCheat.run(userId, 'excessive_coins', `claimed=${coinsClaimed}, max=${maxCoinsPerBatch}, tapPower=${tapPower}`);
      return { safe: false, maxCoins: maxCoinsPerBatch };
    }

    // Check: total coins in last 10 seconds shouldn't exceed 150 * tapPower
    const last10s = history.filter(h => h.time > now - 10000);
    const totalLast10s = last10s.reduce((s, h) => s + h.coins, 0);
    if (totalLast10s > 150 * tapPower) {
      stmts.logAntiCheat.run(userId, 'speed_hack', `${totalLast10s} coins in 10s, tapPower=${tapPower}`);
      this.suspiciousUsers.add(userId);
      return { safe: false, maxCoins: Math.floor(maxCoinsPerBatch * 0.5) };
    }

    return { safe: true, maxCoins: maxCoinsPerBatch };
  },

  // Detect autoclick bots by analyzing timing patterns
  // Autoclickers produce unnaturally consistent intervals between syncs
  checkAutoClick(userId) {
    const now = Date.now();
    if (!this.intervalHistory.has(userId)) {
      this.intervalHistory.set(userId, { lastSync: now, intervals: [] });
      return { isBot: false };
    }

    const data = this.intervalHistory.get(userId);
    const interval = now - data.lastSync;
    data.lastSync = now;
    data.intervals.push(interval);

    // Keep last 20 intervals
    if (data.intervals.length > 20) {
      data.intervals.shift();
    }

    // Need at least 8 data points to analyze
    if (data.intervals.length < 8) {
      return { isBot: false };
    }

    // Calculate coefficient of variation (std / mean)
    // Real humans have high variance; bots have near-zero variance
    const intervals = data.intervals;
    const mean = intervals.reduce((a, b) => a + b, 0) / intervals.length;
    if (mean === 0) return { isBot: true };

    const variance = intervals.reduce((sum, v) => sum + Math.pow(v - mean, 2), 0) / intervals.length;
    const stdDev = Math.sqrt(variance);
    const cv = stdDev / mean; // coefficient of variation

    // CV < 0.05 means intervals are suspiciously uniform (bot-like)
    // Real human tapping has CV > 0.15 typically
    if (cv < 0.05) {
      stmts.logAntiCheat.run(userId, 'autoclick_detected', `cv=${cv.toFixed(4)}, mean_interval=${mean.toFixed(0)}ms, samples=${intervals.length}`);
      this.suspiciousUsers.add(userId);
      return { isBot: true };
    }

    // Also flag if coins per sync are perfectly identical across 10+ syncs
    const history = this.tapHistory.get(userId) || [];
    if (history.length >= 10) {
      const lastCoins = history.slice(-10).map(h => h.coins);
      const allSame = lastCoins.every(c => c === lastCoins[0]);
      if (allSame && lastCoins[0] > 0) {
        stmts.logAntiCheat.run(userId, 'autoclick_uniform_coins', `coins=${lastCoins[0]}, count=10`);
        this.suspiciousUsers.add(userId);
        return { isBot: true };
      }
    }

    return { isBot: false };
  },

  // Rate limit sync requests
  checkSyncRate(userId) {
    const now = Date.now();
    if (!this.syncHistory.has(userId)) {
      this.syncHistory.set(userId, { count: 0, windowStart: now });
    }
    const sync = this.syncHistory.get(userId);

    // Reset window every 10 seconds
    if (now - sync.windowStart > 10000) {
      sync.count = 0;
      sync.windowStart = now;
    }

    sync.count++;

    // Max 10 syncs per 10 seconds
    if (sync.count > 10) {
      stmts.logAntiCheat.run(userId, 'rate_limit', `${sync.count} syncs in 10s`);
      return false;
    }
    return true;
  },

  // Periodic cleanup to prevent memory leaks
  cleanup() {
    const now = Date.now();
    const maxAge = 300000; // 5 minutes of inactivity
    for (const [userId, history] of this.tapHistory) {
      if (history.length === 0 || history[history.length - 1].time < now - maxAge) {
        this.tapHistory.delete(userId);
      }
    }
    for (const [userId, sync] of this.syncHistory) {
      if (now - sync.windowStart > maxAge) {
        this.syncHistory.delete(userId);
      }
    }
    for (const [userId, data] of this.intervalHistory) {
      if (now - data.lastSync > maxAge) {
        this.intervalHistory.delete(userId);
      }
    }
  }
};

// Run cleanup every 5 minutes
setInterval(() => antiCheat.cleanup(), 300000);

// Validate Telegram initData — returns { valid, userId } or { valid: false }
function validateInitData(initData) {
  // If no BOT_TOKEN configured, skip validation (dev mode) but log warning
  if (!BOT_TOKEN) {
    console.warn('[SECURITY] BOT_TOKEN not set — initData validation disabled!');
    return { valid: true, userId: null };
  }
  if (!initData) {
    return { valid: false, userId: null };
  }
  try {
    const params = new URLSearchParams(initData);
    const hash = params.get('hash');
    if (!hash) return { valid: false, userId: null };

    params.delete('hash');
    const sorted = [...params.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}=${v}`)
      .join('\n');
    const secretKey = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
    const computedHash = crypto.createHmac('sha256', secretKey).update(sorted).digest('hex');

    if (computedHash !== hash) {
      return { valid: false, userId: null };
    }

    // Check auth_date freshness — reject stale tokens
    const authDate = parseInt(params.get('auth_date'), 10);
    if (authDate && (Math.floor(Date.now() / 1000) - authDate) > INIT_DATA_MAX_AGE) {
      return { valid: false, userId: null };
    }

    // Extract userId from initData for cross-validation
    let telegramUserId = null;
    const userParam = params.get('user');
    if (userParam) {
      try {
        const userData = JSON.parse(userParam);
        telegramUserId = String(userData.id);
      } catch {}
    }

    return { valid: true, userId: telegramUserId };
  } catch {
    return { valid: false, userId: null };
  }
}

// ===== QUEST PROGRESS CHECKER =====
function updateQuestProgress(userId) {
  const user = stmts.getUser.get(userId);
  if (!user) return;

  const quests = stmts.getAllQuests.all();
  const boostCount = stmts.countUserBoosts.get(userId).cnt;
  const existingQuests = stmts.getUserQuests.all(userId);
  const existingMap = {};
  for (const eq of existingQuests) {
    existingMap[eq.quest_id] = eq;
  }

  for (const quest of quests) {
    let currentProgress = 0;

    switch (quest.type) {
      case 'total_taps':
        currentProgress = user.total_taps;
        break;
      case 'coins':
        currentProgress = user.coins;
        break;
      case 'level':
        currentProgress = user.level;
        break;
      case 'boosts_bought':
        currentProgress = boostCount;
        break;
    }

    const completed = currentProgress >= quest.target ? 1 : 0;
    const progress = Math.min(currentProgress, quest.target);
    const existingClaimed = existingMap[quest.quest_id]?.claimed || 0;

    stmts.upsertUserQuest.run(
      userId, quest.quest_id, progress, completed, existingClaimed,
      progress, completed, existingClaimed
    );
  }
}

// ===== API HANDLERS =====
function handleGetUser(userId, res) {
  const user = getOrCreateUser(userId);
  const { energy, last_energy_update } = calculateEnergy(user);
  const stats = getUserTapPower(userId);

  // Update energy in DB
  stmts.updateUserTap.run(
    user.coins, user.total_taps, user.level, stats.tapPower,
    user.user_name, user.last_tap, energy, last_energy_update,
    stats.maxEnergy, stats.energyRegen, userId
  );

  sendJSON(res, 200, {
    userId: user.user_id,
    userName: user.user_name,
    coins: user.coins,
    totalTaps: user.total_taps,
    level: user.level,
    tapPower: stats.tapPower,
    energy: Math.floor(energy),
    maxEnergy: stats.maxEnergy,
    energyRegen: stats.energyRegen,
    lastTap: user.last_tap
  });
}

function handleTap(body, res) {
  const { userId, userName, coins, taps, initData } = body;

  if (!userId || coins === undefined) {
    return sendJSON(res, 400, { error: 'Missing data' });
  }

  // Validate initData signature + timestamp
  const authResult = validateInitData(initData);
  if (!authResult.valid) {
    return sendJSON(res, 403, { error: 'Invalid initData' });
  }

  // Cross-validate: userId in request must match userId from Telegram initData
  if (authResult.userId && authResult.userId !== String(userId)) {
    stmts.logAntiCheat.run(userId, 'user_id_mismatch', `request=${userId}, telegram=${authResult.userId}`);
    return sendJSON(res, 403, { error: 'User ID mismatch' });
  }

  if (!antiCheat.checkSyncRate(userId)) {
    return sendJSON(res, 429, { error: 'Too fast' });
  }

  // Anti-autoclick detection
  const autoClickCheck = antiCheat.checkAutoClick(userId);

  const user = getOrCreateUser(userId, userName);
  const stats = getUserTapPower(userId);

  // Anti-cheat: check tap speed
  const check = antiCheat.checkTapSpeed(userId, coins, stats.tapPower);

  // If flagged as bot, reduce coins further
  let effectiveMaxCoins = check.maxCoins;
  if (autoClickCheck.isBot) {
    effectiveMaxCoins = Math.floor(effectiveMaxCoins * 0.25);
  }

  const safeCoins = Math.min(Math.max(0, Math.floor(coins)), effectiveMaxCoins);
  const safeTaps = taps ? Math.min(Math.max(0, Math.floor(taps)), Math.ceil(safeCoins / stats.tapPower)) : safeCoins;

  // Calculate current energy
  const { energy: currentEnergy, last_energy_update } = calculateEnergy(user);

  // Strict energy check — reject if not enough energy
  const energyNeeded = safeTaps;
  if (energyNeeded > currentEnergy) {
    // Only allow taps the user actually has energy for
    const allowedTaps = Math.floor(currentEnergy);
    if (allowedTaps <= 0) {
      return sendJSON(res, 400, { error: 'Not enough energy', energy: Math.floor(currentEnergy), maxEnergy: stats.maxEnergy });
    }
    const allowedCoins = allowedTaps * stats.tapPower;
    const finalEnergy = Math.max(0, currentEnergy - allowedTaps);

    const newCoins = user.coins + allowedCoins;
    const newTaps = user.total_taps + allowedTaps;
    const newLevel = calculateLevel(newCoins);

    stmts.updateUserTap.run(
      newCoins, newTaps, newLevel, stats.tapPower,
      userName || user.user_name, Date.now(), finalEnergy, Date.now(),
      stats.maxEnergy, stats.energyRegen, userId
    );
    updateQuestProgress(userId);

    return sendJSON(res, 200, {
      userId,
      userName: userName || user.user_name,
      coins: newCoins,
      totalTaps: newTaps,
      level: newLevel,
      tapPower: stats.tapPower,
      energy: Math.floor(finalEnergy),
      maxEnergy: stats.maxEnergy,
      energyRegen: stats.energyRegen,
      flagged: !check.safe || autoClickCheck.isBot
    });
  }

  const finalEnergy = Math.max(0, currentEnergy - energyNeeded);
  const newCoins = user.coins + safeCoins;
  const newTaps = user.total_taps + safeTaps;
  const newLevel = calculateLevel(newCoins);

  stmts.updateUserTap.run(
    newCoins, newTaps, newLevel, stats.tapPower,
    userName || user.user_name, Date.now(), finalEnergy, Date.now(),
    stats.maxEnergy, stats.energyRegen, userId
  );

  // Update quest progress
  updateQuestProgress(userId);

  sendJSON(res, 200, {
    userId,
    userName: userName || user.user_name,
    coins: newCoins,
    totalTaps: newTaps,
    level: newLevel,
    tapPower: stats.tapPower,
    energy: Math.floor(finalEnergy),
    maxEnergy: stats.maxEnergy,
    energyRegen: stats.energyRegen,
    flagged: !check.safe || autoClickCheck.isBot
  });
}

function handleLeaderboard(res) {
  const rows = stmts.getLeaderboard.all();
  const result = rows.map(u => ({
    userId: u.user_id,
    userName: u.user_name,
    coins: u.coins,
    level: u.level
  }));
  sendJSON(res, 200, result);
}

function handleGetBoosts(userId, res) {
  const allBoosts = stmts.getAllBoosts.all();
  const userBoosts = stmts.getUserBoosts.all(userId);
  const userBoostMap = {};
  for (const ub of userBoosts) {
    userBoostMap[ub.boost_id] = ub.level;
  }

  const result = allBoosts.map(b => {
    const currentLevel = userBoostMap[b.boost_id] || 0;
    const cost = Math.floor(b.cost * Math.pow(b.cost_multiplier, currentLevel));
    return {
      boostId: b.boost_id,
      name: b.name,
      description: b.description,
      type: b.type,
      effectValue: b.effect_value,
      currentLevel,
      maxLevel: b.max_level,
      cost,
      icon: b.icon
    };
  });

  sendJSON(res, 200, result);
}

function handleBuyBoost(body, res) {
  const { userId, boostId, initData } = body;
  if (!userId || !boostId) {
    return sendJSON(res, 400, { error: 'Missing data' });
  }

  // Validate Telegram initData
  const authResult = validateInitData(initData);
  if (!authResult.valid) {
    return sendJSON(res, 403, { error: 'Invalid initData' });
  }
  if (authResult.userId && authResult.userId !== String(userId)) {
    return sendJSON(res, 403, { error: 'User ID mismatch' });
  }

  const user = getOrCreateUser(userId);
  const boost = stmts.getBoost.get(boostId);
  if (!boost) {
    return sendJSON(res, 404, { error: 'Boost not found' });
  }

  const userBoost = stmts.getUserBoost.get(userId, boostId);
  const currentLevel = userBoost ? userBoost.level : 0;

  if (currentLevel >= boost.max_level) {
    return sendJSON(res, 400, { error: 'Max level reached' });
  }

  const cost = Math.floor(boost.cost * Math.pow(boost.cost_multiplier, currentLevel));

  if (user.coins < cost) {
    return sendJSON(res, 400, { error: 'Not enough coins' });
  }

  // Transaction: deduct coins + upgrade boost
  const buyTransaction = db.transaction(() => {
    stmts.upsertUserBoost.run(userId, boostId);
    const stats = getUserTapPower(userId);
    stmts.updateUserCoins.run(user.coins - cost, stats.tapPower, userId);

    // Also update max_energy and energy_regen on user
    const updatedUser = stmts.getUser.get(userId);
    stmts.updateUserTap.run(
      updatedUser.coins, updatedUser.total_taps, updatedUser.level, stats.tapPower,
      updatedUser.user_name, updatedUser.last_tap, updatedUser.energy, updatedUser.last_energy_update,
      stats.maxEnergy, stats.energyRegen, userId
    );

    // Update quest progress for boost purchase
    updateQuestProgress(userId);

    return stats;
  });

  const stats = buyTransaction();

  sendJSON(res, 200, {
    success: true,
    coins: user.coins - cost,
    tapPower: stats.tapPower,
    maxEnergy: stats.maxEnergy,
    energyRegen: stats.energyRegen,
    newLevel: (userBoost ? userBoost.level : 0) + 1
  });
}

function handleGetQuests(userId, res) {
  // First update progress
  updateQuestProgress(userId);

  const quests = stmts.getUserQuests.all(userId);

  const result = quests.map(q => ({
    questId: q.quest_id,
    name: q.name,
    description: q.description,
    type: q.type,
    target: q.target,
    progress: q.progress || 0,
    completed: q.completed === 1,
    claimed: q.claimed === 1,
    rewardCoins: q.reward_coins,
    rewardTapPower: q.reward_tap_power,
    icon: q.icon
  }));

  sendJSON(res, 200, result);
}

function handleClaimQuest(body, res) {
  const { userId, questId, initData } = body;
  if (!userId || !questId) {
    return sendJSON(res, 400, { error: 'Missing data' });
  }

  // Validate Telegram initData
  const authResult = validateInitData(initData);
  if (!authResult.valid) {
    return sendJSON(res, 403, { error: 'Invalid initData' });
  }
  if (authResult.userId && authResult.userId !== String(userId)) {
    return sendJSON(res, 403, { error: 'User ID mismatch' });
  }

  const user = getOrCreateUser(userId);
  const quests = stmts.getAllQuests.all();
  const quest = quests.find(q => q.quest_id === questId);

  if (!quest) {
    return sendJSON(res, 404, { error: 'Quest not found' });
  }

  // Check if quest completed and not claimed
  updateQuestProgress(userId);
  const userQuests = stmts.getUserQuests.all(userId);
  const uq = userQuests.find(q => q.quest_id === questId);

  if (!uq || uq.completed !== 1) {
    return sendJSON(res, 400, { error: 'Quest not completed' });
  }
  if (uq.claimed === 1) {
    return sendJSON(res, 400, { error: 'Already claimed' });
  }

  const claimTransaction = db.transaction(() => {
    stmts.claimQuest.run(Date.now(), userId, questId);

    // Give rewards
    const newCoins = user.coins + quest.reward_coins;
    let newTapPower = user.tap_power;

    if (quest.reward_tap_power > 0) {
      // Add a free boost to tap power (we just increase tap_power base)
      // We store this as a special quest boost
      newTapPower += quest.reward_tap_power;
    }

    const newLevel = calculateLevel(newCoins);
    stmts.updateUserTap.run(
      newCoins, user.total_taps, newLevel, newTapPower,
      user.user_name, user.last_tap, user.energy, user.last_energy_update,
      user.max_energy, user.energy_regen, userId
    );

    return { coins: newCoins, tapPower: newTapPower, level: newLevel };
  });

  const result = claimTransaction();

  sendJSON(res, 200, {
    success: true,
    coins: result.coins,
    tapPower: result.tapPower,
    level: result.level,
    rewardCoins: quest.reward_coins,
    rewardTapPower: quest.reward_tap_power
  });
}

// ===== HTTP SERVER =====
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': ALLOWED_ORIGIN,
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type'
};

function sendJSON(res, status, data) {
  res.writeHead(status, {
    'Content-Type': 'application/json',
    ...CORS_HEADERS
  });
  res.end(JSON.stringify(data));
}

// Safe static file server with path traversal protection
const publicDir = path.resolve(__dirname, 'public');

function serveStatic(filePath, res) {
  // Resolve to absolute and ensure it stays within public/
  const resolved = path.resolve(filePath);
  if (!resolved.startsWith(publicDir)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  const ext = path.extname(resolved);
  const mimeTypes = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'application/javascript',
    '.css': 'text/css',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.svg': 'image/svg+xml',
    '.json': 'application/json'
  };

  fs.readFile(resolved, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }
    res.writeHead(200, { 'Content-Type': mimeTypes[ext] || 'application/octet-stream' });
    res.end(data);
  });
}

// Helper: read POST body with size limit
function readBody(req, res, callback) {
  let body = '';
  let size = 0;
  req.on('data', chunk => {
    size += chunk.length;
    if (size > MAX_BODY_SIZE) {
      req.destroy();
      return sendJSON(res, 413, { error: 'Request body too large' });
    }
    body += chunk;
  });
  req.on('end', () => {
    try {
      callback(JSON.parse(body));
    } catch {
      sendJSON(res, 400, { error: 'Invalid JSON' });
    }
  });
}

const server = http.createServer((req, res) => {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, CORS_HEADERS);
    return res.end();
  }

  const url = new URL(req.url, `http://localhost:${PORT}`);

  // ===== API ROUTES =====
  if (url.pathname.startsWith('/api/')) {

    // GET /api/user/:id
    if (req.method === 'GET' && url.pathname.startsWith('/api/user/')) {
      const userId = url.pathname.split('/')[3];
      if (!userId || userId.length > 64) return sendJSON(res, 400, { error: 'Invalid userId' });
      return handleGetUser(userId, res);
    }

    // POST /api/tap
    if (req.method === 'POST' && url.pathname === '/api/tap') {
      return readBody(req, res, body => handleTap(body, res));
    }

    // GET /api/leaderboard
    if (req.method === 'GET' && url.pathname === '/api/leaderboard') {
      return handleLeaderboard(res);
    }

    // GET /api/boosts/:userId
    if (req.method === 'GET' && url.pathname.startsWith('/api/boosts/')) {
      const userId = url.pathname.split('/')[3];
      if (!userId || userId.length > 64) return sendJSON(res, 400, { error: 'Invalid userId' });
      return handleGetBoosts(userId, res);
    }

    // POST /api/boosts/buy
    if (req.method === 'POST' && url.pathname === '/api/boosts/buy') {
      return readBody(req, res, body => handleBuyBoost(body, res));
    }

    // GET /api/quests/:userId
    if (req.method === 'GET' && url.pathname.startsWith('/api/quests/')) {
      const userId = url.pathname.split('/')[3];
      if (!userId || userId.length > 64) return sendJSON(res, 400, { error: 'Invalid userId' });
      return handleGetQuests(userId, res);
    }

    // POST /api/quests/claim
    if (req.method === 'POST' && url.pathname === '/api/quests/claim') {
      return readBody(req, res, body => handleClaimQuest(body, res));
    }

    return sendJSON(res, 404, { error: 'Not found' });
  }

  // ===== STATIC FILES =====
  let filePath = path.join(publicDir, url.pathname === '/' ? 'index.html' : url.pathname);
  serveStatic(filePath, res);
});

server.listen(PORT, () => {
  console.log(`🪙 TapCoin Server v2.0 running on port ${PORT}`);
  console.log(`   http://localhost:${PORT}`);
  console.log(`   Database: ${DB_PATH}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  db.close();
  server.close();
});
process.on('SIGINT', () => {
  db.close();
  server.close();
  process.exit(0);
});
