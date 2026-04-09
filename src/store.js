const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const DEFAULT_GLOBAL_DIR = path.join(os.homedir(), '.autosubmit');
const GLOBAL_DIR_ENV = 'AUTOSUBMIT_HOME';
const GLOBAL_DIR_POINTER = path.join(os.homedir(), '.autosubmit-home');

function resolveGlobalDir() {
  const fromEnv = String(process.env[GLOBAL_DIR_ENV] || '').trim();
  if (fromEnv) return path.resolve(fromEnv);

  try {
    if (fs.existsSync(GLOBAL_DIR_POINTER)) {
      const fromPointer = String(fs.readFileSync(GLOBAL_DIR_POINTER, 'utf-8') || '').trim();
      if (fromPointer) return path.resolve(fromPointer);
    }
  } catch {
    // ignore pointer read failures and fall back to default
  }

  return DEFAULT_GLOBAL_DIR;
}

function setGlobalDir(targetDir) {
  const resolved = path.resolve(String(targetDir || '').trim());
  if (!resolved) throw new Error('Global directory path is empty.');

  if (resolved === DEFAULT_GLOBAL_DIR) {
    try {
      if (fs.existsSync(GLOBAL_DIR_POINTER)) fs.unlinkSync(GLOBAL_DIR_POINTER);
    } catch {
      // ignore pointer cleanup failures
    }
    return resolved;
  }

  fs.writeFileSync(GLOBAL_DIR_POINTER, `${resolved}\n`, 'utf-8');
  return resolved;
}

const GLOBAL_DIR = resolveGlobalDir();

const DEFAULT_CONFIG = {
  baseURL: '',
  ignoreHTTPSErrors: true,
  timeoutMs: 15000,
  credentials: {
    account: '',
    password: '',
    captcha: '',
  },
};

const DEFAULT_SETTINGS = {
  commandName: 'submit',
  project: {
    image: '',
  },
  singleFile: {
    launcherDir: '.autosubmit/launchers',
    keepLauncher: false,
    scriptArgs: [],
    interpreters: {
      '.py': 'python',
      '.sh': 'bash',
      '.bash': 'bash',
    },
    fallbackInterpreter: '',
  },
  submitDefaults: {
    cpuCores: 4,
    acceleratorCount: 1,
    taskName: '',
  },
  foreground: {
    pollIntervalSec: 2,
  },
  sessionCache: {
    enabled: true,
    maxAgeSec: 43200,
  },
};

function globalPath(fileName) {
  return path.join(resolveGlobalDir(), fileName);
}

function ensureGlobalDir() {
  const dir = resolveGlobalDir();
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function readJson(filePath, fallback = null) {
  if (!fs.existsSync(filePath)) return fallback;
  return JSON.parse(fs.readFileSync(filePath, 'utf-8'));
}

function writeJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, 'utf-8');
}

function deepMerge(base, override) {
  if (Array.isArray(base)) {
    return Array.isArray(override) ? override.slice() : base.slice();
  }

  if (base && typeof base === 'object') {
    const out = { ...base };
    const source = (override && typeof override === 'object' && !Array.isArray(override))
      ? override
      : {};

    for (const [key, value] of Object.entries(source)) {
      out[key] = deepMerge(base[key], value);
    }
    return out;
  }

  return override === undefined ? base : override;
}

function normalizeConfig(rawConfig) {
  const config = deepMerge(DEFAULT_CONFIG, rawConfig || {});
  config.credentials = config.credentials || {};
  config.credentials.account = String(config.credentials.account || '').trim();
  config.credentials.password = String(config.credentials.password || '').trim();
  config.credentials.captcha = String(config.credentials.captcha || '').trim();
  return config;
}

function normalizeSettings(rawSettings) {
  const settings = deepMerge(DEFAULT_SETTINGS, rawSettings || {});

  settings.commandName = String(settings.commandName || 'submit').trim() || 'submit';
  settings.project = settings.project || {};
  settings.project.image = String(settings.project.image || '').trim();

  settings.singleFile = settings.singleFile || {};
  settings.singleFile.launcherDir = String(settings.singleFile.launcherDir || '.autosubmit/launchers').trim() || '.autosubmit/launchers';
  settings.singleFile.keepLauncher = Boolean(settings.singleFile.keepLauncher);
  settings.singleFile.scriptArgs = Array.isArray(settings.singleFile.scriptArgs)
    ? settings.singleFile.scriptArgs.map((item) => String(item))
    : [];
  settings.singleFile.interpreters = settings.singleFile.interpreters || {};
  settings.singleFile.fallbackInterpreter = String(settings.singleFile.fallbackInterpreter || '').trim();

  settings.submitDefaults = settings.submitDefaults || {};
  settings.submitDefaults.cpuCores = Number(settings.submitDefaults.cpuCores);
  settings.submitDefaults.acceleratorCount = Number(settings.submitDefaults.acceleratorCount);
  settings.submitDefaults.taskName = String(settings.submitDefaults.taskName || '').trim();

  settings.foreground = settings.foreground || {};
  settings.foreground.pollIntervalSec = Number(settings.foreground.pollIntervalSec);

  settings.sessionCache = settings.sessionCache || {};
  settings.sessionCache.enabled = settings.sessionCache.enabled !== false;
  settings.sessionCache.maxAgeSec = Number(settings.sessionCache.maxAgeSec);

  return settings;
}

function loadConfig({ required = false } = {}) {
  const configPath = globalPath('config.json');
  if (!fs.existsSync(configPath)) {
    if (required) {
      throw new Error(
        `Global config not found: ${configPath}.\n` +
        'Run submit init first, then fill in baseURL and account credentials.',
      );
    }
    return normalizeConfig({});
  }

  return normalizeConfig(readJson(configPath, {}) || {});
}

function loadSettings() {
  const settingsPath = globalPath('settings.json');
  if (!fs.existsSync(settingsPath)) return normalizeSettings({});
  return normalizeSettings(readJson(settingsPath, {}) || {});
}

function getSessionCachePath() {
  return globalPath('session.json');
}

function clearSessionCache() {
  const cachePath = getSessionCachePath();
  try {
    if (!fs.existsSync(cachePath)) return false;
    fs.unlinkSync(cachePath);
    return true;
  } catch {
    return false;
  }
}

function parseCookieHeader(raw) {
  const out = {};
  const text = String(raw || '').trim();
  if (!text) return out;

  for (const part of text.split(';')) {
    const segment = String(part).trim();
    if (!segment) continue;

    const index = segment.indexOf('=');
    if (index <= 0) continue;

    const key = segment.slice(0, index).trim();
    const value = segment.slice(index + 1).trim();
    if (!key) continue;

    out[key] = value;
  }

  return out;
}

module.exports = {
  DEFAULT_CONFIG,
  DEFAULT_SETTINGS,
  DEFAULT_GLOBAL_DIR,
  GLOBAL_DIR,
  GLOBAL_DIR_ENV,
  GLOBAL_DIR_POINTER,
  clearSessionCache,
  deepMerge,
  ensureGlobalDir,
  getSessionCachePath,
  globalPath,
  loadConfig,
  loadSettings,
  normalizeConfig,
  normalizeSettings,
  parseCookieHeader,
  readJson,
  resolveGlobalDir,
  setGlobalDir,
  writeJson,
};
