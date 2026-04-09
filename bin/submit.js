#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const {
  DEFAULT_CONFIG,
  DEFAULT_SETTINGS,
  clearSessionCache,
  DEFAULT_GLOBAL_DIR,
  deepMerge,
  ensureGlobalDir,
  getSessionCachePath,
  globalPath,
  loadSettings,
  parseCookieHeader,
  readJson,
  resolveGlobalDir,
  setGlobalDir,
  writeJson,
} = require('../src/store');

const toolRoot = path.resolve(__dirname, '..');
const coreScript = path.join(toolRoot, 'src', 'submit-foreground.js');

function readPackage() {
  return JSON.parse(fs.readFileSync(path.join(toolRoot, 'package.json'), 'utf-8'));
}

function printHelp() {
  console.log([
    'Submit CLI v3',
    '',
    'Global state is stored in ~/.autosubmit/ by default.',
    'Launcher scripts are generated inside the current workdir by default:',
    '  ./.autosubmit/launchers/',
    '',
    'Usage:',
    '  submit init [--global-dir <path>]',
    '  submit login [--account <account>] [--password <password>]',
    '  submit logout',
    '  submit session import --token <token> [--cookie "k=v; k2=v2"] [--account <account>]',
    '  submit session clear',
    '  submit set image <image|index|id|keyword>',
    '  submit set accelerator <count>',
    '  submit set cpu <cores>',
    '  submit set task-name <name>',
    '  submit set clear-task-name',
    '  submit set poll-interval <seconds>',
    '  submit set keep-launcher <0|1>',
    '  submit set script-args <args...>',
    '  submit set clear-script-args',
    '  submit get <key|all>',
    '  submit images',
    '  submit captcha fetch [--output <filePath>]',
    '  submit clear-logs',
    '  submit reconnect <handle|taskId|taskName|report.json>',
    '  submit <file> [script args...]',
    '',
    'Examples:',
    '  submit init',
    '  submit init --global-dir /private/path/.autosubmit',
    '  submit login --account alice --password ******',
    '  submit images',
    '  submit set image 2',
    '  submit set accelerator 1',
    '  submit set cpu 8',
    '  submit set script-args --config configs/base.yaml',
    '  submit train.py --epochs 20',
    '  submit reconnect tid_123456',
    '  submit captcha fetch --output /tmp/captcha.png',
    '  submit session import --token xxxxx --cookie "JSESSIONID=..."',
  ].join('\n'));
}

function parseOptionValue(args, key) {
  const prefix = `${key}=`;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === key) return args[index + 1] || '';
    if (arg.startsWith(prefix)) return arg.slice(prefix.length);
  }
  return '';
}

function spawnCore(args, { captureOutput = false } = {}) {
  return spawnSync(process.execPath, [coreScript, ...args], {
    stdio: captureOutput ? ['inherit', 'pipe', 'pipe'] : 'inherit',
    cwd: process.cwd(),
    env: process.env,
    encoding: captureOutput ? 'utf-8' : undefined,
  });
}

function ensureCoreScript() {
  if (!fs.existsSync(coreScript)) {
    throw new Error(`Core script not found: ${coreScript}`);
  }
}

function cmdInit(args) {
  const globalDirArg = String(parseOptionValue(args, '--global-dir') || '').trim();
  const allowed = new Set(['--global-dir']);

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (!arg.startsWith('--')) throw new Error(`init does not accept positional arg: ${arg}`);
    const key = arg.includes('=') ? arg.slice(0, arg.indexOf('=')) : arg;
    if (!allowed.has(key)) throw new Error(`init unsupported arg: ${arg}`);
    if (!arg.includes('=') && index + 1 < args.length) index += 1;
  }

  if (globalDirArg) {
    setGlobalDir(globalDirArg);
  }

  const activeGlobalDir = ensureGlobalDir();

  const configPath = globalPath('config.json');
  const existingConfig = readJson(configPath, {});
  const mergedConfig = deepMerge(DEFAULT_CONFIG, existingConfig || {});
  writeJson(configPath, mergedConfig);

  const settingsPath = globalPath('settings.json');
  const existingSettings = readJson(settingsPath, {});
  const mergedSettings = deepMerge(DEFAULT_SETTINGS, existingSettings || {});
  mergedSettings.commandName = 'submit';
  writeJson(settingsPath, mergedSettings);

  fs.mkdirSync(globalPath('reports'), { recursive: true });

  console.log(`Global dir: ${activeGlobalDir}`);
  console.log(`Written: ${configPath}`);
  console.log(`Written: ${settingsPath}`);
  console.log(`Session cache: ${getSessionCachePath()}`);
  console.log('');
  console.log('Next steps:');
  console.log(`  1. Edit ${globalPath('config.json')}`);
  console.log('  2. submit login --account <account> --password <password>');
  console.log('  3. submit set image <image>');
  console.log('  4. cd <your_project> && submit train.py');
}

function cmdLogin(rawArgs) {
  ensureCoreScript();

  const allowed = new Set(['--account', '--password']);
  for (let index = 0; index < rawArgs.length; index += 1) {
    const arg = rawArgs[index];
    if (!arg.startsWith('--')) throw new Error(`login does not accept positional arg: ${arg}`);
    const key = arg.includes('=') ? arg.slice(0, arg.indexOf('=')) : arg;
    if (!allowed.has(key)) throw new Error(`login unsupported arg: ${arg}`);
    if (!arg.includes('=') && index + 1 < rawArgs.length) index += 1;
  }

  const account = String(parseOptionValue(rawArgs, '--account') || '').trim();
  const password = String(parseOptionValue(rawArgs, '--password') || '').trim();

  const configPath = globalPath('config.json');
  if (!fs.existsSync(configPath)) {
    throw new Error('Global config not found. Run `submit init` first.');
  }

  const config = readJson(configPath, {}) || {};
  config.credentials = config.credentials || {};
  config.credentials.account = account || String(config.credentials.account || '').trim();
  config.credentials.password = password || String(config.credentials.password || '').trim();
  config.credentials.captcha = '';

  if (!config.credentials.account || !config.credentials.password) {
    throw new Error('Missing account or password.\nRun: submit login --account alice --password ******');
  }

  writeJson(configPath, config);

  console.log(`Credentials updated in ${globalPath('config.json')}.`);
  if (clearSessionCache()) {
    console.log('Session cache cleared.');
  }

  process.on('SIGINT', () => {});
  const coreArgs = ['--check-login'];

  const child = spawnCore(coreArgs);
  if (child.error) throw new Error(`执行失败: ${child.error.message}`);
  process.exit(child.status === null ? 130 : child.status);
}

function cmdLogout(args) {
  if (args.length) throw new Error(`logout does not accept args: ${args.join(' ')}`);

  const configPath = globalPath('config.json');
  if (!fs.existsSync(configPath)) {
    console.log('Global config not found. Nothing to clear.');
  } else {
    const config = readJson(configPath, {}) || {};
    config.credentials = config.credentials || {};
    config.credentials.account = '';
    config.credentials.password = '';
    config.credentials.captcha = '';
    writeJson(configPath, config);
    console.log('Cleared global credentials.');
  }

  if (clearSessionCache()) {
    console.log('Session cache cleared.');
  }
}

function cmdSession(rawArgs) {
  const action = String(rawArgs[0] || '').trim();
  const args = rawArgs.slice(1);

  if (action === 'clear') {
    if (clearSessionCache()) {
      console.log('Session cache cleared.');
    } else {
      console.log('No session cache found.');
    }
    return;
  }

  if (action !== 'import') {
    throw new Error('Usage: submit session import --token <token> [--cookie "k=v;..."] [--account <account>] or submit session clear');
  }

  const token = String(parseOptionValue(args, '--token') || '').trim();
  const cookie = String(parseOptionValue(args, '--cookie') || '').trim();
  const account = String(parseOptionValue(args, '--account') || '').trim();

  if (!token) throw new Error('session import requires --token.');

  const configPath = globalPath('config.json');
  if (!fs.existsSync(configPath)) {
    throw new Error(`Missing ${globalPath('config.json')}. Run \`submit init\` first.`);
  }

  const config = readJson(configPath, {}) || {};
  const baseURL = String(config.baseURL || '').trim();
  if (!baseURL) throw new Error('config.json missing baseURL.');

  const sessionPayload = {
    version: 1,
    savedAt: new Date().toISOString(),
    baseURL,
    account: account || String((config.credentials || {}).account || '').trim(),
    token,
    cookieJar: parseCookieHeader(cookie),
  };

  const sessionPath = getSessionCachePath();
  writeJson(sessionPath, sessionPayload);
  try {
    fs.chmodSync(sessionPath, 0o600);
  } catch {
    // ignore chmod failures on unsupported filesystems
  }

  console.log(`Session imported: ${sessionPath}`);
  console.log(`Token: yes, cookie count: ${Object.keys(sessionPayload.cookieJar).length}`);
}

function buildSettingsSnapshot(settings) {
  return {
    image: String(settings.project.image || ''),
    accelerator: Number(settings.submitDefaults.acceleratorCount),
    cpu: Number(settings.submitDefaults.cpuCores),
    'task-name': String(settings.submitDefaults.taskName || ''),
    'poll-interval': Number(settings.foreground.pollIntervalSec),
    'keep-launcher': settings.singleFile.keepLauncher ? 1 : 0,
    'script-args': Array.isArray(settings.singleFile.scriptArgs) ? settings.singleFile.scriptArgs : [],
    'launcher-dir': String(settings.singleFile.launcherDir || ''),
  };
}

function cmdGet(args) {
  const key = String(args[0] || '').trim();
  if (!key) throw new Error('Usage: submit get <key|all>');

  const settings = loadSettings();
  const snapshot = buildSettingsSnapshot(settings);

  if (key === 'all') {
    console.log(JSON.stringify(snapshot, null, 2));
    return;
  }

  if (!Object.prototype.hasOwnProperty.call(snapshot, key)) {
    throw new Error(`Unsupported key: ${key}`);
  }

  const value = snapshot[key];
  if (Array.isArray(value) || (value && typeof value === 'object')) {
    console.log(JSON.stringify(value));
  } else {
    console.log(String(value));
  }
}

function cmdSet(rawArgs) {
  ensureCoreScript();

  const key = String(rawArgs[0] || '').trim();
  const valueArgs = rawArgs.slice(1);
  const value = valueArgs.join(' ').trim();

  if (!key) throw new Error('Usage: submit set <key> <value>');

  const settingsPath = globalPath('settings.json');
  const settings = loadSettings();

  if (key === 'image') {
    if (!value) throw new Error('Usage: submit set image <image|index|id|keyword>');

    const looksLikeExactImage = value.includes('/');
    let finalImage = value;

    if (!looksLikeExactImage) {
      const child = spawnCore(['--resolve-image', value], { captureOutput: true });
      if (child.error) throw new Error(`Image resolve failed: ${child.error.message}`);
      if (child.status !== 0) {
        const reason = String(child.stderr || child.stdout || '').trim() || `exit ${child.status}`;
        throw new Error(`Image resolve failed: ${reason}`);
      }

      finalImage = String(child.stdout || '')
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean)
        .pop();

      if (!finalImage) throw new Error('Image resolve failed: no image returned.');
    }

    settings.project.image = finalImage;
    writeJson(settingsPath, settings);
    console.log(`Image set: ${finalImage}`);
    return;
  }

  if (key === 'accelerator') {
    const count = Number(value);
    if (!Number.isFinite(count) || count < 0) throw new Error('accelerator must be a number >= 0.');
    settings.submitDefaults.acceleratorCount = count;
    writeJson(settingsPath, settings);
    console.log(`Accelerator count set: ${count}`);
    return;
  }

  if (key === 'cpu') {
    const cores = Number(value);
    if (!Number.isFinite(cores) || cores < 1) throw new Error('cpu must be a number >= 1.');
    settings.submitDefaults.cpuCores = cores;
    writeJson(settingsPath, settings);
    console.log(`CPU cores set: ${cores}`);
    return;
  }

  if (key === 'task-name') {
    if (!value) throw new Error('task-name requires a value.');
    settings.submitDefaults.taskName = value;
    writeJson(settingsPath, settings);
    console.log(`Task name set: ${value}`);
    return;
  }

  if (key === 'clear-task-name') {
    settings.submitDefaults.taskName = '';
    writeJson(settingsPath, settings);
    console.log('Task name cleared.');
    return;
  }

  if (key === 'poll-interval') {
    const seconds = Number(value);
    if (!Number.isFinite(seconds) || seconds <= 0) throw new Error('poll-interval must be a number > 0.');
    settings.foreground.pollIntervalSec = seconds;
    writeJson(settingsPath, settings);
    console.log(`Poll interval set: ${seconds}s`);
    return;
  }

  if (key === 'keep-launcher') {
    const normalized = value.toLowerCase();
    if (!['0', '1', 'true', 'false'].includes(normalized)) {
      throw new Error('keep-launcher supports only 0/1/true/false.');
    }
    settings.singleFile.keepLauncher = (normalized === '1' || normalized === 'true');
    writeJson(settingsPath, settings);
    console.log(`Keep launcher set: ${settings.singleFile.keepLauncher ? 1 : 0}`);
    return;
  }

  if (key === 'script-args') {
    settings.singleFile.scriptArgs = valueArgs.map((item) => String(item));
    writeJson(settingsPath, settings);
    console.log(`Script args set: ${JSON.stringify(settings.singleFile.scriptArgs)}`);
    return;
  }

  if (key === 'clear-script-args') {
    settings.singleFile.scriptArgs = [];
    writeJson(settingsPath, settings);
    console.log('Script args cleared.');
    return;
  }

  throw new Error(`Unsupported set key: ${key}`);
}

function cmdSubmitFile(file, passthroughArgs) {
  ensureCoreScript();

  if (!file || file.startsWith('-')) {
    throw new Error('Missing file argument. Example: submit train.py');
  }

  process.on('SIGINT', () => {});
  const child = spawnCore(['--single-file', file, '--script-args-json', JSON.stringify(passthroughArgs)]);
  if (child.error) throw new Error(`执行失败: ${child.error.message}`);
  process.exit(child.status === null ? 130 : child.status);
}

function cmdReconnect(args) {
  ensureCoreScript();

  const target = String(args[0] || '').trim();
  if (!target) throw new Error('reconnect missing target. Example: submit reconnect tid_123456');

  process.on('SIGINT', () => {});
  const child = spawnCore(['--reconnect', target]);
  if (child.error) throw new Error(`执行失败: ${child.error.message}`);
  process.exit(child.status === null ? 130 : child.status);
}

function cmdImages() {
  ensureCoreScript();
  const child = spawnCore(['--list-images']);
  if (child.error) throw new Error(`执行失败: ${child.error.message}`);
  process.exit(child.status === null ? 1 : child.status);
}

function cmdCaptchaFetch(args) {
  ensureCoreScript();
  const child = spawnCore(['--fetch-captcha', ...args]);
  if (child.error) throw new Error(`执行失败: ${child.error.message}`);
  process.exit(child.status === null ? 1 : child.status);
}

function cmdClearLogs() {
  const reportDir = globalPath('reports');
  if (!fs.existsSync(reportDir)) {
    console.log(`Log directory does not exist: ${reportDir}`);
    return;
  }

  const itemCount = fs.readdirSync(reportDir).length;
  fs.rmSync(reportDir, { recursive: true, force: true });
  fs.mkdirSync(reportDir, { recursive: true });
  console.log(`Logs cleared: ${reportDir} (removed ${itemCount} items)`);
}

function main() {
  const argv = process.argv.slice(2);
  const head = argv[0];

  if (!head || head === '-h' || head === '--help' || head === 'help') {
    printHelp();
    return;
  }

  if (head === '--version') {
    console.log(readPackage().version || '0.0.0');
    return;
  }

  if (head === '-v') {
    const pkg = readPackage();
    console.log(`submit ${pkg.version || '0.0.0'}`);
    console.log(`bin: ${process.argv[1]}`);
    console.log(`global-dir: ${resolveGlobalDir()}`);
    console.log(`default-global-dir: ${DEFAULT_GLOBAL_DIR}`);
    return;
  }

  if (head === 'init') { cmdInit(argv.slice(1)); return; }
  if (head === 'login') { cmdLogin(argv.slice(1)); return; }
  if (head === 'logout') { cmdLogout(argv.slice(1)); return; }
  if (head === 'session') { cmdSession(argv.slice(1)); return; }
  if (head === 'set') { cmdSet(argv.slice(1)); return; }
  if (head === 'get') { cmdGet(argv.slice(1)); return; }
  if (head === 'images') { cmdImages(); return; }
  if (head === 'captcha' && argv[1] === 'fetch') { cmdCaptchaFetch(argv.slice(2)); return; }
  if (head === 'clear-logs') { cmdClearLogs(); return; }
  if (head === 'logs' && argv[1] === 'clear') { cmdClearLogs(); return; }
  if (head === 'reconnect') { cmdReconnect(argv.slice(1)); return; }

  cmdSubmitFile(head, argv.slice(1));
}

try {
  main();
} catch (error) {
  console.error('Error:', error.message);
  process.exit(1);
}
