const fs = require('node:fs');
const path = require('node:path');
const https = require('node:https');
const readline = require('node:readline');
const axios = require('axios');
const { sm2 } = require('sm-crypto');

const {
  GLOBAL_DIR,
  clearSessionCache,
  getSessionCachePath,
  loadConfig,
  loadSettings,
} = require('./store');

const TOKEN_HEADER = 'X-Auth-Token';
const PLATFORM_HEADERS = {
  'X-Accept-Language': 'zh_CN',
};

const CAPTCHA_CANDIDATES = [
  { method: 'GET', url: '/api/ibase/v1/captcha', imagePath: 'resData', isBase64: true },
  { method: 'GET', url: '/api/ibase/v1/login/captcha', imagePath: 'resData', isBase64: true },
  { method: 'GET', url: '/api/ibase/v1/captcha/image', isBinary: true },
];

const ACTIVE_TASKS_REQUEST = {
  method: 'GET',
  url: '/api/iresource/v1/train',
  params: {
    page: 1,
    pageSize: 50,
    taskType: '',
    statusFlag: 0,
  },
};

const SUBMIT_PROFILE = {
  projectId: 'cc5469d391594b45ad821b8f6d446203',
  resGroupId: 'da83b2b8-7852-46c9-9e1d-b9c89ae32506',
  imageType: 'pytorch',
  type: 'pytorch',
  switchType: 'ib',
  gpuCardType: 'NVIDIA-A800-80GB-PCIe',
  distFlag: false,
  mpiFlag: false,
  enUpdateDataSet: 0,
  shmSize: 4,
  imageFlag: 0,
  isElastic: false,
  emergencyFlag: false,
};

const TRAINING_GROUP_NAME = 'training';

function safeJsonParse(value, fallback = null) {
  if (typeof value !== 'string') return fallback;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function waitShort(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toTimeName(timestamp = Date.now()) {
  const date = new Date(timestamp);
  const pad = (value) => String(value).padStart(2, '0');
  return `${date.getFullYear()}${pad(date.getMonth() + 1)}${pad(date.getDate())}${pad(date.getHours())}${pad(date.getMinutes())}${pad(date.getSeconds())}`;
}

function shellQuote(input) {
  return `'${String(input).replace(/'/g, `'"'"'`)}'`;
}

function parseCliArgs(argv) {
  const out = {
    singleFile: '',
    reconnectTarget: '',
    listImages: false,
    resolveImageSelector: '',
    fetchCaptcha: false,
    captchaOutput: '',
    checkLogin: false,
    onceCaptcha: '',
    scriptArgs: [],
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--') {
      throw new Error('The -- separator is not supported. Use: submit <file> --epochs 20');
    }

    if (arg === '--single-file') { out.singleFile = argv[index + 1] || ''; index += 1; continue; }
    if (arg.startsWith('--single-file=')) { out.singleFile = arg.slice('--single-file='.length); continue; }

    if (arg === '--reconnect') { out.reconnectTarget = argv[index + 1] || ''; index += 1; continue; }
    if (arg.startsWith('--reconnect=')) { out.reconnectTarget = arg.slice('--reconnect='.length); continue; }

    if (arg === '--list-images') { out.listImages = true; continue; }

    if (arg === '--resolve-image') { out.resolveImageSelector = argv[index + 1] || ''; index += 1; continue; }
    if (arg.startsWith('--resolve-image=')) { out.resolveImageSelector = arg.slice('--resolve-image='.length); continue; }

    if (arg === '--fetch-captcha') { out.fetchCaptcha = true; continue; }
    if (arg === '--check-login') { out.checkLogin = true; continue; }
    if (arg === '--once-captcha') { out.onceCaptcha = argv[index + 1] || ''; index += 1; continue; }
    if (arg.startsWith('--once-captcha=')) { out.onceCaptcha = arg.slice('--once-captcha='.length); continue; }

    if (arg === '--output') { out.captchaOutput = argv[index + 1] || ''; index += 1; continue; }
    if (arg.startsWith('--output=')) { out.captchaOutput = arg.slice('--output='.length); continue; }

    if (arg === '--script-args-json') {
      const parsed = safeJsonParse(argv[index + 1] || '[]', []);
      if (!Array.isArray(parsed)) throw new Error('--script-args-json must be a JSON array.');
      out.scriptArgs = parsed.map((item) => String(item));
      index += 1;
      continue;
    }
    if (arg.startsWith('--script-args-json=')) {
      const parsed = safeJsonParse(arg.slice('--script-args-json='.length), []);
      if (!Array.isArray(parsed)) throw new Error('--script-args-json must be a JSON array.');
      out.scriptArgs = parsed.map((item) => String(item));
      continue;
    }

    if (arg.startsWith('--')) {
      throw new Error(`Unsupported argument: ${arg}`);
    }

    if (!out.singleFile) {
      out.singleFile = arg;
      continue;
    }

    out.scriptArgs.push(arg);
  }

  return out;
}

function extractPlatformMessage(payload) {
  const candidates = [
    payload && payload.errMessage,
    payload && payload.exceptionMsg,
    payload && payload.message,
    payload && payload.resData && payload.resData.reason,
    payload && payload.resData && payload.resData.message,
  ];

  for (const item of candidates) {
    if (item !== undefined && item !== null && String(item).trim()) {
      return String(item).trim();
    }
  }

  return '';
}

function parseSetCookie(setCookieHeader) {
  if (!setCookieHeader) return {};
  const values = Array.isArray(setCookieHeader) ? setCookieHeader : [setCookieHeader];
  const cookieJar = {};

  for (const item of values) {
    const first = String(item).split(';')[0];
    const index = first.indexOf('=');
    if (index > 0) {
      cookieJar[first.slice(0, index).trim()] = first.slice(index + 1).trim();
    }
  }

  return cookieJar;
}

function mergeCookieJar(target, incoming) {
  for (const [key, value] of Object.entries(incoming)) {
    target[key] = value;
  }
}

function cookieHeader(cookieJar) {
  const entries = Object.entries(cookieJar);
  if (!entries.length) return '';
  return entries.map(([key, value]) => `${key}=${value}`).join('; ');
}

async function requestWithSession(client, cookieJar, reqConfig) {
  const headers = {
    ...PLATFORM_HEADERS,
    ...(reqConfig.headers || {}),
  };

  const cookie = cookieHeader(cookieJar);
  if (cookie) headers.Cookie = cookie;

  const response = await client.request({
    method: reqConfig.method || 'GET',
    url: reqConfig.url,
    headers,
    params: reqConfig.params,
    data: reqConfig.data,
    responseType: reqConfig.responseType,
  });

  mergeCookieJar(cookieJar, parseSetCookie(response.headers['set-cookie']));
  return response;
}

function authedHeaders(context, extraHeaders = {}) {
  const headers = { ...extraHeaders };
  if (context.tokenValue) headers[TOKEN_HEADER] = context.tokenValue;
  return headers;
}

function resolveCredentials(configCredentials = {}, overrides = {}, { required = true } = {}) {
  const credentials = {
    account: String(configCredentials.account || '').trim(),
    password: String(configCredentials.password || '').trim(),
    captcha: String(configCredentials.captcha || '').trim(),
  };

  if (Object.prototype.hasOwnProperty.call(overrides, 'captcha')) {
    credentials.captcha = String(overrides.captcha || '').trim();
  }

  if (required && (!credentials.account || !credentials.password)) {
    throw new Error('Missing account or password. Run: submit login --account <account> --password <password>.');
  }

  return credentials;
}

function readSessionCache(settings, config) {
  if (settings.sessionCache.enabled === false) return null;

  const sessionPath = getSessionCachePath();
  if (!fs.existsSync(sessionPath)) return null;

  try {
    const data = JSON.parse(fs.readFileSync(sessionPath, 'utf-8'));
    const token = String(data.token || '').trim();
    const cookieJar = (data.cookieJar && typeof data.cookieJar === 'object' && !Array.isArray(data.cookieJar))
      ? data.cookieJar
      : {};

    if (!token && !Object.keys(cookieJar).length) return null;
    if (String(data.baseURL || '') !== String(config.baseURL || '')) return null;

    const maxAgeSec = Number(settings.sessionCache.maxAgeSec);
    if (Number.isFinite(maxAgeSec) && maxAgeSec > 0) {
      const savedAtMs = Date.parse(String(data.savedAt || ''));
      if (Number.isFinite(savedAtMs) && Date.now() - savedAtMs > maxAgeSec * 1000) {
        return null;
      }
    }

    return {
      token,
      cookieJar,
      account: String(data.account || '').trim(),
    };
  } catch {
    return null;
  }
}

function writeSessionCache(settings, config, context, credentials) {
  if (settings.sessionCache.enabled === false) return;

  const sessionPath = getSessionCachePath();
  const payload = {
    version: 1,
    savedAt: new Date().toISOString(),
    baseURL: String(config.baseURL || ''),
    account: String((credentials && credentials.account) || ''),
    token: String(context.tokenValue || ''),
    cookieJar: context.cookieJar || {},
  };

  fs.mkdirSync(path.dirname(sessionPath), { recursive: true });
  fs.writeFileSync(sessionPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf-8');
  try {
    fs.chmodSync(sessionPath, 0o600);
  } catch {
    // ignore chmod failures on unsupported filesystems
  }
}

function decodeBase64Image(raw) {
  const text = String(raw || '').trim();
  if (!text) return Buffer.from([]);
  const match = text.match(/^data:([^;]+);base64,(.+)$/i);
  return Buffer.from(match ? match[2] : text, 'base64');
}

function detectImageExt(contentType = '') {
  const type = String(contentType || '').toLowerCase();
  if (type.includes('png')) return 'png';
  if (type.includes('jpeg') || type.includes('jpg')) return 'jpg';
  if (type.includes('gif')) return 'gif';
  if (type.includes('bmp')) return 'bmp';
  if (type.includes('webp')) return 'webp';
  if (type.includes('svg')) return 'svg';
  return 'png';
}

function firstNonEmpty(values) {
  for (const value of values) {
    if (value !== undefined && value !== null && String(value).trim()) {
      return String(value).trim();
    }
  }
  return '';
}

function normalizeImageOption(item) {
  if (typeof item === 'string') {
    const text = item.trim();
    return text ? { display: text, id: '', type: '' } : null;
  }

  if (!item || typeof item !== 'object') return null;

  const id = firstNonEmpty([item.id, item.imageId, item.uuid, item.value]);
  const repo = firstNonEmpty([item.image, item.imageName, item.name, item.repository, item.repo, item.fullName, item.displayName, item.label]);
  const tag = firstNonEmpty([item.imageTag, item.tag, item.version]);
  const type = firstNonEmpty([item.type, item.framework, item.jobType, item.imageType]);
  const hasTagInRepo = repo.lastIndexOf(':') > repo.lastIndexOf('/');

  let display = repo;
  if (repo && tag && !hasTagInRepo) display = `${repo}:${tag}`;
  if (!display) display = id;
  if (!display) return null;

  return { display, id, type };
}

function dedupeImageOptions(items) {
  const out = [];
  const seen = new Set();

  for (const item of items) {
    const key = `${item.display}|${item.id}|${item.type}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }

  return out;
}

function extractImageArray(payload) {
  const paths = ['resData', 'resData.data', 'resData.list', 'resData.records', 'data', 'data.list', 'data.records', 'list'];
  for (const candidate of paths) {
    const value = candidate.split('.').reduce((acc, key) => (acc === undefined || acc === null ? undefined : acc[key]), payload);
    if (Array.isArray(value)) return value;
  }
  return [];
}

function selectImageOption(options, selector) {
  const text = String(selector || '').trim();
  if (!text) throw new Error('Image selector is empty. Use an index, id, or keyword.');

  if (/^\d+$/.test(text)) {
    const index = Number(text) - 1;
    if (index >= 0 && index < options.length) return options[index];
    throw new Error(`Image index out of range: ${text} (valid range: 1-${options.length})`);
  }

  const exactById = options.find((item) => item.id === text);
  if (exactById) return exactById;

  const exactByDisplay = options.find((item) => item.display === text);
  if (exactByDisplay) return exactByDisplay;

  const lowered = text.toLowerCase();
  const matches = options.filter((item) =>
    item.display.toLowerCase().includes(lowered) ||
    item.id.toLowerCase().includes(lowered) ||
    item.type.toLowerCase().includes(lowered),
  );

  if (!matches.length) throw new Error(`No image matched: ${text}`);
  if (matches.length === 1) return matches[0];

  const hint = matches
    .slice(0, 5)
    .map((item, index) => `${index + 1}. ${item.display}${item.id ? ` | id=${item.id}` : ''}`)
    .join('\n');

  throw new Error(`Image keyword matched multiple results. Use an index or a more specific keyword:\n${hint}`);
}

function normalizeTaskIdentity(task) {
  return {
    id: String(task.id || task.jobId || task.taskId || '').trim(),
    name: String(task.name || task.taskName || '').trim(),
    status: String(task.status || '').trim(),
    createTime: Number(task.createTime || 0),
  };
}

function buildTaskHandle(taskIdentity) {
  if (taskIdentity.id) return `tid_${taskIdentity.id}`;
  if (taskIdentity.name) return `tname_${encodeURIComponent(taskIdentity.name)}`;
  return '';
}

function parseReconnectTarget(raw) {
  const text = String(raw || '').trim();
  if (!text) return { id: '', name: '' };
  if (text.startsWith('tid_')) return { id: text.slice(4), name: '' };
  if (text.startsWith('tname_')) {
    try {
      return { id: '', name: decodeURIComponent(text.slice(6)) };
    } catch {
      return { id: '', name: text.slice(6) };
    }
  }
  if (/^\d+$/.test(text)) return { id: text, name: '' };
  return { id: '', name: text };
}

function resolveReconnectTargetInput(raw) {
  const candidate = String(raw || '').trim();
  if (!candidate) return { id: '', name: '' };

  const possiblePath = path.isAbsolute(candidate) ? candidate : path.resolve(process.cwd(), candidate);
  if (fs.existsSync(possiblePath) && fs.statSync(possiblePath).isFile()) {
    try {
      const data = JSON.parse(fs.readFileSync(possiblePath, 'utf-8'));
      const handle = String((data.task && data.task.handle) || data.handle || '').trim();
      if (handle) return parseReconnectTarget(handle);

      const id = String((data.task && data.task.id) || '').trim();
      const name = String((data.task && data.task.name) || '').trim();
      if (id || name) return { id, name };
    } catch {
      // fall through to raw handle parsing
    }
  }

  return parseReconnectTarget(candidate);
}

function buildReconnectCommand(settings, taskIdentity) {
  const commandName = String(settings.commandName || 'submit').trim() || 'submit';
  const handle = buildTaskHandle(taskIdentity);
  if (handle) return `${commandName} reconnect ${shellQuote(handle)}`;
  if (taskIdentity.id) return `${commandName} reconnect ${shellQuote(taskIdentity.id)}`;
  if (taskIdentity.name) return `${commandName} reconnect ${shellQuote(taskIdentity.name)}`;
  return `${commandName} reconnect <handle>`;
}

function writeIncidentReport(settings, taskIdentity, reason) {
  const handle = buildTaskHandle(taskIdentity);
  if (!taskIdentity.id && !taskIdentity.name && !handle) return '';

  const reportDir = path.join(GLOBAL_DIR, 'reports');
  fs.mkdirSync(reportDir, { recursive: true });

  const safeReason = String(reason || 'unexpected').toLowerCase().replace(/[^a-z0-9_-]/g, '_');
  const reportPath = path.join(reportDir, `incident-${toTimeName()}-${safeReason}.json`);

  const payload = {
    version: 1,
    generatedAt: new Date().toISOString(),
    reason,
    task: {
      id: taskIdentity.id,
      name: taskIdentity.name,
      handle,
    },
    reconnect: {
      command: buildReconnectCommand(settings, taskIdentity),
      target: handle || taskIdentity.id || taskIdentity.name,
    },
    environment: {
      cwd: process.cwd(),
      commandName: settings.commandName,
      pid: process.pid,
    },
  };

  fs.writeFileSync(reportPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf-8');
  return reportPath;
}

function normalizeNodeName(raw) {
  return String(raw || '').trim();
}

function extractObservedNode(taskSummary) {
  if (!taskSummary || typeof taskSummary !== 'object') return '';
  return firstNonEmpty([taskSummary.nodeName, taskSummary.chosenNodeName]);
}

function isNoSpaceImagePullFailure(statusReason) {
  const text = String(statusReason || '').toLowerCase();
  if (!text) return false;
  return text.includes('no space left on device')
    || (text.includes('failed to register layer') && text.includes('error processing tar file'));
}

function safeNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function summarizeNodeRecord(item) {
  if (!item || typeof item !== 'object') return null;
  const dockerMountInfo = item.dockerMountInfo && typeof item.dockerMountInfo === 'object'
    ? item.dockerMountInfo
    : {};
  return {
    nodeName: normalizeNodeName(item.nodeName),
    groupName: String(item.groupName || '').trim(),
    nodeStatus: String(item.nodeStatus || '').trim(),
    nodeResourceStatus: String(item.nodeResourceStatus || '').trim(),
    cpuTotal: safeNumber(item.cpu),
    cpuUsed: safeNumber(item.cpuUsage),
    gpuTotal: safeNumber(item.acceleratorCard),
    gpuUsed: safeNumber(item.acceleratorCardUsage),
    diskTotal: safeNumber(item.disk),
    rootUsage: String(dockerMountInfo.usage || '').trim(),
    rootAvailable: safeNumber(dockerMountInfo.available),
    rootUsagePercent: safeNumber(String(dockerMountInfo.usage || '').replace('%', ''), 0),
  };
}

function sortEligibleNodeRecords(nodeRecords) {
  return [...nodeRecords].sort((left, right) => {
    if (left.rootAvailable !== right.rootAvailable) return right.rootAvailable - left.rootAvailable;
    if (left.rootUsagePercent !== right.rootUsagePercent) return left.rootUsagePercent - right.rootUsagePercent;
    if (left.gpuUsed !== right.gpuUsed) return left.gpuUsed - right.gpuUsed;
    if (left.cpuUsed !== right.cpuUsed) return left.cpuUsed - right.cpuUsed;
    return String(left.nodeName || '').localeCompare(String(right.nodeName || ''));
  });
}

function nodeHasCapacity(nodeInfo, requiredCpu, requiredGpu) {
  if (!nodeInfo) return false;
  const freeCpu = safeNumber(nodeInfo.cpuTotal) - safeNumber(nodeInfo.cpuUsed);
  const freeGpu = safeNumber(nodeInfo.gpuTotal) - safeNumber(nodeInfo.gpuUsed);
  return freeCpu >= requiredCpu && freeGpu >= requiredGpu;
}

function isEligibleNode(nodeInfo, requiredCpu, requiredGpu) {
  if (!nodeInfo) return false;
  if (String(nodeInfo.groupName || '').trim() && String(nodeInfo.groupName || '').toLowerCase() !== TRAINING_GROUP_NAME) return false;
  if (String(nodeInfo.nodeStatus || '').toLowerCase() !== 'ready') return false;
  if (String(nodeInfo.nodeResourceStatus || '').toLowerCase() !== 'healthy') return false;
  if (!nodeHasCapacity(nodeInfo, requiredCpu, requiredGpu)) return false;
  return nodeInfo.rootAvailable > 0;
}

function chooseBestNode(nodeRecords = [], requiredCpu, requiredGpu) {
  const eligible = nodeRecords.filter((item) => isEligibleNode(item, requiredCpu, requiredGpu));
  const ranked = sortEligibleNodeRecords(eligible);
  return ranked.length ? ranked[0].nodeName : '';
}

function formatGiBFromKiB(valueKiB) {
  const gib = safeNumber(valueKiB) / (1024 * 1024);
  return `${gib.toFixed(1)}G`;
}

function findNodeInfo(nodeRecords, nodeName) {
  const target = normalizeNodeName(nodeName);
  if (!target) return null;
  return nodeRecords.find((item) => item.nodeName === target) || null;
}

function formatNodeInfoBrief(nodeInfo) {
  if (!nodeInfo) return 'node=-';
  const rootAvailPercent = Number.isFinite(nodeInfo.rootUsagePercent)
    ? Math.max(0, 100 - nodeInfo.rootUsagePercent)
    : null;
  return [
    `CPU: ${nodeInfo.cpuUsed}/${nodeInfo.cpuTotal}`,
    `GPU: ${nodeInfo.gpuUsed}/${nodeInfo.gpuTotal}`,
    Number.isFinite(nodeInfo.rootAvailable)
      ? `RootAvail: ${formatGiBFromKiB(nodeInfo.rootAvailable)}${rootAvailPercent === null ? '' : ` (${rootAvailPercent}%)`}`
      : '',
  ].filter(Boolean).join(', ');
}

function resolveRuntimeWorkDir() {
  return process.cwd();
}

function resolveInterpreterForFile(filePath, settings) {
  const ext = String(path.extname(filePath) || '').toLowerCase();
  const interpreters = settings.singleFile.interpreters || {};
  return {
    ext,
    interpreter: String(interpreters[ext] || settings.singleFile.fallbackInterpreter || '').trim(),
  };
}

function prepareSingleFileLauncher(singleFile, runtimeWorkDir, settings, cliScriptArgs = []) {
  const trimmed = String(singleFile || '').trim();
  if (!trimmed) throw new Error('Single-file mode requires a file path.');

  const absoluteFile = path.isAbsolute(trimmed) ? trimmed : path.resolve(process.cwd(), trimmed);
  if (!fs.existsSync(absoluteFile)) throw new Error(`Local file not found: ${absoluteFile}`);

  const { ext, interpreter } = resolveInterpreterForFile(absoluteFile, settings);
  if (!interpreter) {
    throw new Error(`No interpreter configured for file extension ${ext || '(empty)'}. Check settings.singleFile.interpreters.`);
  }

  const launcherDir = path.resolve(runtimeWorkDir, String(settings.singleFile.launcherDir || '.autosubmit/launchers'));
  fs.mkdirSync(launcherDir, { recursive: true });

  const configuredScriptArgs = Array.isArray(settings.singleFile.scriptArgs)
    ? settings.singleFile.scriptArgs.map((item) => String(item))
    : [];
  const passthroughArgs = Array.isArray(cliScriptArgs)
    ? cliScriptArgs.map((item) => String(item))
    : [];
  const scriptArgs = [...configuredScriptArgs, ...passthroughArgs];

  const launcherName = `${path.basename(absoluteFile).replace(/[^a-zA-Z0-9._-]/g, '_')}.${toTimeName()}.submit.sh`;
  const launcherPath = path.join(launcherDir, launcherName);
  const scriptLine = `${interpreter} ${shellQuote(absoluteFile)}${scriptArgs.length ? ` ${scriptArgs.map((item) => shellQuote(item)).join(' ')}` : ''}`;

  const launcherContent = [
    '#!/usr/bin/env bash',
    'set -euo pipefail',
    `cd ${shellQuote(runtimeWorkDir)}`,
    scriptLine,
    '',
  ].join('\n');

  fs.writeFileSync(launcherPath, launcherContent, 'utf-8');
  fs.chmodSync(launcherPath, 0o755);

  return {
    absoluteFile,
    launcherPath,
    scriptArgs,
  };
}

function cleanupLauncherIfNeeded(launcherPath, settings) {
  if (settings.singleFile.keepLauncher || !launcherPath) return;
  try {
    if (fs.existsSync(launcherPath)) fs.unlinkSync(launcherPath);
  } catch {
    // ignore cleanup failures
  }
}

function buildSubmitPayload({ boundImage, imageType, taskName, cpuCores, acceleratorCount, launcherPath, nodeName }) {
  const acceleratorCardKind = acceleratorCount > 0 ? 'GPU' : 'CPU';
  const acceleratorCardType = acceleratorCount > 0 ? SUBMIT_PROFILE.gpuCardType : null;
  const normalizedImageType = String(imageType || SUBMIT_PROFILE.imageType).trim() || SUBMIT_PROFILE.imageType;
  const workerConfig = {
    worker: {
      nodeNum: 1,
      cpuNum: cpuCores,
      acceleratorCardNum: acceleratorCount,
      memory: 0,
      minNodeNum: -1,
    },
  };

  return {
    name: taskName,
    description: '',
    projectId: SUBMIT_PROFILE.projectId,
    imageType: normalizedImageType,
    resGroupId: SUBMIT_PROFILE.resGroupId,
    acceleratorCardType,
    image: boundImage,
    mountDir: '',
    startScript: '',
    logOut: '',
    distFlag: SUBMIT_PROFILE.distFlag,
    enUpdateDataSet: SUBMIT_PROFILE.enUpdateDataSet,
    param: null,
    execDir: '',
    nodeName: String(nodeName || '').trim(),
    mpiFlag: SUBMIT_PROFILE.mpiFlag,
    type: normalizedImageType,
    shmSize: SUBMIT_PROFILE.shmSize,
    datasetId: null,
    emergencyFlag: SUBMIT_PROFILE.emergencyFlag,
    imageFlag: SUBMIT_PROFILE.imageFlag,
    switchType: SUBMIT_PROFILE.switchType,
    isElastic: SUBMIT_PROFILE.isElastic,
    acceleratorCardKind,
    models: [],
    config: JSON.stringify(workerConfig),
    command: `bash ${shellQuote(launcherPath)}`,
    commandScriptList: [],
    jobVolume: [],
  };
}

function validateSubmitPayload(payload) {
  const issues = [];
  const requiredFields = ['name', 'projectId', 'resGroupId', 'image', 'type', 'command', 'config'];

  for (const field of requiredFields) {
    const value = payload[field];
    if (value === undefined || value === null || String(value).trim() === '') {
      issues.push(`Missing required field: ${field}`);
    }
  }

  const workerConfig = safeJsonParse(String(payload.config || '{}'), {});
  const worker = workerConfig && workerConfig.worker ? workerConfig.worker : {};

  if (!Number.isFinite(worker.nodeNum) || worker.nodeNum < 1) issues.push('config.worker.nodeNum must be >= 1');
  if (!Number.isFinite(worker.cpuNum) || worker.cpuNum < 1) issues.push('config.worker.cpuNum must be >= 1');
  if (!Number.isFinite(worker.acceleratorCardNum) || worker.acceleratorCardNum < 0) issues.push('config.worker.acceleratorCardNum must be >= 0');

  return issues;
}

function summarizeSubmitPayload(payload) {
  const workerConfig = safeJsonParse(String(payload.config || '{}'), {});
  return {
    name: payload.name,
    projectId: payload.projectId,
    resGroupId: payload.resGroupId,
    type: payload.type,
    image: payload.image,
    acceleratorCardKind: payload.acceleratorCardKind,
    command: payload.command,
    worker: workerConfig && workerConfig.worker ? workerConfig.worker : {},
  };
}

function summarizeTaskRecord(task, source) {
  if (!task || typeof task !== 'object') return null;
  return {
    source,
    id: firstNonEmpty([task.id, task.jobId, task.taskId]),
    name: firstNonEmpty([task.name, task.taskName]),
    status: firstNonEmpty([task.status]),
    statusReason: firstNonEmpty([task.statusReason, task.reason]),
    nodeName: firstNonEmpty([task.nodeName]),
    chosenNodeName: firstNonEmpty([task.choosedNodeName, task.chosenNodeName]),
    imageType: firstNonEmpty([task.imageType, task.jobType]),
    type: firstNonEmpty([task.type]),
    command: firstNonEmpty([task.command]),
    createTime: Number(task.createTime || 0),
    updateTime: Number(task.updateTime || 0),
  };
}

function extractTaskIdentityFromSubmitResponse(payload, fallbackName = '') {
  const idCandidates = [
    payload && payload.resData && payload.resData.id,
    payload && payload.resData && payload.resData.taskId,
    payload && payload.id,
    payload && payload.taskId,
  ];
  const nameCandidates = [
    payload && payload.resData && payload.resData.name,
    payload && payload.resData && payload.resData.taskName,
    payload && payload.name,
    payload && payload.taskName,
    fallbackName,
  ];

  return {
    id: firstNonEmpty(idCandidates),
    name: firstNonEmpty(nameCandidates),
  };
}

function normalizeLogValue(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value;

  if (Array.isArray(value)) {
    return value
      .map((item) => {
        if (typeof item === 'string') return item;
        if (item && typeof item === 'object') return String(item.content || item.log || item.message || '');
        return String(item);
      })
      .filter(Boolean)
      .join('\n');
  }

  if (typeof value === 'object') {
    return [value.stdout, value.stderr, value.content, value.log].filter((item) => typeof item === 'string').join('\n');
  }

  return String(value);
}

function renderIncrementalLog(nextText, state) {
  if (!nextText) return '';

  const normalized = String(nextText).replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const previous = state.lastText || '';

  if (!previous) {
    state.lastText = normalized;
    return normalized.endsWith('\n') ? normalized : `${normalized}\n`;
  }

  if (normalized === previous) return '';

  if (normalized.startsWith(previous)) {
    const delta = normalized.slice(previous.length);
    state.lastText = normalized;
    return delta;
  }

  state.lastText = normalized;
  return `\n[log-reset]\n${normalized.endsWith('\n') ? normalized : `${normalized}\n`}`;
}

function isSuccessTerminalStatus(status) {
  const text = String(status || '').toLowerCase();
  return ['success', 'succeeded', 'completed', 'done', '成功', '完成'].some((item) => text.includes(item));
}

function isFailedTerminalStatus(status) {
  const text = String(status || '').toLowerCase();
  return ['fail', 'failed', 'error', 'exception', 'cancel', 'stopped', '失败', '终止', '取消'].some((item) => text.includes(item));
}

function isAmbiguousTerminalStatus(status) {
  const text = String(status || '').toLowerCase();
  return ['finished', 'finish'].some((item) => text.includes(item));
}

function isCaptchaError(error) {
  const text = String((error && error.message) || '').toLowerCase();
  return text.includes('captcha') || text.includes('验证码');
}

async function promptLine(question) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      rl.close();
      resolve(String(answer || '').trim());
    });
  });
}

async function fetchPublicKey(context) {
  const response = await requestWithSession(context.client, context.cookieJar, {
    method: 'GET',
    url: '/api/ibase/v1/system/secret',
  });

  if (response.status < 200 || response.status >= 300) {
    throw new Error(`Failed to fetch public key: HTTP ${response.status}`);
  }

  const publicKey = response.data && response.data.resData;
  if (!publicKey) throw new Error('Public key was not returned. Cannot log in.');
  return publicKey;
}

function encryptPasswordLikeFrontend(rawPassword, publicKey) {
  const cipher = sm2.doEncrypt(String(rawPassword), String(publicKey), 0);
  return String(cipher).startsWith('04') ? String(cipher) : `04${cipher}`;
}

async function loginAndBuildSession(context, credentials) {
  const publicKey = await fetchPublicKey(context);
  const body = {
    account: credentials.account,
    password: encryptPasswordLikeFrontend(credentials.password, publicKey),
  };
  if (credentials.captcha) body.captcha = credentials.captcha;

  const response = await requestWithSession(context.client, context.cookieJar, {
    method: 'POST',
    url: '/api/ibase/v1/login',
    headers: { 'Content-Type': 'application/json' },
    data: body,
  });

  if (response.status < 200 || response.status >= 300) {
    throw new Error(`Login failed: HTTP ${response.status}`);
  }

  if (!response.data || response.data.flag !== true) {
    const message = extractPlatformMessage(response.data);
    throw new Error(message ? `Login failed: ${message}` : `Login failed: ${JSON.stringify(response.data)}`);
  }

  const tokenValue = String(((response.data || {}).resData || {}).token || '').trim();
  if (!tokenValue) throw new Error('Login succeeded but no token was returned.');

  context.tokenValue = tokenValue;
  return response.data;
}

async function fetchActiveTasks(context) {
  const response = await requestWithSession(context.client, context.cookieJar, {
    ...ACTIVE_TASKS_REQUEST,
    headers: authedHeaders(context),
  });

  if (response.status < 200 || response.status >= 300) {
    throw new Error(`Failed to fetch task list: HTTP ${response.status}`);
  }

  const tasks = response.data && response.data.resData && response.data.resData.data;
  if (!Array.isArray(tasks)) {
    throw new Error('Unexpected task list response shape: resData.data is not an array.');
  }

  return tasks;
}

async function fetchTasksByStatusFlag(context, statusFlag, extraParams = {}) {
  const response = await requestWithSession(context.client, context.cookieJar, {
    method: 'GET',
    url: '/api/iresource/v1/train',
    headers: authedHeaders(context),
    params: {
      page: 1,
      pageSize: 50,
      taskType: '',
      statusFlag,
      ...extraParams,
    },
  });

  if (response.status < 200 || response.status >= 300) {
    throw new Error(`Failed to fetch task list: HTTP ${response.status}`);
  }

  const tasks = response.data && response.data.resData && response.data.resData.data;
  if (!Array.isArray(tasks)) {
    throw new Error('Unexpected task list response shape: resData.data is not an array.');
  }

  return tasks;
}

async function fetchNodeRecords(context, groupId) {
  const response = await requestWithSession(context.client, context.cookieJar, {
    method: 'GET',
    url: '/api/iresource/v1/node',
    headers: authedHeaders(context),
    params: {
      page: 1,
      pageSize: 999,
      getUsage: 1,
      groupId,
    },
  });

  if (response.status < 200 || response.status >= 300) {
    throw new Error(`Failed to fetch node list: HTTP ${response.status}`);
  }

  const tasks = response.data && response.data.resData && response.data.resData.data;
  if (!Array.isArray(tasks)) {
    throw new Error('Unexpected node list response shape: resData.data is not an array.');
  }

  return tasks.map(summarizeNodeRecord).filter(Boolean);
}

async function fetchImageOptions(context) {
  const response = await requestWithSession(context.client, context.cookieJar, {
    method: 'GET',
    url: '/api/iresource/v1/images/all',
    headers: authedHeaders(context),
  });

  if (response.status < 200 || response.status >= 300) {
    throw new Error(`Failed to fetch image list: HTTP ${response.status}`);
  }

  const options = dedupeImageOptions(
    extractImageArray(response.data).map(normalizeImageOption).filter(Boolean),
  );

  return {
    options,
    source: 'GET /api/iresource/v1/images/all',
  };
}

function inferImageTypeFromImageRef(imageRef) {
  const text = String(imageRef || '').trim().toLowerCase();
  if (!text) return '';
  if (text.includes('/pytorch/')) return 'pytorch';
  if (text.includes('/tensorflow/')) return 'tensorflow';
  if (text.includes('/other/')) return 'other';
  return '';
}

async function resolveBoundImageSpec(context, boundImage) {
  const exact = String(boundImage || '').trim();
  if (!exact) {
    return { image: '', imageType: SUBMIT_PROFILE.imageType, inferred: false };
  }

  try {
    const result = await fetchImageOptions(context);
    const match = result.options.find((item) => item.display === exact);
    if (match && match.type) {
      return {
        image: exact,
        imageType: match.type,
        inferred: false,
      };
    }
  } catch {
    // Fall back to image-ref inference if image lookup is unavailable.
  }

  return {
    image: exact,
    imageType: inferImageTypeFromImageRef(exact) || SUBMIT_PROFILE.imageType,
    inferred: true,
  };
}

async function fetchCaptchaImage(context) {
  const tried = [];

  for (const candidate of CAPTCHA_CANDIDATES) {
    const response = await requestWithSession(context.client, context.cookieJar, {
      method: candidate.method,
      url: candidate.url,
      responseType: candidate.isBinary ? 'arraybuffer' : undefined,
    });

    tried.push(`${candidate.method} ${candidate.url} -> HTTP ${response.status}`);
    if (response.status < 200 || response.status >= 300) continue;

    if (candidate.isBinary) {
      const buffer = Buffer.isBuffer(response.data) ? response.data : Buffer.from(response.data || '');
      if (buffer.length) {
        return {
          buffer,
          ext: detectImageExt(response.headers['content-type'] || ''),
          source: `${candidate.method} ${candidate.url}`,
          tried,
        };
      }
      continue;
    }

    const raw = candidate.imagePath.split('.').reduce((acc, key) => (acc === undefined || acc === null ? undefined : acc[key]), response.data);
    const buffer = decodeBase64Image(raw);
    if (!buffer.length) continue;

    return {
      buffer,
      ext: 'png',
      source: `${candidate.method} ${candidate.url}`,
      tried,
    };
  }

  return {
    buffer: Buffer.from([]),
    ext: 'png',
    source: '',
    tried,
  };
}

async function checkResources(context, payload) {
  const response = await requestWithSession(context.client, context.cookieJar, {
    method: 'POST',
    url: '/api/iresource/v1/train/check-resources',
    headers: authedHeaders(context, { 'Content-Type': 'application/json' }),
    data: {
      type: payload.type,
      acceleratorCardKind: payload.acceleratorCardKind,
      distFlag: payload.distFlag,
      mpiFlag: payload.mpiFlag,
      config: payload.config,
    },
  });

  if (response.status < 200 || response.status >= 300) {
    throw new Error(`Resource check failed: HTTP ${response.status}`);
  }

  if (!response.data || response.data.flag !== true) {
    const message = extractPlatformMessage(response.data);
    throw new Error(message ? `Resource check failed: ${message}` : `Resource check failed: ${JSON.stringify(response.data)}`);
  }

  const checkResult = response.data.resData || {};
  if (checkResult.flag === false) {
    throw new Error(`Resource check failed: ${String(checkResult.reason || 'platform reported insufficient resources').trim()}`);
  }

  return response.data;
}

async function submitTask(context, payload) {
  const response = await requestWithSession(context.client, context.cookieJar, {
    method: 'POST',
    url: '/api/iresource/v1/train',
    headers: authedHeaders(context, { 'Content-Type': 'application/json' }),
    data: payload,
  });

  if (response.status < 200 || response.status >= 300) {
    throw new Error(`Submit failed HTTP ${response.status}`);
  }

  if (!response.data || response.data.flag !== true) {
    const message = extractPlatformMessage(response.data);
    throw new Error(message ? `Submit failed: ${message}` : `Submit failed: ${JSON.stringify(response.data)}`);
  }

  return response.data;
}

async function fetchTaskSummary(context, taskIdentity) {
  if (!taskIdentity || (!taskIdentity.id && !taskIdentity.name)) return null;

  try {
    const activeTasks = await fetchTasksByStatusFlag(context, 0);
    const activeMatch = activeTasks.find((item) =>
      (taskIdentity.id && String(item.id || item.jobId || item.taskId || '').trim() === taskIdentity.id) ||
      (taskIdentity.name && String(item.name || item.taskName || '').trim() === taskIdentity.name),
    );
    if (activeMatch) return summarizeTaskRecord(activeMatch, 'active');
  } catch {
    // ignore transient active-list failures during foreground polling
  }

  if (!taskIdentity.id) return null;

  try {
    const historyTasks = await fetchTasksByStatusFlag(context, 3, { id: taskIdentity.id });
    const historyMatch = historyTasks.find((item) =>
      String(item.id || item.jobId || item.taskId || '').trim() === taskIdentity.id,
    );
    if (historyMatch) return summarizeTaskRecord(historyMatch, 'history');
  } catch {
    // ignore transient history-list failures during foreground polling
  }

  return null;
}

async function deleteTask(context, taskId) {
  const candidates = [
    {
      method: 'DELETE',
      url: '/api/iresource/v1/train',
      headers: { 'Content-Type': 'application/json' },
      data: { jobIdList: [taskId], endTaskListFlag: false },
    },
    {
      method: 'DELETE',
      url: `/api/iresource/v1/train/${encodeURIComponent(taskId)}`,
    },
    {
      method: 'DELETE',
      url: `/api/iresource/v1/train/history-job/${encodeURIComponent(taskId)}`,
    },
    {
      method: 'POST',
      url: `/api/iresource/v1/train/${encodeURIComponent(taskId)}/delete`,
    },
  ];

  for (const candidate of candidates) {
    const response = await requestWithSession(context.client, context.cookieJar, {
      ...candidate,
      headers: authedHeaders(context, candidate.headers || {}),
    });

    if (response.status >= 200 && response.status < 300) {
      return { ok: true, status: response.status, method: candidate.method, url: candidate.url };
    }
  }

  return { ok: false };
}

async function fetchStatusAndLogs(context, taskId, logState) {
  if (!taskId) return { status: '', logDelta: '' };

  let response;
  try {
    response = await requestWithSession(context.client, context.cookieJar, {
      method: 'GET',
      url: `/api/iresource/v1/train/${encodeURIComponent(taskId)}/read-log`,
      headers: authedHeaders(context),
      params: {
        offsetFrom: 20000000,
        offsetTo: 20000300,
      },
    });
  } catch {
    return { status: '', logDelta: '' };
  }

  if (response.status < 200 || response.status >= 300) {
    return { status: '', logDelta: '' };
  }

  const data = (response.data && response.data.resData) || {};
  return {
    status: String(data.status || ''),
    logDelta: renderIncrementalLog(normalizeLogValue(data.logs), logState),
  };
}

async function ensureSession(config, settings, context, getCredentials) {
  const hintedCredentials = resolveCredentials(config.credentials || {}, {}, { required: false });
  const cached = readSessionCache(settings, config);

  if (cached) {
    const hintedAccount = String(hintedCredentials.account || '').trim();
    const cachedAccount = String(cached.account || '').trim();

    if (!hintedAccount || !cachedAccount || hintedAccount === cachedAccount) {
      context.tokenValue = cached.token;
      context.cookieJar = { ...(cached.cookieJar || {}) };

      try {
        const probeTasks = await fetchActiveTasks(context);
        return { reused: true, loginPayload: {}, probeTasks };
      } catch {
        clearSessionCache();
        context.tokenValue = '';
        context.cookieJar = {};
      }
    } else {
      clearSessionCache();
    }
  }

  const credentials = getCredentials();
  const loginPayload = await loginAndBuildSession(context, credentials);
  writeSessionCache(settings, config, context, credentials);
  return { reused: false, loginPayload, probeTasks: null };
}

async function resolveTaskIdByNameIfNeeded(context, taskIdentity) {
  if (taskIdentity.id || !taskIdentity.name) return taskIdentity.id;

  try {
    const tasks = await fetchActiveTasks(context);
    const candidates = tasks
      .map(normalizeTaskIdentity)
      .filter((item) => item.name === taskIdentity.name)
      .sort((left, right) => right.createTime - left.createTime);

    if (candidates.length) {
      taskIdentity.id = candidates[0].id;
      return taskIdentity.id;
    }
  } catch {
    // ignore transient lookup failures
  }

  return '';
}

async function locateTaskIdentity(context, identity, beforeIds, submittedAt, startupTimeoutMs, pollIntervalMs) {
  const startAt = Date.now();

  while (Date.now() - startAt <= startupTimeoutMs) {
    const tasks = await fetchActiveTasks(context);
    const all = tasks.map(normalizeTaskIdentity);

    if (identity.id) {
      const byId = all.find((item) => item.id === identity.id);
      if (byId) {
        identity.name = identity.name || byId.name;
        return identity;
      }
    }

    if (identity.name) {
      const byName = all
        .filter((item) => item.name === identity.name)
        .sort((left, right) => right.createTime - left.createTime);

      if (byName.length) {
        identity.id = byName[0].id;
        return identity;
      }
    }

    const newTasks = all
      .filter((item) => item.id && !beforeIds.has(item.id))
      .filter((item) => !submittedAt || item.createTime >= submittedAt - 5000)
      .sort((left, right) => right.createTime - left.createTime);

    if (newTasks.length) {
      identity.id = newTasks[0].id;
      identity.name = identity.name || newTasks[0].name;
      return identity;
    }

    await waitShort(pollIntervalMs);
  }

  return identity;
}

async function monitorTaskForeground(context, taskIdentity, pollIntervalMs, runtimeState) {
  const logState = { lastText: '' };
  let pullingPrinted = false;

  while (!runtimeState.interrupted) {
    if (!taskIdentity.id && taskIdentity.name) {
      await resolveTaskIdByNameIfNeeded(context, taskIdentity);
    }

    const result = await fetchStatusAndLogs(context, taskIdentity.id, logState);
    if (result.logDelta) process.stdout.write(result.logDelta);

    if (!pullingPrinted) {
      const taskSummary = await fetchTaskSummary(context, taskIdentity);
      if (taskSummary) {
        if (String(taskSummary.status || '').toLowerCase() === 'imagepulling') {
          const observedNode = extractObservedNode(taskSummary);
          const nodeInfo = findNodeInfo(runtimeState.nodeRecords || [], observedNode);
          const details = [
            `Node: ${observedNode || '-'}`,
            nodeInfo ? formatNodeInfoBrief(nodeInfo) : '',
          ].filter(Boolean).join(', ');
          console.log(`ImagePulling -> ${details}`);
          pullingPrinted = true;
        }
      }
    }

    if (result.status && (isSuccessTerminalStatus(result.status) || isFailedTerminalStatus(result.status) || isAmbiguousTerminalStatus(result.status))) {
      await waitShort(pollIntervalMs);

      const finalResult = await fetchStatusAndLogs(context, taskIdentity.id, logState);
      if (finalResult.logDelta) process.stdout.write(finalResult.logDelta);
      const finalTaskSummary = await fetchTaskSummary(context, taskIdentity);

      runtimeState.taskFinalized = true;
      const resolvedStatus = finalTaskSummary && finalTaskSummary.status
        ? String(finalTaskSummary.status)
        : String(finalResult.status || result.status || '');
      const outcome = {
        success: isSuccessTerminalStatus(resolvedStatus),
        failed: isFailedTerminalStatus(resolvedStatus),
        ambiguous: !isSuccessTerminalStatus(resolvedStatus) && !isFailedTerminalStatus(resolvedStatus),
        status: String(result.status || ''),
        finalStatus: String(finalResult.status || ''),
        resolvedStatus,
        finalTaskSummary,
        retryableImagePullFailure: Boolean(
          isFailedTerminalStatus(resolvedStatus)
          && finalTaskSummary
          && isNoSpaceImagePullFailure(finalTaskSummary.statusReason),
        ),
      };

      if (outcome.success) {
        console.log('Submit succeeded');
        console.log(`Task completed: status=${resolvedStatus}`);
      } else if (outcome.retryableImagePullFailure) {
        process.exitCode = 0;
      } else if (outcome.failed) {
        console.error(`\nTask finished with non-success status: ${resolvedStatus}`);
        if (finalTaskSummary && String(finalTaskSummary.statusReason || '').trim()) {
          console.error(`Status reason: ${String(finalTaskSummary.statusReason).trim()}`);
        }
        process.exitCode = 1;
      } else if (isSuccessTerminalStatus(result.status)) {
        console.log(`\nTask finished with status: ${result.status}`);
      } else if (isFailedTerminalStatus(result.status)) {
        console.error(`\nTask finished with non-success status: ${result.status}`);
        if (finalTaskSummary && String(finalTaskSummary.statusReason || '').trim()) {
          console.error(`Status reason: ${String(finalTaskSummary.statusReason).trim()}`);
        }
        process.exitCode = 1;
      } else {
        console.error(`\nTask reached ambiguous terminal status: ${result.status}`);
        if (finalTaskSummary && String(finalTaskSummary.status || '').trim()) {
          console.error(`Final task summary status: ${String(finalTaskSummary.status).trim()}`);
        }
        if (finalTaskSummary && String(finalTaskSummary.statusReason || '').trim()) {
          console.error(`Status reason: ${String(finalTaskSummary.statusReason).trim()}`);
        }
        console.error('Result is not verified as success. Check the web UI before trusting this run.');
        process.exitCode = 1;
      }
      return outcome;
    }

    await waitShort(pollIntervalMs);
  }
}

async function main() {
  const config = loadConfig({ required: true });
  const settings = loadSettings();
  const cli = parseCliArgs(process.argv.slice(2));

  if (!String(config.baseURL || '').trim()) {
    throw new Error('Missing baseURL in ~/.autosubmit/config.json.');
  }

  const pollIntervalSec = Number(settings.foreground.pollIntervalSec);
  if (!Number.isFinite(pollIntervalSec) || pollIntervalSec <= 0) {
    throw new Error('settings.foreground.pollIntervalSec must be greater than 0.');
  }

  const cpuCores = Number(settings.submitDefaults.cpuCores);
  if (!Number.isFinite(cpuCores) || cpuCores < 1) {
    throw new Error('settings.submitDefaults.cpuCores must be >= 1.');
  }

  const acceleratorCount = Number(settings.submitDefaults.acceleratorCount);
  if (!Number.isFinite(acceleratorCount) || acceleratorCount < 0) {
    throw new Error('settings.submitDefaults.acceleratorCount must be >= 0.');
  }

  const runtimeWorkDir = resolveRuntimeWorkDir();
  const boundImage = String(settings.project.image || '').trim();
  const pollIntervalMs = Math.floor(pollIntervalSec * 1000);
  const taskLookupTimeoutMs = 120000;

  const client = axios.create({
    baseURL: config.baseURL,
    timeout: Number(config.timeoutMs) || 15000,
    httpsAgent: new https.Agent({ rejectUnauthorized: !config.ignoreHTTPSErrors }),
    validateStatus: () => true,
  });

  const context = {
    client,
    cookieJar: {},
    tokenValue: '',
  };

  const credentialOverrides = {};
  if (String(cli.onceCaptcha || '').trim()) {
    credentialOverrides.captcha = String(cli.onceCaptcha).trim();
  }

  let credentials = null;
  let cachedProbeTasks = null;
  let authReady = false;
  let cachedLoginPayload = {};

  const getCredentials = () => {
    if (!credentials) {
      credentials = resolveCredentials(config.credentials || {}, credentialOverrides);
    }
    return credentials;
  };

  const ensureAuthenticated = async () => {
    if (authReady) return cachedLoginPayload;
    const session = await ensureSession(config, settings, context, getCredentials);
    authReady = true;
    cachedLoginPayload = session.loginPayload || {};
    cachedProbeTasks = Array.isArray(session.probeTasks) ? session.probeTasks : null;
    return cachedLoginPayload;
  };

  const runtimeState = {
    preparedSingleFile: null,
    nodeRecords: [],
    interrupted: false,
    interruptRunning: false,
    controlInProgress: false,
    taskSubmitted: false,
    taskFinalized: false,
    taskIdentity: {
      id: '',
      name: '',
    },
  };

  const emitIncidentReport = (reason) => {
    if (!runtimeState.taskSubmitted || runtimeState.taskFinalized || runtimeState.controlInProgress) return '';
    return writeIncidentReport(settings, runtimeState.taskIdentity, reason);
  };

  const onCtrlC = async () => {
    if (runtimeState.interruptRunning) {
      process.exit(130);
      return;
    }

    runtimeState.interruptRunning = true;
    runtimeState.interrupted = true;
    runtimeState.controlInProgress = true;
    process.stderr.write('\nInterrupted. Deleting remote task...\n');

    try {
      await resolveTaskIdByNameIfNeeded(context, runtimeState.taskIdentity);
      if (!runtimeState.taskIdentity.id) throw new Error('task ID unknown, cannot delete');

      const result = await deleteTask(context, runtimeState.taskIdentity.id);
      if (result.ok) {
        process.stderr.write('Remote task deleted.\n');
      } else {
        process.stderr.write('Remote task deletion failed.\n');
      }
    } catch (error) {
      process.stderr.write(`Remote task deletion failed: ${error.message}\n`);
    } finally {
      if (runtimeState.preparedSingleFile) {
        cleanupLauncherIfNeeded(runtimeState.preparedSingleFile.launcherPath, settings);
      }
      process.exit(130);
    }
  };

  const onUnexpectedSignal = (signal, code) => {
    if (runtimeState.controlInProgress) {
      process.exit(code);
      return;
    }

    runtimeState.interrupted = true;
    const reportPath = emitIncidentReport(`signal_${String(signal).toLowerCase()}`);
    if (reportPath) {
      process.stderr.write(`\nUnexpected signal ${signal}. Incident report written: ${reportPath}\n`);
      process.stderr.write(`Reconnect command: ${buildReconnectCommand(settings, runtimeState.taskIdentity)}\n`);
    }

    if (runtimeState.preparedSingleFile) {
      cleanupLauncherIfNeeded(runtimeState.preparedSingleFile.launcherPath, settings);
    }

    process.exit(code);
  };

  process.on('SIGINT', () => { void onCtrlC(); });
  process.once('SIGTERM', () => { onUnexpectedSignal('SIGTERM', 143); });
  process.once('SIGHUP', () => { onUnexpectedSignal('SIGHUP', 129); });

  try {
    if (cli.fetchCaptcha) {
      const result = await fetchCaptchaImage(context);
      if (!result.buffer.length) {
        console.log('No captcha image fetched.');
        if (result.tried.length) {
          result.tried.forEach((line) => console.log(`- ${line}`));
        }
        return;
      }

      const outputPath = String(cli.captchaOutput || '').trim()
        ? path.resolve(process.cwd(), cli.captchaOutput)
        : path.join(process.cwd(), `captcha.${result.ext}`);

      fs.mkdirSync(path.dirname(outputPath), { recursive: true });
      fs.writeFileSync(outputPath, result.buffer);
      console.log(`Captcha image saved: ${outputPath}`);
      if (result.source) console.log(`Source: ${result.source}`);
      return;
    }

    if (cli.listImages) {
      await ensureAuthenticated();
      const result = await fetchImageOptions(context);
      if (!result.options.length) {
        console.log('No available images found.');
        return;
      }

      console.log(`Source: ${result.source}`);
      console.log(`Available images: ${result.options.length}`);
      result.options.forEach((item, index) => {
        const detail = [item.id ? `id=${item.id}` : '', item.type ? `type=${item.type}` : ''].filter(Boolean).join(' | ');
        console.log(detail ? `${index + 1}. ${item.display} | ${detail}` : `${index + 1}. ${item.display}`);
      });
      return;
    }

    if (cli.resolveImageSelector) {
      await ensureAuthenticated();
      const result = await fetchImageOptions(context);
      if (!result.options.length) throw new Error('No available images found. Cannot resolve image selector.');
      console.log(selectImageOption(result.options, cli.resolveImageSelector).display);
      return;
    }

    if (cli.checkLogin) {
      try {
        await ensureAuthenticated();
        console.log('Login succeeded.');
        return;
      } catch (firstError) {
        if (!isCaptchaError(firstError)) throw firstError;

        let lastError = firstError;
        for (let attempt = 1; attempt <= 3; attempt += 1) {
          let imagePath = '';

          try {
            const result = await fetchCaptchaImage(context);
            if (result.buffer.length) {
              imagePath = path.join(process.cwd(), `captcha-login.${result.ext}`);
              fs.writeFileSync(imagePath, result.buffer);
              console.log(`Captcha image saved: ${imagePath}`);
            }
          } catch {
            // ignore captcha image fetch failures
          }

          const userCaptcha = await promptLine(imagePath ? `Enter captcha (see ${imagePath}): ` : 'Enter captcha: ');
          if (!userCaptcha) {
            console.log('Captcha is empty. Login cancelled.');
            throw lastError;
          }

          credentials = null;
          authReady = false;
          cachedProbeTasks = null;
          cachedLoginPayload = {};
          context.tokenValue = '';
          context.cookieJar = {};
          clearSessionCache();
          credentialOverrides.captcha = userCaptcha;

          try {
            await ensureAuthenticated();
            console.log('Login succeeded.');
            return;
          } catch (retryError) {
            lastError = retryError;
            if (!isCaptchaError(retryError)) throw retryError;
            console.log(`Captcha verification failed (attempt ${attempt}).`);
          }
        }

        throw lastError;
      }
    }

    if (cli.reconnectTarget) {
      const parsedTarget = resolveReconnectTargetInput(cli.reconnectTarget);
      if (!parsedTarget.id && !parsedTarget.name) {
        throw new Error('Invalid reconnect target. Use a handle, taskId, taskName, or incident report file.');
      }

      runtimeState.taskIdentity.id = parsedTarget.id;
      runtimeState.taskIdentity.name = parsedTarget.name;
      runtimeState.taskSubmitted = true;

      await ensureAuthenticated();
      await locateTaskIdentity(context, runtimeState.taskIdentity, new Set(), 0, taskLookupTimeoutMs, pollIntervalMs);

      console.log(`Reconnected task: id=${runtimeState.taskIdentity.id || '-'} name=${runtimeState.taskIdentity.name || '-'}`);
      console.log(`Reconnect command: ${buildReconnectCommand(settings, runtimeState.taskIdentity)}`);
      await monitorTaskForeground(context, runtimeState.taskIdentity, pollIntervalMs, runtimeState);
      return;
    }

    if (!boundImage) throw new Error('No image is bound. Run: submit set image <image>.');
    if (!fs.existsSync(runtimeWorkDir)) throw new Error(`Workdir does not exist: ${runtimeWorkDir}`);
    if (!fs.statSync(runtimeWorkDir).isDirectory()) throw new Error(`Workdir is not a directory: ${runtimeWorkDir}`);
    if (!cli.singleFile) throw new Error('Missing script file. Use: submit <file> ...');

    runtimeState.preparedSingleFile = prepareSingleFileLauncher(cli.singleFile, runtimeWorkDir, settings, cli.scriptArgs);
    if (runtimeState.preparedSingleFile.scriptArgs.length) {
      console.log(`Script args: ${JSON.stringify(runtimeState.preparedSingleFile.scriptArgs)}`);
    }

    await ensureAuthenticated();

    const tasksBefore = cachedProbeTasks || await fetchActiveTasks(context);
    cachedProbeTasks = null;
    const beforeIds = new Set(tasksBefore.map((item) => normalizeTaskIdentity(item).id).filter(Boolean));
    try {
      runtimeState.nodeRecords = await fetchNodeRecords(context, SUBMIT_PROFILE.resGroupId);
    } catch {
      runtimeState.nodeRecords = [];
    }

    const boundImageSpec = await resolveBoundImageSpec(context, boundImage);
    if (boundImageSpec.inferred) {
      console.log(`Image type inferred for submit payload: image=${boundImageSpec.image} type=${boundImageSpec.imageType}`);
    }

    runtimeState.taskSubmitted = false;
    runtimeState.taskFinalized = false;
    runtimeState.taskIdentity = { id: '', name: '' };

    try {
      runtimeState.nodeRecords = await fetchNodeRecords(context, SUBMIT_PROFILE.resGroupId);
    } catch {
      // keep previous snapshot when live refresh fails
    }

    const selectedNode = chooseBestNode(runtimeState.nodeRecords, cpuCores, acceleratorCount);
    if (!selectedNode) {
      throw new Error('No ready and healthy node has enough free CPU, GPU, and root disk space for this request. Please try again later.');
    }

    const payload = buildSubmitPayload({
      boundImage: boundImageSpec.image,
      imageType: boundImageSpec.imageType,
      taskName: String(settings.submitDefaults.taskName || '').trim() || toTimeName(),
      cpuCores,
      acceleratorCount,
      launcherPath: runtimeState.preparedSingleFile.launcherPath,
      nodeName: selectedNode,
    });

    const issues = validateSubmitPayload(payload);
    if (issues.length) {
      throw new Error(`Local submit validation failed:\n- ${issues.join('\n- ')}\nSubmit summary: ${JSON.stringify(summarizeSubmitPayload(payload))}`);
    }

    await checkResources(context, payload);

    const submittedAt = Date.now();
    const submitResponse = await submitTask(context, payload);

    const taskIdentity = extractTaskIdentityFromSubmitResponse(submitResponse, payload.name);
    runtimeState.taskIdentity.id = taskIdentity.id;
    runtimeState.taskIdentity.name = taskIdentity.name || payload.name;
    runtimeState.taskSubmitted = true;

    await locateTaskIdentity(context, runtimeState.taskIdentity, beforeIds, submittedAt, taskLookupTimeoutMs, pollIntervalMs);

    console.log(`Reconnect command: ${buildReconnectCommand(settings, runtimeState.taskIdentity)}`);

    const outcome = await monitorTaskForeground(context, runtimeState.taskIdentity, pollIntervalMs, runtimeState);
    const finalTaskSummary = outcome && outcome.finalTaskSummary ? outcome.finalTaskSummary : null;
    const observedNode = extractObservedNode(finalTaskSummary) || selectedNode;
    const statusReason = finalTaskSummary ? String(finalTaskSummary.statusReason || '').trim() : '';

    if (outcome && outcome.success) {
      process.exitCode = 0;
    } else if (outcome && outcome.failed && observedNode && isNoSpaceImagePullFailure(statusReason)) {
      process.exitCode = 1;
      console.error(`ImagePulling failed node=${observedNode} reason=${statusReason}`);
      console.error('The best eligible node still failed image pulling because of insufficient disk space. Please try again later.');
    }
  } catch (error) {
    const reportPath = emitIncidentReport('runtime_error');
    if (reportPath) {
      console.error(`Incident report written: ${reportPath}`);
      console.error(`Reconnect command: ${buildReconnectCommand(settings, runtimeState.taskIdentity)}`);
    }
    throw error;
  } finally {
    if (runtimeState.preparedSingleFile) {
      cleanupLauncherIfNeeded(runtimeState.preparedSingleFile.launcherPath, settings);
    }
  }
}

main().catch((error) => {
  console.error('Run failed:', error.message);
  process.exit(1);
});
