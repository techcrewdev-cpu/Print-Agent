/**
 * Smart Xerox Print Agent — Electron Main Process
 * Manages: window, tray icon, IPC handlers, auto-launch, print engine.
 */

const { app, BrowserWindow, Tray, Menu, ipcMain, nativeImage, shell } = require('electron');
const path        = require('path');
const Store       = require('electron-store');
const AutoLaunch  = require('auto-launch');
const axios       = require('axios');
const engine      = require('./print-engine-multi');

// ─── CONFIGURE THESE ONCE before building the installer ───────────────────
const API_URL      = process.env.SMARTXEROX_API_URL      || 'https://smart-xerox-backend-dhz3.onrender.com/api';
const FRONTEND_URL = process.env.SMARTXEROX_FRONTEND_URL || 'https://smart-xerox-frontend.vercel.app';

// ─── Register custom protocol (smartxerox://) ─────────────────────────────
if (process.defaultApp) {
  // Dev mode — register with argv
  if (process.argv.length >= 2) {
    app.setAsDefaultProtocolClient('smartxerox', process.execPath, [path.resolve(process.argv[1])]);
  }
} else {
  app.setAsDefaultProtocolClient('smartxerox');
}

// ─── Persistent Store (encrypted) ────────────────────────────────────────────
// ✅ FIX #18: Generate unique encryption key per installation
// Instead of hardcoded 'smartxerox-agent-v1', use machine-specific key
// This prevents anyone with the store file from decrypting it with a known key
const crypto = require('crypto');
const os = require('os');
const fs = require('fs');

// Generate a unique encryption key based on machine ID + app data path
// This ensures each installation has a different key
function getEncryptionKey() {
  const keyFile = path.join(app.getPath('userData'), '.encryption-key');
  
  // If key already exists, use it (consistent across app restarts)
  if (fs.existsSync(keyFile)) {
    try {
      return fs.readFileSync(keyFile, 'utf8').trim();
    } catch {}
  }
  
  // Generate new key: hash of machine ID + app path + random salt
  const machineId = os.hostname() + os.platform() + os.arch();
  const appPath = app.getPath('userData');
  const salt = crypto.randomBytes(16).toString('hex');
  const key = crypto
    .createHash('sha256')
    .update(machineId + appPath + salt)
    .digest('hex');
  
  // Save key for future use
  try {
    fs.writeFileSync(keyFile, key, { mode: 0o600 }); // Read-only by owner
  } catch (err) {
    console.warn('Could not save encryption key:', err.message);
  }
  
  return key;
}

let store;
const storeOptions = {
  encryptionKey: getEncryptionKey(),  // ✅ FIX #18: Unique per installation
  schema: {
    apiUrl:         { type: 'string',  default: '' },
    token:          { type: 'string',  default: '' },
    refreshToken:   { type: 'string',  default: '' },
    userEmail:      { type: 'string',  default: '' },
    userName:       { type: 'string',  default: '' },
    shopName:       { type: 'string',  default: '' },
    printerNames:   { type: 'array',   default: [] },  // Multiple printers
    incompleteJobs: { type: 'array',   default: [] },
  },
};

try {
  store = new Store(storeOptions);
  // Force a read check to trigger decryption and schema validation immediately
  store.get('apiUrl');
} catch (err) {
  console.error('Failed to initialize electron-store (decryption or schema parse failure). Resetting config...', err);
  try {
    const configPath = path.join(app.getPath('userData'), 'config.json');
    if (fs.existsSync(configPath)) {
      fs.unlinkSync(configPath);
      console.log('Successfully deleted corrupted config file:', configPath);
    }
  } catch (deleteErr) {
    console.error('Failed to delete config file:', deleteErr);
  }
  
  // Re-attempt creating the store with the encryption key after clearing the corrupted config
  try {
    store = new Store(storeOptions);
  } catch (secondErr) {
    console.error('Failed to initialize store after clearing config. Initializing without encryption key.', secondErr);
    const unencryptedOptions = { ...storeOptions };
    delete unencryptedOptions.encryptionKey;
    store = new Store(unencryptedOptions);
  }
}

let mainWindow = null;
let tray       = null;
let isEngineRunning = false;

// ─── Auto Launch on Windows Boot ─────────────────────────────────────────────
const autoLauncher = new AutoLaunch({
  name: 'Smart Xerox Print Agent',
  isHidden: true,
});

// ─── Create Main Window ──────────────────────────────────────────────────────
function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    minWidth: 900,
    minHeight: 600,
    resizable: true,
    maximizable: true,
    title: 'Smart Xerox Print Agent',
    icon: getTrayIcon(),
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  mainWindow.loadFile(path.join(__dirname, 'renderer', 'index.html'));
  mainWindow.setMenuBarVisibility(false);

  // Minimize to tray instead of closing
  mainWindow.on('close', (e) => {
    if (!app.isQuitting) {
      e.preventDefault();
      mainWindow.hide();
    }
  });
}

// ─── System Tray ─────────────────────────────────────────────────────────────
function createTray() {
  const icon = getTrayIcon();
  tray = new Tray(icon);
  tray.setToolTip('Smart Xerox Print Agent');

  const contextMenu = Menu.buildFromTemplate([
    { label: 'Show Status',   click: () => { mainWindow.show(); mainWindow.focus(); } },
    { label: 'Reconnect',     click: () => { engine.disconnect(); startEngine(); } },
    { type: 'separator' },
    { label: 'Quit', click: () => { app.isQuitting = true; app.quit(); } },
  ]);

  tray.setContextMenu(contextMenu);
  tray.on('double-click', () => { mainWindow.show(); mainWindow.focus(); });
}

function getTrayIcon() {
  // Simple programmatic icon — green circle for "printer"
  const size = 32;
  const canvas = nativeImage.createEmpty();
  try {
    const iconPath = path.join(__dirname, 'assets', 'icon.png');
    const fs = require('fs');
    if (fs.existsSync(iconPath)) {
      return nativeImage.createFromPath(iconPath);
    }
  } catch {}
  // Fallback: use default Electron icon
  return nativeImage.createEmpty();
}

// ─── IPC Handlers ────────────────────────────────────────────────────────────

// Login with shopkeeper credentials
ipcMain.handle('login', async (_event, { email, password }) => {
  try {
    const cleanUrl = API_URL.replace(/\/+$/, '');
    const res = await axios.post(`${cleanUrl}/auth/login`, { email, password });
    const { token, refreshToken, user } = res.data.data;

    if (user.role !== 'shopkeeper') {
      return { success: false, error: 'Only shopkeeper accounts can use the Print Agent.' };
    }

    // Fetch shop info
    let shopName = '';
    try {
      const shopRes = await axios.get(`${cleanUrl}/shops/my-shop/dashboard`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      shopName = shopRes.data.data?.shop?.name || shopRes.data.data?.name || '';
    } catch {}

    // Persist credentials
    store.set('apiUrl', cleanUrl);
    store.set('token', token);
    store.set('refreshToken', refreshToken);
    store.set('userEmail', user.email);
    store.set('userName', user.name);
    store.set('shopName', shopName);

    // Enable auto-launch
    try { await autoLauncher.enable(); } catch {}

    return { success: true, user: { name: user.name, email: user.email, shopName } };
  } catch (err) {
    const msg = err.response?.data?.message || err.message || 'Login failed';
    return { success: false, error: msg };
  }
});

// Get saved session
ipcMain.handle('getSession', () => {
  const token = store.get('token');
  if (!token) return null;
  return {
    apiUrl:       store.get('apiUrl'),
    userEmail:    store.get('userEmail'),
    userName:     store.get('userName'),
    shopName:     store.get('shopName'),
    printerNames: store.get('printerNames') || [],
  };
});

// Logout
ipcMain.handle('logout', () => {
  engine.disconnect();
  stopTokenRefreshTimer();
  store.clear();
  isEngineRunning = false;
  // Send logout event to renderer to show login screen
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send('engine-event', {
      type: 'logout_success',
      message: 'Logged out successfully',
    });
  }
  return true;
});

// List available printers
ipcMain.handle('getPrinters', async () => {
  return await engine.listPrinters();
});

// Select printer(s) — now supports multiple printers
ipcMain.handle('selectPrinter', (_event, printerNames) => {
  // printerNames can be a string (single) or array (multiple)
  const names = Array.isArray(printerNames) ? printerNames : [printerNames];
  store.set('printerNames', names);
  engine.setPrinters(names);
  return true;
});

// Toggle a specific printer on/off
ipcMain.handle('togglePrinter', (_event, printerName, enabled) => {
  engine.togglePrinter(printerName, enabled);
  return true;
});

// Connect the print engine
ipcMain.handle('connectEngine', () => {
  startEngine();
  return true;
});

// Refresh (manual poll)
ipcMain.handle('refreshEngine', () => {
  engine.refresh();
  return true;
});

// Disconnect
ipcMain.handle('disconnectEngine', () => {
  engine.disconnect();
  isEngineRunning = false;
  return true;
});

// Get live status
ipcMain.handle('getStatus', () => {
  return engine.getStatus();
});

// Get logs
ipcMain.handle('getLogs', () => {
  return engine.getLogs();
});

// ── Fault-Tolerant Print Handlers ──────────────────────────────────────────

// Pause a running print job
ipcMain.handle('pausePrintJob', (_event, orderId) => {
  engine.pausePrintJob(orderId, 'manual');
  return true;
});

// Resume a paused print job
ipcMain.handle('resumePrintJob', (_event, orderId) => {
  engine.resumePrintJob(orderId);
  return true;
});

// Get all paused/incomplete jobs
ipcMain.handle('getPausedJobs', () => {
  return engine.getPausedJobs();
});

// ─── Start Print Engine ──────────────────────────────────────────────────────
function startEngine(force = false) {
  const apiUrl       = API_URL;
  const token        = store.get('token');
  const printerNames = store.get('printerNames') || [];

  if (!token) return;

  if (isEngineRunning && !force) {
    console.log('[Main] Print engine is already running. Skipping duplicate initialization.');
    return;
  }

  isEngineRunning = true;

  engine.init({
    apiUrl,
    token,
    printerNames,  // Will auto-detect if empty
    store,  // Pass electron-store for local state persistence
    onEvent: (event) => {
      // Forward all engine events to renderer
      if (mainWindow && !mainWindow.isDestroyed()) {
        mainWindow.webContents.send('engine-event', event);
      }

      // Handle auth expiry — auto re-login
      if (event.type === 'auth_expired') {
        handleTokenRefresh();
      }

      // Handle proactive token refresh request from engine interceptor
      if (event.type === 'token_refresh_needed') {
        handleTokenRefresh();
      }

      // Show window on important events (including when printing starts)
      // Commented out to make the print agent completely silent in the background
      /*
      if (event.type === 'printing' || event.type === 'recovery_start' || event.type === 'print_paused' || event.type === 'printers_detected') {
        if (mainWindow && !mainWindow.isDestroyed()) {
          mainWindow.show();
          mainWindow.focus();
        }
      }
      */
    },
  });

  engine.connect();
  
  // Start auto token refresh
  startTokenRefreshTimer();
}

// ─── Token Refresh ───────────────────────────────────────────────────────────
let isRefreshing = false; // prevents concurrent refresh attempts

async function handleTokenRefresh() {
  if (isRefreshing) return; // already in progress
  isRefreshing = true;

  const apiUrl       = API_URL;
  const refreshToken = store.get('refreshToken');
  if (!refreshToken) {
    // No refresh token stored — session is definitively dead
    isRefreshing = false;
    isEngineRunning = false;
    engine.disconnect();
    if (mainWindow && !mainWindow.isDestroyed()) {
      mainWindow.webContents.send('engine-event', { type: 'auth_expired', message: 'Session expired. Please re-login.' });
      mainWindow.show();
    }
    return;
  }

  try {
    const res = await axios.post(`${apiUrl}/auth/refresh-token`, { refreshToken });
    const { token: newToken, refreshToken: newRefresh } = res.data.data;
    store.set('token', newToken);
    store.set('refreshToken', newRefresh);

    // Refresh token in-place — preserves queues and in-flight print jobs
    if (isEngineRunning) {
      engine.updateToken(newToken);
    }
  } catch {
    // Refresh definitively failed — clear stale tokens and force re-login
    store.delete('token');
    store.delete('refreshToken');
    isEngineRunning = false;
    engine.disconnect();
    if (mainWindow && !mainWindow.isDestroyed()) {
      mainWindow.webContents.send('engine-event', {
        type: 'auth_expired',
        message: 'Session expired. Please re-login.',
      });
      mainWindow.show();
    }
  } finally {
    isRefreshing = false;
  }
}

// ─── Auto Token Refresh (runs every 50 minutes) ──────────────────────────────
let tokenRefreshInterval = null;

function startTokenRefreshTimer() {
  if (tokenRefreshInterval) clearInterval(tokenRefreshInterval);
  
  // Refresh token proactively every 20 minutes.
  // This is intentionally conservative — works whether JWT_EXPIRES_IN is 30min, 1h, or 7d.
  // The /auth/refresh-token endpoint uses token rotation so calling it early is safe.
  tokenRefreshInterval = setInterval(async () => {
    const token = store.get('token');
    if (token) {
      console.log('🔄 Auto-refreshing token...');
      await handleTokenRefresh();
    }
  }, 20 * 60 * 1000); // 20 minutes
}

function stopTokenRefreshTimer() {
  if (tokenRefreshInterval) {
    clearInterval(tokenRefreshInterval);
    tokenRefreshInterval = null;
  }
}

// ─── App Lifecycle ───────────────────────────────────────────────────────────
const gotLock = app.requestSingleInstanceLock();
if (!gotLock) {
  app.quit();
} else {
  app.on('second-instance', (_event, argv) => {
    if (mainWindow) { mainWindow.show(); mainWindow.focus(); }
    // Deep link from second instance (Windows)
    const deepLink = argv.find(a => a.startsWith('smartxerox://'));
    if (deepLink) handleDeepLink(deepLink);
  });

  app.whenReady().then(async () => {
    createWindow();
    createTray();

    // Auto-connect if we have a saved session
    // Proactively refresh the access token before starting the engine so we
    // never start with a stale token and trigger the 401 loop.
    const token = store.get('token');
    if (token) {
      // Try to silently refresh first; if it fails handleTokenRefresh clears
      // the stored tokens and shows the login window instead of starting.
      await handleTokenRefresh();
      // Only start the engine if we still have a valid token after refresh
      if (store.get('token')) {
        setTimeout(startEngine, 500);
      }
    }

    // Check if launched via deep link (Windows)
    const deepLink = process.argv.find(a => a.startsWith('smartxerox://'));
    if (deepLink) setTimeout(() => handleDeepLink(deepLink), 1500);
  });

  app.on('window-all-closed', (e) => {
    // Don't quit — keep running in tray
    e?.preventDefault?.();
  });

  app.on('before-quit', () => {
    app.isQuitting = true;
    stopTokenRefreshTimer();
    engine.disconnect();
  });

  app.on('activate', () => {
    if (mainWindow) mainWindow.show();
  });

  // macOS deep link
  app.on('open-url', (_event, url) => {
    handleDeepLink(url);
  });
}

// ─── Deep Link Handler ──────────────────────────────────────────────────
// ─── Deep Link Handler ──────────────────────────────────────────────────
// Handles:
// 1. smartxerox://print/ORDER_ID (trigger print)
// 2. smartxerox://autologin?token=...&email=...&name=...&shopName=...
function handleDeepLink(url) {
  try {
    const parsed = new URL(url);

    // ── Auto Login Flow ──
    if (parsed.host === 'autologin' || parsed.pathname?.startsWith('/autologin')) {
      const token        = parsed.searchParams.get('token');
      const refreshToken = parsed.searchParams.get('refreshToken') || '';
      const email        = parsed.searchParams.get('email');
      const name         = parsed.searchParams.get('name');
      const shopName     = parsed.searchParams.get('shopName') || '';

      if (token && email) {
        // Save to store
        store.set('token', token);
        store.set('refreshToken', refreshToken);
        store.set('userEmail', email);
        store.set('userName', name);
        store.set('shopName', shopName);

        // Turn on auto launch
        try { autoLauncher.enable(); } catch {}

        // Connect engine
        isEngineRunning = false; // reset so startEngine doesn't bail on duplicate check
        engine.disconnect();
        startEngine();

        // Bring to front
        if (mainWindow) { mainWindow.show(); mainWindow.focus(); }

        // Tell UI it logged in successfully
        if (mainWindow && !mainWindow.isDestroyed()) {
          mainWindow.webContents.send('engine-event', {
            type: 'auto_login_success',
          });
        }
        return;
      }
    }

    // ── Print Flow ──
    if (parsed.host === 'print' || parsed.pathname?.startsWith('/print')) {
      // Support both: smartxerox://print/ORDER_ID and smartxerox://print?id=ORDER_ID
      const id = parsed.searchParams?.get('id') ||
                 parsed.pathname?.replace(/^\/?(print\/?)?/, '').split('/')[0];

      if (id && id.length > 5) {
        if (mainWindow) { mainWindow.show(); mainWindow.focus(); }

        if (mainWindow && !mainWindow.isDestroyed()) {
          mainWindow.webContents.send('engine-event', {
            type:    'deep_link_print',
            orderId: id,
            message: `Print job received for order ${id.slice(-6).toUpperCase()}`,
          });
        }

        engine.printOrder(id);
      }
    }
  } catch (err) {
    console.error('Deep link parse error:', err.message);
  }
}
