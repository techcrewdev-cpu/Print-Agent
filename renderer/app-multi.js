/**
 * Smart Xerox Print Agent - Multi-Printer UI (Real-World Implementation)
 * Auto-detects all connected printers and shows live dashboard
 * Syncs with website in real-time
 */

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

// ─── DOM Elements ─────────────────────────────────────────────────────────────
const loginScreen         = $('#loginScreen');
const setupScreen         = $('#setupScreen');
const dashboardScreen     = $('#dashboardScreen');
const loginForm           = $('#loginForm');
const emailIn             = $('#email');
const passwordIn          = $('#password');
const loginBtn            = $('#loginBtn');
const loginError          = $('#loginError');
const statusDot           = $('#statusDot');
const statusLabel         = $('#statusLabel');
const shopNameLbl         = $('#shopNameLabel');
const logoutBtn           = $('#logoutBtn');
const refreshBtn          = $('#refreshBtn');
const clearLogBtn         = $('#clearLogBtn');
const clearHistoryBtn     = $('#clearHistoryBtn');
const logContainer        = $('#logContainer');
const historyContainer    = $('#historyContainer');
const printersGrid        = $('#printersGrid');
const globalQueueList     = $('#globalQueueList');
const globalQueueCount    = $('#globalQueueCount');
const printerLabel        = $('#printerLabel');

// ─── State ────────────────────────────────────────────────────────────────────
let currentScreen  = 'login';
let failCount      = 0;
let statusInterval = null;
let detectedPrinters = [];
let printerStates = new Map();
let printHistory = [];  // Store completed print jobs

// ─── Screen Navigation ────────────────────────────────────────────────────────
function showScreen(name) {
  $$('.screen').forEach(s => s.classList.remove('active'));
  if (name === 'login')     loginScreen.classList.add('active');
  if (name === 'setup')     setupScreen.classList.add('active');
  if (name === 'dashboard') dashboardScreen.classList.add('active');
  currentScreen = name;
}

// ─── Init ─────────────────────────────────────────────────────────────────────
async function init() {
  // Load print history from localStorage
  loadHistoryFromStorage();
  
  const session = await window.agent.getSession();
  if (session && session.userName) {
    // Auto-connect to dashboard (skip setup)
    shopNameLbl.textContent = session.shopName || 'Your Shop';
    showScreen('dashboard');
    await window.agent.connectEngine();
    startStatusPolling();
  } else {
    showScreen('login');
  }
}

// ─── Login ────────────────────────────────────────────────────────────────────
loginForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  loginError.classList.add('hidden');
  const email    = emailIn.value.trim();
  const password = passwordIn.value;
  if (!email || !password) return;
  
  setLoading(loginBtn, true);
  const result = await window.agent.login({ email, password });
  setLoading(loginBtn, false);
  
  if (result.success) {
    shopNameLbl.textContent = result.user.shopName || 'Your Shop';
    // Skip setup, go directly to dashboard
    showScreen('dashboard');
    await window.agent.connectEngine();
    startStatusPolling();
  } else {
    loginError.textContent = result.error;
    loginError.classList.remove('hidden');
  }
});

// ─── Dashboard ────────────────────────────────────────────────────────────────
function startStatusPolling() {
  if (statusInterval) clearInterval(statusInterval);
  updateStatus();
  statusInterval = setInterval(updateStatus, 2000);
}

async function updateStatus() {
  const s = await window.agent.getStatus();
  if (!s) return;

  statusDot.className     = 'status-dot ' + (s.connected ? 'connected' : 'disconnected');
  statusLabel.textContent = s.connected ? 'Connected - Listening' : 'Disconnected';

  // Update printer grid and queue
  if (s.printers && s.printers.length > 0) {
    // First time: render grid
    if (detectedPrinters.length === 0) {
      detectedPrinters = s.printers.map(p => p.name);
      renderPrinterGrid(detectedPrinters);
    }
    
    updatePrinterGrid(s.printers);
    updateQueueDisplay(s.printers);
    updateStats(s);
  }
}

// ─── Render Printer Grid (All Detected Printers) ───────────────────────────────
function renderPrinterGrid(printerNames) {
  printersGrid.innerHTML = '';
  
  for (const name of printerNames) {
    const card = document.createElement('div');
    card.className = 'printer-card';
    card.id = 'printer-' + name;
    card.innerHTML = `
      <div class="printer-header">
        <span class="printer-name">${name}</span>
        <div class="printer-toggle enabled" data-printer="${name}"></div>
      </div>
      <div class="printer-status">
        <span class="status-indicator idle"></span>
        <span>Idle</span>
      </div>
      <div class="printer-stats">
        <div class="printer-stat">
          <span class="printer-stat-label">Load</span>
          <span class="printer-stat-value load">0 pages</span>
        </div>
        <div class="printer-stat">
          <span class="printer-stat-label">Queue</span>
          <span class="printer-stat-value queue">0 orders</span>
        </div>
        <div class="printer-stat">
          <span class="printer-stat-label">Printed</span>
          <span class="printer-stat-value printed">0</span>
        </div>
      </div>
      <div class="printer-health">
        <span class="health-indicator ok">📄 Paper OK</span>
        <span class="health-indicator ok">🟢 Online</span>
      </div>
    `;
    
    const toggle = card.querySelector('.printer-toggle');
    toggle.addEventListener('click', async (e) => {
      e.stopPropagation();
      const enabled = !toggle.classList.contains('enabled');
      toggle.classList.toggle('enabled', enabled);
      card.classList.toggle('disabled', !enabled);
      
      await window.agent.togglePrinter(name, enabled);
      addLog(`Printer ${name} ${enabled ? 'enabled' : 'disabled'}`, 'info');
    });
    
    printersGrid.appendChild(card);
  }
}

// ─── Update Printer Grid (Real-Time Status) ───────────────────────────────────
function updatePrinterGrid(printers) {
  for (const p of printers) {
    const card = document.getElementById('printer-' + p.name);
    if (!card) continue;

    // Update status
    const statusText = card.querySelector('.printer-status');
    const isIdle = p.status === 'idle';
    statusText.innerHTML = `<span class="status-indicator ${isIdle ? 'idle' : 'printing'}"></span><span>${isIdle ? 'Idle' : 'Printing...'}</span>`;

    // Update stats
    card.querySelector('.printer-stat-value.load').textContent = (p.load || 0) + ' pages';
    card.querySelector('.printer-stat-value.queue').textContent = (p.queued || 0) + ' order' + ((p.queued || 0) !== 1 ? 's' : '');
    card.querySelector('.printer-stat-value.printed').textContent = p.printed || 0;

    // Update health
    const healthItems = card.querySelectorAll('.health-indicator');
    const health = p.health || {};
    
    if (healthItems[0]) {
      if (health.paperOut) {
        healthItems[0].className = 'health-indicator error';
        healthItems[0].textContent = '📄 Paper Out!';
      } else {
        healthItems[0].className = 'health-indicator ok';
        healthItems[0].textContent = '📄 Paper OK';
      }
    }

    if (healthItems[1]) {
      if (health.offline) {
        healthItems[1].className = 'health-indicator error';
        healthItems[1].textContent = '🔴 Offline';
      } else {
        healthItems[1].className = 'health-indicator ok';
        healthItems[1].textContent = '🟢 Online';
      }
    }

    // Update toggle state
    const toggle = card.querySelector('.printer-toggle');
    if (p.enabled) {
      toggle.classList.add('enabled');
      card.classList.remove('disabled');
    } else {
      toggle.classList.remove('enabled');
      card.classList.add('disabled');
    }

    // Disable toggle if offline
    if (health.offline) {
      toggle.style.pointerEvents = 'none';
      toggle.style.opacity = '0.5';
    } else {
      toggle.style.pointerEvents = 'auto';
      toggle.style.opacity = '1';
    }
  }
}

// ─── Queue Display ────────────────────────────────────────────────────────────
function updateQueueDisplay(printers) {
  let totalQueued = 0;
  for (const p of printers) {
    totalQueued += (p.queued || 0);
  }

  globalQueueCount.textContent = totalQueued + ' order' + (totalQueued !== 1 ? 's' : '');

  if (totalQueued === 0) {
    globalQueueList.innerHTML = '<div class="queue-empty">All queues empty - Ready to print!</div>';
    return;
  }

  globalQueueList.innerHTML = '';
  for (const p of printers) {
    if ((p.queued || 0) > 0) {
      const item = document.createElement('div');
      item.className = 'queue-item';
      item.innerHTML = `
        <span class="queue-item-order">${p.name}</span>
        <span class="queue-item-printer">${p.queued} order${p.queued !== 1 ? 's' : ''}</span>
      `;
      globalQueueList.appendChild(item);
    }
  }
}

// ─── Stats Display ────────────────────────────────────────────────────────────
function updateStats(status) {
  const totalLoad = (status.printers || []).reduce((sum, p) => sum + (p.load || 0), 0);
  const totalQueued = (status.printers || []).reduce((sum, p) => sum + (p.queued || 0), 0);
  const activePrinters = (status.printers || []).filter(p => p.enabled).length;
  const totalPrinted = (status.printers || []).reduce((sum, p) => sum + (p.printed || 0), 0);

  $('#totalLoad').textContent = totalLoad;
  $('#activePrinters').textContent = activePrinters + '/' + (status.printers || []).length;
  $('#totalQueued').textContent = totalQueued;
  $('#printedCount').textContent = totalPrinted;

  printerLabel.textContent = 'Printers: ' + activePrinters + ' active, ' + totalQueued + ' queued';
}

// ─── Engine Events ────────────────────────────────────────────────────────────
window.agent.onEvent((event) => {
  switch (event.type) {
    case 'connected':
      addLog('Connected to server', 'success');
      updateStatus();
      break;

    case 'force_disconnect':
      addLog(`🚨 ${event.message}`, 'error');
      alert(`CRITICAL: ${event.message}\n\nThis agent will now close. Please ensure only one agent is running per shop.`);
      break;

    case 'printers_detected':
      addLog(`🔍 Detected ${event.count} printers: ${event.printers.join(', ')}`, 'success');
      detectedPrinters = event.printers;
      renderPrinterGrid(event.printers);
      updateStatus();
      break;

    case 'agent_registered':
      addLog(`✅ Agent registered - ${event.printers?.length || 0} printers ready`, 'success');
      updateStatus();
      break;

    case 'printing':
      addLog(`🖨️ Printing #${event.orderNumber} on ${event.printerName}`, 'info');
      updateStatus();
      break;

    case 'print_queued':
      addLog(`📋 Order #${event.orderNumber} queued for ${event.printerName}`, 'info');
      updateStatus();
      break;

    case 'print_complete':
      addLog(`✅ Order #${event.orderNumber} completed on ${event.printerName}`, 'success');
      addToHistory({
        orderNumber: event.orderNumber,
        printerName: event.printerName,
        totalPages: event.specs?.totalPages || 0,
        status: 'completed',
        timestamp: new Date()
      });
      updateStatus();
      break;

    case 'print_timeout':
      addLog(`⏱️ Order #${event.orderNumber} TIMED OUT on ${event.printerName}`, 'error');
      updateStatus();
      break;

    case 'print_error':
      addLog(`❌ Order #${event.orderNumber} failed on ${event.printerName}: ${event.error}`, 'error');
      updateStatus();
      break;

    case 'print_paused':
      addLog(`⏸️ Order #${event.orderId?.slice(-6)} paused on ${event.printerName}: ${event.reason}`, 'warning');
      updateStatus();
      break;

    case 'printer_toggled':
      addLog(`🔄 Printer ${event.printerName} ${event.enabled ? 'enabled' : 'disabled'}`, 'info');
      updateStatus();
      break;

    case 'queue_empty':
      addLog(`✅ Queue empty for ${event.printerName}`, 'success');
      updateStatus();
      break;

    case 'orders_reassigned':
      addLog(`↪️  ${event.count} orders reassigned from ${event.from} to ${event.to}`, 'warning');
      updateStatus();
      break;

    case 'reassignment_failed':
      addLog(`❌ Failed to reassign orders from ${event.offlinePrinter}: ${event.reason}`, 'error');
      break;

    case 'health_check':
      updateStatus();
      break;

    case 'error':
      addLog(`❌ Error: ${event.message}`, 'error');
      break;
  }
});

// ─── Logging ──────────────────────────────────────────────────────────────────
function addLog(message, type) {
  type = type || 'info';
  const entry = document.createElement('div');
  entry.className   = 'log-entry log-' + type;
  entry.textContent = new Date().toLocaleTimeString() + ' — ' + message;
  logContainer.prepend(entry);
  while (logContainer.children.length > 50) logContainer.lastChild.remove();
}

// ─── Print History ────────────────────────────────────────────────────────────
function addToHistory(job) {
  // Add to beginning of history array
  printHistory.unshift(job);
  
  // Keep only last 100 jobs
  if (printHistory.length > 100) {
    printHistory.pop();
  }
  
  // Update UI
  updateHistoryUI();
  
  // Save to localStorage for persistence
  try {
    localStorage.setItem('printHistory', JSON.stringify(printHistory));
  } catch (e) {
    console.warn('Could not save history to localStorage:', e);
  }
}

function updateHistoryUI() {
  if (printHistory.length === 0) {
    historyContainer.innerHTML = `
      <div class="history-empty">
        <div class="history-empty-icon">✓</div>
        <p>No completed print jobs yet</p>
      </div>
    `;
    return;
  }
  
  historyContainer.innerHTML = '';
  
  printHistory.forEach((job, idx) => {
    const item = document.createElement('div');
    item.className = 'history-item';
    
    const time = new Date(job.timestamp).toLocaleTimeString('en-IN');
    const date = new Date(job.timestamp).toLocaleDateString('en-IN');
    
    item.innerHTML = `
      <div class="history-item-header">
        <span class="history-order">#${job.orderNumber}</span>
        <span class="history-status ${job.status}">✓ ${job.status}</span>
      </div>
      <div class="history-item-details">
        <span class="history-printer">🖨️ ${job.printerName}</span>
        <span class="history-pages">📄 ${job.totalPages} pages</span>
      </div>
      <div class="history-item-time">
        ${date} at ${time}
      </div>
    `;
    
    historyContainer.appendChild(item);
  });
}

function loadHistoryFromStorage() {
  try {
    const saved = localStorage.getItem('printHistory');
    if (saved) {
      printHistory = JSON.parse(saved);
      updateHistoryUI();
    }
  } catch (e) {
    console.warn('Could not load history from localStorage:', e);
  }
}

// ─── Controls ─────────────────────────────────────────────────────────────────
clearLogBtn.addEventListener('click', () => {
  logContainer.innerHTML = '';
  failCount = 0;
});

clearHistoryBtn.addEventListener('click', () => {
  if (confirm('Clear all print history? This cannot be undone.')) {
    printHistory = [];
    updateHistoryUI();
    try {
      localStorage.removeItem('printHistory');
    } catch (e) {
      console.warn('Could not clear history from localStorage:', e);
    }
  }
});

refreshBtn.addEventListener('click', async () => {
  refreshBtn.classList.add('rotate-anim');
  await window.agent.refresh();
  updateStatus();
  setTimeout(() => refreshBtn.classList.remove('rotate-anim'), 800);
});

logoutBtn.addEventListener('click', async () => {
  await window.agent.disconnectEngine();
  await window.agent.logout();
  if (statusInterval) clearInterval(statusInterval);
  logContainer.innerHTML = '';
  failCount = 0;
  detectedPrinters = [];
  showScreen('login');
});

// ─── Helpers ──────────────────────────────────────────────────────────────────
function setLoading(btn, loading) {
  const text = btn.querySelector('.btn-text');
  const load = btn.querySelector('.btn-loading');
  if (loading) {
    text?.classList.add('hidden');
    load?.classList.remove('hidden');
    btn.disabled = true;
  } else {
    text?.classList.remove('hidden');
    load?.classList.add('hidden');
    btn.disabled = false;
  }
}

// ─── Start ────────────────────────────────────────────────────────────────────
init();
