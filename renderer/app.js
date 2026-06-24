/**
 * Smart Xerox Print Agent - Renderer (UI Logic)
 * Communicates with main process via window.agent (preload bridge).
 */

// --- DOM Helpers -------------------------------------------------------------
const $  = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

// --- DOM Elements ------------------------------------------------------------
const loginScreen         = $('#loginScreen');
const setupScreen         = $('#setupScreen');
const dashboardScreen     = $('#dashboardScreen');
const loginForm           = $('#loginForm');
const emailIn             = $('#email');
const passwordIn          = $('#password');
const loginBtn            = $('#loginBtn');
const loginError          = $('#loginError');
const printerSelect       = $('#printerSelect');
const connectBtn          = $('#connectBtn');
const welcomeText         = $('#welcomeText');
const statusDot           = $('#statusDot');
const statusLabel         = $('#statusLabel');
const shopNameLbl         = $('#shopNameLabel');
const printerLbl          = $('#printerLabel');
const logoutBtn           = $('#logoutBtn');
const refreshBtn          = $('#refreshBtn');
const clearLogBtn         = $('#clearLogBtn');
const printedCount        = $('#printedCount');
const activeCount         = $('#activeCount');
const queuedCount         = $('#queuedCount');
const failedCount         = $('#failedCount');
const pausedCount         = $('#pausedCount');
const pausedStatCard      = $('#pausedStatCard');
const logContainer        = $('#logContainer');
const recoveryBanner      = $('#recoveryBanner');
const recoveryMessage     = $('#recoveryMessage');
const dismissRecovery     = $('#dismissRecovery');
const pausedJobsSection   = $('#pausedJobsSection');
const pausedJobsContainer = $('#pausedJobsContainer');

// --- State -------------------------------------------------------------------
let currentScreen  = 'login';
let failCount      = 0;
let statusInterval = null;

// --- Screen Navigation -------------------------------------------------------
function showScreen(name) {
  $$('.screen').forEach(s => s.classList.remove('active'));
  if (name === 'login')     loginScreen.classList.add('active');
  if (name === 'setup')     setupScreen.classList.add('active');
  if (name === 'dashboard') dashboardScreen.classList.add('active');
  currentScreen = name;
}

// --- Init: Check Saved Session -----------------------------------------------
async function init() {
  const session = await window.agent.getSession();
  if (session && session.userName) {
    if (session.printerName) {
      shopNameLbl.textContent = session.shopName || 'Your Shop';
      printerLbl.textContent  = 'Printer: ' + session.printerName;
      showScreen('dashboard');
      await window.agent.connectEngine();
      startStatusPolling();
    } else {
      welcomeText.textContent = 'Welcome, ' + (session.userName || 'Shopkeeper') + '!';
      showScreen('setup');
      await loadPrinters();
      
      // Auto-select first real printer if available
      const printers = await window.agent.getPrinters();
      if (printers && printers.length > 0) {
        const firstPrinter = printers[0];
        printerSelect.value = firstPrinter.name;
        addLog('Auto-selected printer: ' + firstPrinter.name, 'info');
      }
    }
  } else {
    showScreen('login');
  }
}

// --- Login -------------------------------------------------------------------
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
    welcomeText.textContent = 'Welcome, ' + result.user.name + '!';
    showScreen('setup');
    await loadPrinters();
  } else {
    loginError.textContent = result.error;
    loginError.classList.remove('hidden');
  }
});

// --- Printer Selection -------------------------------------------------------
async function loadPrinters() {
  printerSelect.innerHTML = '<option value="">Loading printers...</option>';
  const printers = await window.agent.getPrinters();
  if (!printers || printers.length === 0) {
    printerSelect.innerHTML = '<option value="">(system default printer)</option>';
    return;
  }
  printerSelect.innerHTML = '';
  const defaultOpt = document.createElement('option');
  defaultOpt.value = '';
  defaultOpt.textContent = '-- Use System Default --';
  printerSelect.appendChild(defaultOpt);
  printers.forEach(p => {
    const opt = document.createElement('option');
    opt.value = p.name;
    opt.textContent = p.name + (p.isDefault ? '  * Default' : '');
    printerSelect.appendChild(opt);
  });
}

connectBtn.addEventListener('click', async () => {
  const printerName = printerSelect.value;
  if (!printerName) {
    addLog('Please select a printer', 'warning');
    return;
  }
  await window.agent.selectPrinter(printerName);
  const session = await window.agent.getSession();
  shopNameLbl.textContent = session?.shopName || 'Your Shop';
  printerLbl.textContent  = 'Printer: ' + (printerName || 'System Default');
  setLoading(connectBtn, true);
  addLog('Connecting to printer: ' + printerName, 'info');
  await window.agent.connectEngine();
  setLoading(connectBtn, false);
  addLog('Connected to printer: ' + printerName, 'success');
  showScreen('dashboard');
  startStatusPolling();
});

// Auto-connect when printer is selected (if only one printer available)
printerSelect.addEventListener('change', async () => {
  const printerName = printerSelect.value;
  if (printerName && printerSelect.options.length === 2) {
    // Only auto-connect if there's exactly one real printer (plus the default option)
    setTimeout(() => {
      connectBtn.click();
    }, 500);
  }
});

// --- Dashboard ---------------------------------------------------------------
function startStatusPolling() {
  if (statusInterval) clearInterval(statusInterval);
  updateStatus();
  statusInterval = setInterval(updateStatus, 3000);
}

async function updateStatus() {
  const s = await window.agent.getStatus();
  if (!s) return;

  statusDot.className     = 'status-dot ' + (s.connected ? 'connected' : 'disconnected');
  statusLabel.textContent = s.connected ? 'Connected - Listening' : 'Disconnected';
  printedCount.textContent = s.printed || 0;
  activeCount.textContent  = s.active  || 0;
  failedCount.textContent  = failCount;

  const paused = s.paused || 0;
  pausedCount.textContent      = paused;
  pausedStatCard.style.display = paused > 0 ? '' : 'none';

  if (paused > 0) {
    window.agent.getPausedJobs().then(jobs => {
      if (jobs && jobs.length > 0) updatePausedJobsUI(jobs);
    }).catch(() => {});
  } else if (paused === 0 && pausedJobsContainer && pausedJobsContainer.children.length > 0) {
    pausedJobsSection.classList.add('hidden');
    pausedJobsContainer.innerHTML = '';
  }

  if (queuedCount) queuedCount.textContent = s.queued || 0;
}

// --- Recovery Banner ---------------------------------------------------------
dismissRecovery.addEventListener('click', () => {
  recoveryBanner.classList.add('hidden');
});

function showRecoveryBanner(message) {
  recoveryMessage.textContent = message;
  recoveryBanner.classList.remove('hidden');
}

// --- Paused Jobs Display -----------------------------------------------------
function updatePausedJobsUI(pausedJobs) {
  if (!pausedJobs || pausedJobs.length === 0) {
    pausedJobsSection.classList.add('hidden');
    pausedJobsContainer.innerHTML = '';
    return;
  }
  pausedJobsSection.classList.remove('hidden');

  const existingIds = new Set(
    Array.from(pausedJobsContainer.querySelectorAll('.paused-job-item')).map(el => el.dataset.orderId)
  );
  const newIds = new Set(pausedJobs.map(j => j.orderId));

  existingIds.forEach(id => {
    if (!newIds.has(id)) {
      const el = pausedJobsContainer.querySelector('[data-order-id="' + id + '"]');
      if (el) el.remove();
    }
  });

  for (const job of pausedJobs) {
    const pct = job.totalPages > 0 ? Math.round((job.printedPages / job.totalPages) * 100) : 0;
    const reasonLabel = {
      'out_of_paper':  'Out of Paper',
      'printer_error': 'Printer Error',
      'power_failure': 'Power Interrupted',
      'manual':        'Manually Paused',
    }[job.pauseReason] || (job.pauseReason || 'Paused');

    const resumeLabel = job.pauseReason === 'power_failure'
      ? '⚡ Resume from where it stopped'
      : job.pauseReason === 'out_of_paper'
      ? 'Paper added? Resume Printing'
      : 'Resume Printing';

    let el = pausedJobsContainer.querySelector('[data-order-id="' + job.orderId + '"]');
    if (!el) {
      el = document.createElement('div');
      el.className      = 'paused-job-item';
      el.dataset.orderId = job.orderId;
      el.id             = 'paused-' + job.orderId;
      pausedJobsContainer.appendChild(el);
    }

    el.innerHTML =
      '<div class="paused-job-header">' +
        '<span class="paused-job-number">#' + job.orderNumber + '</span>' +
        '<span class="paused-job-reason">' + reasonLabel + '</span>' +
      '</div>' +
      '<div class="paused-job-progress">' +
        '<div class="progress-bar"><div class="progress-fill" style="width:' + pct + '%"></div></div>' +
        '<span class="progress-label">' + job.printedPages + '/' + job.totalPages + ' pages - ' + pct + '% done</span>' +
      '</div>' +
      '<button class="btn-resume" data-order-id="' + job.orderId + '">' + resumeLabel + '</button>';

    el.querySelector('.btn-resume').onclick = async () => {
      const btn = el.querySelector('.btn-resume');
      btn.disabled    = true;
      btn.textContent = 'Resuming...';
      await window.agent.resumePrintJob(job.orderId);
    };
  }
}

// --- Engine Events -----------------------------------------------------------
window.agent.onEvent((event) => {
  switch (event.type) {

    case 'connected':
      addLog('Connected to server (' + event.socketId + ')', 'success');
      updateStatus();
      break;

    case 'agent_registered':
      addLog('Agent registered - listening for orders from ' + (event.shopName || 'your shop'), 'success');
      statusLabel.textContent = 'Connected - Listening';
      updateStatus();
      setTimeout(() => {
        window.agent.getPausedJobs().then(jobs => {
          if (jobs && jobs.length > 0) {
            updatePausedJobsUI(jobs);
            showRecoveryBanner('⚡ Found ' + jobs.length + ' interrupted job(s)');
          }
        }).catch(() => {});
      }, 4000);
      break;

    case 'disconnected':
      addLog('Disconnected: ' + event.reason, 'warning');
      // If disconnected due to logout or auth failure, show login screen
      if (event.reason === 'io server disconnect' || event.reason === 'auth_expired' || event.reason === 'io client disconnect') {
        if (statusInterval) clearInterval(statusInterval);
        logContainer.innerHTML = '<div class="log-empty"><div class="log-empty-icon">P</div><p>Ready to print!</p></div>';
        failCount = 0;
        pausedJobsSection.classList.add('hidden');
        recoveryBanner.classList.add('hidden');
        showScreen('login');
      } else {
        updateStatus();
      }
      break;

    case 'error':
      if (event.message?.toLowerCase().includes('websocket error')) return;
      addLog('Error: ' + event.message, 'error');
      break;

    case 'print_queued':
      updateJobCard(event.orderId, event.orderNumber, 'queued');
      addLog('Order #' + (event.orderNumber || event.orderId?.slice(-6)) + ' queued (' + (event.queueLength || 0) + ' waiting)', 'info');
      updateStatus();
      break;

    case 'printing':
      updateJobCard(event.orderId, event.orderNumber, 'printing', event.specs);
      addLog('Printing #' + (event.orderNumber || event.orderId?.slice(-6)) + ' — ' + (event.specs?.totalPages || '?') + ' pages', 'info');
      updateStatus();
      break;

    case 'print_progress':
      updateJobCardProgress(event.orderId, event.orderNumber, event.printedPages, event.totalPages, event.currentDocIndex, event.totalDocs);
      break;

    case 'print_recovering':
      updateJobCard(event.orderId, event.orderNumber, 'recovering', event.specs);
      addLog('Resuming #' + event.orderNumber + ' from page ' + (event.specs?.printedPages || 0), 'info');
      updateStatus();
      break;

    case 'print_complete':
      updateJobCard(event.orderId, event.orderNumber, 'complete', event.specs);
      removePausedJob(event.orderId);
      updateStatus();
      addLog('✅ Order #' + (event.orderNumber || event.orderId?.slice(-6)) + ' printed & verified — auto-completed', 'success');
      break;

    case 'print_incomplete':
      updateJobCard(event.orderId, event.orderNumber, 'incomplete', event.specs);
      removePausedJob(event.orderId);
      updateStatus();
      addLog('⚠️ Order #' + (event.orderNumber || event.orderId?.slice(-6)) + ' verification failed: ' + event.reason, 'error');
      break;

    case 'print_failed':
      failCount++;
      updateJobCard(event.orderId, event.orderNumber, 'failed');
      addLog('❌ Order #' + (event.orderNumber || event.orderId?.slice(-6)) + ' failed after retries', 'error');
      updateStatus();
      break;

    case 'print_range_start':
      updateJobCardRangeStart(event);
      addLog(
        '#' + (event.orderNumber || event.orderId?.slice(-6)) +
        ' Range ' + (event.rangeIndex + 1) + '/' + event.totalRanges +
        ' (pp.' + event.range?.start + '-' + event.range?.end +
        ', ' + (event.range?.colorMode === 'color' ? 'Color' : 'B&W') +
        ', ' + event.range?.copies + 'x)', 'info'
      );
      break;

    case 'print_range_copy':
      updateJobCardRangeCopy(event);
      break;

    case 'print_range_complete':
      updateJobCardRangeDone(event);
      addLog(
        '#' + (event.orderNumber || event.orderId?.slice(-6)) +
        ' Range ' + (event.rangeIndex + 1) + '/' + event.totalRanges + ' done', 'info'
      );
      break;

    case 'queue_empty':
      addLog('Print queue empty — printer idle', 'success');
      updateStatus();
      break;

    case 'print_paused':
      updateJobCard(event.orderId, event.orderNumber, 'paused', {
        printedPages: event.printedPages,
        totalPages:   event.totalPages,
        pauseReason:  event.reason,
      });
      if (event.pausedJobs) updatePausedJobsUI(event.pausedJobs);
      addLog(
        '#' + (event.orderNumber || event.orderId?.slice(-6)) +
        ' paused — ' + formatPauseReason(event.reason) +
        ' (' + event.printedPages + '/' + event.totalPages + ' pages)', 'warning'
      );
      updateStatus();
      break;

    case 'print_resumed':
      removePausedJob(event.orderId);
      addLog('#' + event.orderNumber + ' resuming from doc ' + ((event.fromDocIndex || 0) + 1) + ', page ' + (event.fromPage || 0), 'success');
      updateStatus();
      break;

    case 'resume_failed':
      addLog('Resume failed for #' + event.orderId?.slice(-6) + ': ' + event.error, 'error');
      break;

    case 'recovery_start':
      showRecoveryBanner('⚡ Found ' + event.count + ' interrupted job(s)');
      addLog('Found ' + event.count + ' interrupted job(s) — check Paused Jobs below', 'warning');
      if (event.jobs) window.agent.getPausedJobs().then(updatePausedJobsUI).catch(() => {});
      break;

    case 'recovery_complete':
      showRecoveryBanner('⚡ Found ' + event.count + ' interrupted job(s)');
      window.agent.getPausedJobs().then(jobs => {
        if (jobs && jobs.length > 0) updatePausedJobsUI(jobs);
      }).catch(() => {});
      break;

    case 'auto_resume':
      addLog('Auto-resuming #' + (event.orderNumber || event.orderId?.slice(-6)) + ' — ' + event.reason, 'info');
      updateStatus();
      break;

    case 'printer_ready':
      if (event.pausedJobs && event.pausedJobs.length > 0) {
        updatePausedJobsUI(event.pausedJobs);
        addLog('Printer ready — ' + event.pausedJobs.length + ' paused job(s) can be resumed', 'success');
      }
      break;

    case 'deep_link_print':
      updateStatus();
      break;

    case 'auth_expired':
      if (event.message?.includes('re-login')) {
        showScreen('login');
        if (statusInterval) clearInterval(statusInterval);
      }
      break;

    case 'auto_login_success':
      init();
      break;

    case 'logout_success':
      addLog('Logged out successfully', 'success');
      if (statusInterval) clearInterval(statusInterval);
      logContainer.innerHTML = '<div class="log-empty"><div class="log-empty-icon">P</div><p>Ready to print!</p></div>';
      failCount = 0;
      pausedJobsSection.classList.add('hidden');
      recoveryBanner.classList.add('hidden');
      showScreen('login');
      break;

    case 'log':
      console.log(event.message);
      break;
  }
});

// --- Job Cards ---------------------------------------------------------------
function updateJobCard(orderId, orderNumber, status, specs) {
  const empty = logContainer.querySelector('.log-empty');
  if (empty) empty.remove();

  let card = document.getElementById('job-' + orderId);
  if (!card) {
    card = document.createElement('div');
    card.id        = 'job-' + orderId;
    card.className = 'job-item';
    logContainer.prepend(card);
  }

  const statusText = {
    queued:     'Queued',
    printing:   'Printing...',
    complete:   'Completed ✅',
    incomplete: 'Verification Failed',
    failed:     'Failed ❌',
    paused:     'Paused ⏸',
    recovering: 'Recovering...',
  }[status] || status;

  let progressHtml = '';
  if (specs && specs.printedPages !== undefined && specs.totalPages) {
    const pct = Math.round((specs.printedPages / specs.totalPages) * 100);
    progressHtml =
      '<div class="job-progress">' +
        '<div class="progress-bar">' +
          '<div class="progress-fill ' + (status === 'paused' ? 'paused' : '') + '" style="width:' + pct + '%"></div>' +
        '</div>' +
        '<span class="progress-label">' + specs.printedPages + '/' + specs.totalPages + ' pages</span>' +
      '</div>';
  }

  let reasonHtml = '';
  if (status === 'paused' && specs?.pauseReason) {
    reasonHtml = '<div class="pause-reason">' + formatPauseReason(specs.pauseReason) + '</div>';
  }

  card.className = 'job-item job-' + status;
  card.innerHTML =
    '<div class="job-header">' +
      '<span class="job-number">#' + (orderNumber || orderId.slice(-6).toUpperCase()) + '</span>' +
      '<span class="job-status ' + status + '">' + statusText + '</span>' +
    '</div>' +
    progressHtml + reasonHtml;

  logContainer.scrollTop = 0;
}

function updateJobCardProgress(orderId, orderNumber, printedPages, totalPages, currentDocIndex, totalDocs) {
  let card = document.getElementById('job-' + orderId);
  if (!card) { updateJobCard(orderId, orderNumber, 'printing', { printedPages, totalPages }); return; }
  const fill  = card.querySelector('.progress-fill');
  const label = card.querySelector('.progress-label');
  if (fill && label) {
    const pct = totalPages > 0 ? Math.round((printedPages / totalPages) * 100) : 0;
    fill.style.width    = pct + '%';
    label.textContent   = printedPages + '/' + totalPages + ' pages — Doc ' + currentDocIndex + '/' + totalDocs;
  } else {
    const div = document.createElement('div');
    div.className = 'job-progress';
    const pct = totalPages > 0 ? Math.round((printedPages / totalPages) * 100) : 0;
    div.innerHTML =
      '<div class="progress-bar"><div class="progress-fill" style="width:' + pct + '%"></div></div>' +
      '<span class="progress-label">' + printedPages + '/' + totalPages + ' pages — Doc ' + currentDocIndex + '/' + totalDocs + '</span>';
    const header = card.querySelector('.job-header');
    if (header) header.after(div); else card.appendChild(div);
  }
}

function removePausedJob(orderId) {
  const el = document.getElementById('paused-' + orderId);
  if (el) el.remove();
  if (pausedJobsContainer.children.length === 0) pausedJobsSection.classList.add('hidden');
}

// --- Range-wise Live Progress ------------------------------------------------
function getRangeSection(orderId) {
  const card = document.getElementById('job-' + orderId);
  if (!card) return null;
  let section = card.querySelector('.range-section');
  if (!section) {
    section = document.createElement('div');
    section.className = 'range-section';
    const ref = card.querySelector('.job-progress') || card.querySelector('.job-header');
    if (ref) ref.after(section); else card.appendChild(section);
  }
  return section;
}

function getRangeRow(orderId, rangeIndex) {
  const section = getRangeSection(orderId);
  if (!section) return null;
  let row = section.querySelector('[data-range="' + rangeIndex + '"]');
  if (!row) {
    row = document.createElement('div');
    row.className    = 'range-row range-pending';
    row.dataset.range = rangeIndex;
    section.appendChild(row);
  }
  return row;
}

function renderRangeRow(row, event, status, currentCopy) {
  const r          = event.range;
  const colorBadge = r.colorMode === 'color' ? '<span class="badge range-color">Color</span>' : '<span class="badge range-bw">B&W</span>';
  const sidesBadge = r.sides === 'double' ? '<span class="badge">2-sided</span>' : '';
  const copyText   = r.copies > 1 ? 'Copy ' + (currentCopy || r.copies) + '/' + r.copies : '';
  const icon       = { active: '[P]', done: '[OK]', pending: '[..]' }[status] || '[..]';
  row.className    = 'range-row range-' + status;
  row.innerHTML    =
    '<span class="range-icon">' + icon + '</span>' +
    '<span class="range-label">Range ' + (event.rangeIndex + 1) + '/' + event.totalRanges + ' pp.' + r.start + '-' + r.end + '</span>' +
    '<div class="range-badges">' + colorBadge + sidesBadge + '</div>' +
    (copyText ? '<span class="range-copy">' + copyText + '</span>' : '');
}

function updateJobCardRangeStart(event) {
  if (!document.getElementById('job-' + event.orderId)) {
    updateJobCard(event.orderId, event.orderNumber, 'printing', { printedPages: event.completedRanges, totalPages: event.totalRanges });
  }
  const row = getRangeRow(event.orderId, event.rangeIndex);
  if (row) renderRangeRow(row, event, 'active', 1);
}

function updateJobCardRangeCopy(event) {
  const row = getRangeRow(event.orderId, event.rangeIndex);
  if (row) renderRangeRow(row, event, 'active', event.currentCopy);
}

function updateJobCardRangeDone(event) {
  const row = getRangeRow(event.orderId, event.rangeIndex);
  if (row) renderRangeRow(row, event, 'done', event.range?.copies);
  const card = document.getElementById('job-' + event.orderId);
  if (!card) return;
  const fill  = card.querySelector('.progress-fill');
  const label = card.querySelector('.progress-label');
  if (fill && label && event.totalRanges > 0) {
    fill.style.width  = Math.round((event.completedRanges / event.totalRanges) * 100) + '%';
    label.textContent = event.completedRanges + '/' + event.totalRanges + ' ranges';
  }
}

// --- Logging -----------------------------------------------------------------
function addLog(message, type) {
  type = type || 'info';
  const empty = logContainer.querySelector('.log-empty');
  if (empty) empty.remove();
  const entry = document.createElement('div');
  entry.className   = 'log-entry log-' + type;
  entry.textContent = new Date().toLocaleTimeString() + ' — ' + message;
  logContainer.prepend(entry);
  while (logContainer.children.length > 50) logContainer.lastChild.remove();
}

function formatPauseReason(reason) {
  return {
    out_of_paper:  'Out of Paper — Add paper to printer',
    printer_error: 'Printer Error — Check printer status',
    power_failure: 'Power was interrupted — click Resume to continue',
    manual:        'Manually paused',
  }[reason] || (reason || 'Unknown reason');
}

// --- Controls ----------------------------------------------------------------
clearLogBtn.addEventListener('click', () => {
  logContainer.innerHTML = '<div class="log-empty"><div class="log-empty-icon">P</div><p>Ready to print!</p></div>';
  failCount = 0;
  failedCount.textContent = '0';
});

refreshBtn.addEventListener('click', async () => {
  refreshBtn.classList.add('rotate-anim');
  await window.agent.refresh();
  const paused = await window.agent.getPausedJobs();
  updatePausedJobsUI(paused);
  setTimeout(() => refreshBtn.classList.remove('rotate-anim'), 800);
});

logoutBtn.addEventListener('click', async () => {
  await window.agent.disconnectEngine();
  await window.agent.logout();
  if (statusInterval) clearInterval(statusInterval);
  logContainer.innerHTML = '<div class="log-empty"><div class="log-empty-icon">P</div><p>Ready to print!</p></div>';
  failCount = 0;
  pausedJobsSection.classList.add('hidden');
  recoveryBanner.classList.add('hidden');
  showScreen('login');
});

// --- Helpers -----------------------------------------------------------------
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

// --- Start -------------------------------------------------------------------
init();
