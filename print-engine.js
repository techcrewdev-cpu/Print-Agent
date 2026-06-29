/**
 * Smart Xerox — Print Engine (Production-Grade Fault-Tolerant System)
 * ─────────────────────────────────────────────────────────────────────
 * CRITICAL FEATURES (PRODUCTION-READY):
 *  ✅ PRINT VERIFICATION LAYER — Never marks incomplete prints as completed
 *     → Post-print verification: check OS printer queue (spooler)
 *     → Ensure job is fully processed and removed from queue
 *     → Only mark as "completed" after verification succeeds
 *  ✅ POWER FAILURE HANDLING — Persist progress after EVERY copy
 *     → On restart: fetch backend + local state, compare expected vs printed
 *     → Resume from exact page/copy, never reprint already-completed copies
 *  ✅ SPOOLER STATE TRACKING — Monitor OS-level print queue
 *     → Detect stuck jobs, paused jobs, failed jobs
 *     → Trigger retry or pause if job remains in queue too long
 *  ✅ HARDWARE FAILURE DETECTION — Detect & handle real printer issues
 *     → Paper out, paper jam, printer offline, driver/spooler crash
 *     → Pause job, save checkpoint, notify UI
 *  ✅ SAFE COMPLETION RULE — Job is COMPLETE only if:
 *     → All documents processed, all ranges printed, all copies printed
 *     → Printer queue cleared, no interruption detected
 *  ✅ MULTI-ORDER CONTROL — Realistic single-queue printing
 *     → Limit actual printing to 1 job per printer queue
 *     → Other jobs stay queued logically, prevent interleaving
 *  ✅ DUPLICATE PRINT PREVENTION — Resume is idempotent
 *     → Never reprint already-completed copies
 *     → Use copy-level checkpoints, always resume from NEXT unprinted copy
 *  ✅ NETWORK FAILURE SAFETY — Validate file integrity after download
 *     → Retry download if corrupted, don't start print on incomplete file
 *  ✅ UNKNOWN STATE HANDLING — Fail-safe approach
 *     → If unsure: mark as "INCOMPLETE", require manual/auto recovery
 *     → Never assume success
 *  ✅ BACKEND SYNC GUARANTEE — Backend state always reflects real printer
 *     → Detect mismatch, trigger reconciliation
 *  ✅ TIMEOUT & LONG JOB HANDLING — Adaptive timeout based on page count
 *  ✅ LOCAL + SERVER RECOVERY — Dual checkpoint system
 *     → Local (electron-store) + backend (API)
 *     → On startup: merge both states safely, resolve conflicts
 *  ✅ REAL-TIME UI STATES — Frontend shows accurate status
 *     → Printing, Queued, Paused (Paper Out/Error), Incomplete (Recovery), Completed (Verified)
 *  ✅ CONCURRENT MULTI-ORDER — Each order runs independently
 *  ✅ FIFO QUEUE — New orders start immediately if printer is free
 *  ✅ Per-range checkpoints — Survives crash mid-range
 *  ✅ Local electron-store persistence — Offline recovery
 *  ✅ Fallback polling every 5 min — Catch missed socket events
 */

const axios  = require('axios');
const printer = require('pdf-to-printer');
const fs     = require('fs');
const path   = require('path');
const os     = require('os');
const { io } = require('socket.io-client');
const { PDFDocument, rgb, StandardFonts, degrees } = require('pdf-lib');

// ─── Constants ────────────────────────────────────────────────────────────────
const MAX_RETRIES          = 3;
const RANGE_DELAY_MS       = 800;
const MODE_SWITCH_DELAY_MS = 2000;
const MAX_CONCURRENT       = 3;   // max orders printing simultaneously in memory
const PAPER_CHECK_INTERVAL = 15000; // check printer every 15s when paused

// ─── Hardware Spooler Lock ────────────────────────────────────────────────────
// Prevents interleaved pages. Orders can prepare concurrently (download, slice PDFs),
// but we ONLY send to the physical spooler queue sequentially.
class Mutex {
  constructor() {
    this.queue = [];
    this.locked = false;
  }
  async acquire() {
    if (!this.locked) {
      this.locked = true;
      return;
    }
    return new Promise(resolve => this.queue.push(resolve));
  }
  release() {
    if (this.queue.length > 0) {
      const next = this.queue.shift();
      next();
    } else {
      this.locked = false;
    }
  }
}
const spoolerLock = new Mutex();

// ─── State ────────────────────────────────────────────────────────────────────
const printedOrders    = new Set();          // completed order IDs
const printingNow      = new Set();          // currently printing order IDs
const fetchingNow      = new Set();          // order IDs mid pre-print re-fetch (dedup guard)
const pausedOrders     = new Map();          // orderId → { order, checkpoint }
const retryCount       = new Map();          // orderId → attempt count
const pendingQueue     = [];                 // FIFO queue of { orderId, order }
const rangeCheckpoints = new Map();          // orderId → { docIndex, rangeIndex, copyIndex }

let socket           = null;
let api              = null;
let localStore       = null;
let config           = { apiUrl: '', token: '', printerName: '', socketUrl: '' };
let eventCallback    = () => {};
let fallbackTimer    = null;
let printerCheckTimer = null;
let polling          = false;

// ─── Init ─────────────────────────────────────────────────────────────────────
function init({ apiUrl, token, printerName, onEvent, store }) {
  config.apiUrl    = apiUrl.replace(/\/+$/, '');
  config.token     = token;
  config.printerName = printerName || '';
  config.socketUrl = apiUrl.replace('/api', '');
  eventCallback    = onEvent || (() => {});
  localStore       = store || null;

  api = axios.create({
    baseURL: config.apiUrl,
    headers: { Authorization: `Bearer ${token}` },
    timeout: 30000,
  });

  api.interceptors.response.use(
    r => r,
    async err => {
      const originalRequest = err.config;
      // On 401, attempt a silent token refresh once before giving up
      if (err.response?.status === 401 && !originalRequest._retried) {
        originalRequest._retried = true;
        try {
          eventCallback({ type: 'token_refresh_needed' });
          await new Promise(resolve => setTimeout(resolve, 1500));
          if (config.token) {
            originalRequest.headers['Authorization'] = `Bearer ${config.token}`;
            return api(originalRequest);
          }
        } catch (_) { /* fall through to auth_expired */ }
        eventCallback({ type: 'auth_expired', message: 'Session expired. Please re-login.' });
      }
      return Promise.reject(err);
    }
  );

  log('Print engine initialised (multi-order fault-tolerant mode)');
}

// ─── Connect ──────────────────────────────────────────────────────────────────
function connect() {
  if (socket) socket.disconnect();

  socket = io(config.socketUrl, {
    auth: { token: config.token },
    transports: ['websocket', 'polling'],
    reconnection: true,
    reconnectionDelay: 3000,
    reconnectionDelayMax: 10000,
    timeout: 20000,
  });

  socket.on('connect', () => {
    log(`Socket connected: ${socket.id}`);
    socket.emit('join:agent', { token: config.token });
    eventCallback({ type: 'connected', socketId: socket.id });
    // AGGRESSIVE: Poll immediately on connect
    fallbackPoll(); // Immediate poll
    setTimeout(() => fallbackPoll(), 500);  // Poll again after 0.5 seconds
    setTimeout(() => fallbackPoll(), 1500); // Poll again after 1.5 seconds
    // Recover any jobs that were in progress before disconnect/power loss
    setTimeout(() => recoverIncompleteJobs(), 2000);
  });

  socket.on('agent:connected', (data) => {
    log(`✅ Agent registered — shop: ${data.shopName} (${data.shopId})`);
    eventCallback({ type: 'agent_registered', shopId: data.shopId, shopName: data.shopName });
    // AGGRESSIVE: Poll immediately and then every 3 seconds
    fallbackPoll(); // Immediate poll
    setTimeout(() => fallbackPoll(), 1000); // Poll again after 1 second
    setTimeout(() => fallbackPoll(), 3000); // Poll again after 3 seconds
  });

  socket.on('disconnect', (reason) => {
    log(`Socket disconnected: ${reason}`);
    eventCallback({ type: 'disconnected', reason });
    saveLocalState(); // persist in-progress jobs to survive power loss
  });

  socket.on('connect_error', (err) => {
    log(`Connection error: ${err.message}`);
    eventCallback({ type: 'error', message: err.message });
  });

  // ── New order accepted by shopkeeper ──────────────────────────────────────
  socket.on('order:accepted', async (data) => {
    const orderId = data.orderId?.toString();
    log(`🔔 order:accepted EVENT RECEIVED — #${data.orderNumber || orderId}`);
    log(`  Socket ID: ${socket.id}`);
    log(`  Shop ID: ${data.shopId}`);
    log(`  Order ID: ${orderId}`);
    
    if (!orderId) {
      log(`  ❌ No orderId in event data`);
      return;
    }
    if (printedOrders.has(orderId)) {
      log(`  ⏭️ Skipping - already printed`);
      return;
    }
    if (printingNow.has(orderId)) {
      log(`  ⏭️ Skipping - currently printing`);
      return;
    }
    if (pendingQueue.some(q => q.orderId === orderId)) {
      log(`  ⏭️ Skipping - already in queue`);
      return;
    }
    if (pausedOrders.has(orderId)) {
      log(`  ⏭️ Skipping - paused`);
      return;
    }

    // Small delay to ensure DB write is committed before we fetch
    await sleep(500);

    let order = null;
    for (let attempt = 1; attempt <= 5; attempt++) {
      try {
        const res = await api.get(`/orders/${orderId}`);
        order = res.data.data?.order || res.data.order;
        // Ensure order is in accepted/printing state before proceeding
        if (order && ['accepted', 'printing', 'queued'].includes(order.status)) break;
        order = null; // wrong state — retry
      } catch (err) {
        log(`  Fetch attempt ${attempt}/5 failed: ${err.message}`);
      }
      if (attempt < 5) await sleep(2000 * attempt);
    }

    if (order) {
      log(`  ✅ Order fetched (status: ${order.status}) — enqueuing`);
      enqueueOrder(order);
    } else {
      log(`❌ Could not fetch order ${orderId} in accepted/printing state after 5 attempts`);
      eventCallback({ type: 'error', message: `Failed to fetch order #${data.orderNumber}` });
    }
  });

  // ── Manual trigger from dashboard ─────────────────────────────────────────
  socket.on('print:trigger', async (data) => {
    const orderId = data.orderId?.toString();
    log(`🖨️  Manual trigger — #${data.orderNumber || orderId}`);
    // Force-reset so it can be re-queued
    printingNow.delete(orderId);
    printedOrders.delete(orderId);
    pausedOrders.delete(orderId);
    const qi = pendingQueue.findIndex(q => q.orderId === orderId);
    if (qi !== -1) pendingQueue.splice(qi, 1);
    try {
      const res = await api.get(`/orders/${orderId}`);
      const order = res.data.data?.order || res.data.order;
      if (order) enqueueOrder(order, true);
    } catch (err) {
      log(`Manual trigger failed: ${err.message}`);
    }
  });

  // ── Resume from dashboard (shopkeeper added paper / fixed printer) ─────────
  socket.on('print:resume', async (data) => {
    const orderId = data.orderId?.toString();
    log(`🔄 print:resume — #${data.orderNumber || orderId}`);
    await doResume(orderId, data.currentDocIndex || 0, data.resumeFromPage || 0);
  });

  // ── Timers ─────────────────────────────────────────────────────────────────
  if (fallbackTimer) clearInterval(fallbackTimer);
  // ULTRA AGGRESSIVE: Poll every 1 second for instant order pickup
  // This ensures orders are picked up almost immediately
  fallbackTimer = setInterval(fallbackPoll, 1000); // every 1 second

  if (printerCheckTimer) clearInterval(printerCheckTimer);
  printerCheckTimer = setInterval(checkPrinterStatus, PAPER_CHECK_INTERVAL);

  return socket;
}

// ─── Disconnect ───────────────────────────────────────────────────────────────
function disconnect() {
  saveLocalState();
  if (socket) { socket.disconnect(); socket = null; }
  if (fallbackTimer) { clearInterval(fallbackTimer); fallbackTimer = null; }
  if (printerCheckTimer) { clearInterval(printerCheckTimer); printerCheckTimer = null; }
  printedOrders.clear();
  printingNow.clear();
  fetchingNow.clear();
  retryCount.clear();
  // Keep pausedOrders — they survive reconnect
  log('Print engine disconnected');
}

// ─── Public helpers ───────────────────────────────────────────────────────────
async function listPrinters() {
  try { return await printer.getPrinters(); } catch { return []; }
}

function setPrinter(name) {
  config.printerName = name;
  log(`Printer set to: ${name || '(system default)'}`);
}

function getStatus() {
  return {
    connected:   socket?.connected || false,
    socketId:    socket?.id || null,
    printer:     config.printerName || '(default)',
    printed:     printedOrders.size,
    active:      printingNow.size,
    paused:      pausedOrders.size,
    queued:      pendingQueue.length,
    inProgress:  [...printingNow],
  };
}

function getPausedJobs() {
  const jobs = [];
  for (const [orderId, data] of pausedOrders) {
    jobs.push({
      orderId,
      orderNumber:     data.order?.orderNumber || orderId.slice(-6).toUpperCase(),
      printedPages:    data.checkpoint?.printedPages || 0,
      totalPages:      data.checkpoint?.totalPages || 0,
      currentDocIndex: data.checkpoint?.currentDocIndex || 0,
      totalDocs:       data.order?.documents?.length || 0,
      pauseReason:     data.checkpoint?.pauseReason || 'unknown',
      pausedAt:        data.checkpoint?.pausedAt || new Date().toISOString(),
    });
  }
  return jobs;
}

async function pausePrintJob(orderId, reason = 'manual') {
  log(`[Pause ${orderId.slice(-6)}] reason: ${reason}`);
  try { await saveCheckpoint(orderId, { status: 'paused', pauseReason: reason }); } catch {}
  eventCallback({ type: 'print_paused', orderId, reason, pausedJobs: getPausedJobs() });
}

async function resumePrintJob(orderId) {
  log(`[Resume ${orderId.slice(-6)}] local resume`);
  const paused = pausedOrders.get(orderId);
  if (paused) {
    const cp = paused.checkpoint;
    await doResume(orderId, cp?.currentDocIndex || 0, cp?.printedPages || 0);
  } else {
    // Fetch from backend
    try {
      const res = await api.get(`/orders/${orderId}`);
      const order = res.data.data?.order || res.data.order;
      if (order) {
        const pj = order.printJob || {};
        // Restore range+copy from backend checkpoint
        if (pj.currentRangeIndex !== undefined) {
          rangeCheckpoints.set(orderId, {
            docIndex:   pj.currentDocIndex   || 0,
            rangeIndex: pj.currentRangeIndex || 0,
            copyIndex:  pj.currentCopyIndex  || 0,
          });
        }
        await doResume(orderId, pj.currentDocIndex || 0, pj.printedPages || 0);
      }
    } catch (err) {
      log(`Resume fetch failed: ${err.message}`);
      eventCallback({ type: 'resume_failed', orderId, error: err.message });
    }
  }
}

async function printOrder(orderId) {
  if (!orderId || !api) return;
  log(`🎯 Manual print request: ${orderId}`);
  printingNow.delete(orderId);
  printedOrders.delete(orderId);
  pausedOrders.delete(orderId);
  const qi = pendingQueue.findIndex(q => q.orderId === orderId);
  if (qi !== -1) pendingQueue.splice(qi, 1);
  try {
    const res = await api.get(`/orders/${orderId}`);
    const order = res.data.data?.order || res.data.order;
    if (order) enqueueOrder(order, true);
  } catch (err) {
    log(`Print request failed: ${err.message}`);
  }
}

// ─── Queue & Concurrency ──────────────────────────────────────────────────────
/**
 * Enqueue an order. If slots are free, start immediately.
 * Multiple orders can print concurrently up to MAX_CONCURRENT.
 */
function enqueueOrder(order, force = false) {
  const orderId = order._id?.toString();
  if (!orderId) return;
  if (!force && (printedOrders.has(orderId) || printingNow.has(orderId))) return;
  if (!force && fetchingNow.has(orderId)) return;  // mid pre-print re-fetch
  if (!force && pendingQueue.some(q => q.orderId === orderId)) return;

  // If we have a free slot, start immediately — re-fetch for latest shop settings
  if (printingNow.size < MAX_CONCURRENT) {
    log(`▶ Starting Order #${order.orderNumber || orderId} immediately (${printingNow.size + 1}/${MAX_CONCURRENT} slots)`);
    fetchingNow.add(orderId);
    // Re-fetch asynchronously so we always use the latest otpPlacement / shop settings.
    // After the fetch, re-check slot availability — another order may have filled it.
    (async () => {
      let freshOrder = order;
      try {
        const res = await api.get(`/orders/${orderId}`);
        const fetched = res.data.data?.order || res.data.order;
        if (fetched && ['accepted', 'printing', 'queued'].includes(fetched.status)) {
          freshOrder = fetched;
        }
      } catch (err) {
        log(`▶ Order #${order.orderNumber} — pre-print re-fetch failed (${err.message}), using cached order`);
      } finally {
        fetchingNow.delete(orderId);
      }

      // Re-check: slot may have been taken while we were fetching
      if (printingNow.has(orderId) || printedOrders.has(orderId)) return; // already started elsewhere
      if (printingNow.size >= MAX_CONCURRENT) {
        // No slot left — push to queue so drainQueue picks it up
        if (!pendingQueue.some(q => q.orderId === orderId)) {
          log(`🧾 Order #${freshOrder.orderNumber} — slot taken during re-fetch, re-queuing`);
          pendingQueue.push({ orderId, order: freshOrder });
          eventCallback({
            type: 'print_queued',
            orderId,
            orderNumber: freshOrder.orderNumber,
            queueLength: pendingQueue.length,
            activeCount: printingNow.size,
          });
        }
        return;
      }

      processOrder(freshOrder);
    })();
    return;
  }

  // All slots busy — queue it (store the current snapshot; drainQueue will re-fetch when slot opens)
  log(`🧾 Queuing Order #${order.orderNumber || orderId} (queue: ${pendingQueue.length + 1}, active: ${printingNow.size})`);
  pendingQueue.push({ orderId, order });
  eventCallback({
    type: 'print_queued',
    orderId,
    orderNumber: order.orderNumber,
    queueLength: pendingQueue.length,
    activeCount: printingNow.size,
  });
}

/**
 * Called when a slot frees up — dequeue and start next order.
 * Re-fetches the order from the backend before starting so it always uses
 * the latest shop settings (e.g. otpPlacement changed while queued).
 */
async function drainQueue() {
  if (pendingQueue.length === 0) {
    if (printingNow.size === 0) {
      log('✅ Queue empty — printer idle');
      eventCallback({ type: 'queue_empty' });
    }
    return;
  }

  // Find next non-paused order
  const idx = pendingQueue.findIndex(q => !pausedOrders.has(q.orderId));
  if (idx === -1) {
    log(`⏸️ All ${pendingQueue.length} queued orders are paused — waiting for resume`);
    return;
  }

  const { orderId, order: staleOrder } = pendingQueue.splice(idx, 1)[0];
  log(`▶ Dequeued Order #${staleOrder.orderNumber || orderId} — re-fetching for latest settings`);
  eventCallback({ type: 'queue_dequeued', orderId, remaining: pendingQueue.length });

  // Re-fetch the order so any shop setting changes (e.g. otpPlacement) made
  // while this order was queued are picked up before printing starts.
  let freshOrder = staleOrder; // fallback to stale if fetch fails
  try {
    const res = await api.get(`/orders/${orderId}`);
    const fetched = res.data.data?.order || res.data.order;
    if (fetched && ['accepted', 'printing', 'queued'].includes(fetched.status)) {
      freshOrder = fetched;
      const oldPlacement   = staleOrder.shop?.otpPlacement || 'all_pages';
      const freshPlacement = freshOrder.shop?.otpPlacement || 'all_pages';
      if (oldPlacement !== freshPlacement) {
        log(`▶ Order #${freshOrder.orderNumber} — otpPlacement updated: ${oldPlacement} → ${freshPlacement}`);
      }
    }
  } catch (err) {
    log(`▶ Order #${staleOrder.orderNumber} — re-fetch failed (${err.message}), using cached order`);
  }

  processOrder(freshOrder); // intentionally not awaited
}

// ─── Internal: Resume helper ──────────────────────────────────────────────────
async function doResume(orderId, fromDocIndex, fromPage) {
  const paused = pausedOrders.get(orderId);
  pausedOrders.delete(orderId);
  printingNow.delete(orderId);
  const qi = pendingQueue.findIndex(q => q.orderId === orderId);
  if (qi !== -1) pendingQueue.splice(qi, 1);

  // Restore range checkpoint from paused data so processOrder resumes exactly
  if (paused?.checkpoint?.currentRangeIndex !== undefined) {
    rangeCheckpoints.set(orderId, {
      docIndex:   paused.checkpoint.currentDocIndex   || fromDocIndex || 0,
      rangeIndex: paused.checkpoint.currentRangeIndex || 0,
      copyIndex:  paused.checkpoint.currentCopyIndex  || 0,
    });
    log(`[Resume ${orderId.slice(-6)}] restored checkpoint: doc=${paused.checkpoint.currentDocIndex}, range=${paused.checkpoint.currentRangeIndex}, copy=${paused.checkpoint.currentCopyIndex}`);
  }

  try {
    await saveCheckpoint(orderId, { status: 'printing', pauseReason: null });
  } catch {}

  try {
    const res = await api.get(`/orders/${orderId}`);
    const order = res.data.data?.order || res.data.order;
    if (!order) { log(`Resume: order ${orderId} not found`); return; }

    eventCallback({
      type:        'print_resumed',
      orderId,
      orderNumber: order.orderNumber,
      fromDocIndex,
      fromPage,
    });

    log(`🔄 Resuming #${order.orderNumber} from doc ${fromDocIndex}, page ${fromPage}`);
    processOrder(order, { resumeFromDocIndex: fromDocIndex, resumeFromPage: fromPage });
  } catch (err) {
    log(`Resume failed: ${err.message}`);
    eventCallback({ type: 'resume_failed', orderId, error: err.message });
  }
}

// ─── Core: Process a single order (runs concurrently with other orders) ───────
async function processOrder(order, resumeOpts = null) {
  if (!['accepted', 'printing'].includes(order.status)) return;

  const orderId = order._id?.toString();
  if (printedOrders.has(orderId) || printingNow.has(orderId)) return;

  printingNow.add(orderId);
  pausedOrders.delete(orderId);

  // ── Effective printer name ────────────────────────────────────────────────
  // Sub-orders (color/bw division) carry their own assigned printer system name.
  // Fall back to the globally configured printer if not set.
  const effectivePrinterName = order.assignedPrinterSystemName || config.printerName || '';
  if (effectivePrinterName && effectivePrinterName !== config.printerName) {
    log(`Order #${order.orderNumber} → using assigned printer: "${effectivePrinterName}" (global: "${config.printerName || 'default'}")`);
  }

  const tag  = `Order #${order.orderNumber || orderId.slice(-6).toUpperCase()}`;
  const docs  = order.documents || [];

  // ── Calculate total sheets ────────────────────────────────────────────────
  const totalPages = docs.reduce((sum, doc) => {
    if (doc.printingRanges?.length) {
      return sum + doc.printingRanges.reduce((rs, r) => {
        const sheets = r.sides === 'double'
          ? Math.ceil((r.rangeEnd - r.rangeStart + 1) / 2)
          : (r.rangeEnd - r.rangeStart + 1);
        return rs + sheets * r.copies;
      }, 0);
    }
    return sum + calculateDocPages(doc, doc.detectedPages || 1);
  }, 0);

  // ── Resume point ──────────────────────────────────────────────────────────
  const startDocIndex   = resumeOpts?.resumeFromDocIndex || 0;
  const savedRange      = rangeCheckpoints.get(orderId);
  // Prefer in-memory range checkpoint, then fall back to backend printJob fields
  const backendRangeIdx = order.printJob?.currentRangeIndex || 0;
  const backendCopyIdx  = order.printJob?.currentCopyIndex  || 0;

  let startRangeIndex, startCopyIndex;
  if (savedRange?.docIndex === startDocIndex) {
    startRangeIndex = savedRange.rangeIndex || 0;
    startCopyIndex  = savedRange.copyIndex  || 0;
  } else if (startDocIndex === (order.printJob?.currentDocIndex || 0)) {
    startRangeIndex = backendRangeIdx;
    startCopyIndex  = backendCopyIdx;
  } else {
    startRangeIndex = 0;
    startCopyIndex  = 0;
  }

  if (startCopyIndex > 0) {
    log(`${tag} ↩ Resuming at doc ${startDocIndex}, range ${startRangeIndex}, copy ${startCopyIndex + 1} (copies 1-${startCopyIndex} already printed)`);
  }

  let cumulativePages = 0;
  for (let i = 0; i < startDocIndex && i < docs.length; i++) {
    cumulativePages += calculateDocPages(docs[i], docs[i].detectedPages || 1);
  }

  const isResuming = startDocIndex > 0 || startRangeIndex > 0 || startCopyIndex > 0;
  log(`${tag} ▶ ${isResuming ? 'RESUMING' : 'Starting'} — ${docs.length} doc(s), ~${totalPages} sheets`);

  eventCallback({
    type:        isResuming ? 'print_recovering' : 'printing',
    orderId,
    orderNumber: order.orderNumber,
    specs: {
      fileCount:       docs.length,
      totalPages,
      printedPages:    cumulativePages,
      currentDocIndex: startDocIndex,
      isResuming,
    },
  });

  try {
    await saveCheckpoint(orderId, {
      status:          'printing',
      printedPages:    cumulativePages,
      totalPages,
      currentDocIndex: startDocIndex,
      agentId:         socket?.id || 'local',
    });
  } catch {}

  try {
    for (let i = startDocIndex; i < docs.length; i++) {
      const doc      = docs[i];
      const docTag   = `${tag} [Doc ${i + 1}/${docs.length}: ${doc.originalName || 'file'}]`;
      const rangeStart = (i === startDocIndex) ? startRangeIndex : 0;
      const copyStart  = (i === startDocIndex && startRangeIndex === rangeStart) ? startCopyIndex : 0;

      let attempt = 0, success = false;

      while (attempt < MAX_RETRIES && !success) {
        try {
          await printDocument(order, doc, rangeStart, i, copyStart, cumulativePages, totalPages, effectivePrinterName);
          success = true;
          if (rangeCheckpoints.get(orderId)?.docIndex === i) {
            rangeCheckpoints.delete(orderId);
          }
        } catch (err) {
          attempt++;
          const errMsg = err.message || '';

          // ── Hardware error → pause this order, free the slot ──────────
          if (isPaperOutError(errMsg) || isPrinterOfflineError(errMsg)) {
            const reason = isPaperOutError(errMsg) ? 'out_of_paper' : 'printer_error';
            log(`${docTag} ⚠️ ${reason} — pausing order`);
            pausedOrders.set(orderId, {
              order,
              checkpoint: {
                printedPages:      cumulativePages,
                totalPages,
                currentDocIndex:   i,
                currentRangeIndex: rangeCheckpoints.get(orderId)?.rangeIndex || 0,
                currentCopyIndex:  rangeCheckpoints.get(orderId)?.copyIndex  || 0,
                pauseReason:       reason,
                pausedAt:          new Date().toISOString(),
              },
            });

            try {
              await saveCheckpoint(orderId, {
                status:            'paused',
                printedPages:      cumulativePages,
                totalPages,
                currentDocIndex:   i,
                rangeIndex:        rangeCheckpoints.get(orderId)?.rangeIndex || 0,
                currentCopyIndex:  rangeCheckpoints.get(orderId)?.copyIndex  || 0,
                pauseReason:       reason,
                lastError:         errMsg,
                agentId:           socket?.id || 'local',
              });
            } catch {}

            saveLocalState();

            eventCallback({
              type:            'print_paused',
              orderId,
              orderNumber:     order.orderNumber,
              reason,
              printedPages:    cumulativePages,
              totalPages,
              currentDocIndex: i,
              totalDocs:       docs.length,
              pausedJobs:      getPausedJobs(),
            });

            // Free the slot so other orders can continue
            printingNow.delete(orderId);
            drainQueue();
            return;
          }

          // ── Network/S3 error → longer backoff, don't count as hard failure ──
          if (isNetworkError(errMsg)) {
            log(`${docTag} 🌐 Network error (attempt ${attempt}/${MAX_RETRIES}) — retrying in ${5 * attempt}s: ${errMsg}`);
            if (attempt < MAX_RETRIES) await sleep(5000 * attempt);
            continue;
          }

          // ── Transient error → retry with backoff ──────────────────────
          log(`${docTag} attempt ${attempt}/${MAX_RETRIES} failed: ${errMsg}`);
          if (attempt < MAX_RETRIES) await sleep(3000 * attempt);
        }
      }

      if (!success) {
        log(`${docTag} ❌ FAILED after ${MAX_RETRIES} attempts`);
        retryCount.set(orderId, (retryCount.get(orderId) || 0) + 1);
        try {
          await saveCheckpoint(orderId, {
            status: 'failed', printedPages: cumulativePages, totalPages,
            currentDocIndex: i, lastError: 'Max retries exceeded', agentId: socket?.id || 'local',
          });
        } catch {}
        eventCallback({ type: 'print_failed', orderId, orderNumber: order.orderNumber });
        printingNow.delete(orderId);
        drainQueue();
        return;
      }

      // ── Doc done ──────────────────────────────────────────────────────
      cumulativePages += calculateDocPages(doc, doc.detectedPages || 1);
      log(`${docTag} ✅ Done (${cumulativePages}/${totalPages} sheets)`);

      try {
        await saveCheckpoint(orderId, {
          status: 'printing', printedPages: cumulativePages, totalPages,
          currentDocIndex: i + 1, agentId: socket?.id || 'local',
        });
      } catch {}

      saveLocalState();

      eventCallback({
        type:            'print_progress',
        orderId,
        orderNumber:     order.orderNumber,
        printedPages:    cumulativePages,
        totalPages,
        currentDocIndex: i + 1,
        totalDocs:       docs.length,
      });
    }

    // ── All docs done ─────────────────────────────────────────────────────
    // CRITICAL: Verify printer queue is clear before marking complete
    log(`${tag} All documents processed — verifying printer queue...`);

    const verifyTimeout = calculateAdaptiveTimeout(totalPages, 1);
    const verification = await verifyPrintCompletion(effectivePrinterName, orderId, verifyTimeout);

    if (!verification.verified) {
      log(`${tag} ❌ VERIFICATION FAILED: ${verification.reason}`);
      try {
        await saveCheckpoint(orderId, {
          status: 'incomplete',
          lastError: `Verification failed: ${verification.reason}`,
          printedPages: totalPages,
          totalPages,
          agentId: socket?.id || 'local',
        });
      } catch {}

      eventCallback({
        type: 'print_incomplete',
        orderId,
        orderNumber: order.orderNumber,
        reason: verification.reason,
        specs: { totalPages, printedPages: totalPages },
      });

      printingNow.delete(orderId);
      drainQueue();
      return;
    }

    log(`${tag} ✅ Verification passed — printer queue cleared`);

    try { await api.patch(`/orders/${orderId}/auto-printed`); } catch (err) {
      log(`${tag} auto-printed patch failed: ${err.message}`);
    }

    printedOrders.add(orderId);
    pausedOrders.delete(orderId);
    rangeCheckpoints.delete(orderId);
    retryCount.delete(orderId);
    clearLocalJob(orderId);

    log(`${tag} 🎉 COMPLETE & VERIFIED — ${totalPages} sheets, OTP sent to customer`);
    eventCallback({
      type:        'print_complete',
      orderId,
      orderNumber: order.orderNumber,
      specs:       { totalPages, printedPages: totalPages, verified: true },
    });

  } catch (err) {
    log(`${tag} Unexpected error: ${err.message}`);
    try {
      await saveCheckpoint(orderId, { status: 'failed', lastError: err.message, agentId: socket?.id || 'local' });
    } catch {}
  } finally {
    printingNow.delete(orderId);
    drainQueue(); // always try to start next order when this one finishes/fails/pauses
  }
}

// ─── PRINT VERIFICATION LAYER (CRITICAL) ─────────────────────────────────────
// NEVER mark a job as completed without verifying the printer queue is clear.
// This is the most critical safety mechanism — prevents false-positive completions.
//
// Returns: { verified: boolean, reason: string, jobsInQueue: number }
async function verifyPrintCompletion(printerName, jobId, timeoutMs = 30000) {
  const startTime = Date.now();
  const pollInterval = 2000; // Check queue every 2 seconds
  let lastQueueCount = -1;

  while (Date.now() - startTime < timeoutMs) {
    try {
      const printers = await printer.getPrinters();
      const target = printerName
        ? printers.find(p => p.name === printerName)
        : printers.find(p => p.isDefault);

      if (!target) {
        return { verified: false, reason: 'Printer not found during verification', jobsInQueue: -1 };
      }

      // Check printer status — must be idle/ready (not printing, not error)
      const isReady = target.statusNumber === 0 ||
                      target.statusNumber === undefined ||
                      (target.status || '').toLowerCase().includes('idle') ||
                      (target.status || '').toLowerCase().includes('ready');

      if (!isReady) {
        const elapsed = Math.round((Date.now() - startTime) / 1000);
        log(`[Verify] Printer not ready (status: ${target.status || 'unknown'}) — waiting... (${elapsed}s)`);
        await sleep(pollInterval);
        continue;
      }

      // Printer is ready — check if our job is still in the queue
      // On Windows, we can't directly query job status, but we can infer from printer state
      // If printer is idle and we've waited, the job should be processed
      const jobsInQueue = target.jobCount || 0;

      if (jobsInQueue === 0) {
        // Queue is empty — job was processed
        log(`[Verify] ✅ Printer queue cleared (${Math.round((Date.now() - startTime) / 1000)}s)`);
        return { verified: true, reason: 'Printer queue cleared', jobsInQueue: 0 };
      }

      // Queue still has jobs — wait a bit more
      if (jobsInQueue !== lastQueueCount) {
        log(`[Verify] Queue has ${jobsInQueue} job(s) — waiting for processing...`);
        lastQueueCount = jobsInQueue;
      }

      await sleep(pollInterval);
    } catch (err) {
      log(`[Verify] Error checking printer status: ${err.message}`);
      // On error, assume verification failed (fail-safe)
      return { verified: false, reason: `Verification error: ${err.message}`, jobsInQueue: -1 };
    }
  }

  // Timeout reached
  return {
    verified: false,
    reason: `Verification timeout after ${Math.round(timeoutMs / 1000)}s — queue may not be clear`,
    jobsInQueue: lastQueueCount,
  };
}

// ─── SPOOLER STATE TRACKING ───────────────────────────────────────────────────
// Monitor OS-level print queue for stuck/failed jobs.
// Returns: { healthy: boolean, issues: string[], jobCount: number }
async function checkSpoolerHealth(printerName) {
  try {
    const printers = await printer.getPrinters();
    const target = printerName
      ? printers.find(p => p.name === printerName)
      : printers.find(p => p.isDefault);

    if (!target) {
      return { healthy: false, issues: ['Printer not found'], jobCount: -1 };
    }

    const issues = [];
    const jobCount = target.jobCount || 0;

    // Detect common spooler issues
    const status = (target.status || '').toLowerCase();

    if (status.includes('error') || status.includes('fault')) {
      issues.push('Printer error state detected');
    }
    if (status.includes('offline') || status.includes('unavailable')) {
      issues.push('Printer offline');
    }
    if (status.includes('paused')) {
      issues.push('Printer paused');
    }
    if (jobCount > 10) {
      issues.push(`Spooler backlog: ${jobCount} jobs queued`);
    }

    const healthy = issues.length === 0 && (target.statusNumber === 0 || target.statusNumber === undefined);

    return { healthy, issues, jobCount, status: target.status };
  } catch (err) {
    return { healthy: false, issues: [`Spooler check failed: ${err.message}`], jobCount: -1 };
  }
}

// ─── COPY WAIT & HARDWARE DETECTION LOOP ───────────────────────────────────────
// HP 1020 and dumb USB printers hide their 'Out of Paper' WMI state from Windows.
// The ONLY way to know they crashed is to check if the job remains stuck in the spooler 
// longer than realistically possible for the requested page count.
async function waitForCopyCompletion(printerName, adaptiveTimeoutMs) {
  const start = Date.now();
  let lastJobCount = -1;

  while (Date.now() - start < adaptiveTimeoutMs) {
    const printers = await printer.getPrinters();
    const target = printerName
      ? printers.find(p => p.name === printerName)
      : printers.find(p => p.isDefault);

    if (!target) return; // Ignore if printer vanishes

    // 1. Explicit Hardware check (works on robust Ethernet/Cloud printers)
    const status = (target.status || '').toLowerCase();
    if (status.includes('paper') || status.includes('error') || status.includes('offline') || status.includes('jam') || status.includes('paused')) {
      throw new Error(`Printer hardware error detected: ${target.status}`);
    }

    // 2. Poll the Queue length
    const jobCount = target.jobCount || 0;
    if (jobCount === 0) return; // Success: job successfully digested by physical printer

    if (jobCount !== lastJobCount) lastJobCount = jobCount;
    await sleep(Math.min(adaptiveTimeoutMs / 10, 2000));
  }
  
  // Job didn't clear the queue inside the timeout. Printer is physically jammed or empty.
  throw new Error('Printer queue is stuck - Out of paper or printer offline.');
}

// ─── ADAPTIVE TIMEOUT CALCULATION ──────────────────────────────────────────────
// Longer jobs get longer timeouts to avoid false-positive failures.
// Formula: base (15s) + (pages * 5s) + (copies * 1s)
function calculateAdaptiveTimeout(pageCount, copyCount = 1) {
  const baseTimeout = 15000;
  const perPageTimeout = 5000;
  const perCopyTimeout = 1000;
  const timeout = baseTimeout + (pageCount * perPageTimeout) + (copyCount * perCopyTimeout);
  // Cap at 10 minutes for extreme safety
  return Math.min(timeout, 600000);
}

// ─── NETWORK FILE INTEGRITY CHECK ──────────────────────────────────────────────
// Validate downloaded file before printing.
// Returns: { valid: boolean, reason: string, size: number }
async function validateDownloadedFile(fileBuffer, fileName, expectedMimeType) {
  if (!fileBuffer || fileBuffer.byteLength === 0) {
    return { valid: false, reason: 'File is empty', size: 0 };
  }

  const size = fileBuffer.byteLength;
  const ext = (fileName || '').split('.').pop().toLowerCase();
  const mimeType = (expectedMimeType || '').toLowerCase();

  // Check magic bytes (file signature) to detect corruption
  const magicBytes = fileBuffer.slice(0, 8);

  // PDF: %PDF
  if (ext === 'pdf' || mimeType.includes('pdf')) {
    const pdfSig = Buffer.from('%PDF').toString();
    const fileSig = magicBytes.slice(0, 4).toString();
    if (fileSig !== pdfSig) {
      return { valid: false, reason: 'PDF signature mismatch — file may be corrupted', size };
    }
  }

  // PNG: 89 50 4E 47
  if (ext === 'png' || mimeType.includes('png')) {
    if (magicBytes[0] !== 0x89 || magicBytes[1] !== 0x50 || magicBytes[2] !== 0x4e || magicBytes[3] !== 0x47) {
      return { valid: false, reason: 'PNG signature mismatch — file may be corrupted', size };
    }
  }

  // JPEG: FF D8 FF
  if (['jpg', 'jpeg'].includes(ext) || mimeType.includes('jpeg')) {
    if (magicBytes[0] !== 0xff || magicBytes[1] !== 0xd8 || magicBytes[2] !== 0xff) {
      return { valid: false, reason: 'JPEG signature mismatch — file may be corrupted', size };
    }
  }

  // DOCX: PK (ZIP archive)
  if (ext === 'docx' || mimeType.includes('wordprocessingml')) {
    if (magicBytes[0] !== 0x50 || magicBytes[1] !== 0x4b) {
      return { valid: false, reason: 'DOCX signature mismatch — file may be corrupted', size };
    }
  }

  // File looks valid
  return { valid: true, reason: 'File integrity verified', size };
}

// ─── RECONCILIATION: Compare Backend vs Local State ────────────────────────────
// On startup, merge backend and local state safely.
// Returns: { reconciled: boolean, action: string, order: Order }
async function reconcileOrderState(orderId, backendOrder, localCheckpoint) {
  const tag = `[Reconcile ${orderId.slice(-6)}]`;

  // If backend says completed/picked_up, trust it — local state is stale
  if (['ready', 'picked_up'].includes(backendOrder.status)) {
    log(`${tag} Backend shows ${backendOrder.status} — local state is stale, skipping`);
    return { reconciled: true, action: 'skip', order: backendOrder };
  }

  // If backend says paused, restore paused state
  if (backendOrder.printJob?.status === 'paused') {
    log(`${tag} Backend shows paused (${backendOrder.printJob.pauseReason}) — restoring paused state`);
    return { reconciled: true, action: 'restore_paused', order: backendOrder };
  }

  // If backend says printing but local says it was printing too, compare checkpoints
  if (backendOrder.printJob?.status === 'printing' && localCheckpoint?.status === 'printing') {
    const backendPages = backendOrder.printJob.printedPages || 0;
    const localPages = localCheckpoint.printedPages || 0;

    // If local is ahead, use local (more recent)
    if (localPages > backendPages) {
      log(`${tag} Local checkpoint ahead (${localPages} vs ${backendPages} pages) — using local`);
      return { reconciled: true, action: 'use_local', order: backendOrder };
    }

    // Otherwise use backend (more authoritative)
    log(`${tag} Backend checkpoint ahead or equal — using backend`);
    return { reconciled: true, action: 'use_backend', order: backendOrder };
  }

  // Default: use backend state
  return { reconciled: true, action: 'use_backend', order: backendOrder };
}

// ─── Document Printing ────────────────────────────────────────────────────────
async function printDocument(order, doc, startRangeIndex = 0, docIndex = 0, startCopyIndex = 0, cumulativePages = 0, totalPages = 0, effectivePrinterName = '') {
  const tag = `[#${order.orderNumber || order._id.slice(-6)}]`;
  // Use the order-specific printer if assigned, otherwise fall back to global config
  const printerName = effectivePrinterName || config.printerName || '';

  const urlRes = await api.get(`/orders/${order._id}/documents/${doc._id}/url`);
  const s3Url  = urlRes.data.data?.downloadUrl;
  if (!s3Url) throw new Error('Backend returned no download URL');

  // ── NETWORK FAILURE SAFETY: Download with retry ────────────────────────────
  let dlRes = null;
  let downloadAttempt = 0;
  const maxDownloadAttempts = 3;

  while (downloadAttempt < maxDownloadAttempts && !dlRes) {
    try {
      downloadAttempt++;
      dlRes = await axios.get(s3Url, { responseType: 'arraybuffer', timeout: 60000 });
    } catch (err) {
      log(`${tag} Download attempt ${downloadAttempt}/${maxDownloadAttempts} failed: ${err.message}`);
      if (downloadAttempt < maxDownloadAttempts) {
        await sleep(3000 * downloadAttempt);
      } else {
        throw new Error(`Download failed after ${maxDownloadAttempts} attempts: ${err.message}`);
      }
    }
  }

  if (!dlRes.data || dlRes.data.byteLength === 0) {
    throw new Error('Downloaded file is empty');
  }

  // ── FILE INTEGRITY VALIDATION ──────────────────────────────────────────────
  const fileBuffer = Buffer.from(dlRes.data);
  const integrityCheck = await validateDownloadedFile(fileBuffer, doc.originalName, doc.mimeType);
  if (!integrityCheck.valid) {
    throw new Error(`File integrity check failed: ${integrityCheck.reason}`);
  }

  log(`${tag} Downloaded (${(fileBuffer.byteLength / 1024).toFixed(0)} KB) — integrity verified`);

  // ── Detect file type from doc metadata or magic bytes ──────────────────────
  const fileName  = (doc.originalName || '').toLowerCase();
  const mimeType  = (doc.mimeType || '').toLowerCase();
  const ext       = fileName.split('.').pop();
  const isImage   = mimeType.startsWith('image/') || ['jpg','jpeg','png','gif','bmp','webp'].includes(ext);
  const isDocx    = mimeType.includes('wordprocessingml') || mimeType === 'application/msword' || ['docx','doc'].includes(ext);
  const isPDF     = mimeType === 'application/pdf' || ext === 'pdf';

  if (isImage) {
    // ── Image: embed into a new PDF page, then print ──────────────────────
    log(`${tag} Image file — embedding into PDF for printing`);
    const imgPdf  = await PDFDocument.create();
    let   imgEmbed;
    if (ext === 'png') {
      imgEmbed = await imgPdf.embedPng(fileBuffer);
    } else {
      imgEmbed = await imgPdf.embedJpg(fileBuffer);
    }
    const { width: iw, height: ih } = imgEmbed.scale(1);
    // A4 dimensions in points
    const A4W = 595.28, A4H = 841.89;
    const scale = Math.min(A4W / iw, A4H / ih, 1);
    const page  = imgPdf.addPage([A4W, A4H]);
    page.drawImage(imgEmbed, {
      x:      (A4W - iw * scale) / 2,
      y:      (A4H - ih * scale) / 2,
      width:  iw * scale,
      height: ih * scale,
    });
    await stampOTPIfFirst(order, doc, imgPdf, tag, true, docIndex === 0);
    const tmpPath = path.join(os.tmpdir(), `sx_${order._id}_${doc._id}_img.pdf`);
    fs.writeFileSync(tmpPath, Buffer.from(await imgPdf.save()));
    const opts = buildPrinterOptions(doc, printerName);
    log(`${tag} 🛠️ ${JSON.stringify(opts)} | 1 page (image)`);
    await printer.print(tmpPath, opts);
    log(`${tag} ✅ Image sent to printer`);
    try { fs.unlinkSync(tmpPath); } catch {}
    return;
  }

  if (isDocx) {
    log(`${tag} DOCX file — printing via Windows shell`);
    const tmpPath = path.join(os.tmpdir(), `sx_${order._id}_${doc._id}.docx`);
    fs.writeFileSync(tmpPath, fileBuffer);
    await printFileViaShell(tmpPath, tag, printerName);
    try { fs.unlinkSync(tmpPath); } catch {}
    return;
  }

  // ── PDF (default path) ────────────────────────────────────────────────────
  let pdfDoc;
  try {
    pdfDoc = await PDFDocument.load(fileBuffer, { ignoreEncryption: true });
  } catch (loadErr) {
    log(`${tag} ⚠️ pdf-lib parse failed (${loadErr.message}) — printing raw file`);
    const tmpPath = path.join(os.tmpdir(), `sx_${order._id}_${doc._id}_raw.pdf`);
    fs.writeFileSync(tmpPath, fileBuffer);
    const opts = buildPrinterOptions(doc, printerName);
    await printer.print(tmpPath, opts);
    log(`${tag} ✅ Raw PDF sent to printer`);
    try { fs.unlinkSync(tmpPath); } catch {}
    return;
  }

  const totalOriginalPages = pdfDoc.getPageCount();

  if (!doc.printingRanges?.length) {
    await printDocumentOldFormat(order, doc, pdfDoc, totalOriginalPages, tag, docIndex, printerName);
  } else {
    await printDocumentNewFormat(order, doc, pdfDoc, totalOriginalPages, tag, startRangeIndex, docIndex, startCopyIndex, cumulativePages, totalPages, printerName);
  }
}

// ─── Windows Shell Print (for DOCX and other native-printable files) ─────────
function printFileViaShell(filePath, tag, printerName = '') {
  return new Promise((resolve, reject) => {
    const { exec } = require('child_process');
    // Use Windows print verb with optional printer name for sub-order routing
    const resolvedPrinter = printerName || config.printerName;
    const printerArg = resolvedPrinter
      ? `-PrinterName '${resolvedPrinter.replace(/'/g, "''")}'`
      : '';
    const cmd = `powershell -Command "Start-Process -FilePath '${filePath}' -Verb Print ${printerArg} -Wait"`;
    log(`${tag} Shell print: ${path.basename(filePath)}${resolvedPrinter ? ` → ${resolvedPrinter}` : ''}`);
    exec(cmd, { timeout: 60000 }, (err, stdout, stderr) => {
      if (err) {
        log(`${tag} Shell print error: ${err.message}`);
        reject(err);
      } else {
        log(`${tag} ✅ Shell print accepted`);
        resolve();
      }
    });
  });
}

async function printDocumentOldFormat(order, doc, pdfDoc, totalOriginalPages, tag, docIndex = 0, printerName = '') {
  const p           = doc.printingOptions || doc;
  const pageIndices = parsePageIndices(p.pageRange || 'all', totalOriginalPages);
  if (pageIndices.length === 0) throw new Error(`Invalid page range for ${totalOriginalPages} pages`);

  const chunkDoc    = await PDFDocument.create();
  const copiedPages = await chunkDoc.copyPages(pdfDoc, pageIndices);
  copiedPages.forEach(pg => chunkDoc.addPage(pg));

  // single chunk — always the last; isFirstDoc = true only for doc index 0
  await stampOTPIfFirst(order, doc, chunkDoc, tag, true, docIndex === 0);

  const tmpPath = path.join(os.tmpdir(), `sx_${order._id}_${doc._id}_print.pdf`);
  fs.writeFileSync(tmpPath, Buffer.from(await chunkDoc.save()));

  const opts = buildPrinterOptions(doc, printerName);
  log(`${tag} 🛠️ ${JSON.stringify(opts)} | ${pageIndices.length} pages`);
  await printer.print(tmpPath, opts);
  log(`${tag} ✅ Sent to printer`);
  try { fs.unlinkSync(tmpPath); } catch {}
}

async function printDocumentNewFormat(order, doc, pdfDoc, totalOriginalPages, tag, startRangeIndex = 0, docIndex = 0, startCopyIndex = 0, cumulativePages = 0, totalPages = 0, printerName = '') {
  const orderId       = order._id?.toString();
  const totalRanges   = doc.printingRanges.length;
  let completedRanges = startRangeIndex;
  let firstRangeDone  = startRangeIndex > 0;

  let printedInDoc = 0;
  for (let r = 0; r < startRangeIndex; r++) {
    const range = doc.printingRanges[r];
    const pages = Math.max((range.rangeEnd - range.rangeStart + 1), 1);
    const sheets = range.sides === 'double' ? Math.ceil(pages / 2) : pages;
    printedInDoc += sheets * range.copies;
  }
  if (startRangeIndex < totalRanges) {
    const range = doc.printingRanges[startRangeIndex];
    const pages = Math.max((range.rangeEnd - range.rangeStart + 1), 1);
    const sheets = range.sides === 'double' ? Math.ceil(pages / 2) : pages;
    printedInDoc += sheets * startCopyIndex;
  }

  for (let rangeIndex = startRangeIndex; rangeIndex < totalRanges; rangeIndex++) {
    const range    = doc.printingRanges[rangeIndex];
    const rangeTag = `${tag} [Range ${rangeIndex + 1}/${totalRanges}: pp.${range.rangeStart}-${range.rangeEnd}]`;

    if (range.rangeStart < 1 || range.rangeEnd > totalOriginalPages || range.rangeStart > range.rangeEnd) {
      log(`${rangeTag} ⚠️ Invalid range — skipping`);
      completedRanges++;
      continue;
    }

    const pageIndices = [];
    for (let p = range.rangeStart; p <= range.rangeEnd; p++) pageIndices.push(p - 1);

    const chunkDoc    = await PDFDocument.create();
    const copiedPages = await chunkDoc.copyPages(pdfDoc, pageIndices);
    copiedPages.forEach(pg => chunkDoc.addPage(pg));

    // OTP stamping logic per placement mode:
    //  - 'first_page': stamp only the first range chunk's first page
    //  - 'last_page' / 'extra_page': stamp only the LAST range chunk
    //  - 'all_pages': stamp every page of every range chunk
    // isFirstDoc = true only for the first document (docIndex 0) — applies to all modes
    const placement   = order.shop?.otpPlacement || 'all_pages';
    const isLastRange = rangeIndex === totalRanges - 1;
    const isFirstDoc  = docIndex === 0;

    if (placement === 'all_pages') {
      await stampOTPIfFirst(order, doc, chunkDoc, tag, true, isFirstDoc);
    } else if (placement === 'last_page' || placement === 'extra_page') {
      if (isLastRange) {
        await stampOTPIfFirst(order, doc, chunkDoc, tag, true, isFirstDoc);
      }
    } else {
      // 'first_page' (default) — stamp only the first range chunk
      if (!firstRangeDone) {
        await stampOTPIfFirst(order, doc, chunkDoc, tag, true, isFirstDoc);
      }
    }
    if (!firstRangeDone) firstRangeDone = true;

    const tmpPath = path.join(os.tmpdir(), `sx_${orderId}_${doc._id}_r${rangeIndex}.pdf`);
    fs.writeFileSync(tmpPath, Buffer.from(await chunkDoc.save()));

    const opts = buildPrinterOptionsFromRange(range, printerName, doc);

    const colorLabel = range.colorMode === 'color' ? '🌈 COLOR' : '⬛ B&W';
    const sidesLabel = range.sides === 'double' ? 'Double' : 'Single';

    // On resume: startCopyIndex applies only to the first resumed range
    // For all subsequent ranges, always start from copy 0
    const copyFrom = (rangeIndex === startRangeIndex) ? startCopyIndex : 0;

    if (copyFrom > 0) {
      log(`${rangeTag} ↩ Resuming from copy ${copyFrom + 1}/${range.copies} (copies 1-${copyFrom} already printed)`);
    }

    log(`${rangeTag} ${colorLabel} | ${sidesLabel}-sided | ${range.copies}x | ${pageIndices.length} page(s)${copyFrom > 0 ? ` | resuming copy ${copyFrom + 1}` : ''}`);

    eventCallback({
      type:        'print_range_start',
      orderId,
      orderNumber: order.orderNumber,
      rangeIndex,
      totalRanges,
      completedRanges,
      currentCopy: copyFrom + 1,
      resumingFromCopy: copyFrom,
      range: {
        start:     range.rangeStart,
        end:       range.rangeEnd,
        colorMode: range.colorMode,
        sides:     range.sides,
        copies:    range.copies,
        pages:     pageIndices.length,
      },
    });

    for (let copy = copyFrom; copy < range.copies; copy++) {
      log(`${rangeTag} 🖨️ Copy ${copy + 1}/${range.copies}...`);

      eventCallback({
        type:        'print_range_copy',
        orderId,
        orderNumber: order.orderNumber,
        rangeIndex,
        totalRanges,
        currentCopy: copy + 1,
        totalCopies: range.copies,
        range: {
          start:     range.rangeStart,
          end:       range.rangeEnd,
          colorMode: range.colorMode,
          sides:     range.sides,
          pages:     pageIndices.length,
        },
      });

      try {
        // ── CRITICAL: Save checkpoint BEFORE printing ──────────────────────
        // If power dies after printer.print() but before checkpoint save,
        // we lose the copy index and will reprint. Save FIRST to be safe.
        rangeCheckpoints.set(orderId, { docIndex, rangeIndex, copyIndex: copy });
        try {
          await saveCheckpoint(orderId, {
            status:            'printing',
            currentDocIndex:   docIndex,
            rangeIndex:        rangeIndex,
            currentCopyIndex:  copy,  // Current copy (0-indexed), will increment after success
            printedPages:      cumulativePages + printedInDoc,
            agentId:           socket?.id || 'local',
          });
        } catch { /* non-fatal */ }

        // ── PHYSICAL PRINTER LOCK ─────────────────────────────────────────────
        // We acquire the Mutex here so different Orders don't interleave PDF chunk submissions.
        try {
          await spoolerLock.acquire();

          // ✅ FIX: Use minimal options for virtual printers to avoid SumatraPDF errors
          const isVirtualPrinter = printerName && (
            printerName.toLowerCase().includes('microsoft print to pdf') ||
            printerName.toLowerCase().includes('xps document writer') ||
            printerName.toLowerCase().includes('onenote') ||
            printerName.toLowerCase().includes('fax')
          );

          if (isVirtualPrinter) {
            // Virtual printers: use minimal options but include presentation settings for testing
            log(`${rangeTag} 🖨️ Virtual printer detected, using minimal options`);
            const virtualOpts = { 
              printer: printerName,
              silent: true 
            };
            
            // Add presentation options for virtual printers (may not work but worth trying)
            if (doc?.presentationOptions?.isPresentationFile) {
              const slidesPerPage = doc.presentationOptions.slidesPerPage || 1;
              if (slidesPerPage > 1) {
                virtualOpts.pagesPerSheet = slidesPerPage;
                log(`${rangeTag} ⚠️ Virtual printer: attempting ${slidesPerPage}-up layout (may not work)`);
              }
              if (doc.presentationOptions.autoLandscape) {
                virtualOpts.orientation = 'landscape';
                log(`${rangeTag} ⚠️ Virtual printer: attempting landscape orientation`);
              }
            }
            
            await printer.print(tmpPath, virtualOpts);
            log(`${rangeTag} ✅ Copy ${copy + 1} sent to virtual printer`);
          } else {
            // Physical printers: use full options
            await printer.print(tmpPath, { ...opts, copies: 1 });
            log(`${rangeTag} ✅ Copy ${copy + 1} accepted by printer`);
          }

          // ── PRINT VERIFICATION & DUMB-PRINTER HARDWARE DETECTION ─────────────
          if (!isVirtualPrinter) {
            const adaptiveTimeout = calculateAdaptiveTimeout(pageIndices.length, 1);
            await waitForCopyCompletion(printerName, adaptiveTimeout);
            log(`${rangeTag} ✅ Copy ${copy + 1} processed by physical printer`);
          } else {
            // Virtual printers don't need verification, just a small delay
            await new Promise(resolve => setTimeout(resolve, 500));
          }
        } catch (spoolError) {
          log(`${rangeTag} ⚠️ Hardware Alert: ${spoolError.message}`);
          throw spoolError; // Will trigger isPaperOutError in processOrder
        } finally {
          spoolerLock.release(); // Free physical printer queue for next sequence
        }

        // ── Copy-level checkpoint: save after EACH successful copy ────────────
        // If power dies after copy 3 of 5, we resume from copy 4 (not copy 1)
        const nextCopy = copy + 1;
        
        const pagesInRange = Math.max((range.rangeEnd - range.rangeStart + 1), 1);
        const sheetsInRange = range.sides === 'double' ? Math.ceil(pagesInRange / 2) : pagesInRange;
        printedInDoc += sheetsInRange;
        const currentPrintedPages = cumulativePages + printedInDoc;

        rangeCheckpoints.set(orderId, { docIndex, rangeIndex, copyIndex: nextCopy });
        try {
          await saveCheckpoint(orderId, {
            status:            'printing',
            currentDocIndex:   docIndex,
            rangeIndex:        rangeIndex,
            currentCopyIndex:  nextCopy,
            printedPages:      currentPrintedPages,
            agentId:           socket?.id || 'local',
          });
        } catch { /* non-fatal */ }

        eventCallback({
          type:            'print_progress',
          orderId,
          orderNumber:     order.orderNumber,
          printedPages:    currentPrintedPages,
          totalPages,
          currentDocIndex: docIndex + 1,
          totalDocs:       doc.printingRanges ? order.documents?.length : 1,
        });

        // ── Also persist to local store after every copy ──────────────────
        // This ensures power-failure recovery works even if backend is slow
        saveLocalState();

      } catch (err) {
        try { fs.unlinkSync(tmpPath); } catch {}
        throw err;
      }

      if (copy < range.copies - 1) await sleep(RANGE_DELAY_MS);
    }

    // Range fully done — advance to next range, reset copy index
    completedRanges++;
    rangeCheckpoints.set(orderId, { docIndex, rangeIndex: completedRanges, copyIndex: 0 });

    try {
      await saveRangeCheckpoint(orderId, docIndex, completedRanges);
    } catch { /* non-fatal */ }

    eventCallback({
      type: 'print_range_complete',
      orderId,
      orderNumber:     order.orderNumber,
      rangeIndex,
      totalRanges,
      completedRanges,
      range: { start: range.rangeStart, end: range.rangeEnd, colorMode: range.colorMode, sides: range.sides, copies: range.copies, pages: pageIndices.length },
    });

    try { fs.unlinkSync(tmpPath); } catch {}

    if (rangeIndex < totalRanges - 1) {
      const nextRange    = doc.printingRanges[rangeIndex + 1];
      const modeChanging = nextRange && nextRange.colorMode !== range.colorMode;
      await sleep(modeChanging ? MODE_SWITCH_DELAY_MS : RANGE_DELAY_MS);
    }
  }
}

// ─── OTP Stamp ────────────────────────────────────────────────────────────────
// Stamps OTP vertically (90° rotated) in the LEFT margin.
// Placement is controlled by order.shop.otpPlacement:
//   'first_page' — stamp only the first page of the first document
//   'last_page'  — stamp only the last page of the LAST chunk of the first document
//   'all_pages'  — stamp every page of every chunk of the first document (DEFAULT)
//   'extra_page' — append a blank page with the OTP printed prominently
//                  (appended to the LAST chunk of the first document)
//
// Works for both normal orders AND sub-orders (color/bw division).
// For sub-orders: Each sub-order is treated as independent, so OTP is stamped
// on all documents in the sub-order (not just the first document).
//
// isLastChunk — pass true when this is the final range/chunk of the document
//               (used so last_page and extra_page target the correct chunk)
// isFirstDoc  — pass true only when printing the first document of the order
//               (controls first_page / last_page / extra_page placement)
//               For sub-orders, this should be true for ALL documents since
//               each sub-order is independent.
async function stampOTPIfFirst(order, doc, chunkDoc, tag, isLastChunk = true, isFirstDoc = true) {
  // Guard: OTP must exist
  if (!order.pickup?.pickupCode) return;

  // Guard: For normal orders (not sub-orders), only stamp the first document.
  // For sub-orders, stamp ALL documents since each sub-order is independent.
  // Sub-orders are identified by having a parentOrder field.
  const isSubOrder = !!order.parentOrder;
  
  if (!isSubOrder && !isFirstDoc) {
    // Normal order: only stamp first document
    return;
  }
  
  // For sub-orders: stamp all documents (skip the isFirstDoc check)

  const pages = chunkDoc.getPages();
  if (!pages.length) return;

  const otp        = order.pickup.pickupCode;
  const stampText  = `${otp}`;  // Only the number, no "OTP:" prefix
  const placement  = order.shop?.otpPlacement || 'all_pages';

  // Helper: draw stamp on a single page — vertical (90° rotated), bottom-left corner
  async function drawSmallStamp(page) {
    const { width, height } = page.getSize();
    const font       = await chunkDoc.embedFont(StandardFonts.HelveticaBold);
    const fontSize   = 12;
    const margin     = 10;
    const textWidth  = font.widthOfTextAtSize(stampText, fontSize);
    
    // Position at bottom-left corner with 90° rotation
    // When rotated 90°, text extends upward from the y coordinate
    const x = margin;
    let y = margin;
    
    // Check if text would go outside page bounds
    if (y + textWidth > height - margin) {
      // Adjust y so text ends before top margin
      y = height - textWidth - margin;
    }

    page.drawText(stampText, {
      x:      x,
      y:      y,
      size:   fontSize,
      font,
      color:  rgb(0, 0, 0),
      rotate: degrees(90),
    });
  }

  switch (placement) {
    case 'all_pages':
      for (const page of pages) {
        await drawSmallStamp(page);
      }
      log(`${tag} ✅ OTP stamped on ALL ${pages.length} page(s) in chunk: "${stampText}"`);
      break;

    case 'last_page':
      if (isLastChunk) {
        await drawSmallStamp(pages[pages.length - 1]);
        log(`${tag} ✅ OTP stamped on LAST page: "${stampText}"`);
      }
      break;

    case 'extra_page':
      if (isLastChunk) {
        const A4W       = 595.28, A4H = 841.89;
        const extraPage = chunkDoc.addPage([A4W, A4H]);
        const font      = await chunkDoc.embedFont(StandardFonts.HelveticaBold);
        const fontSize  = 48;                       // large centred OTP on blank page
        const textW     = font.widthOfTextAtSize(stampText, fontSize);
        extraPage.drawText(stampText, {
          x:    (A4W - textW) / 2,
          y:    A4H / 2 - fontSize / 2,
          size: fontSize,
          font,
          color: rgb(0, 0, 0),
        });
        log(`${tag} ✅ OTP stamped on EXTRA page: "${stampText}"`);
      }
      break;

    case 'first_page':
    default:
      await drawSmallStamp(pages[0]);
      log(`${tag} ✅ OTP stamped on FIRST page: "${stampText}"`);
      break;
  }
}

// ─── Printer Options ──────────────────────────────────────────────────────────
// printerName: OS system name for the target printer.
// Pass the order-specific printer (order.assignedPrinterSystemName) when available;
// falls back to config.printerName (global default) if empty.
function buildPrinterOptionsFromRange(range, printerName = '', doc = null) {
  const opts = {};
  const resolvedPrinter = printerName || config.printerName;
  if (resolvedPrinter) opts.printer = resolvedPrinter;

  // ✅ FIX: Detect virtual printers and use simplified options
  const isVirtualPrinter = resolvedPrinter && (
    resolvedPrinter.toLowerCase().includes('microsoft print to pdf') ||
    resolvedPrinter.toLowerCase().includes('xps document writer') ||
    resolvedPrinter.toLowerCase().includes('onenote') ||
    resolvedPrinter.toLowerCase().includes('fax')
  );

  // ✅ NEW: Handle presentation handout layouts (same as buildPrinterOptions)
  if (doc?.presentationOptions?.isPresentationFile) {
    const slidesPerPage = doc.presentationOptions.slidesPerPage || 1;
    if (slidesPerPage > 1) {
      if (!isVirtualPrinter) {
        // Use printer's N-up feature for handout layouts
        opts.pagesPerSheet = slidesPerPage;
        log(`[PRESENTATION] Handout layout: ${slidesPerPage} slides per page`);
      } else {
        // Virtual printers may not support N-up, but try anyway
        opts.pagesPerSheet = slidesPerPage;
        log(`[PRESENTATION] Virtual printer: attempting ${slidesPerPage}-up layout (may print full-size slides)`);
      }
    }
    
    // Apply user-selected orientation (or fall back to autoLandscape for backward compatibility)
    const orientation = doc.presentationOptions.orientation || 
                       (doc.presentationOptions.autoLandscape ? 'landscape' : 'auto');
    
    if (orientation === 'landscape' || orientation === 'portrait') {
      if (!isVirtualPrinter) {
        opts.orientation = orientation;
        log(`[PRESENTATION] ${orientation.charAt(0).toUpperCase() + orientation.slice(1)} orientation enabled`);
      } else {
        opts.orientation = orientation;
        log(`[PRESENTATION] Virtual printer: attempting ${orientation} orientation`);
      }
    }
  }

  // Color mode — monochrome:true = grayscale, monochrome:false = color
  const isColor = (range.colorMode || 'bw').toLowerCase() === 'color';
  opts.monochrome = !isColor;

  // ✅ FIX: Skip duplex and advanced options for virtual printers
  if (!isVirtualPrinter) {
    // Duplex — duplexlong = long-edge (standard for portrait A4)
    //          duplexshort = short-edge (landscape)
    //          simplex = single-sided
    const isDouble = (range.sides || 'single').toLowerCase() === 'double';
    if (isDouble) {
      // CRITICAL FIX: Always use duplexlong for A4 portrait (back-to-back, not landscape)
      // Canon iR6000 requires explicit long-edge binding for standard duplex
      // Short-edge is only for landscape documents (flip on short edge)
      
      // Check if presentation has explicit orientation
      const presOrientation = doc?.presentationOptions?.orientation || 
                             (doc?.presentationOptions?.autoLandscape ? 'landscape' : null);
      const isLandscape = presOrientation === 'landscape' || 
                         (range.orientation || '').toLowerCase() === 'landscape';
      
      opts.side = isLandscape ? 'duplexshort' : 'duplexlong';
      
      // FORCE portrait orientation for A4 duplex to prevent landscape printing
      // UNLESS it's a presentation with landscape enabled
      if (!isLandscape && range.paperSize !== 'A3' && presOrientation !== 'landscape') {
        opts.orientation = 'portrait';
      }
      
      log(`[DUPLEX] Enabled: ${opts.side} (landscape: ${isLandscape}, orientation: ${opts.orientation || 'auto'})`);
    } else {
      opts.side = 'simplex';
      log(`[SIMPLEX] Single-sided printing enabled`);
    }

    opts.paperSize = 'A4';
  } else {
    // Virtual printer: use minimal options
    log(`[VIRTUAL PRINTER] Using simplified options for ${resolvedPrinter}`);
    opts.side = 'simplex';  // Virtual printers don't support duplex
  }

  opts.scale     = 'fit';
  opts.silent    = true;

  // NOTE: copies are handled by the caller loop (1 copy per print call)
  // so we do NOT set opts.copies here — avoids double-printing
  return opts;
}

function buildPrinterOptions(doc, printerName = '') {
  const opts = {};
  const resolvedPrinter = printerName || config.printerName;
  if (resolvedPrinter) opts.printer = resolvedPrinter;
  const p = doc.printingOptions || doc;

  // ✅ FIX: Detect virtual printers and use simplified options
  const isVirtualPrinter = resolvedPrinter && (
    resolvedPrinter.toLowerCase().includes('microsoft print to pdf') ||
    resolvedPrinter.toLowerCase().includes('xps document writer') ||
    resolvedPrinter.toLowerCase().includes('onenote') ||
    resolvedPrinter.toLowerCase().includes('fax')
  );

  // ✅ NEW: Handle presentation handout layouts
  if (doc.presentationOptions?.isPresentationFile) {
    const slidesPerPage = doc.presentationOptions.slidesPerPage || 1;
    if (slidesPerPage > 1) {
      // Use printer's N-up feature for handout layouts
      opts.pagesPerSheet = slidesPerPage;
      if (isVirtualPrinter) {
        log(`[PRESENTATION] Virtual printer: attempting ${slidesPerPage}-up layout (may print full-size slides)`);
      } else {
        log(`[PRESENTATION] Handout layout: ${slidesPerPage} slides per page`);
      }
    }
    
    // Apply user-selected orientation (or fall back to autoLandscape for backward compatibility)
    const orientation = doc.presentationOptions.orientation || 
                       (doc.presentationOptions.autoLandscape ? 'landscape' : 'auto');
    
    if (orientation === 'landscape' || orientation === 'portrait') {
      opts.orientation = orientation;
      if (isVirtualPrinter) {
        log(`[PRESENTATION] Virtual printer: attempting ${orientation} orientation`);
      } else {
        log(`[PRESENTATION] ${orientation.charAt(0).toUpperCase() + orientation.slice(1)} orientation enabled`);
      }
    }
  }

  // copies handled by caller for new-format; set here only for old-format
  opts.copies     = Math.max(1, parseInt(p.copies || 1));

  const isColor   = (p.colorMode || 'bw').toLowerCase() === 'color';
  opts.monochrome = !isColor;

  // ✅ FIX: Skip duplex and advanced options for virtual printers
  if (!isVirtualPrinter) {
    const isDouble  = (p.sides || 'single').toLowerCase() === 'double';
    if (isDouble) {
      // Check if presentation has explicit orientation, otherwise use printingOptions
      const presOrientation = doc.presentationOptions?.orientation || 
                             (doc.presentationOptions?.autoLandscape ? 'landscape' : null);
      const isLandscape = presOrientation === 'landscape' || 
                         (p.orientation || '').toLowerCase() === 'landscape';
      
      opts.side = isLandscape ? 'duplexshort' : 'duplexlong';
      
      // FORCE portrait orientation for A4 duplex to prevent landscape printing
      // UNLESS it's a presentation with landscape enabled
      if (!isLandscape && p.paperSize !== 'A3' && presOrientation !== 'landscape') {
        opts.orientation = 'portrait';
      }
      
      log(`[DUPLEX] Enabled: ${opts.side} (landscape: ${isLandscape}, orientation: ${opts.orientation || 'auto'})`);
    } else {
      opts.side = 'simplex';
      log(`[SIMPLEX] Single-sided printing enabled`);
    }

    const ps = p.paperSize || 'A4';
    if (['A4', 'A3', 'Letter'].includes(ps)) opts.paperSize = ps;
    
    // Don't override orientation if presentation already set it
    const presOrientation = doc.presentationOptions?.orientation || 
                           (doc.presentationOptions?.autoLandscape ? 'landscape' : null);
    if (p.orientation && p.orientation !== 'auto' && !presOrientation) {
      opts.orientation = p.orientation;
    }
  } else {
    // Virtual printer: use minimal options
    log(`[VIRTUAL PRINTER] Using simplified options for ${resolvedPrinter}`);
    opts.side = 'simplex';  // Virtual printers don't support duplex
  }

  opts.scale  = 'fit';
  opts.silent = true;
  return opts;
}

// ─── Page Calculation ─────────────────────────────────────────────────────────
function calculateDocPages(doc, maxPagesFallback = 1) {
  if (doc.printingRanges?.length) {
    return doc.printingRanges.reduce((sum, range) => {
      const pagesInRange = Math.max((range.rangeEnd - range.rangeStart + 1), 1);
      const sheets = range.sides === 'double' ? Math.ceil(pagesInRange / 2) : pagesInRange;
      return sum + sheets * Math.max(range.copies, 1);
    }, 0);
  }
  const p       = doc.printingOptions || doc;
  const detected = doc.detectedPages || maxPagesFallback;
  const indices  = parsePageIndices(p.pageRange || 'all', detected);
  const copies   = parseInt(p.copies || 1);
  const sheets   = p.sides === 'double' ? Math.ceil(indices.length / 2) : indices.length;
  return sheets * copies;
}

function parsePageIndices(rangeStr, totalPages) {
  if (!rangeStr || rangeStr.toLowerCase() === 'all') {
    return Array.from({ length: totalPages }, (_, i) => i);
  }
  const indices = new Set();
  for (const part of rangeStr.split(',')) {
    const trimmed = part.trim();
    if (!trimmed) continue;
    if (trimmed.includes('-')) {
      const [s, e] = trimmed.split('-');
      const start = parseInt(s, 10), end = parseInt(e, 10);
      if (!isNaN(start) && !isNaN(end) && start > 0 && end >= start) {
        for (let i = start; i <= Math.min(end, totalPages); i++) indices.add(i - 1);
      }
    } else {
      const n = parseInt(trimmed, 10);
      if (!isNaN(n) && n > 0 && n <= totalPages) indices.add(n - 1);
    }
  }
  return Array.from(indices).sort((a, b) => a - b);
}

// ─── Error Detection ──────────────────────────────────────────────────────────
function isPaperOutError(msg) {
  const l = (msg || '').toLowerCase();
  return l.includes('out of paper') || l.includes('paper out') || l.includes('paper empty') ||
         l.includes('no paper') || l.includes('paper jam') || l.includes('load paper') ||
         l.includes('paper tray') || l.includes('media empty') || l.includes('tray empty') ||
         // Canon iR series specific messages
         l.includes('add paper') || l.includes('paper supply') || l.includes('cassette') ||
         l.includes('drawer') || l.includes('feed') || l.includes('paper size mismatch');
}

function isPrinterOfflineError(msg) {
  const l = (msg || '').toLowerCase();
  return l.includes('printer offline') || l.includes('printer not available') ||
         l.includes('printer not found') || l.includes('not ready') ||
         l.includes('cannot access printer') || l.includes('spooler') ||
         l.includes('printer error') || l.includes('printer is offline') ||
         l.includes('access is denied') || l.includes('rpc server') ||
         l.includes('the printer') || l.includes('win32 error') ||
         // Canon iR series specific
         l.includes('service call') || l.includes('door open') || l.includes('cover open') ||
         l.includes('toner') || l.includes('drum') || l.includes('waste toner');
}

function isNetworkError(msg) {
  const l = (msg || '').toLowerCase();
  return l.includes('enotfound') || l.includes('econnrefused') || l.includes('econnreset') ||
         l.includes('etimedout') || l.includes('network') || l.includes('socket hang up') ||
         l.includes('getaddrinfo') || l.includes('connect timeout');
}

// ─── Checkpoint ───────────────────────────────────────────────────────────────
async function saveCheckpoint(orderId, data) {
  if (!api) return;
  try {
    await api.patch(`/orders/${orderId}/print-job`, data);
  } catch (err) {
    log(`Checkpoint save failed (non-fatal): ${err.message}`);
  }
}

// ─── Range Checkpoint — saves exact range+copy position for power-failure recovery ─
async function saveRangeCheckpoint(orderId, docIndex, rangeIndex, copyIndex, agentId) {
  return saveCheckpoint(orderId, {
    status:            'printing',
    currentDocIndex:   docIndex,
    rangeIndex:        rangeIndex,
    currentCopyIndex:  copyIndex || 0,
    agentId:           agentId || socket?.id || 'local',
  });
}

// ─── Printer Status Monitor ───────────────────────────────────────────────────
// Runs every 15s. When paused orders exist and their assigned printer becomes
// ready → auto-resume. Handles both global and per-order (sub-order) printers.
async function checkPrinterStatus() {
  if (pausedOrders.size === 0) return;

  try {
    const printers = await printer.getPrinters();

    // Helper: check if a specific printer (by system name) is ready
    function isPrinterReady(printerName) {
      const name   = printerName || config.printerName;
      const target = name
        ? printers.find(p => p.name === name)
        : printers.find(p => p.isDefault);
      if (!target) return false;
      return target.statusNumber === 0 ||
             target.statusNumber === undefined ||
             (target.status || '').toLowerCase().includes('idle') ||
             (target.status || '').toLowerCase().includes('ready');
    }

    // Find paper-out paused orders whose assigned printer is now ready
    const paperOutJobs = [...pausedOrders.entries()].filter(([, d]) => {
      if (d.checkpoint?.pauseReason !== 'out_of_paper') return false;
      const orderPrinter = d.order?.assignedPrinterSystemName || '';
      return isPrinterReady(orderPrinter);
    });

    if (paperOutJobs.length === 0) {
      // Check if at least the global printer is ready — notify UI for manual resume
      if (isPrinterReady('')) {
        eventCallback({ type: 'printer_ready', message: 'Printer ready — resume paused jobs', pausedJobs: getPausedJobs() });
      }
      return;
    }

    log(`🖨️ Printer ready — auto-resuming ${paperOutJobs.length} paper-out job(s)`);

    for (const [orderId, data] of paperOutJobs) {
      const orderPrinter = data.order?.assignedPrinterSystemName || config.printerName || 'default';
      log(`  Auto-resuming Order #${data.order?.orderNumber || orderId} (printer: ${orderPrinter})`);
      eventCallback({
        type:        'auto_resume',
        orderId,
        orderNumber: data.order?.orderNumber,
        reason:      'Printer paper refilled — auto-resuming',
      });
      await doResume(
        orderId,
        data.checkpoint?.currentDocIndex || 0,
        data.checkpoint?.printedPages || 0
      );
      await sleep(2000); // stagger multiple resumes
    }
  } catch {
    // Silent — printer query can fail on some systems
  }
}

// ─── Power-Failure Recovery ───────────────────────────────────────────────────
async function recoverIncompleteJobs() {
  if (!api) return;
  log('🔍 Checking for incomplete print jobs (power-failure recovery)...');

  let incompleteOrders = [];

  try {
    const res = await api.get('/orders/incomplete-jobs');
    incompleteOrders = res.data.data?.orders || [];
  } catch (err) {
    log(`Backend recovery check failed: ${err.message}`);
  }

  // Also check local store for jobs that were printing when power died
  const localJobs = getLocalJobs();
  if (localJobs.length > 0) {
    log(`Found ${localJobs.length} locally saved job(s) — cross-checking with backend`);
    for (const localJob of localJobs) {
      const alreadyInList = incompleteOrders.some(o => o._id?.toString() === localJob.orderId);
      if (alreadyInList) continue;
      try {
        const res   = await api.get(`/orders/${localJob.orderId}`);
        const order = res.data.data?.order || res.data.order;
        if (order && ['accepted', 'printing'].includes(order.status)) {
          incompleteOrders.push(order);
        } else {
          clearLocalJob(localJob.orderId);
        }
      } catch {
        clearLocalJob(localJob.orderId);
      }
    }
  }

  if (incompleteOrders.length === 0) {
    log('✅ No incomplete jobs found');
    return;
  }

  log(`⚠️ Found ${incompleteOrders.length} incomplete job(s) — recovering`);
  eventCallback({
    type:  'recovery_start',
    count: incompleteOrders.length,
    jobs:  incompleteOrders.map(o => ({
      orderId:      o._id,
      orderNumber:  o.orderNumber,
      printedPages: o.printJob?.printedPages || 0,
      totalPages:   o.printJob?.totalPages || 0,
      currentDoc:   o.printJob?.currentDocIndex || 0,
      totalDocs:    o.documents?.length || 0,
      status:       o.printJob?.status || 'unknown',
      pauseReason:  o.printJob?.pauseReason || null,
    })),
  });

  for (const order of incompleteOrders) {
    const oid = order._id?.toString();
    if (printedOrders.has(oid) || printingNow.has(oid)) continue;

    const pj = order.printJob || {};

    // ── RECONCILIATION: Compare backend vs local state ──────────────────────
    const localJob = localJobs.find(j => j.orderId === oid);
    const reconciliation = await reconcileOrderState(oid, order, localJob?.checkpoint);

    if (reconciliation.action === 'skip') {
      // Order is already complete/picked_up — skip
      clearLocalJob(oid);
      continue;
    }

    if (pj.status === 'paused') {
      // Restore to paused map — shopkeeper must manually resume
      pausedOrders.set(oid, {
        order,
        checkpoint: {
          printedPages:      pj.printedPages || 0,
          totalPages:        pj.totalPages || 0,
          currentDocIndex:   pj.currentDocIndex || 0,
          currentRangeIndex: pj.currentRangeIndex || 0,
          currentCopyIndex:  pj.currentCopyIndex  || 0,
          pauseReason:       pj.pauseReason || 'power_failure',
          pausedAt:          pj.pausedAt || new Date().toISOString(),
        },
      });
      // Also restore in-memory range checkpoint
      if (pj.currentRangeIndex !== undefined) {
        rangeCheckpoints.set(oid, {
          docIndex:   pj.currentDocIndex   || 0,
          rangeIndex: pj.currentRangeIndex || 0,
          copyIndex:  pj.currentCopyIndex  || 0,
        });
      }
      log(`  ⏸️ #${order.orderNumber} — restored as paused (${pj.pauseReason || 'power_failure'}) at doc=${pj.currentDocIndex}, range=${pj.currentRangeIndex}, copy=${pj.currentCopyIndex}`);
      eventCallback({
        type:            'print_paused',
        orderId:         oid,
        orderNumber:     order.orderNumber,
        reason:          pj.pauseReason || 'power_failure',
        printedPages:    pj.printedPages || 0,
        totalPages:      pj.totalPages || 0,
        currentDocIndex: pj.currentDocIndex || 0,
        totalDocs:       order.documents?.length || 0,
        isRecovered:     true,
        pausedJobs:      getPausedJobs(),
      });
    } else if (pj.status === 'incomplete') {
      // Previous attempt failed verification — mark as incomplete, require manual action
      log(`  ⚠️ #${order.orderNumber} — marked INCOMPLETE (verification failed: ${pj.lastError})`);
      eventCallback({
        type:            'print_incomplete',
        orderId:         oid,
        orderNumber:     order.orderNumber,
        reason:          pj.lastError || 'Verification failed',
        printedPages:    pj.printedPages || 0,
        totalPages:      pj.totalPages || 0,
        isRecovered:     true,
      });
    } else {
      // Was mid-print when power died — restore as paused with power_failure reason
      // so the UI shows the "Resume" button and the shopkeeper knows what happened.
      const resumeDoc   = pj.currentDocIndex   || 0;
      const resumeRange = pj.currentRangeIndex || 0;
      const resumeCopy  = pj.currentCopyIndex  || 0;
      const resumePage  = pj.printedPages      || 0;

      // Restore range checkpoint so processOrder resumes from exact position
      rangeCheckpoints.set(oid, {
        docIndex:   resumeDoc,
        rangeIndex: resumeRange,
        copyIndex:  resumeCopy,
      });

      // Put in pausedOrders so UI shows Resume button
      pausedOrders.set(oid, {
        order,
        checkpoint: {
          printedPages:      resumePage,
          totalPages:        pj.totalPages || 0,
          currentDocIndex:   resumeDoc,
          currentRangeIndex: resumeRange,
          currentCopyIndex:  resumeCopy,
          pauseReason:       'power_failure',
          pausedAt:          new Date().toISOString(),
        },
      });

      log(`  ⚡ #${order.orderNumber} — power failure recovery: paused at doc=${resumeDoc}, range=${resumeRange}, copy=${resumeCopy}, page=${resumePage}`);
      eventCallback({
        type:            'print_paused',
        orderId:         oid,
        orderNumber:     order.orderNumber,
        reason:          'power_failure',
        printedPages:    resumePage,
        totalPages:      pj.totalPages || 0,
        currentDocIndex: resumeDoc,
        totalDocs:       order.documents?.length || 0,
        isRecovered:     true,
        pausedJobs:      getPausedJobs(),
      });
    }
  }

  eventCallback({ type: 'recovery_complete', count: incompleteOrders.length });
}

// ─── Local State Persistence ──────────────────────────────────────────────────
function saveLocalState() {
  if (!localStore) return;
  try {
    const jobs = [];
    // Save paused orders
    for (const [orderId, data] of pausedOrders) {
      jobs.push({
        orderId,
        orderNumber: data.order?.orderNumber,
        checkpoint:  data.checkpoint,
        savedAt:     new Date().toISOString(),
      });
    }
    // Save actively-printing orders with their current range checkpoint
    for (const orderId of printingNow) {
      if (!pausedOrders.has(orderId)) {
        const rc = rangeCheckpoints.get(orderId);
        jobs.push({
          orderId,
          status:          'was_printing',
          rangeCheckpoint: rc,
          // Store a full checkpoint so recovery can restore exact position
          checkpoint: rc ? {
            printedPages:      0,   // unknown — backend has the real value
            totalPages:        0,
            currentDocIndex:   rc.docIndex   || 0,
            currentRangeIndex: rc.rangeIndex || 0,
            currentCopyIndex:  rc.copyIndex  || 0,
            pauseReason:       'power_failure',
          } : null,
          savedAt: new Date().toISOString(),
        });
      }
    }
    localStore.set('incompleteJobs', jobs);
  } catch (err) {
    console.error('Failed to save local state:', err.message);
  }
}

function getLocalJobs() {
  if (!localStore) return [];
  try { return localStore.get('incompleteJobs', []); } catch { return []; }
}

function clearLocalJob(orderId) {
  if (!localStore) return;
  try {
    const jobs = localStore.get('incompleteJobs', []);
    localStore.set('incompleteJobs', jobs.filter(j => j.orderId !== orderId));
  } catch {}
}

// ─── Fallback Polling ─────────────────────────────────────────────────────────
async function fallbackPoll() {
  if (polling || !api) return;
  // Don't poll if we're already at max capacity — no point fetching more
  if (printingNow.size >= MAX_CONCURRENT && pendingQueue.length > 0) return;
  polling = true;
  try {
    const res    = await api.get('/orders/incomplete-jobs');
    const orders = res.data.data?.orders || [];
    const missed = orders.filter(o => {
      const id = o._id?.toString();
      return !printedOrders.has(id) && !printingNow.has(id) &&
             !pausedOrders.has(id) && !pendingQueue.some(q => q.orderId === id) &&
             (retryCount.get(id) || 0) < MAX_RETRIES;
    });
    if (missed.length > 0) {
      log(`Fallback poll: ${missed.length} unprinted order(s) found`);
      for (const order of missed) {
        // enqueueOrder will re-fetch the full order (with latest shop settings)
        // before calling processOrder — pass the lightweight poll result as seed
        enqueueOrder(order);
      }
    }
  } catch (err) {
    if (err.response?.status !== 401) log(`Fallback poll error: ${err.message}`);
  } finally {
    polling = false;
  }
}

// ─── Logging ──────────────────────────────────────────────────────────────────
const logs = [];
function log(msg) {
  const entry = `[${new Date().toLocaleTimeString()}] ${msg}`;
  logs.push(entry);
  if (logs.length > 300) logs.shift();
  console.log(entry);
  eventCallback({ type: 'log', message: entry });
}

function getLogs() { return [...logs]; }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── Exports ──────────────────────────────────────────────────────────────────
module.exports = {
  init,
  connect,
  disconnect,
  listPrinters,
  setPrinter,
  getStatus,
  getLogs,
  printOrder,
  getPausedJobs,
  pausePrintJob,
  resumePrintJob,
  refresh: fallbackPoll,
};
