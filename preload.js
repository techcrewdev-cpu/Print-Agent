/**
 * Smart Xerox Print Agent — Preload Script
 * Secure bridge between renderer (UI) and main process via contextBridge.
 */

const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('agent', {
  // Auth
  login:       (creds)       => ipcRenderer.invoke('login', creds),
  getSession:  ()            => ipcRenderer.invoke('getSession'),
  logout:      ()            => ipcRenderer.invoke('logout'),

  // Printers
  getPrinters:    ()         => ipcRenderer.invoke('getPrinters'),
  selectPrinter:  (names)    => ipcRenderer.invoke('selectPrinter', names),  // Now accepts array
  togglePrinter:  (name, enabled) => ipcRenderer.invoke('togglePrinter', name, enabled),

  // Engine
  connectEngine:     ()      => ipcRenderer.invoke('connectEngine'),
  disconnectEngine:  ()      => ipcRenderer.invoke('disconnectEngine'),
  getStatus:         ()      => ipcRenderer.invoke('getStatus'),
  getLogs:           ()      => ipcRenderer.invoke('getLogs'),
  refresh:           ()      => ipcRenderer.invoke('refreshEngine'),

  // Fault-Tolerant Print — Pause / Resume / Recovery
  pausePrintJob:   (orderId) => ipcRenderer.invoke('pausePrintJob', orderId),
  resumePrintJob:  (orderId) => ipcRenderer.invoke('resumePrintJob', orderId),
  getPausedJobs:   ()        => ipcRenderer.invoke('getPausedJobs'),

  // Events from main process
  onEvent: (callback) => {
    ipcRenderer.on('engine-event', (_event, data) => callback(data));
  },
});
