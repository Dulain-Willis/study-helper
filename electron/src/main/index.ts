import { app, shell, BrowserWindow, ipcMain } from 'electron'
import { join } from 'path'
import { electronApp, optimizer, is } from '@electron-toolkit/utils'
import icon from '../../resources/icon.png?asset'
import * as db from './db'

let mainWindow: BrowserWindow | null = null

// #13: single-instance lock so relaunching focuses the existing window
// instead of opening a second process (avoids two processes fighting
// over the same SQLite file).
const gotLock = app.requestSingleInstanceLock()
if (!gotLock) {
  app.quit()
} else {
  app.on('second-instance', () => {
    if (mainWindow && !mainWindow.isDestroyed()) {
      if (mainWindow.isMinimized()) mainWindow.restore()
      mainWindow.focus()
    }
  })
}

function createWindow(): void {
  // Create the browser window.
  const window = new BrowserWindow({
    width: 900,
    height: 670,
    show: false,
    autoHideMenuBar: true,
    ...(process.platform === 'linux' ? { icon } : {}),
    webPreferences: {
      preload: join(__dirname, '../preload/index.js'),
      sandbox: false
    }
  })
  mainWindow = window

  window.on('ready-to-show', () => {
    window.show()
  })

  window.webContents.setWindowOpenHandler((details) => {
    shell.openExternal(details.url)
    return { action: 'deny' }
  })

  // HMR for renderer base on electron-vite cli.
  // Load the remote URL for development or the local html file for production.
  if (is.dev && process.env['ELECTRON_RENDERER_URL']) {
    window.loadURL(process.env['ELECTRON_RENDERER_URL'])
  } else {
    window.loadFile(join(__dirname, '../renderer/index.html'))
  }
}

// This method will be called when Electron has finished
// initialization and is ready to create browser windows.
// Some APIs can only be used after this event occurs.
if (gotLock) {
  app.whenReady().then(() => {
    // Set app user model id for windows
    electronApp.setAppUserModelId('com.electron')

    // Default open or close DevTools by F12 in development
    // and ignore CommandOrControl + R in production.
    // see https://github.com/alex8088/electron-toolkit/tree/master/packages/utils
    app.on('browser-window-created', (_, window) => {
      optimizer.watchWindowShortcuts(window)
    })

    ipcMain.handle('db:getTree', () => db.getTree())
    ipcMain.handle('db:createGroup', (_e, name: string, parentId: number | null) =>
      db.createGroup(name, parentId)
    )
    ipcMain.handle('db:renameGroup', (_e, id: number, name: string) => db.renameGroup(id, name))
    ipcMain.handle('db:getGroupDeleteSummary', (_e, id: number) => db.getGroupDeleteSummary(id))
    ipcMain.handle('db:deleteGroup', (_e, id: number) => db.deleteGroup(id))
    ipcMain.handle('db:createSet', (_e, name: string, groupId: number) =>
      db.createSet(name, groupId)
    )
    ipcMain.handle('db:renameSet', (_e, id: number, name: string) => db.renameSet(id, name))
    ipcMain.handle('db:getSetDeleteSummary', () => db.getSetDeleteSummary())
    ipcMain.handle('db:deleteSet', (_e, id: number) => db.deleteSet(id))

    createWindow()

    app.on('activate', function () {
      // On macOS it's common to re-create a window in the app when the
      // dock icon is clicked and there are no other windows open.
      if (BrowserWindow.getAllWindows().length === 0) createWindow()
    })
  })

  // #13: standard Mac app, not a menu-bar utility — closing the window
  // fully quits the app on every platform, including macOS.
  app.on('window-all-closed', () => {
    app.quit()
  })
}

// In this file you can include the rest of your app's specific main process
// code. You can also put them in separate files and require them here.
