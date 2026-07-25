import { contextBridge, ipcRenderer } from 'electron'
import { electronAPI } from '@electron-toolkit/preload'
import type { Group, GroupDeleteSummary, Set } from '../main/db'

export type { Group, GroupDeleteSummary, Set }

const api = {
  getTree: (): Promise<{ groups: Group[]; sets: Set[] }> => ipcRenderer.invoke('db:getTree'),
  createGroup: (name: string, parentId: number | null): Promise<Group> =>
    ipcRenderer.invoke('db:createGroup', name, parentId),
  renameGroup: (id: number, name: string): Promise<void> =>
    ipcRenderer.invoke('db:renameGroup', id, name),
  getGroupDeleteSummary: (id: number): Promise<GroupDeleteSummary> =>
    ipcRenderer.invoke('db:getGroupDeleteSummary', id),
  deleteGroup: (id: number): Promise<void> => ipcRenderer.invoke('db:deleteGroup', id),
  createSet: (name: string, groupId: number): Promise<Set> =>
    ipcRenderer.invoke('db:createSet', name, groupId),
  renameSet: (id: number, name: string): Promise<void> =>
    ipcRenderer.invoke('db:renameSet', id, name),
  getSetDeleteSummary: (): Promise<{ cardCount: number }> =>
    ipcRenderer.invoke('db:getSetDeleteSummary'),
  deleteSet: (id: number): Promise<void> => ipcRenderer.invoke('db:deleteSet', id)
}

if (process.contextIsolated) {
  try {
    contextBridge.exposeInMainWorld('electron', electronAPI)
    contextBridge.exposeInMainWorld('api', api)
  } catch (error) {
    console.error(error)
  }
} else {
  // @ts-ignore (define in dts)
  window.electron = electronAPI
  // @ts-ignore (define in dts)
  window.api = api
}
