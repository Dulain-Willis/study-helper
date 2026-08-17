import { contextBridge, ipcRenderer } from 'electron'
import { electronAPI } from '@electron-toolkit/preload'
import type { Card, Group, GroupDeleteSummary, Set, StudyProgress } from '../main/db'

export type { Card, Group, GroupDeleteSummary, Set, StudyProgress }

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
  getSetDeleteSummary: (id: number): Promise<{ cardCount: number }> =>
    ipcRenderer.invoke('db:getSetDeleteSummary', id),
  deleteSet: (id: number): Promise<void> => ipcRenderer.invoke('db:deleteSet', id),
  getCards: (setId: number): Promise<Card[]> => ipcRenderer.invoke('db:getCards', setId),
  addCard: (setId: number, front: string, back: string): Promise<Card> =>
    ipcRenderer.invoke('db:addCard', setId, front, back),
  updateCard: (id: number, front: string, back: string): Promise<void> =>
    ipcRenderer.invoke('db:updateCard', id, front, back),
  deleteCard: (id: number): Promise<void> => ipcRenderer.invoke('db:deleteCard', id),
  mergeSets: (setIds: number[], name: string, groupId: number): Promise<Set> =>
    ipcRenderer.invoke('db:mergeSets', setIds, name, groupId),
  getStudyProgress: (setId: number): Promise<StudyProgress | null> =>
    ipcRenderer.invoke('db:getStudyProgress', setId),
  saveStudyProgress: (setId: number, progress: StudyProgress): Promise<void> =>
    ipcRenderer.invoke('db:saveStudyProgress', setId, progress),
  clearStudyProgress: (setId: number): Promise<void> =>
    ipcRenderer.invoke('db:clearStudyProgress', setId)
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
