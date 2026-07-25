import { ElectronAPI } from '@electron-toolkit/preload'
import type { Group, GroupDeleteSummary, Set } from '../main/db'

export type { Group, GroupDeleteSummary, Set }

export interface Api {
  getTree: () => Promise<{ groups: Group[]; sets: Set[] }>
  createGroup: (name: string, parentId: number | null) => Promise<Group>
  renameGroup: (id: number, name: string) => Promise<void>
  getGroupDeleteSummary: (id: number) => Promise<GroupDeleteSummary>
  deleteGroup: (id: number) => Promise<void>
  createSet: (name: string, groupId: number) => Promise<Set>
  renameSet: (id: number, name: string) => Promise<void>
  getSetDeleteSummary: () => Promise<{ cardCount: number }>
  deleteSet: (id: number) => Promise<void>
}

declare global {
  interface Window {
    electron: ElectronAPI
    api: Api
  }
}
