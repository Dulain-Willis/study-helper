import { ElectronAPI } from '@electron-toolkit/preload'
import type { Card, Group, GroupDeleteSummary, Set } from '../main/db'

export type { Card, Group, GroupDeleteSummary, Set }

export interface Api {
  getTree: () => Promise<{ groups: Group[]; sets: Set[] }>
  createGroup: (name: string, parentId: number | null) => Promise<Group>
  renameGroup: (id: number, name: string) => Promise<void>
  getGroupDeleteSummary: (id: number) => Promise<GroupDeleteSummary>
  deleteGroup: (id: number) => Promise<void>
  createSet: (name: string, groupId: number) => Promise<Set>
  renameSet: (id: number, name: string) => Promise<void>
  getSetDeleteSummary: (id: number) => Promise<{ cardCount: number }>
  deleteSet: (id: number) => Promise<void>
  getCards: (setId: number) => Promise<Card[]>
  addCard: (setId: number, front: string, back: string) => Promise<Card>
  updateCard: (id: number, front: string, back: string) => Promise<void>
  deleteCard: (id: number) => Promise<void>
}

declare global {
  interface Window {
    electron: ElectronAPI
    api: Api
  }
}
