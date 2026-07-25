import Database from 'better-sqlite3'
import { app } from 'electron'
import { join } from 'path'

export interface Group {
  id: number
  name: string
  parentId: number | null
}

export interface Set {
  id: number
  name: string
  groupId: number
}

export interface GroupDeleteSummary {
  subgroupCount: number
  setCount: number
}

const db = new Database(join(app.getPath('userData'), 'study-helper.db'))

// WAL mode set once here at DB creation — MCP server (direct SQLite access)
// relies on this already being set. See #12.
db.pragma('journal_mode = WAL')
db.pragma('foreign_keys = ON')

db.exec(`
  CREATE TABLE IF NOT EXISTS groups (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    parent_id INTEGER REFERENCES groups(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS sets (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE
  );
`)

export function getTree(): { groups: Group[]; sets: Set[] } {
  const groups = db.prepare('SELECT id, name, parent_id AS parentId FROM groups').all() as Group[]
  const sets = db.prepare('SELECT id, name, group_id AS groupId FROM sets').all() as Set[]
  return { groups, sets }
}

export function createGroup(name: string, parentId: number | null): Group {
  const { lastInsertRowid } = db
    .prepare('INSERT INTO groups (name, parent_id) VALUES (?, ?)')
    .run(name, parentId)
  return { id: Number(lastInsertRowid), name, parentId }
}

export function renameGroup(id: number, name: string): void {
  db.prepare('UPDATE groups SET name = ? WHERE id = ?').run(name, id)
}

export function getGroupDeleteSummary(id: number): GroupDeleteSummary {
  const { subgroupCount } = db
    .prepare(
      `WITH RECURSIVE descendants(id) AS (
         SELECT id FROM groups WHERE parent_id = ?
         UNION ALL
         SELECT g.id FROM groups g JOIN descendants d ON g.parent_id = d.id
       )
       SELECT COUNT(*) AS subgroupCount FROM descendants`
    )
    .get(id) as { subgroupCount: number }

  const { setCount } = db
    .prepare(
      `WITH RECURSIVE descendants(id) AS (
         SELECT ? AS id
         UNION ALL
         SELECT g.id FROM groups g JOIN descendants d ON g.parent_id = d.id
       )
       SELECT COUNT(*) AS setCount FROM sets WHERE group_id IN (SELECT id FROM descendants)`
    )
    .get(id) as { setCount: number }

  return { subgroupCount, setCount }
}

export function deleteGroup(id: number): void {
  // ON DELETE CASCADE on groups.parent_id and sets.group_id handles the
  // recursive cleanup of subgroups/sets (and cards, once #4 adds them).
  db.prepare('DELETE FROM groups WHERE id = ?').run(id)
}

export function createSet(name: string, groupId: number): Set {
  const { lastInsertRowid } = db
    .prepare('INSERT INTO sets (name, group_id) VALUES (?, ?)')
    .run(name, groupId)
  return { id: Number(lastInsertRowid), name, groupId }
}

export function renameSet(id: number, name: string): void {
  db.prepare('UPDATE sets SET name = ? WHERE id = ?').run(name, id)
}

export function getSetDeleteSummary(): { cardCount: number } {
  // Cards aren't implemented yet (#4) — cascade wiring is a no-op for now.
  return { cardCount: 0 }
}

export function deleteSet(id: number): void {
  db.prepare('DELETE FROM sets WHERE id = ?').run(id)
}
