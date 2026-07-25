import { useEffect, useState } from 'react'
import type { Group, Set } from '../../preload'
import './GroupTree.css'

type EditTarget = { kind: 'group' | 'set'; id: number } | null
type MenuTarget = { kind: 'group' | 'set'; id: number } | null

export default function GroupTree({
  onOpenSet,
  onStudyAdhoc
}: {
  onOpenSet: (set: Set) => void
  onStudyAdhoc: (selection: { title: string; setIds: number[] }) => void
}): React.JSX.Element {
  const [groups, setGroups] = useState<Group[]>([])
  const [sets, setSets] = useState<Set[]>([])
  const [editing, setEditing] = useState<EditTarget>(null)
  const [editValue, setEditValue] = useState('')
  const [openMenu, setOpenMenu] = useState<MenuTarget>(null)
  const [mergeMode, setMergeMode] = useState(false)
  const [selectedSetIds, setSelectedSetIds] = useState<number[]>([])
  const [mergeName, setMergeName] = useState('')
  const [mergeTargetGroupId, setMergeTargetGroupId] = useState<number | null>(null)
  const [studyMode, setStudyMode] = useState(false)
  const [studySelectedSetIds, setStudySelectedSetIds] = useState<number[]>([])

  async function refresh(): Promise<void> {
    const tree = await window.api.getTree()
    setGroups(tree.groups)
    setSets(tree.sets)
  }

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- initial data load on mount
    refresh()
  }, [])

  async function handleCreateGroup(parentId: number | null): Promise<void> {
    const group = await window.api.createGroup('New Group', parentId)
    await refresh()
    startRename('group', group.id, group.name)
  }

  async function handleCreateSet(groupId: number): Promise<void> {
    const set = await window.api.createSet('New Set', groupId)
    await refresh()
    startRename('set', set.id, set.name)
  }

  function startRename(kind: 'group' | 'set', id: number, currentName: string): void {
    setOpenMenu(null)
    setEditing({ kind, id })
    setEditValue(currentName)
  }

  async function confirmRename(): Promise<void> {
    if (!editing) return
    const name = editValue.trim()
    if (name) {
      if (editing.kind === 'group') await window.api.renameGroup(editing.id, name)
      else await window.api.renameSet(editing.id, name)
      await refresh()
    }
    setEditing(null)
  }

  async function handleDeleteGroup(id: number, name: string): Promise<void> {
    setOpenMenu(null)
    const summary = await window.api.getGroupDeleteSummary(id)
    const parts = [`the group "${name}"`]
    if (summary.subgroupCount > 0) parts.push(`${summary.subgroupCount} subgroup(s)`)
    if (summary.setCount > 0) parts.push(`${summary.setCount} set(s)`)
    const confirmed = window.confirm(`Delete ${parts.join(', ')}? This cannot be undone.`)
    if (!confirmed) return
    await window.api.deleteGroup(id)
    await refresh()
  }

  async function handleDeleteSet(id: number, name: string): Promise<void> {
    setOpenMenu(null)
    const summary = await window.api.getSetDeleteSummary(id)
    const cards = summary.cardCount > 0 ? `, ${summary.cardCount} card(s)` : ''
    const confirmed = window.confirm(`Delete the set "${name}"${cards}? This cannot be undone.`)
    if (!confirmed) return
    await window.api.deleteSet(id)
    await refresh()
  }

  function toggleMergeMode(): void {
    setMergeMode((m) => !m)
    setSelectedSetIds([])
    setMergeName('')
    setMergeTargetGroupId(null)
  }

  function toggleSetSelection(id: number): void {
    setSelectedSetIds((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]))
  }

  function toggleStudyMode(): void {
    setStudyMode((m) => !m)
    setStudySelectedSetIds([])
  }

  function toggleStudySetSelection(id: number): void {
    setStudySelectedSetIds((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]
    )
  }

  function handleStartAdhocStudy(): void {
    if (studySelectedSetIds.length === 0) return
    const names = sets.filter((s) => studySelectedSetIds.includes(s.id)).map((s) => s.name)
    onStudyAdhoc({ title: names.join(', '), setIds: studySelectedSetIds })
    toggleStudyMode()
  }

  function flattenGroups(
    parentId: number | null,
    depth: number
  ): { group: Group; depth: number }[] {
    return groups
      .filter((g) => g.parentId === parentId)
      .flatMap((g) => [{ group: g, depth }, ...flattenGroups(g.id, depth + 1)])
  }

  async function handleMerge(): Promise<void> {
    const name = mergeName.trim()
    if (selectedSetIds.length < 2 || !name || mergeTargetGroupId === null) return
    await window.api.mergeSets(selectedSetIds, name, mergeTargetGroupId)
    toggleMergeMode()
    await refresh()
  }

  function renderMenu(kind: 'group' | 'set', id: number, name: string): React.JSX.Element {
    const isOpen = openMenu?.kind === kind && openMenu.id === id
    return (
      <div className="node-menu">
        <button
          className="node-menu-trigger"
          onClick={() => setOpenMenu(isOpen ? null : { kind, id })}
          aria-label="Actions"
        >
          ⋯
        </button>
        {isOpen && (
          <div className="node-menu-dropdown">
            <button onClick={() => startRename(kind, id, name)}>Rename</button>
            <button
              onClick={() =>
                kind === 'group' ? handleDeleteGroup(id, name) : handleDeleteSet(id, name)
              }
            >
              Delete
            </button>
          </div>
        )}
      </div>
    )
  }

  function renderName(
    kind: 'group' | 'set',
    id: number,
    name: string,
    onClick?: () => void
  ): React.JSX.Element {
    const isEditing = editing?.kind === kind && editing.id === id
    if (!isEditing) {
      return (
        <span className={onClick ? 'node-name node-name-clickable' : 'node-name'} onClick={onClick}>
          {name}
        </span>
      )
    }
    return (
      <span className="node-edit">
        <input
          autoFocus
          onFocus={(e) => e.target.select()}
          value={editValue}
          onChange={(e) => setEditValue(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') confirmRename()
            if (e.key === 'Escape') setEditing(null)
          }}
        />
        <button onClick={confirmRename} aria-label="Confirm">
          ✓
        </button>
      </span>
    )
  }

  function renderGroup(group: Group): React.JSX.Element {
    const childGroups = groups.filter((g) => g.parentId === group.id)
    const childSets = sets.filter((s) => s.groupId === group.id)
    return (
      <li key={`group-${group.id}`} className="node">
        <div className="node-row">
          {renderName('group', group.id, group.name)}
          <span className="node-actions">
            <button onClick={() => handleCreateGroup(group.id)}>+ Group</button>
            <button onClick={() => handleCreateSet(group.id)}>+ Set</button>
            {renderMenu('group', group.id, group.name)}
          </span>
        </div>
        {(childGroups.length > 0 || childSets.length > 0) && (
          <ul>
            {childGroups.map(renderGroup)}
            {childSets.map((set) => (
              <li key={`set-${set.id}`} className="node">
                <div className="node-row">
                  {mergeMode ? (
                    <label className="node-name">
                      <input
                        type="checkbox"
                        checked={selectedSetIds.includes(set.id)}
                        onChange={() => toggleSetSelection(set.id)}
                      />{' '}
                      {set.name}
                    </label>
                  ) : studyMode ? (
                    <label className="node-name">
                      <input
                        type="checkbox"
                        checked={studySelectedSetIds.includes(set.id)}
                        onChange={() => toggleStudySetSelection(set.id)}
                      />{' '}
                      {set.name}
                    </label>
                  ) : (
                    <>
                      {renderName('set', set.id, set.name, () => onOpenSet(set))}
                      <span className="node-actions">{renderMenu('set', set.id, set.name)}</span>
                    </>
                  )}
                </div>
              </li>
            ))}
          </ul>
        )}
      </li>
    )
  }

  const rootGroups = groups.filter((g) => g.parentId === null)

  return (
    <div className="group-tree">
      <div className="group-tree-header">
        <h1>Groups</h1>
        <span className="group-tree-header-actions">
          {!mergeMode && !studyMode && (
            <button onClick={() => handleCreateGroup(null)}>+ New Group</button>
          )}
          {!studyMode && (
            <button onClick={toggleMergeMode}>{mergeMode ? 'Cancel Merge' : 'Merge Sets'}</button>
          )}
          {!mergeMode && (
            <button onClick={toggleStudyMode}>{studyMode ? 'Cancel Study' : 'Study Sets'}</button>
          )}
        </span>
      </div>
      <ul>{rootGroups.map(renderGroup)}</ul>
      {studyMode && (
        <div className="merge-panel">
          <span>{studySelectedSetIds.length} set(s) selected</span>
          <button onClick={handleStartAdhocStudy} disabled={studySelectedSetIds.length === 0}>
            Start Study Session
          </button>
        </div>
      )}
      {mergeMode && (
        <div className="merge-panel">
          <span>{selectedSetIds.length} set(s) selected</span>
          <input
            placeholder="New set name"
            value={mergeName}
            onChange={(e) => setMergeName(e.target.value)}
          />
          <select
            value={mergeTargetGroupId ?? ''}
            onChange={(e) => setMergeTargetGroupId(e.target.value ? Number(e.target.value) : null)}
          >
            <option value="">Select group…</option>
            {flattenGroups(null, 0).map(({ group, depth }) => (
              <option key={group.id} value={group.id}>
                {'—'.repeat(depth)} {group.name}
              </option>
            ))}
          </select>
          <button
            onClick={handleMerge}
            disabled={selectedSetIds.length < 2 || !mergeName.trim() || mergeTargetGroupId === null}
          >
            Merge
          </button>
        </div>
      )}
    </div>
  )
}
