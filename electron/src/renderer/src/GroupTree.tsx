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
  const [currentGroupId, setCurrentGroupId] = useState<number | null>(null)
  const [editing, setEditing] = useState<EditTarget>(null)
  const [editValue, setEditValue] = useState('')
  const [openMenu, setOpenMenu] = useState<MenuTarget>(null)
  const [newGroupName, setNewGroupName] = useState('')
  const [newSetName, setNewSetName] = useState('')
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
    const name = newGroupName.trim()
    if (!name) return
    await window.api.createGroup(name, parentId)
    setNewGroupName('')
    await refresh()
  }

  async function handleCreateSet(groupId: number): Promise<void> {
    const name = newSetName.trim()
    if (!name) return
    await window.api.createSet(name, groupId)
    setNewSetName('')
    await refresh()
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
          •••
        </button>
        {isOpen && (
          <div className="node-menu-dropdown">
            <button onClick={() => startRename(kind, id, name)}>Rename</button>
            <button
              className="danger"
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

  function renderRow(
    kind: 'group' | 'set',
    id: number,
    name: string,
    onClick: () => void
  ): React.JSX.Element {
    const isEditing = editing?.kind === kind && editing.id === id
    if (isEditing) {
      return (
        <div key={`${kind}-${id}`} className="node-row">
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
          </span>
          <button onClick={confirmRename} aria-label="Confirm">
            ✓
          </button>
        </div>
      )
    }
    return (
      <div key={`${kind}-${id}`} className="node-row">
        <span className="node-name-clickable" onClick={onClick}>
          {name}
        </span>
        <span className="node-arrow">→</span>
        <span className="node-actions">{renderMenu(kind, id, name)}</span>
      </div>
    )
  }

  const childGroups = groups.filter((g) => g.parentId === currentGroupId)
  const childSets = currentGroupId === null ? [] : sets.filter((s) => s.groupId === currentGroupId)
  const currentGroup = currentGroupId === null ? null : groups.find((g) => g.id === currentGroupId)

  function breadcrumbTrail(): Group[] {
    const trail: Group[] = []
    let cursor = currentGroup
    while (cursor) {
      trail.unshift(cursor)
      cursor = cursor.parentId === null ? null : groups.find((g) => g.id === cursor!.parentId) || null
    }
    return trail
  }

  if (mergeMode || studyMode) {
    function renderModeGroup(group: Group): React.JSX.Element {
      const children = groups.filter((g) => g.parentId === group.id)
      const groupSets = sets.filter((s) => s.groupId === group.id)
      return (
        <li key={`group-${group.id}`} className="node">
          <div className="node-row">
            <span className="node-name">{group.name}</span>
          </div>
          {(children.length > 0 || groupSets.length > 0) && (
            <ul>
              {children.map(renderModeGroup)}
              {groupSets.map((set) => (
                <li key={`set-${set.id}`} className="node">
                  <div className="node-row">
                    <label className="node-name">
                      <input
                        type="checkbox"
                        checked={
                          mergeMode
                            ? selectedSetIds.includes(set.id)
                            : studySelectedSetIds.includes(set.id)
                        }
                        onChange={() =>
                          mergeMode ? toggleSetSelection(set.id) : toggleStudySetSelection(set.id)
                        }
                      />{' '}
                      {set.name}
                    </label>
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
          <h1>{mergeMode ? 'Merge Sets' : 'Study Sets'}</h1>
          <span className="group-tree-header-actions">
            <button onClick={mergeMode ? toggleMergeMode : toggleStudyMode}>Cancel</button>
          </span>
        </div>
        <ul>{rootGroups.map(renderModeGroup)}</ul>
        {studyMode && (
          <div className="merge-panel">
            <span>{studySelectedSetIds.length} set(s) selected</span>
            <button
              className="primary"
              onClick={handleStartAdhocStudy}
              disabled={studySelectedSetIds.length === 0}
            >
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
              className="primary"
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

  const trail = breadcrumbTrail()

  return (
    <div className="group-tree">
      <nav className="breadcrumb">
        <span className="breadcrumb-link" onClick={() => setCurrentGroupId(null)}>
          Library
        </span>
        {trail.map((g, i) => (
          <span key={g.id}>
            <span className="breadcrumb-sep">/</span>
            {i === trail.length - 1 ? (
              <span className="breadcrumb-current">{g.name}</span>
            ) : (
              <span className="breadcrumb-link" onClick={() => setCurrentGroupId(g.id)}>
                {g.name}
              </span>
            )}
          </span>
        ))}
      </nav>

      <div className="group-tree-header">
        <h1>{currentGroup ? currentGroup.name : 'Library'}</h1>
        <span className="group-tree-header-actions">
          <button onClick={toggleMergeMode}>Merge Sets</button>
          <button onClick={toggleStudyMode}>Study Sets</button>
        </span>
      </div>
      {currentGroupId === null && (
        <p className="section-subtitle">Organize your flashcard sets into groups</p>
      )}

      {childGroups.length === 0 && childSets.length === 0 && (
        <p className="empty-message">
          {currentGroupId === null
            ? 'No groups yet. Create one below to get started.'
            : 'Empty group. Add a subgroup or set below.'}
        </p>
      )}

      {childGroups.length > 0 && (
        <section className="node-section">
          {currentGroupId !== null && <h2 className="section-label">Groups</h2>}
          <ul className="node-list">
            {childGroups.map((group) => renderRow('group', group.id, group.name, () => setCurrentGroupId(group.id)))}
          </ul>
        </section>
      )}

      {childSets.length > 0 && (
        <section className="node-section">
          <h2 className="section-label">Sets</h2>
          <ul className="node-list">
            {childSets.map((set) =>
              renderRow('set', set.id, set.name, () => onOpenSet(set))
            )}
          </ul>
        </section>
      )}

      <section className="node-section add-section">
        <h2 className="section-label">Add</h2>
        <div className="add-row">
          <input
            placeholder={currentGroupId === null ? 'New group name' : 'New subgroup name'}
            value={newGroupName}
            onChange={(e) => setNewGroupName(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleCreateGroup(currentGroupId)}
          />
          <button onClick={() => handleCreateGroup(currentGroupId)}>+ Group</button>
        </div>
        {currentGroupId !== null && (
          <div className="add-row">
            <input
              placeholder="New set name"
              value={newSetName}
              onChange={(e) => setNewSetName(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleCreateSet(currentGroupId)}
            />
            <button className="primary" onClick={() => handleCreateSet(currentGroupId)}>
              + Set
            </button>
          </div>
        )}
      </section>
    </div>
  )
}
