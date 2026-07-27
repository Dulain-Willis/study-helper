import { useEffect, useState } from 'react'
import type { Card, Set } from '../../preload'
import './CardList.css'

export default function CardList({
  set,
  onBack,
  onStudy
}: {
  set: Set
  onBack: () => void
  onStudy: () => void
}): React.JSX.Element {
  const [cards, setCards] = useState<Card[]>([])
  const [editingId, setEditingId] = useState<number | null>(null)
  const [editFront, setEditFront] = useState('')
  const [editBack, setEditBack] = useState('')
  const [newFront, setNewFront] = useState('')
  const [newBack, setNewBack] = useState('')

  async function refresh(): Promise<void> {
    setCards(await window.api.getCards(set.id))
  }

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- initial data load on mount/set-change
    refresh()
    // eslint-disable-next-line react-hooks/exhaustive-deps -- refresh is stable per render, only set.id matters
  }, [set.id])

  async function handleAdd(): Promise<void> {
    const front = newFront.trim()
    const back = newBack.trim()
    if (!front || !back) return
    await window.api.addCard(set.id, front, back)
    setNewFront('')
    setNewBack('')
    await refresh()
  }

  function startEdit(card: Card): void {
    setEditingId(card.id)
    setEditFront(card.front)
    setEditBack(card.back)
  }

  async function confirmEdit(): Promise<void> {
    if (editingId === null) return
    const front = editFront.trim()
    const back = editBack.trim()
    if (front && back) {
      await window.api.updateCard(editingId, front, back)
      await refresh()
    }
    setEditingId(null)
  }

  async function handleDelete(id: number): Promise<void> {
    const confirmed = window.confirm('Delete this card? This cannot be undone.')
    if (!confirmed) return
    await window.api.deleteCard(id)
    await refresh()
  }

  return (
    <div className="card-list">
      <div className="card-list-header">
        <button onClick={onBack}>← Back</button>
        <h1>{set.name}</h1>
        <button className="primary" onClick={onStudy}>
          Study
        </button>
      </div>

      <div className="card-new">
        <textarea
          placeholder="Front"
          rows={2}
          value={newFront}
          onChange={(e) => setNewFront(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
              e.preventDefault()
              handleAdd()
            }
          }}
        />
        <textarea
          placeholder="Back"
          rows={2}
          value={newBack}
          onChange={(e) => setNewBack(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
              e.preventDefault()
              handleAdd()
            }
          }}
        />
        <button className="primary" onClick={handleAdd}>
          + Add Card
        </button>
      </div>

      <ul>
        {cards.map((card) => (
          <li key={card.id} className="card-row">
            {editingId === card.id ? (
              <>
                <textarea
                  autoFocus
                  rows={2}
                  onFocus={(e) => e.target.select()}
                  value={editFront}
                  onChange={(e) => setEditFront(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && !e.shiftKey) {
                      e.preventDefault()
                      confirmEdit()
                    }
                    if (e.key === 'Escape') setEditingId(null)
                  }}
                />
                <textarea
                  rows={2}
                  value={editBack}
                  onChange={(e) => setEditBack(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && !e.shiftKey) {
                      e.preventDefault()
                      confirmEdit()
                    }
                    if (e.key === 'Escape') setEditingId(null)
                  }}
                />
                <button onClick={confirmEdit} aria-label="Confirm">
                  ✓
                </button>
              </>
            ) : (
              <>
                <span className="card-front">{card.front}</span>
                <span className="card-back">{card.back}</span>
                <span className="card-actions">
                  <button onClick={() => startEdit(card)}>Edit</button>
                  <button className="danger" onClick={() => handleDelete(card.id)}>
                    Delete
                  </button>
                </span>
              </>
            )}
          </li>
        ))}
      </ul>
    </div>
  )
}
