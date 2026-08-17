import { useEffect, useState } from 'react'
import type { Card } from '../../preload'
import CardContent from './CardContentDisplay'
import './StudySession.css'

type Mode = 'pick' | 'browse' | 'practice' | 'complete'

export default function StudySession({
  title,
  setIds,
  onExit
}: {
  title: string
  setIds: number[]
  onExit: () => void
}): React.JSX.Element {
  const [mode, setMode] = useState<Mode>('pick')
  const [cards, setCards] = useState<Card[]>([])
  const [deck, setDeck] = useState<Card[]>([])
  const [index, setIndex] = useState(0)
  const [flipped, setFlipped] = useState(false)
  const [wrong, setWrong] = useState<Card[]>([])
  const [round, setRound] = useState(1)

  const setIdsKey = setIds.join(',')

  useEffect(() => {
    Promise.all(setIds.map((id) => window.api.getCards(id))).then((lists) => setCards(lists.flat()))
    // setIdsKey is the stable dependency; setIds itself is a new array each render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [setIdsKey])

  function startBrowse(): void {
    setDeck(cards)
    setIndex(0)
    setFlipped(false)
    setMode('browse')
  }

  function startPractice(): void {
    setDeck(cards)
    setIndex(0)
    setFlipped(false)
    setWrong([])
    setRound(1)
    setMode('practice')
  }

  function mark(correct: boolean): void {
    const next = correct ? wrong : [...wrong, deck[index]]
    if (index + 1 < deck.length) {
      setWrong(next)
      setIndex(index + 1)
      setFlipped(false)
      return
    }
    if (next.length === 0) {
      setMode('complete')
      return
    }
    setDeck(next)
    setWrong([])
    setIndex(0)
    setFlipped(false)
    setRound(round + 1)
  }

  if (mode === 'pick') {
    return (
      <div className="study-session">
        <div className="study-header">
          <button onClick={onExit}>← Back</button>
          <h1>{title}</h1>
        </div>
        {cards.length === 0 ? (
          <p>No cards in this set.</p>
        ) : (
          <div className="study-pick">
            <button onClick={startBrowse}>Browse</button>
            <button className="primary" onClick={startPractice}>
              Practice
            </button>
          </div>
        )}
      </div>
    )
  }

  if (mode === 'complete') {
    return (
      <div className="study-session">
        <div className="study-end">
          <h2>Session complete</h2>
          <p>All cards correct.</p>
          <button className="primary" onClick={onExit}>
            Back to Set
          </button>
        </div>
      </div>
    )
  }

  const card = deck[index]

  return (
    <div className="study-session">
      <div className="study-header">
        <button onClick={onExit}>← Back</button>
        <h1>{title}</h1>
      </div>
      <p className="study-progress">
        {mode === 'practice' ? `Round ${round} — ` : ''}
        Card {index + 1} of {deck.length}
      </p>
      <div className="study-progress-bar">
        <div
          className="study-progress-bar-fill"
          style={{ width: `${((index + 1) / deck.length) * 100}%` }}
        />
      </div>
      <div className="study-card" onClick={() => setFlipped(!flipped)}>
        <div className="study-card-body">
          <CardContent text={flipped ? card.back : card.front} />
        </div>
      </div>
      {mode === 'browse' ? (
        <div className="study-controls">
          <button
            disabled={index === 0}
            onClick={() => {
              setIndex(index - 1)
              setFlipped(false)
            }}
          >
            Prev
          </button>
          <button
            className="primary"
            disabled={index === deck.length - 1}
            onClick={() => {
              setIndex(index + 1)
              setFlipped(false)
            }}
          >
            Next
          </button>
        </div>
      ) : (
        <div className="study-controls">
          <button
            className="danger"
            disabled={!flipped}
            onClick={() => mark(false)}
            aria-label="Incorrect"
          >
            ✗
          </button>
          <button
            className="success"
            disabled={!flipped}
            onClick={() => mark(true)}
            aria-label="Correct"
          >
            ✓
          </button>
        </div>
      )}
    </div>
  )
}
