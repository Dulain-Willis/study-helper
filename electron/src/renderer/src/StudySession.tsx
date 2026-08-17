import { useEffect, useState } from 'react'
import type { Card } from '../../preload'
import CardContent from './CardContentDisplay'
import './StudySession.css'

type Mode = 'pick' | 'browse' | 'practice' | 'complete'

export default function StudySession({
  title,
  setIds,
  setId,
  onExit
}: {
  title: string
  setIds: number[]
  // Explicit single real-Set marker (distinct from setIds) — progress only
  // persists for this case, never for ad-hoc multi-set study. See #36.
  setId?: number
  onExit: () => void
}): React.JSX.Element {
  const [mode, setMode] = useState<Mode>('pick')
  const [cards, setCards] = useState<Card[]>([])
  const [deck, setDeck] = useState<Card[]>([])
  const [index, setIndex] = useState(0)
  const [flipped, setFlipped] = useState(false)
  const [wrong, setWrong] = useState<Card[]>([])
  const [round, setRound] = useState(1)
  const [correctCount, setCorrectCount] = useState(0)
  const [total, setTotal] = useState(0)

  const setIdsKey = setIds.join(',')

  useEffect(() => {
    Promise.all([
      Promise.all(setIds.map((id) => window.api.getCards(id))).then((lists) => lists.flat()),
      setId ? window.api.getStudyProgress(setId) : Promise.resolve(null)
    ]).then(([allCards, progress]) => {
      setCards(allCards)
      if (!progress) return
      const cardById = new Map(allCards.map((c) => [c.id, c]))
      const resumedDeck = progress.deck
        .filter((id) => cardById.has(id))
        .map((id) => cardById.get(id)!)
      const resumedWrong = progress.wrong
        .filter((id) => cardById.has(id))
        .map((id) => cardById.get(id)!)
      if (resumedDeck.length === 0) {
        window.api.clearStudyProgress(setId!)
        return
      }
      // Deleted cards shift positions — count surviving cards before the saved
      // index to land on the same card (or the next surviving one) rather than
      // reusing the raw numeric index against the now-shorter deck.
      const survivedBeforeIndex = progress.deck
        .slice(0, progress.index)
        .filter((id) => cardById.has(id)).length
      setDeck(resumedDeck)
      setIndex(Math.min(survivedBeforeIndex, resumedDeck.length - 1))
      setWrong(resumedWrong)
      setRound(progress.round)
      setCorrectCount(progress.correctCount)
      setTotal(progress.total)
      setMode('practice')
    })
    // setIdsKey is the stable dependency; setIds itself is a new array each render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [setIdsKey, setId])

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
    setCorrectCount(0)
    setTotal(cards.length)
    setMode('practice')
  }

  function restart(): void {
    if (setId) window.api.clearStudyProgress(setId)
    startPractice()
  }

  function persistProgress(
    nextDeck: Card[],
    nextIndex: number,
    nextWrong: Card[],
    nextRound: number,
    nextCorrectCount: number
  ): void {
    if (!setId) return
    window.api.saveStudyProgress(setId, {
      deck: nextDeck.map((c) => c.id),
      index: nextIndex,
      wrong: nextWrong.map((c) => c.id),
      round: nextRound,
      correctCount: nextCorrectCount,
      total
    })
  }

  function mark(correct: boolean): void {
    const nextCorrectCount = round === 1 && correct ? correctCount + 1 : correctCount
    if (nextCorrectCount !== correctCount) setCorrectCount(nextCorrectCount)
    const nextWrong = correct ? wrong : [...wrong, deck[index]]
    if (index + 1 < deck.length) {
      const nextIndex = index + 1
      setWrong(nextWrong)
      setIndex(nextIndex)
      setFlipped(false)
      persistProgress(deck, nextIndex, nextWrong, round, nextCorrectCount)
      return
    }
    if (nextWrong.length === 0) {
      setMode('complete')
      if (setId) window.api.clearStudyProgress(setId)
      return
    }
    setDeck(nextWrong)
    setWrong([])
    setIndex(0)
    setFlipped(false)
    setRound(round + 1)
    persistProgress(nextWrong, 0, [], round + 1, nextCorrectCount)
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
          <p className="study-score">
            {correctCount} / {total}
          </p>
          <div className="study-end-actions">
            <button onClick={restart}>Restart</button>
            <button className="primary" onClick={onExit}>
              Back to Set
            </button>
          </div>
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
        {mode === 'practice' && <button onClick={restart}>Restart</button>}
      </div>
      <p className="study-progress">
        {mode === 'practice' ? `Round ${round} — ` : ''}
        Card {index + 1} of {deck.length}
        {mode === 'practice' ? ` — ${correctCount} / ${total}` : ''}
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
