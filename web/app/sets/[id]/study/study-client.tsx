"use client";

import { useState } from "react";
import Link from "next/link";

type Card = { id: string; front: string; back: string };
type Mark = "correct" | "incorrect";

export default function StudyClient({
  setId,
  setName,
  cards,
  mode,
}: {
  setId: string;
  setName: string;
  cards: Card[];
  mode: "browse" | "practice";
}) {
  const [index, setIndex] = useState(0);
  const [flipped, setFlipped] = useState(false);

  // Browse mode
  const [done, setDone] = useState(false);

  // Practice mode
  const [deck, setDeck] = useState<Card[]>(cards);
  const [marks, setMarks] = useState<Record<string, Mark>>({});
  const [roundDone, setRoundDone] = useState(false);
  const [allDone, setAllDone] = useState(false);

  const card = mode === "practice" ? deck[index] : cards[index];
  const total = mode === "practice" ? deck.length : cards.length;

  // Browse handlers
  function next() {
    if (index + 1 >= total) {
      setDone(true);
    } else {
      setIndex(index + 1);
      setFlipped(false);
    }
  }

  function prev() {
    if (index > 0) {
      setIndex(index - 1);
      setFlipped(false);
    }
  }

  // Practice handlers
  function mark(result: Mark) {
    const newMarks = { ...marks, [card.id]: result };
    setMarks(newMarks);
    if (index + 1 >= total) {
      const wrongCards = deck.filter((c) => newMarks[c.id] === "incorrect");
      if (wrongCards.length === 0) {
        setAllDone(true);
      } else {
        setRoundDone(true);
      }
    } else {
      setIndex(index + 1);
      setFlipped(false);
    }
  }

  function restartFull() {
    setDeck(cards);
    setMarks({});
    setIndex(0);
    setFlipped(false);
    setRoundDone(false);
    setAllDone(false);
  }

  function retryWrong() {
    const wrongCards = deck.filter((c) => marks[c.id] === "incorrect");
    setDeck(wrongCards);
    setMarks({});
    setIndex(0);
    setFlipped(false);
    setRoundDone(false);
  }

  // Browse: end screen
  if (mode === "browse" && done) {
    return (
      <div className="flex flex-col items-center justify-center gap-6 py-24 text-center">
        <h2 className="text-2xl font-bold">Done!</h2>
        <p className="text-zinc-500 dark:text-zinc-400 text-sm">You went through all {total} cards.</p>
        <div className="flex gap-3">
          <button
            onClick={() => { setIndex(0); setFlipped(false); setDone(false); }}
            className="rounded-lg border border-zinc-200 dark:border-zinc-800 px-4 py-2 text-sm font-medium hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
          >
            Restart
          </button>
          <Link
            href={`/sets/${setId}`}
            className="rounded-lg bg-zinc-900 dark:bg-zinc-100 text-white dark:text-zinc-900 px-4 py-2 text-sm font-medium hover:bg-zinc-700 dark:hover:bg-zinc-300 transition-colors"
          >
            Back to Set
          </Link>
        </div>
      </div>
    );
  }

  // Practice: all done (zero wrong remaining)
  if (mode === "practice" && allDone) {
    return (
      <div className="flex flex-col items-center justify-center gap-6 py-24 text-center">
        <h2 className="text-2xl font-bold">All done!</h2>
        <p className="text-zinc-500 dark:text-zinc-400 text-sm">You got every card correct.</p>
        <Link
          href={`/sets/${setId}`}
          className="rounded-lg bg-zinc-900 dark:bg-zinc-100 text-white dark:text-zinc-900 px-4 py-2 text-sm font-medium hover:bg-zinc-700 dark:hover:bg-zinc-300 transition-colors"
        >
          Back to Set
        </Link>
      </div>
    );
  }

  // Practice: end of round with wrong cards remaining
  if (mode === "practice" && roundDone) {
    const correctCount = deck.filter((c) => marks[c.id] === "correct").length;
    const wrongCount = deck.length - correctCount;
    return (
      <div className="flex flex-col items-center justify-center gap-6 py-24 text-center">
        <h2 className="text-2xl font-bold">Round complete</h2>
        <p className="text-zinc-500 dark:text-zinc-400 text-sm">
          {correctCount} / {deck.length} correct · {wrongCount} to retry
        </p>
        <div className="flex gap-3">
          <button
            onClick={restartFull}
            className="rounded-lg border border-zinc-200 dark:border-zinc-800 px-4 py-2 text-sm font-medium hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
          >
            Restart
          </button>
          <button
            onClick={retryWrong}
            className="rounded-lg bg-zinc-900 dark:bg-zinc-100 text-white dark:text-zinc-900 px-4 py-2 text-sm font-medium hover:bg-zinc-700 dark:hover:bg-zinc-300 transition-colors"
          >
            Retry wrong ({wrongCount})
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex items-center justify-between">
        <Link href={`/sets/${setId}`} className="text-sm text-zinc-500 dark:text-zinc-400 hover:text-zinc-900 dark:hover:text-zinc-100">
          ← {setName}
        </Link>
        <span className="text-sm text-zinc-500 dark:text-zinc-400">{index + 1} / {total}</span>
      </div>

      {/* Progress bar */}
      <div className="w-full h-1 bg-zinc-200 dark:bg-zinc-800 rounded-full">
        <div
          className="h-1 bg-zinc-900 dark:bg-zinc-100 rounded-full transition-all"
          style={{ width: `${((index + 1) / total) * 100}%` }}
        />
      </div>

      {/* Card */}
      <button
        onClick={() => setFlipped(!flipped)}
        className="w-full min-h-64 rounded-xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 p-8 text-left flex flex-col justify-between hover:border-zinc-400 dark:hover:border-zinc-600 transition-colors cursor-pointer"
      >
        <span className="text-xs font-semibold uppercase tracking-wider text-zinc-400">
          {flipped ? "Answer" : "Question"}
        </span>
        <p className="text-lg leading-relaxed mt-4">
          {flipped ? card.back : card.front}
        </p>
        <span className="text-xs text-zinc-400 mt-4">
          {flipped ? "Click to see question" : "Click to reveal answer"}
        </span>
      </button>

      {/* Nav */}
      {mode === "browse" ? (
        <div className="flex gap-3">
          <button
            onClick={prev}
            disabled={index === 0}
            className="flex-1 rounded-lg border border-zinc-200 dark:border-zinc-800 px-4 py-2 text-sm font-medium hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors disabled:opacity-30 disabled:cursor-not-allowed"
          >
            ← Prev
          </button>
          <button
            onClick={next}
            className="flex-1 rounded-lg bg-zinc-900 dark:bg-zinc-100 text-white dark:text-zinc-900 px-4 py-2 text-sm font-medium hover:bg-zinc-700 dark:hover:bg-zinc-300 transition-colors"
          >
            {index + 1 === total ? "Finish" : "Next →"}
          </button>
        </div>
      ) : (
        <div className="flex gap-3">
          <button
            onClick={() => mark("incorrect")}
            disabled={!flipped}
            className="flex-1 rounded-lg border border-red-200 dark:border-red-900 px-4 py-2 text-sm font-medium text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-950 transition-colors disabled:opacity-30 disabled:cursor-not-allowed"
          >
            ✗ Incorrect
          </button>
          <button
            onClick={() => mark("correct")}
            disabled={!flipped}
            className="flex-1 rounded-lg border border-green-200 dark:border-green-900 px-4 py-2 text-sm font-medium text-green-700 dark:text-green-400 hover:bg-green-50 dark:hover:bg-green-950 transition-colors disabled:opacity-30 disabled:cursor-not-allowed"
          >
            ✓ Correct
          </button>
        </div>
      )}
    </div>
  );
}
