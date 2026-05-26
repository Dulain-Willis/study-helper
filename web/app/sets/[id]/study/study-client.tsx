"use client";

import { useState } from "react";
import Link from "next/link";

type Card = { id: string; front: string; back: string };

export default function StudyClient({
  setId,
  setName,
  cards,
}: {
  setId: string;
  setName: string;
  cards: Card[];
}) {
  const [index, setIndex] = useState(0);
  const [flipped, setFlipped] = useState(false);
  const [done, setDone] = useState(false);

  const card = cards[index];
  const total = cards.length;

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

  if (done) {
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
    </div>
  );
}
