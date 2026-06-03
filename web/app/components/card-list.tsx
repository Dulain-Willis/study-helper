"use client";

import { useState, useRef } from "react";
import { updateCard } from "@/app/actions";

interface Card {
  id: string;
  front: string;
  back: string;
}

interface CardListProps {
  cards: Card[];
}

function CardRow({ card, index }: { card: Card; index: number }) {
  const [front, setFront] = useState(card.front);
  const [back, setBack] = useState(card.back);
  const [editingField, setEditingField] = useState<"front" | "back" | null>(null);
  const [draftValue, setDraftValue] = useState("");

  function startEdit(field: "front" | "back") {
    setEditingField(field);
    setDraftValue(field === "front" ? front : back);
  }

  async function commit() {
    if (!editingField) return;
    const trimmed = draftValue.trim();
    const original = editingField === "front" ? front : back;
    setEditingField(null);
    if (!trimmed || trimmed === original) return;
    if (editingField === "front") {
      setFront(trimmed);
      await updateCard(card.id, trimmed, back);
    } else {
      setBack(trimmed);
      await updateCard(card.id, front, trimmed);
    }
  }

  function cancel() {
    setEditingField(null);
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      commit();
    }
    if (e.key === "Escape") {
      cancel();
    }
  }

  const editingClass =
    "w-full bg-transparent text-sm focus:outline-none resize-none";

  return (
    <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 overflow-hidden">
      {/* Front */}
      <div
        className="px-5 py-3 border-b border-zinc-100 dark:border-zinc-800 cursor-text hover:bg-zinc-50 dark:hover:bg-zinc-800/50 transition-colors"
        onClick={() => editingField !== "front" && startEdit("front")}
      >
        <span className="text-xs text-zinc-400 mr-2">{index + 1}</span>
        {editingField === "front" ? (
          <textarea
            autoFocus
            className={editingClass}
            value={draftValue}
            rows={Math.max(1, draftValue.split("\n").length)}
            onChange={(e) => setDraftValue(e.target.value)}
            onBlur={commit}
            onKeyDown={handleKeyDown}
          />
        ) : (
          <span className="text-sm">{front}</span>
        )}
      </div>

      {/* Back */}
      <div
        className="px-5 py-3 bg-zinc-50 dark:bg-zinc-900/50 cursor-text hover:bg-zinc-100 dark:hover:bg-zinc-800/50 transition-colors"
        onClick={() => editingField !== "back" && startEdit("back")}
      >
        {editingField === "back" ? (
          <textarea
            autoFocus
            className={`${editingClass} text-zinc-500 dark:text-zinc-400`}
            value={draftValue}
            rows={Math.max(1, draftValue.split("\n").length)}
            onChange={(e) => setDraftValue(e.target.value)}
            onBlur={commit}
            onKeyDown={handleKeyDown}
          />
        ) : (
          <span className="text-sm text-zinc-500 dark:text-zinc-400">{back}</span>
        )}
      </div>
    </div>
  );
}

export function CardList({ cards }: CardListProps) {
  return (
    <div className="space-y-3">
      {cards.map((card, i) => (
        <CardRow key={card.id} card={card} index={i} />
      ))}
    </div>
  );
}
