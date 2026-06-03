"use client";

import { useState, useRef, useEffect } from "react";
import Link from "next/link";
import { renameGroup, renameSet, deleteGroup, deleteSet } from "@/app/actions";

interface ItemRowProps {
  id: string;
  name: string;
  href: string;
  type: "group" | "set";
}

export function ItemRow({ id, name, href, type }: ItemRowProps) {
  const [hovered, setHovered] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const [renaming, setRenaming] = useState(false);
  const [inputValue, setInputValue] = useState(name);
  const cancelRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!renaming) setInputValue(name);
  }, [name, renaming]);

  useEffect(() => {
    if (!menuOpen) return;
    function handleClick(e: MouseEvent) {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [menuOpen]);

  async function handleRename() {
    const trimmed = inputValue.trim();
    setRenaming(false);
    if (!trimmed || trimmed === name) {
      setInputValue(name);
      return;
    }
    if (type === "group") await renameGroup(id, trimmed);
    else await renameSet(id, trimmed);
  }

  function handleBlur(e: React.FocusEvent<HTMLInputElement>) {
    // If focus moves to the Cancel button, let its onClick handle it
    if (e.relatedTarget === cancelRef.current) return;
    handleRename();
  }

  async function handleDelete() {
    const message =
      type === "group"
        ? "This will delete the group and everything inside it (all subgroups, sets, and cards).\n\nAre you sure?"
        : "This will delete the set and all its cards.\n\nAre you sure?";
    if (!window.confirm(message)) return;
    if (type === "group") await deleteGroup(id);
    else await deleteSet(id);
  }

  const rowClass =
    "flex items-center rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 transition-colors";

  if (renaming) {
    return (
      <div className={rowClass}>
        <input
          autoFocus
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") handleRename();
            if (e.key === "Escape") {
              setRenaming(false);
              setInputValue(name);
            }
          }}
          onBlur={handleBlur}
          className="flex-1 px-5 py-4 bg-transparent text-sm font-medium focus:outline-none"
        />
        <div className="flex gap-3 px-5 py-4 shrink-0">
          <button
            onMouseDown={(e) => e.preventDefault()} // prevent blur before onClick
            onClick={handleRename}
            className="text-xs text-zinc-500 hover:text-zinc-900 dark:hover:text-zinc-100 transition-colors"
          >
            Save
          </button>
          <button
            ref={cancelRef}
            onClick={() => {
              setRenaming(false);
              setInputValue(name);
            }}
            className="text-xs text-zinc-400 hover:text-zinc-600 dark:hover:text-zinc-300 transition-colors"
          >
            Cancel
          </button>
        </div>
      </div>
    );
  }

  return (
    <div
      className={`${rowClass} hover:border-zinc-400 dark:hover:border-zinc-600`}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      <Link href={href} className="flex-1 px-5 py-4 font-medium text-sm">
        {name}
      </Link>

      <div className="px-5 py-4 shrink-0 relative" ref={menuRef}>
        {hovered || menuOpen ? (
          <>
            <button
              onClick={(e) => {
                e.preventDefault();
                setMenuOpen((o) => !o);
              }}
              className="text-xs text-zinc-400 hover:text-zinc-900 dark:hover:text-zinc-100 hover:bg-zinc-100 dark:hover:bg-zinc-800 px-1 rounded transition-colors"
            >
              •••
            </button>
            {menuOpen && (
              <div className="absolute right-0 top-full mt-1 w-32 rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 shadow-md z-20 overflow-hidden">
                <button
                  onClick={() => {
                    setMenuOpen(false);
                    setRenaming(true);
                  }}
                  className="w-full text-left px-4 py-2 text-sm hover:bg-zinc-50 dark:hover:bg-zinc-800 transition-colors"
                >
                  Rename
                </button>
                <button
                  onClick={() => {
                    setMenuOpen(false);
                    handleDelete();
                  }}
                  className="w-full text-left px-4 py-2 text-sm text-red-600 dark:text-red-400 hover:bg-zinc-50 dark:hover:bg-zinc-800 transition-colors"
                >
                  Delete
                </button>
              </div>
            )}
          </>
        ) : (
          <span className="text-zinc-400 text-sm">→</span>
        )}
      </div>
    </div>
  );
}
