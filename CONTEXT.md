# Study Helper — Domain Glossary

## Card
A flashcard with a front (question) and a back (answer). Owned by exactly one Set. Cards are never shared between Sets — a Card belongs to its Set and only its Set.

## Set
A named collection of Cards. Lives inside exactly one Group. A Set is a leaf node in the Group hierarchy — it cannot contain other Groups.

## Group
A named container that can hold other Groups, Sets, or a mix of both. Groups are arbitrarily deep. A Group with no parent is a root Group.

## Merge
A snapshot operation that creates a new Set by duplicating the Cards from two or more selected Sets. The resulting Set is fully independent — changes to source Sets after the merge do not affect it.

## Ad-hoc Study
A temporary combination of Sets chosen at study time. Nothing is persisted. The user selects Sets, studies the combined cards, and the selection is discarded when the session ends.

## Study Session
Browsing through the Cards in a Set (or Ad-hoc Study combination) one at a time, flipping from front to back. Has two modes: Browse and Practice. The mode is chosen at the point of entering the session.

## Browse
A Study Session mode where no scoring is tracked. The user flips cards freely and navigates with Prev/Next. This is the existing behaviour.

## Practice
A Study Session mode where the user self-assesses each Card after flipping it. The Prev/Next buttons are replaced by ✗ (incorrect) and ✓ (correct). Both buttons are disabled until the Card is flipped. Marking auto-advances to the next Card. There is no undo — a mark is final for that Round.

## Round
One full pass through a deck of Cards in a Practice session. The first Round uses all Cards in the Set. Subsequent Rounds are Retry Rounds. Cards are shown in the same order each Round.

## Retry Round
A Round that contains only the Cards marked ✗ in the previous Round, in their original deck order. When a Retry Round ends with zero Cards marked ✗, a completion screen is shown and the session ends.

## MCP Tool
A deterministic function exposed by the local MCP server. The LLM calls these tools to read and write app data. The LLM is responsible for parsing documents and deciding card content — the tools only perform CRUD.
