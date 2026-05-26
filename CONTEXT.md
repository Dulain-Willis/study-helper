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
Browsing through the Cards in a Set (or Ad-hoc Study combination) by flipping from front to back. No scoring is tracked. Cards are shown one at a time.

## MCP Tool
A deterministic function exposed by the local MCP server. The LLM calls these tools to read and write app data. The LLM is responsible for parsing documents and deciding card content — the tools only perform CRUD.
