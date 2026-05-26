# 0001 — Card Ownership Over Join Table

## Status
Accepted

## Context
Cards need to appear in Sets. The standard approach for content that can belong to multiple collections is a many-to-many join table (Set-Card). The alternative is direct ownership: each Card has a `set_id` FK and belongs to exactly one Set.

## Decision
Cards are owned by their Set via a `set_id` FK. A Card belongs to exactly one Set.

## Consequences
- Merging two Sets duplicates the Card rows into the new Set. At personal-tool scale this is irrelevant.
- The merged Set is truly independent after creation — editing a Card in a source Set has no effect on the merged Set.
- Schema is simpler: no join table, no polymorphic associations.
- A join table was rejected because it weakens the snapshot semantic: shared Card rows mean edits to a source Card propagate silently to all Sets that reference it, breaking the expectation that a merged Set is frozen at creation time.
