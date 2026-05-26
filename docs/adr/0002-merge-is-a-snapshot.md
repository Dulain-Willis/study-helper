# 0002 — Merge is a Snapshot

## Status
Accepted

## Context
When the user combines two or more Sets into a new Set, the resulting Set could work in two ways: as a live reference (dynamically pulling Cards from source Sets at query time) or as a snapshot (duplicating Cards into the new Set at creation time).

## Decision
Merge is a snapshot. When Sets are merged, their Cards are duplicated into the new Set at creation time. The merged Set has no ongoing relationship with its source Sets.

## Consequences
- Adding a Card to a source Set after a merge does not affect the merged Set. The user must re-merge to pick up new Cards.
- The merged Set can be edited, renamed, or deleted independently of its sources.
- A live reference model was rejected because it blurs the identity of a Set — a Set would become a query over other Sets rather than a first-class collection. It also requires tracking provenance and handling cases where source Sets are deleted.
