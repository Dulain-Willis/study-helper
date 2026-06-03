# Study Helper — Build Plan

## What it is
A local flashcard app for personal use. Browser-based at localhost:3000. Data lives in a local Postgres database.

## Stack
- **Frontend + API:** Next.js (API routes handle backend, no separate server)
- **ORM:** Prisma (defines schema, generates migrations, type-safe queries)
- **Database:** Postgres (local)
- **MCP Server:** FastMCP (Python, separate process, connects to same Postgres DB)

## Running it
- `npm run dev` — app at localhost:3000
- `python mcp_server.py` — MCP server Claude Desktop connects to

## Core Entities

### Group
- Arbitrary depth — Groups can contain other Groups, Sets, or both
- Has an optional `parent_id` pointing to another Group (null = root)

### Set
- A named collection of Cards
- Lives inside exactly one Group
- Can be created manually or via the MCP server

### Card
- Has a front (question) and a back (answer)
- Owned by exactly one Set via `set_id` FK

## Key Behaviors

### Merge
Combines two or more Sets into a new Set by duplicating their Cards at creation time. The result is fully independent — edits to source Sets after the merge do not affect the merged Set.

### Ad-hoc Study
Pick any combination of Sets at study time, study them together, nothing is saved. Selection is discarded when done.

### Study Session
Flip through Cards one at a time — front then back, move on. No scoring tracked yet (planned for later without rearchitecting).

### Rename
Groups and Sets can be renamed in-place. In the web UI, clicking Rename turns the name into an inline text input; confirmed with Enter or a checkmark button. Also available via MCP tools.

### Delete
Groups and Sets can be deleted. Deleting a Group cascades — all subgroups, their subgroups, all Sets, and all Cards within are destroyed. Deleting a Set cascades — all its Cards are destroyed. The web UI shows a confirmation dialog summarising what will be destroyed before proceeding. Also available via MCP tools.

### Three-dot Menu
Each Group and Set row in the web UI shows a `…` button on hover (replaces the `→` arrow). Opening it reveals Rename and Delete actions.

## MCP Tool Surface
The FastMCP server exposes deterministic CRUD tools. Claude handles parsing any document format and deciding what goes on each card — the tools only perform data operations.

**Read**
- `list_groups(parent_group_id?)` — browse the hierarchy
- `list_sets(group_id?)` — see sets in a group
- `get_set(set_id)` — see cards inside a set
- `search(query)` — find groups/sets by name

**Write**
- `create_group(name, parent_group_id?)`
- `create_set(name, group_id)`
- `add_card(set_id, question, answer)`
- `merge_sets(set_ids, new_set_name, group_id)`
- `rename_group(group_id, name)`
- `rename_set(set_id, name)`
- `delete_group(group_id)` — cascades: deletes all subgroups, sets, and cards recursively
- `delete_set(set_id)` — cascades: deletes all cards in the set

## ADRs
- [0001 — Card Ownership Over Join Table](docs/adr/0001-card-ownership-over-join-table.md)
- [0002 — Merge is a Snapshot](docs/adr/0002-merge-is-a-snapshot.md)
