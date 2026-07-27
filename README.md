# Study Helper

A local, single-user flashcard Electron app. No auth, no server — data lives in
a SQLite file in the OS's app-data folder.

## Stack

- **App** — Electron + React + TypeScript (`electron/`), data via `better-sqlite3` directly in the main process, no local web server
- **MCP Server** — FastMCP (Python, `mcp/`), talks to the same SQLite file directly

## Run it

```bash
cd electron
npm install
npm run dev
```

## Build a packaged app

```bash
cd electron
npm run build:mac   # or build:win / build:linux
```

Produces an installable `.dmg`/`.exe`/etc. in `electron/dist/`.

## MCP server

Registered at Claude CLI user scope. Starts automatically when Claude CLI is used in this directory.

Tools: `list_groups`, `list_sets`, `get_set`, `search`, `create_group`, `create_set`, `add_card`, `merge_sets`, `rename_group`, `rename_set`, `delete_group`, `delete_set`
