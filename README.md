# Study Helper

A local single-user flashcard app running at `http://localhost:3000`.

## Stack

- **Frontend + API** — Next.js 16 (App Router, TypeScript, Tailwind)
- **ORM** — Prisma 7
- **Database** — Postgres 16 (Docker)
- **MCP Server** — FastMCP (Python)

## Start the app

**1. Start Postgres + Adminer**

```bash
docker compose up -d
```

**2. Start Next.js**

```bash
cd web && npm run dev
```

App is at `http://localhost:3000`.

## Stop the app

```bash
# Stop Next.js: Ctrl+C in the terminal running npm run dev

# Stop Docker containers
docker compose down
```

## Adminer (DB browser)

`http://localhost:8080` — use these credentials:

| Field    | Value        |
|----------|--------------|
| System   | PostgreSQL   |
| Server   | db           |
| Username | studyhelper  |
| Password | studyhelper  |
| Database | studyhelper  |

## MCP server

Registered at Claude CLI user scope. Starts automatically when Claude CLI is used in this directory.

Tools: `list_groups`, `list_sets`, `get_set`, `search`, `create_group`, `create_set`, `add_card`, `merge_sets`
