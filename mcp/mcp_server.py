import os
import psycopg2
import psycopg2.extras
from fastmcp import FastMCP

mcp = FastMCP("study-helper")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://studyhelper:studyhelper@localhost:5432/studyhelper"
)


def get_conn():
    return psycopg2.connect(DATABASE_URL)


# ---------------------------------------------------------------------------
# Read tools
# ---------------------------------------------------------------------------

@mcp.tool
def list_groups(parent_group_id: str | None = None) -> list[dict]:
    """List groups. Pass parent_group_id to list children of a group, or omit to list root groups."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if parent_group_id is None:
                cur.execute('SELECT id, name, "parentId", "createdAt" FROM "Group" WHERE "parentId" IS NULL ORDER BY name')
            else:
                cur.execute('SELECT id, name, "parentId", "createdAt" FROM "Group" WHERE "parentId" = %s ORDER BY name', (parent_group_id,))
            return [dict(r) for r in cur.fetchall()]


@mcp.tool
def list_sets(group_id: str | None = None) -> list[dict]:
    """List sets. Pass group_id to list sets in a specific group, or omit to list all sets."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if group_id is None:
                cur.execute('SELECT id, name, "groupId", "createdAt" FROM "Set" ORDER BY name')
            else:
                cur.execute('SELECT id, name, "groupId", "createdAt" FROM "Set" WHERE "groupId" = %s ORDER BY name', (group_id,))
            return [dict(r) for r in cur.fetchall()]


@mcp.tool
def get_set(set_id: str) -> dict:
    """Get a set and all its cards."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute('SELECT id, name, "groupId", "createdAt" FROM "Set" WHERE id = %s', (set_id,))
            set_row = cur.fetchone()
            if not set_row:
                return {"error": f"Set {set_id} not found"}
            cur.execute('SELECT id, front, back, "createdAt" FROM "Card" WHERE "setId" = %s ORDER BY "createdAt"', (set_id,))
            cards = [dict(r) for r in cur.fetchall()]
            result = dict(set_row)
            result["cards"] = cards
            return result


@mcp.tool
def search(query: str) -> dict:
    """Search for groups and sets by name."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            like = f"%{query}%"
            cur.execute('SELECT id, name, "parentId" FROM "Group" WHERE name ILIKE %s ORDER BY name', (like,))
            groups = [dict(r) for r in cur.fetchall()]
            cur.execute('SELECT id, name, "groupId" FROM "Set" WHERE name ILIKE %s ORDER BY name', (like,))
            sets = [dict(r) for r in cur.fetchall()]
            return {"groups": groups, "sets": sets}


# ---------------------------------------------------------------------------
# Write tools
# ---------------------------------------------------------------------------

@mcp.tool
def create_group(name: str, parent_group_id: str | None = None) -> dict:
    """Create a new group. Optionally nest it under a parent group."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                'INSERT INTO "Group" (id, name, "parentId", "createdAt") VALUES (gen_random_uuid(), %s, %s, NOW()) RETURNING id, name, "parentId"',
                (name, parent_group_id)
            )
            conn.commit()
            return dict(cur.fetchone())


@mcp.tool
def create_set(name: str, group_id: str) -> dict:
    """Create a new flashcard set inside a group."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                'INSERT INTO "Set" (id, name, "groupId", "createdAt") VALUES (gen_random_uuid(), %s, %s, NOW()) RETURNING id, name, "groupId"',
                (name, group_id)
            )
            conn.commit()
            return dict(cur.fetchone())


@mcp.tool
def add_card(set_id: str, front: str, back: str) -> dict:
    """Add a flashcard to a set. front = question, back = answer."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                'INSERT INTO "Card" (id, front, back, "setId", "createdAt") VALUES (gen_random_uuid(), %s, %s, %s, NOW()) RETURNING id, front, back, "setId"',
                (front, back, set_id)
            )
            conn.commit()
            return dict(cur.fetchone())


@mcp.tool
def merge_sets(set_ids: list[str], new_set_name: str, group_id: str) -> dict:
    """Merge multiple sets into a new set by duplicating all their cards (snapshot). Returns the new set."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                'INSERT INTO "Set" (id, name, "groupId", "createdAt") VALUES (gen_random_uuid(), %s, %s, NOW()) RETURNING id, name, "groupId"',
                (new_set_name, group_id)
            )
            new_set = dict(cur.fetchone())
            new_set_id = new_set["id"]

            for source_id in set_ids:
                cur.execute(
                    '''
                    INSERT INTO "Card" (id, front, back, "setId", "createdAt")
                    SELECT gen_random_uuid(), front, back, %s, NOW()
                    FROM "Card" WHERE "setId" = %s
                    ''',
                    (new_set_id, source_id)
                )

            conn.commit()
            cur.execute('SELECT COUNT(*) as count FROM "Card" WHERE "setId" = %s', (new_set_id,))
            new_set["card_count"] = cur.fetchone()["count"]
            return new_set


@mcp.tool
def rename_group(group_id: str, name: str) -> dict:
    """Rename a group."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                'UPDATE "Group" SET name = %s WHERE id = %s RETURNING id, name, "parentId"',
                (name, group_id)
            )
            row = cur.fetchone()
            if not row:
                return {"error": f"Group {group_id} not found"}
            conn.commit()
            return dict(row)


@mcp.tool
def rename_set(set_id: str, name: str) -> dict:
    """Rename a set."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                'UPDATE "Set" SET name = %s WHERE id = %s RETURNING id, name, "groupId"',
                (name, set_id)
            )
            row = cur.fetchone()
            if not row:
                return {"error": f"Set {set_id} not found"}
            conn.commit()
            return dict(row)


def _collect_group_ids(cur, root_id: str) -> list[str]:
    """Recursively collect all group IDs in a subtree."""
    ids = [root_id]
    queue = [root_id]
    while queue:
        parent_id = queue.pop(0)
        cur.execute('SELECT id FROM "Group" WHERE "parentId" = %s', (parent_id,))
        for row in cur.fetchall():
            ids.append(row["id"])
            queue.append(row["id"])
    return ids


@mcp.tool
def delete_group(group_id: str) -> dict:
    """Delete a group and everything inside it (all subgroups, sets, and cards) recursively."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute('SELECT id FROM "Group" WHERE id = %s', (group_id,))
            if not cur.fetchone():
                return {"error": f"Group {group_id} not found"}

            all_group_ids = _collect_group_ids(cur, group_id)
            placeholders = ','.join(['%s'] * len(all_group_ids))

            cur.execute(f'SELECT id FROM "Set" WHERE "groupId" IN ({placeholders})', all_group_ids)
            set_ids = [row["id"] for row in cur.fetchall()]

            if set_ids:
                set_placeholders = ','.join(['%s'] * len(set_ids))
                cur.execute(f'DELETE FROM "Card" WHERE "setId" IN ({set_placeholders})', set_ids)

            cur.execute(f'DELETE FROM "Set" WHERE "groupId" IN ({placeholders})', all_group_ids)
            cur.execute(f'UPDATE "Group" SET "parentId" = NULL WHERE id IN ({placeholders})', all_group_ids)
            cur.execute(f'DELETE FROM "Group" WHERE id IN ({placeholders})', all_group_ids)

            conn.commit()
            return {"deleted": group_id}


@mcp.tool
def delete_set(set_id: str) -> dict:
    """Delete a set and all its cards."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute('SELECT id FROM "Set" WHERE id = %s', (set_id,))
            if not cur.fetchone():
                return {"error": f"Set {set_id} not found"}

            cur.execute('DELETE FROM "Card" WHERE "setId" = %s', (set_id,))
            cur.execute('DELETE FROM "Set" WHERE id = %s', (set_id,))
            conn.commit()
            return {"deleted": set_id}


if __name__ == "__main__":
    mcp.run()
