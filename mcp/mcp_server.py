import os
import platform
import sqlite3
from contextlib import closing
from pathlib import Path

from fastmcp import FastMCP

mcp = FastMCP("study-helper")

# Must match the Electron app's package.json "name" — that's what
# app.getPath('userData') derives the folder from. See #12.
APP_NAME = "electron"


def _user_data_dir() -> Path:
    home = Path.home()
    system = platform.system()
    if system == "Darwin":
        return home / "Library" / "Application Support" / APP_NAME
    if system == "Windows":
        return Path(os.environ.get("APPDATA", home)) / APP_NAME
    return home / ".config" / APP_NAME


DB_PATH = os.environ.get("STUDY_HELPER_DB_PATH", str(_user_data_dir() / "study-helper.db"))


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Electron's main process sets WAL mode once at DB creation (db.ts).
    # busy_timeout here handles lock contention against a running app.
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ---------------------------------------------------------------------------
# Read tools
# ---------------------------------------------------------------------------

@mcp.tool
def list_groups(parent_group_id: int | None = None) -> list[dict]:
    """List groups. Pass parent_group_id to list children of a group, or omit to list root groups."""
    with closing(get_conn()) as conn:
        if parent_group_id is None:
            rows = conn.execute(
                "SELECT id, name, parent_id AS parentId FROM groups WHERE parent_id IS NULL ORDER BY name"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, name, parent_id AS parentId FROM groups WHERE parent_id = ? ORDER BY name",
                (parent_group_id,),
            ).fetchall()
        return [dict(r) for r in rows]


@mcp.tool
def list_sets(group_id: int | None = None) -> list[dict]:
    """List sets. Pass group_id to list sets in a specific group, or omit to list all sets."""
    with closing(get_conn()) as conn:
        if group_id is None:
            rows = conn.execute("SELECT id, name, group_id AS groupId FROM sets ORDER BY name").fetchall()
        else:
            rows = conn.execute(
                "SELECT id, name, group_id AS groupId FROM sets WHERE group_id = ? ORDER BY name",
                (group_id,),
            ).fetchall()
        return [dict(r) for r in rows]


@mcp.tool
def get_set(set_id: int) -> dict:
    """Get a set and all its cards."""
    with closing(get_conn()) as conn:
        set_row = conn.execute(
            "SELECT id, name, group_id AS groupId FROM sets WHERE id = ?", (set_id,)
        ).fetchone()
        if not set_row:
            return {"error": f"Set {set_id} not found"}
        cards = conn.execute(
            "SELECT id, front, back FROM cards WHERE set_id = ? ORDER BY id", (set_id,)
        ).fetchall()
        result = dict(set_row)
        result["cards"] = [dict(c) for c in cards]
        return result


@mcp.tool
def search(query: str) -> dict:
    """Search for groups and sets by name."""
    with closing(get_conn()) as conn:
        like = f"%{query}%"
        groups = conn.execute(
            "SELECT id, name, parent_id AS parentId FROM groups WHERE name LIKE ? ORDER BY name", (like,)
        ).fetchall()
        sets = conn.execute(
            "SELECT id, name, group_id AS groupId FROM sets WHERE name LIKE ? ORDER BY name", (like,)
        ).fetchall()
        return {"groups": [dict(r) for r in groups], "sets": [dict(r) for r in sets]}


# ---------------------------------------------------------------------------
# Write tools
# ---------------------------------------------------------------------------

@mcp.tool
def create_group(name: str, parent_group_id: int | None = None) -> dict:
    """Create a new group. Optionally nest it under a parent group."""
    with closing(get_conn()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO groups (name, parent_id) VALUES (?, ?)", (name, parent_group_id)
        )
        return {"id": cur.lastrowid, "name": name, "parentId": parent_group_id}


@mcp.tool
def create_set(name: str, group_id: int) -> dict:
    """Create a new flashcard set inside a group."""
    with closing(get_conn()) as conn, conn:
        cur = conn.execute("INSERT INTO sets (name, group_id) VALUES (?, ?)", (name, group_id))
        return {"id": cur.lastrowid, "name": name, "groupId": group_id}


@mcp.tool
def add_card(set_id: int, front: str, back: str) -> dict:
    """Add a flashcard to a set. front = question, back = answer."""
    with closing(get_conn()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO cards (front, back, set_id) VALUES (?, ?, ?)", (front, back, set_id)
        )
        return {"id": cur.lastrowid, "front": front, "back": back, "setId": set_id}


@mcp.tool
def merge_sets(set_ids: list[int], new_set_name: str, group_id: int) -> dict:
    """Merge multiple sets into a new set by duplicating all their cards (snapshot, ADR 0002). Returns the new set."""
    with closing(get_conn()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO sets (name, group_id) VALUES (?, ?)", (new_set_name, group_id)
        )
        new_set_id = cur.lastrowid

        for source_id in set_ids:
            conn.execute(
                """
                INSERT INTO cards (front, back, set_id)
                SELECT front, back, ? FROM cards WHERE set_id = ?
                """,
                (new_set_id, source_id),
            )

        card_count = conn.execute(
            "SELECT COUNT(*) AS count FROM cards WHERE set_id = ?", (new_set_id,)
        ).fetchone()["count"]
        return {"id": new_set_id, "name": new_set_name, "groupId": group_id, "card_count": card_count}


@mcp.tool
def rename_group(group_id: int, name: str) -> dict:
    """Rename a group."""
    with closing(get_conn()) as conn, conn:
        row = conn.execute(
            "SELECT parent_id AS parentId FROM groups WHERE id = ?", (group_id,)
        ).fetchone()
        if not row:
            return {"error": f"Group {group_id} not found"}
        conn.execute("UPDATE groups SET name = ? WHERE id = ?", (name, group_id))
        return {"id": group_id, "name": name, "parentId": row["parentId"]}


@mcp.tool
def rename_set(set_id: int, name: str) -> dict:
    """Rename a set."""
    with closing(get_conn()) as conn, conn:
        row = conn.execute("SELECT group_id AS groupId FROM sets WHERE id = ?", (set_id,)).fetchone()
        if not row:
            return {"error": f"Set {set_id} not found"}
        conn.execute("UPDATE sets SET name = ? WHERE id = ?", (name, set_id))
        return {"id": set_id, "name": name, "groupId": row["groupId"]}


@mcp.tool
def delete_group(group_id: int) -> dict:
    """Delete a group and everything inside it (all subgroups, sets, and cards) recursively."""
    with closing(get_conn()) as conn, conn:
        row = conn.execute("SELECT id FROM groups WHERE id = ?", (group_id,)).fetchone()
        if not row:
            return {"error": f"Group {group_id} not found"}
        # ON DELETE CASCADE on groups.parent_id, sets.group_id, and cards.set_id
        # handles the recursive cleanup of subgroups/sets/cards.
        conn.execute("DELETE FROM groups WHERE id = ?", (group_id,))
        return {"deleted": group_id}


@mcp.tool
def delete_set(set_id: int) -> dict:
    """Delete a set and all its cards."""
    with closing(get_conn()) as conn, conn:
        row = conn.execute("SELECT id FROM sets WHERE id = ?", (set_id,)).fetchone()
        if not row:
            return {"error": f"Set {set_id} not found"}
        # ON DELETE CASCADE on cards.set_id handles the cleanup of the Set's Cards.
        conn.execute("DELETE FROM sets WHERE id = ?", (set_id,))
        return {"deleted": set_id}


if __name__ == "__main__":
    mcp.run()
