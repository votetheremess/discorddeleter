"""SQLite checkpoint for discord-deleter.

Two tables:
  channels  - one row per guild or DM; tracks scan status and count deleted
  messages  - one row per delete attempt; audit log keyed by (channel, msg)

All writes are autocommit (isolation_level=None + WAL) so a crash leaves the
DB consistent and the next run can resume.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS channels (
    id              TEXT PRIMARY KEY,
    type            TEXT NOT NULL,     -- 'guild' | 'dm' | 'group_dm'
    name            TEXT,
    guild_id        TEXT,
    guild_name      TEXT,
    last_message_id TEXT,              -- snowflake of last known message (DMs only); NULL for guilds
    deleted_count   INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending|in_progress|done|error|excluded
    error_reason    TEXT,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS messages (
    channel_id   TEXT NOT NULL,
    message_id   TEXT NOT NULL,
    status       TEXT NOT NULL,       -- deleted|missing|error
    error_reason TEXT,
    attempted_at TEXT NOT NULL,
    PRIMARY KEY (channel_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status);

-- Adaptive rate limiter persistence. One row per Discord bucket hash
-- (X-RateLimit-Bucket header), shared across runs so the next launch warm-
-- starts from the previous run's learned (limit, window, safety_factor)
-- instead of paying cold-start latency on every bucket again.
CREATE TABLE IF NOT EXISTS rate_limit_buckets (
    bucket_key      TEXT PRIMARY KEY,
    limit_n         INTEGER,
    window_seconds  REAL,
    safety_factor   REAL NOT NULL DEFAULT 1.0,
    sample_count    INTEGER NOT NULL DEFAULT 0,
    calibrated      INTEGER NOT NULL DEFAULT 0,  -- 1 once we've seen X-RateLimit-* headers
    last_seen_at    TEXT
);

-- Maps caller-supplied bucket keys (e.g. 'search-c-{cid}', 'del-c-{cid}')
-- to Discord's actual X-RateLimit-Bucket hash. Multiple route keys can
-- share one Discord bucket; in that case they pool the same model.
CREATE TABLE IF NOT EXISTS route_buckets (
    route_key    TEXT PRIMARY KEY,
    bucket_key   TEXT NOT NULL,
    last_seen_at TEXT
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Checkpoint:
    def __init__(self, db_path: str | Path):
        self.path = str(db_path)
        self.db = sqlite3.connect(self.path, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        """Add columns introduced after the initial schema. Idempotent."""
        cur = self.db.execute("PRAGMA table_info(channels)")
        existing = {row[1] for row in cur.fetchall()}
        if "last_message_id" not in existing:
            self.db.execute("ALTER TABLE channels ADD COLUMN last_message_id TEXT")
        # rate_limit_buckets / route_buckets are CREATE TABLE IF NOT EXISTS
        # in SCHEMA, so executescript() above already added them on existing
        # DBs. The `calibrated` column was added in a later iteration and
        # needs an explicit ALTER for DBs that pre-date it.
        cur = self.db.execute("PRAGMA table_info(rate_limit_buckets)")
        existing_cols = {row[1] for row in cur.fetchall()}
        if "calibrated" not in existing_cols:
            self.db.execute(
                "ALTER TABLE rate_limit_buckets "
                "ADD COLUMN calibrated INTEGER NOT NULL DEFAULT 0"
            )

    def upsert_channel(
        self,
        channel_id: str,
        ctype: str,
        name: str | None,
        guild_id: str | None = None,
        guild_name: str | None = None,
        last_message_id: str | None = None,
    ) -> None:
        self.db.execute(
            """INSERT INTO channels (id, type, name, guild_id, guild_name, last_message_id, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                   name=COALESCE(excluded.name, channels.name),
                   guild_id=COALESCE(excluded.guild_id, channels.guild_id),
                   guild_name=COALESCE(excluded.guild_name, channels.guild_name),
                   last_message_id=COALESCE(excluded.last_message_id, channels.last_message_id),
                   updated_at=excluded.updated_at""",
            (channel_id, ctype, name, guild_id, guild_name, last_message_id, _now()),
        )

    def set_channel_status(
        self,
        channel_id: str,
        status: str,
        error_reason: str | None = None,
    ) -> None:
        self.db.execute(
            "UPDATE channels SET status=?, error_reason=?, updated_at=? WHERE id=?",
            (status, error_reason, _now(), channel_id),
        )

    def apply_exclusion(self, channel_id: str, excluded: bool, reason: str | None = None) -> None:
        """Mark a channel excluded/unexcluded, respecting terminal states.

        - excluded=True: flip pending/in_progress/excluded rows to 'excluded';
          leave 'done' and 'error' rows alone (already resolved).
        - excluded=False: flip 'excluded' rows back to 'pending' (re-include
          when the user drops a --exclude flag); leave all other states alone.
        """
        if excluded:
            self.db.execute(
                """UPDATE channels
                   SET status='excluded', error_reason=?, updated_at=?
                   WHERE id=? AND status IN ('pending', 'in_progress', 'excluded')""",
                (reason, _now(), channel_id),
            )
        else:
            self.db.execute(
                """UPDATE channels
                   SET status='pending', error_reason=NULL, updated_at=?
                   WHERE id=? AND status='excluded'""",
                (_now(), channel_id),
            )

    def mark_pending_done_if(self, channel_id: str) -> bool:
        """Flip a 'pending' channel to 'done' (e.g. refresh-confirmed empty).

        Guarded so it leaves in_progress / excluded / error / already-done
        rows alone — refresh shouldn't clobber state another worker owns.
        Returns True if a row was actually updated.
        """
        cur = self.db.execute(
            """UPDATE channels SET status='done', updated_at=?
               WHERE id=? AND status='pending'""",
            (_now(), channel_id),
        )
        return cur.rowcount > 0

    def bump_deleted_count(self, channel_id: str, n: int = 1) -> None:
        self.db.execute(
            "UPDATE channels SET deleted_count = deleted_count + ?, updated_at=? WHERE id=?",
            (n, _now(), channel_id),
        )

    def reset_in_progress(self) -> int:
        """Flip all 'in_progress' channels back to 'pending'.

        Used after a TUI cancel (or to repair a crashed run) so the next
        attempt sees a clean slate. Per-channel work is naturally idempotent
        — the search/list loop just finds whatever messages remain.
        """
        cur = self.db.execute(
            "UPDATE channels SET status='pending', updated_at=? WHERE status='in_progress'",
            (_now(),),
        )
        return cur.rowcount

    def iter_pending_channels(self):
        cur = self.db.execute(
            """SELECT id, type, name, guild_id, guild_name
               FROM channels
               WHERE status IN ('pending', 'in_progress')
               ORDER BY type, id"""
        )
        for row in cur.fetchall():
            yield {
                "id": row[0],
                "type": row[1],
                "name": row[2],
                "guild_id": row[3],
                "guild_name": row[4],
            }

    def list_channels(self, ctypes: tuple[str, ...] | None = None):
        """All channels optionally filtered by type. Includes done/error/excluded.

        DMs/group DMs are sorted by `last_message_id` descending (snowflake
        IDs encode timestamp, so this is "most recently active first" with
        NULL/no-activity entries falling to the bottom).

        Guilds and the unfiltered case sort alphabetically by display name.
        """
        select = ("id, type, name, guild_id, guild_name, last_message_id, "
                  "status, deleted_count, error_reason")
        is_dm_only = bool(ctypes) and set(ctypes) <= {"dm", "group_dm"}
        if is_dm_only:
            placeholders = ",".join("?" * len(ctypes))
            cur = self.db.execute(
                f"""SELECT {select} FROM channels
                    WHERE type IN ({placeholders})
                    ORDER BY CAST(COALESCE(last_message_id, '0') AS INTEGER) DESC,
                             COALESCE(name, id)""",
                ctypes,
            )
        elif ctypes:
            placeholders = ",".join("?" * len(ctypes))
            cur = self.db.execute(
                f"""SELECT {select} FROM channels
                    WHERE type IN ({placeholders})
                    ORDER BY COALESCE(guild_name, name, id)""",
                ctypes,
            )
        else:
            cur = self.db.execute(
                f"""SELECT {select} FROM channels
                    ORDER BY type, COALESCE(guild_name, name, id)"""
            )
        for row in cur.fetchall():
            yield {
                "id": row[0], "type": row[1], "name": row[2],
                "guild_id": row[3], "guild_name": row[4],
                "last_message_id": row[5],
                "status": row[6], "deleted_count": row[7],
                "error_reason": row[8],
            }

    def record_message(
        self,
        channel_id: str,
        message_id: str,
        status: str,
        error_reason: str | None = None,
    ) -> None:
        self.db.execute(
            """INSERT OR REPLACE INTO messages
               (channel_id, message_id, status, error_reason, attempted_at)
               VALUES (?, ?, ?, ?, ?)""",
            (channel_id, message_id, status, error_reason, _now()),
        )

    def message_errored_before(self, channel_id: str, message_id: str) -> bool:
        cur = self.db.execute(
            "SELECT 1 FROM messages WHERE channel_id=? AND message_id=? AND status='error'",
            (channel_id, message_id),
        )
        return cur.fetchone() is not None

    # --- adaptive rate limiter persistence ----------------------------

    def load_bucket_models(self) -> dict[str, dict]:
        """Return all stored Discord-bucket models keyed by bucket hash."""
        cur = self.db.execute(
            "SELECT bucket_key, limit_n, window_seconds, safety_factor, "
            "sample_count, calibrated FROM rate_limit_buckets"
        )
        out: dict[str, dict] = {}
        for row in cur.fetchall():
            out[row[0]] = {
                "limit_n": row[1],
                "window_seconds": row[2],
                "safety_factor": row[3],
                "sample_count": row[4],
                "calibrated": bool(row[5]),
            }
        return out

    def save_bucket_model(
        self,
        bucket_key: str,
        limit_n: int | None,
        window_seconds: float | None,
        safety_factor: float,
        sample_count: int,
        calibrated: bool = False,
    ) -> None:
        self.db.execute(
            """INSERT INTO rate_limit_buckets
                   (bucket_key, limit_n, window_seconds, safety_factor,
                    sample_count, calibrated, last_seen_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(bucket_key) DO UPDATE SET
                   limit_n=COALESCE(excluded.limit_n, rate_limit_buckets.limit_n),
                   window_seconds=COALESCE(excluded.window_seconds, rate_limit_buckets.window_seconds),
                   safety_factor=excluded.safety_factor,
                   sample_count=excluded.sample_count,
                   calibrated=excluded.calibrated,
                   last_seen_at=excluded.last_seen_at""",
            (bucket_key, limit_n, window_seconds, safety_factor, sample_count,
             1 if calibrated else 0, _now()),
        )

    def reset_rate_limit_state(self) -> tuple[int, int]:
        """Wipe persisted rate-limit learning. Returns (n_buckets, n_routes)
        deleted. Useful when iterating on the limiter logic itself.
        """
        cur = self.db.execute("DELETE FROM rate_limit_buckets")
        n_buckets = cur.rowcount
        cur = self.db.execute("DELETE FROM route_buckets")
        n_routes = cur.rowcount
        return n_buckets, n_routes

    def delete_bucket_model(self, bucket_key: str) -> None:
        """Drop a stale bucket-model row. Used after a Discord bucket-hash
        rotation, so the old hash's row doesn't accumulate forever.
        """
        self.db.execute(
            "DELETE FROM rate_limit_buckets WHERE bucket_key=?", (bucket_key,)
        )

    def load_route_buckets(self) -> dict[str, str]:
        """Return route_key -> Discord bucket_key map."""
        cur = self.db.execute("SELECT route_key, bucket_key FROM route_buckets")
        return {row[0]: row[1] for row in cur.fetchall()}

    def save_route_bucket(self, route_key: str, bucket_key: str) -> None:
        self.db.execute(
            """INSERT INTO route_buckets (route_key, bucket_key, last_seen_at)
               VALUES (?, ?, ?)
               ON CONFLICT(route_key) DO UPDATE SET
                   bucket_key=excluded.bucket_key,
                   last_seen_at=excluded.last_seen_at""",
            (route_key, bucket_key, _now()),
        )

    def summary(self) -> dict:
        cur = self.db.execute("SELECT status, COUNT(*) FROM messages GROUP BY status")
        msg_counts = {s: c for s, c in cur.fetchall()}
        cur = self.db.execute("SELECT status, COUNT(*) FROM channels GROUP BY status")
        ch_counts = {s: c for s, c in cur.fetchall()}
        cur = self.db.execute("SELECT COALESCE(SUM(deleted_count), 0) FROM channels")
        total_deleted = cur.fetchone()[0]
        return {
            "channels_by_status": ch_counts,
            "messages_by_status": msg_counts,
            "total_deleted": total_deleted,
        }

    def close(self) -> None:
        self.db.close()


if __name__ == "__main__":
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        cp = Checkpoint(f.name)
        cp.upsert_channel("111", "dm", "alice")
        cp.upsert_channel("222", "guild", None, guild_id="222", guild_name="my-guild")
        cp.set_channel_status("111", "in_progress")
        cp.record_message("111", "m1", "deleted")
        cp.record_message("111", "m2", "missing")
        cp.record_message("222", "m3", "error", "403 Forbidden")
        cp.bump_deleted_count("111", 1)
        print("pending channels:")
        for ch in cp.iter_pending_channels():
            print(" ", ch)
        print("summary:", cp.summary())

        # rate-limit persistence smoke test
        cp.save_bucket_model("abc123", limit_n=5, window_seconds=5.0,
                             safety_factor=1.0, sample_count=42, calibrated=True)
        cp.save_bucket_model("abc123", limit_n=5, window_seconds=5.0,
                             safety_factor=1.5, sample_count=43, calibrated=True)
        cp.save_bucket_model("uncal", limit_n=1, window_seconds=1.0,
                             safety_factor=1.0, sample_count=10, calibrated=False)
        cp.save_route_bucket("search-c-111", "abc123")
        cp.save_route_bucket("search-c-222", "abc123")
        models = cp.load_bucket_models()
        routes = cp.load_route_buckets()
        assert "abc123" in models, models
        assert models["abc123"]["safety_factor"] == 1.5
        assert models["abc123"]["sample_count"] == 43
        assert models["abc123"]["calibrated"] is True
        assert models["uncal"]["calibrated"] is False
        assert routes == {"search-c-111": "abc123", "search-c-222": "abc123"}

        nb, nr = cp.reset_rate_limit_state()
        assert nb == 2 and nr == 2, (nb, nr)
        assert cp.load_bucket_models() == {}
        assert cp.load_route_buckets() == {}
        print("rate-limit persistence: ok")

        cp.close()
