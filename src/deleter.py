"""discord-deleter — delete every message the authenticated user authored.

Runs in two phases:
  1. Discovery: enumerate guilds + DMs, write rows to the channels table.
  2. Deletion: for each channel, loop (search -> delete page -> re-search)
     until the channel is empty; mark it 'done' and move on.

Resume semantics: the channels table is the source of truth. Any channel
whose status is 'pending' or 'in_progress' is re-processed on next run;
'done' channels are skipped.

Usage:
    DISCORD_TOKEN=... python src/deleter.py [--dry-run] [--concurrency N]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).parent))

from checkpoint import Checkpoint  # noqa: E402
from discovery import (  # noqa: E402
    get_active_threads,
    get_dm_channels,
    get_guilds,
    get_self,
    list_channel_messages,
    search_channel,
    search_guild,
)
from ratelimit import RateLimiter  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"


def _merge_ids(cli_list: list[str] | None, env_val: str | None) -> set[str]:
    """Combine CLI --exclude-* flags with comma/whitespace-separated env var."""
    out: set[str] = set(cli_list or [])
    if env_val:
        for tok in env_val.replace(",", " ").split():
            tok = tok.strip()
            if tok and not tok.startswith("#"):
                out.add(tok)
    return out


def load_dotenv(path: Path) -> None:
    """Minimal .env loader — KEY=value per line, # comments, optional quotes.

    Won't overwrite variables already set in the environment (export > file).
    No external deps.

    Encoding-tolerant: Windows tools (Notepad, PowerShell `>` / `Out-File`
    in PS 5.1) often produce UTF-16 LE or UTF-8-with-BOM; default
    `read_text()` on Windows decodes as cp1252 and silently produces
    null-interleaved garbage that crashes `os.environ[k] = v` with
    "embedded null character". Try BOM-aware UTF-8 first, then UTF-16,
    then a last-resort UTF-8 with replacement so we at least make
    progress on a partially-corrupt file.
    """
    if not path.exists():
        return
    text = None
    for encoding in ("utf-8-sig", "utf-16", "utf-8"):
        try:
            text = path.read_text(encoding=encoding)
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
    if text is None:
        text = path.read_text(encoding="utf-8", errors="replace")
    for raw in text.splitlines():
        # Strip null bytes that survived any partial-decode path; without
        # this, `os.environ[k] = v` raises ValueError on Windows.
        line = raw.replace("\x00", "").strip().lstrip("﻿")
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) discord/0.0.327 Chrome/124.0.0.0 "
    "Electron/30.0.9 Safari/537.36"
)

# Base64 of {"os":"Mac OS X","browser":"Discord Client","release_channel":"stable",
# "client_build_number":309876,"client_event_source":null,"client_version":"0.0.327",
# "os_version":"23.4.0","os_arch":"arm64","system_locale":"en-US"}
# Mirrors what the Discord desktop client sends. Helps requests blend in with
# normal client traffic (researcher flagged this as detection-avoidance signal).
X_SUPER_PROPERTIES = (
    "eyJvcyI6Ik1hYyBPUyBYIiwiYnJvd3NlciI6IkRpc2NvcmQgQ2xpZW50IiwicmVsZWFzZV9jaGFubmVs"
    "Ijoic3RhYmxlIiwiY2xpZW50X2J1aWxkX251bWJlciI6MzA5ODc2LCJjbGllbnRfZXZlbnRfc291cmNl"
    "IjpudWxsLCJjbGllbnRfdmVyc2lvbiI6IjAuMC4zMjciLCJvc192ZXJzaW9uIjoiMjMuNC4wIiwib3Nf"
    "YXJjaCI6ImFybTY0Iiwic3lzdGVtX2xvY2FsZSI6ImVuLVVTIn0="
)


def setup_logging(verbose: bool, quiet_console: bool = False) -> Path:
    """Set up file logging to logs/deleter-<stamp>.log. If `quiet_console` is
    true, skip the stderr handler so stdout/stderr don't corrupt the TUI.
    """
    LOG_DIR.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_file = LOG_DIR / f"deleter-{stamp}.log"
    fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    fh = logging.FileHandler(log_file)
    fh.setFormatter(logging.Formatter(fmt))
    fh.setLevel(logging.DEBUG)
    root.addHandler(fh)
    if not quiet_console:
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        ch.setLevel(logging.DEBUG if verbose else logging.INFO)
        root.addHandler(ch)
    return log_file


async def discover_all(
    rl: RateLimiter,
    cp: Checkpoint,
    exclude_guilds: set[str],
    exclude_users: set[str],
) -> str:
    log = logging.getLogger("discover")
    me = await get_self(rl)
    self_id = me["id"]
    log.info("authenticated as %s (%s)", me.get("username") or me.get("global_name"), self_id)

    guilds = await get_guilds(rl)
    excluded_g = 0
    for g in guilds:
        cp.upsert_channel(
            g["id"], "guild", None, guild_id=g["id"], guild_name=g.get("name")
        )
        if g["id"] in exclude_guilds:
            cp.apply_exclusion(g["id"], True, reason="--exclude-guild")
            excluded_g += 1
        else:
            cp.apply_exclusion(g["id"], False)  # re-include if previously excluded
    log.info("guilds: %d (%d excluded)", len(guilds), excluded_g)

    dms = await get_dm_channels(rl)
    excluded_d = 0
    total_dms = 0
    for ch in dms:
        dm_type = ch.get("type")
        recipients = ch.get("recipients") or []
        recipient_ids = {r["id"] for r in recipients if r.get("id")}
        if dm_type == 1:
            ctype = "dm"
            name = (
                recipients[0].get("global_name") or recipients[0].get("username")
                if recipients else None
            )
        elif dm_type == 3:
            ctype = "group_dm"
            name = ch.get("name") or ",".join(
                (r.get("global_name") or r.get("username") or "?")
                for r in recipients
            )
        else:
            continue  # not a DM type we recognize
        cp.upsert_channel(ch["id"], ctype, name, last_message_id=ch.get("last_message_id"))
        total_dms += 1
        if recipient_ids & exclude_users:
            matched = recipient_ids & exclude_users
            cp.apply_exclusion(ch["id"], True, reason=f"--exclude-user {','.join(sorted(matched))}")
            excluded_d += 1
        else:
            cp.apply_exclusion(ch["id"], False)
    log.info("open DMs / group DMs: %d (%d excluded)", total_dms, excluded_d)
    return self_id


async def _delete_one(rl, channel_id, msg_id) -> tuple[int, object]:
    return await rl.request(
        "DELETE",
        f"/channels/{channel_id}/messages/{msg_id}",
        bucket=f"del-c-{channel_id}",
    )


async def _try_unarchive_thread(rl, channel_id: str) -> bool:
    """PATCH a thread to unarchived state so we can delete in it.

    Discord returns 400 / `code: 50083` ("Thread is archived") on DELETE
    in archived threads. Any thread member can unarchive a public thread;
    private threads need MANAGE_THREADS or membership. We try, and either
    way the caller falls back to recording the message as error if the
    retry still fails.

    Time-boxed at 30s. Discord rate-limits PATCH /channels (Scope: shared)
    aggressively — a single 429 can return Retry-After in the *minutes*
    range. Without this timeout one bad rate-limit window would freeze the
    whole channel scan for 5+ minutes; the time-boxed bail-out lets the
    caller record the affected message as an error and move on.
    """
    log = logging.getLogger("unarchive")
    try:
        status, body = await asyncio.wait_for(
            rl.request(
                "PATCH",
                f"/channels/{channel_id}",
                bucket=f"unarchive-c-{channel_id}",
                json={"archived": False, "locked": False},
            ),
            timeout=30.0,
        )
    except asyncio.TimeoutError:
        log.warning(
            "unarchive %s timed out after 30s (rate-limited, scope likely 'shared')",
            channel_id,
        )
        return False
    if status in (200, 204):
        log.info("unarchived thread %s", channel_id)
        return True
    log.warning(
        "failed to unarchive thread %s: %s %s",
        channel_id, status, _short(body, 200),
    )
    return False


async def _delete_with_unarchive(
    rl, channel_id, msg_id, unarchived_threads: set[str],
) -> tuple[int, object]:
    """DELETE, with one unarchive-and-retry on the 'thread archived' error.

    `unarchived_threads` accumulates channel_ids we've already unarchived
    in this run so we don't PATCH the same thread on every message.
    """
    status, body = await _delete_one(rl, channel_id, msg_id)
    if (
        status == 400
        and isinstance(body, dict)
        and body.get("code") == 50083
        and channel_id not in unarchived_threads
    ):
        if await _try_unarchive_thread(rl, channel_id):
            unarchived_threads.add(channel_id)
            status, body = await _delete_one(rl, channel_id, msg_id)
    return status, body


async def clear_via_search(
    rl: RateLimiter,
    cp: Checkpoint,
    scan_id: str,          # guild_id if is_guild, else channel_id
    is_guild: bool,
    self_id: str,
    dry_run: bool,
    progress_cb=None,      # optional callable(status: str) after each delete
    total_cb=None,         # optional callable(total_remaining: int) after each search
) -> tuple[int, int]:
    """Undiscord-style loop: search, delete each hit, re-search until empty."""
    log = logging.getLogger(("guild." if is_guild else "chan.") + scan_id)
    deleted = 0
    errors = 0
    offset = 0
    consecutive_empty_pages = 0
    # Discord's /messages/search index lags behind deletions: after a batch
    # delete we frequently get an empty page back even though total_results
    # is still > 0. Without retry, we'd mark the channel done and miss
    # everything in the long tail (often hundreds of messages across other
    # channels in a guild). Backoff up to ~62s before giving up.
    empty_with_total_retries = 0
    EMPTY_WITH_TOTAL_MAX_RETRIES = 5
    # Track threads we've unarchived this scan so we don't PATCH the same
    # one on every message in it. See `_delete_with_unarchive`.
    unarchived_threads: set[str] = set()

    while True:
        if is_guild:
            msgs, total, retry_after = await search_guild(rl, scan_id, self_id, offset)
        else:
            msgs, total, retry_after = await search_channel(rl, scan_id, self_id, offset)

        if retry_after is not None:
            log.info("search index warming (202), sleeping %.1fs", retry_after)
            await asyncio.sleep(retry_after)
            continue

        if msgs is None:
            log.warning("search returned failure for %s", scan_id)
            return deleted, errors  # caller decides status

        if total is not None and total_cb is not None:
            total_cb(total)

        if not msgs:
            # Search returned empty. If `total > 0` Discord's search index
            # is lagging behind our recent deletes — wait and retry rather
            # than declaring the channel done. This is the load-bearing
            # fix for guilds where a single search-then-delete batch
            # would mark the whole guild done after only ~25 messages,
            # even though the remaining ~150 lived in other channels.
            #
            # IMPORTANT: don't reset offset to 0 here. If we got here by
            # advancing past a page of pre-errored messages (offset=21
            # after a page where every message was already in the error
            # log), resetting to 0 would just re-fetch those same errored
            # messages and infinite-loop. Preserving the current offset
            # lets us either (a) retry the same offset and pick up new
            # results once Discord's index catches up, or (b) cleanly
            # exhaust the search if the residual is all un-deletable.
            if total and total > 0 and empty_with_total_retries < EMPTY_WITH_TOTAL_MAX_RETRIES:
                empty_with_total_retries += 1
                delay = min(2 ** empty_with_total_retries, 30)  # 2, 4, 8, 16, 30
                log.info(
                    "empty page with total=%d remaining (offset=%d); "
                    "retry %d/%d after %.1fs",
                    total, offset, empty_with_total_retries,
                    EMPTY_WITH_TOTAL_MAX_RETRIES, delay,
                )
                await asyncio.sleep(delay)
                continue
            if total and total > 0:
                log.warning(
                    "empty page persisted with total=%d remaining after %d retries; "
                    "aborting — re-run to retry the residual",
                    total, empty_with_total_retries,
                )
            break

        # Non-empty response: reset the empty-with-total counter.
        empty_with_total_retries = 0
        log.info("page: %d results (total=%s, offset=%d)", len(msgs), total, offset)
        progressed = False

        for m in msgs:
            msg_id = m["id"]
            channel_id = m["channel_id"]
            author_id = (m.get("author") or {}).get("id")
            if author_id and author_id != self_id:
                # belt-and-suspenders — shouldn't happen given author_id filter
                continue
            if cp.message_errored_before(channel_id, msg_id):
                continue  # don't retry permanent failures
            if dry_run:
                cp.record_message(channel_id, msg_id, "deleted", "DRY_RUN")
                deleted += 1
                progressed = True
                if progress_cb: progress_cb("deleted")
                continue

            status, body = await _delete_with_unarchive(rl, channel_id, msg_id, unarchived_threads)
            if status == 204:
                cp.record_message(channel_id, msg_id, "deleted")
                cp.bump_deleted_count(scan_id, 1)
                deleted += 1
                progressed = True
                if progress_cb: progress_cb("deleted")
            elif status == 404:
                cp.record_message(channel_id, msg_id, "missing")
                progressed = True
                if progress_cb: progress_cb("missing")
            elif status == 403:
                reason = _short(body, 200)
                cp.record_message(channel_id, msg_id, "error", f"403: {reason}")
                errors += 1
                log.warning("403 deleting %s/%s: %s", channel_id, msg_id, reason)
                if progress_cb: progress_cb("error")
            else:
                reason = f"{status}: {_short(body, 200)}"
                cp.record_message(channel_id, msg_id, "error", reason)
                errors += 1
                log.warning("delete failed %s/%s: %s", channel_id, msg_id, reason)
                if progress_cb: progress_cb("error")

        if dry_run:
            # in dry-run nothing is actually deleted, so we must paginate by offset
            offset += len(msgs)
            if len(msgs) < 25:
                break
            continue

        if progressed:
            offset = 0         # results shift back — re-query from 0
            consecutive_empty_pages = 0
        else:
            # nothing progressed this page (all errored or filtered out);
            # advance past them
            offset += len(msgs)
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= 3:
                log.warning("3 unprogressed pages, aborting channel")
                break

    return deleted, errors


async def clear_via_list(
    rl: RateLimiter,
    cp: Checkpoint,
    channel_id: str,
    self_id: str,
    dry_run: bool,
    progress_cb=None,
) -> tuple[int, int]:
    """Fallback when search isn't available on a channel: raw /messages scan."""
    log = logging.getLogger(f"list.{channel_id}")
    deleted = 0
    errors = 0
    before: str | None = None
    # Per-channel scope is fine — list iterates one channel at a time, but
    # threads sweep through here too, so the unarchive-and-retry path can
    # still trigger.
    unarchived_threads: set[str] = set()
    while True:
        page = await list_channel_messages(rl, channel_id, before=before, limit=100)
        if page is None:
            log.warning("list failed for %s", channel_id)
            break
        if not page:
            break
        before = page[-1]["id"]
        mine = [m for m in page if (m.get("author") or {}).get("id") == self_id]
        for m in mine:
            msg_id = m["id"]
            if cp.message_errored_before(channel_id, msg_id):
                continue
            if dry_run:
                cp.record_message(channel_id, msg_id, "deleted", "DRY_RUN")
                deleted += 1
                if progress_cb: progress_cb("deleted")
                continue
            status, body = await _delete_with_unarchive(rl, channel_id, msg_id, unarchived_threads)
            if status == 204:
                cp.record_message(channel_id, msg_id, "deleted")
                cp.bump_deleted_count(channel_id, 1)
                deleted += 1
                if progress_cb: progress_cb("deleted")
            elif status == 404:
                cp.record_message(channel_id, msg_id, "missing")
                if progress_cb: progress_cb("missing")
            else:
                reason = f"{status}: {_short(body, 200)}"
                cp.record_message(channel_id, msg_id, "error", reason)
                errors += 1
                if progress_cb: progress_cb("error")
        if len(page) < 100:
            break
    return deleted, errors


def _short(body, n):
    if body is None:
        return ""
    s = body if isinstance(body, str) else str(body)
    return s[:n]


async def _clear_guild_threads(
    rl: RateLimiter,
    cp: Checkpoint,
    guild_id: str,
    self_id: str,
    dry_run: bool,
    progress_cb=None,
    total_cb=None,
) -> tuple[int, int]:
    """Sweep active threads in a guild after the guild-level search pass.

    Discord's `/guilds/{id}/messages/search?author_id=X` doesn't reliably
    cover thread messages — and forum channels are containers of threads,
    so forum posts are entirely invisible to it. After the main guild
    sweep, enumerate active threads via `/guilds/{id}/threads/active` and
    clear each one with search + list (mirrors the group_dm strategy).

    Archived threads are out of scope here — that's a re-run-after-
    unarchive workflow, documented in CLAUDE.md.
    """
    log = logging.getLogger("guild-threads." + guild_id)
    threads = await get_active_threads(rl, guild_id)
    if not threads:
        return 0, 0
    log.info("sweeping %d active thread(s) / forum post(s)", len(threads))
    deleted = 0
    errors = 0
    for thread in threads:
        thread_id = thread.get("id")
        if not thread_id:
            continue
        thread_name = thread.get("name") or thread_id
        log.info("thread %s (%s)", thread_id, thread_name[:40])
        # Search first — fast bulk delete for threads with many user msgs.
        td, te = await clear_via_search(
            rl, cp, thread_id, is_guild=False, self_id=self_id, dry_run=dry_run,
            progress_cb=progress_cb, total_cb=total_cb,
        )
        deleted += td
        errors += te
        # List sweep covers what search missed (threads have the same
        # search-index lag and missing-from-index quirks as group DMs).
        td2, te2 = await clear_via_list(
            rl, cp, thread_id, self_id, dry_run, progress_cb=progress_cb,
        )
        deleted += td2
        errors += te2
    return deleted, errors


async def process_channel(
    rl: RateLimiter,
    cp: Checkpoint,
    ch: dict,
    self_id: str,
    dry_run: bool,
    sem: asyncio.Semaphore,
    progress_cb=None,
    total_cb=None,
) -> tuple[int, int]:
    log = logging.getLogger("worker")
    async with sem:
        cp.set_channel_status(ch["id"], "in_progress")
        try:
            if ch["type"] == "guild":
                d, e = await clear_via_search(
                    rl, cp, ch["id"], is_guild=True, self_id=self_id, dry_run=dry_run,
                    progress_cb=progress_cb, total_cb=total_cb,
                )
                # Discord's guild-level search misses thread / forum-channel
                # messages inconsistently (and skips archived threads
                # entirely). Enumerate active threads explicitly and
                # search-then-list each one so forum posts and thread
                # replies actually get cleared.
                td, te = await _clear_guild_threads(
                    rl, cp, ch["id"], self_id, dry_run,
                    progress_cb=progress_cb, total_cb=total_cb,
                )
                d += td
                e += te
            elif ch["type"] == "dm":
                # 1:1 DMs have only two authors — history+filter is cheaper
                # than search and sidesteps the stricter search rate limit.
                d, e = await clear_via_list(
                    rl, cp, ch["id"], self_id, dry_run, progress_cb=progress_cb,
                )
            else:  # group_dm
                d, e = await clear_via_search(
                    rl, cp, ch["id"], is_guild=False, self_id=self_id, dry_run=dry_run,
                    progress_cb=progress_cb, total_cb=total_cb,
                )
                # Always sweep with list afterward — Discord's search index
                # can miss messages (lag, NSFW edges, scattered system msgs
                # tripping the no-progress abort). List iterates raw channel
                # history so it catches whatever search left behind.
                d2, e2 = await clear_via_list(
                    rl, cp, ch["id"], self_id, dry_run, progress_cb=progress_cb,
                )
                d += d2
                e += e2
            # In dry-run, leave the channel as pending so a later real run
            # picks it up. Real runs mark it done.
            cp.set_channel_status(ch["id"], "pending" if dry_run else "done")
            label = ch.get("name") or ch.get("guild_name") or ch["id"]
            log.info("%s %-8s %-40s deleted=%d errors=%d",
                     "dry-run" if dry_run else "done",
                     ch["type"], label[:40], d, e)
            return d, e
        except Exception as ex:
            logging.getLogger("worker").exception("channel %s failed", ch["id"])
            cp.set_channel_status(ch["id"], "error", str(ex)[:300])
            return 0, 1


def _is_batch_mode(args) -> bool:
    """Any flag that only makes sense in all-at-once mode → batch.

    --dry-run is compatible with TUI (dry-run a single channel), so it does
    NOT force batch on its own. --batch always forces it.
    """
    if args.batch:
        return True
    if args.exclude_guild or args.exclude_user:
        return True
    if args.aggressive:
        return True
    if args.concurrency and args.concurrency > 1:
        return True
    return False


async def main_async(args) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    batch = _is_batch_mode(args)
    log_file = setup_logging(args.verbose, quiet_console=not batch)
    log = logging.getLogger("main")
    log.info("log file: %s", log_file)
    log.info("checkpoint db: %s", args.db)
    log.info("mode=%s dry_run=%s concurrency=%d aggressive=%s",
             "batch" if batch else "tui", args.dry_run, args.concurrency, args.aggressive)

    token = os.environ.get("DISCORD_TOKEN")
    if not token:
        print("error: DISCORD_TOKEN env var not set (see .env.example)", file=sys.stderr)
        return 2

    cp = Checkpoint(args.db)
    # Any 'in_progress' rows at startup are stranded from a previous crashed
    # or force-killed run (the TUI Esc-cancel path resets them on its own).
    # Flip them back to 'pending' so they show clean and resume cleanly.
    stale = cp.reset_in_progress()
    if stale:
        log.info("reset %d stale in_progress channel(s) from a prior run", stale)
    if args.reset_rate_limits:
        nb, nr = cp.reset_rate_limit_state()
        log.info("--reset-rate-limits: cleared %d bucket model(s) and %d route mapping(s)", nb, nr)
    delay = 0.55 if args.aggressive else 1.1
    timeout = aiohttp.ClientTimeout(total=30)

    rl: RateLimiter | None = None
    try:
        async with aiohttp.ClientSession(
            headers={
                "Authorization": token,
                "User-Agent": USER_AGENT,
                "X-Super-Properties": X_SUPER_PROPERTIES,
                "X-Discord-Locale": "en-US",
                "X-Debug-Options": "bugReporterEnabled",
                "Accept": "*/*",
                "Accept-Language": "en-US",
            },
            timeout=timeout,
        ) as session:
            rl = RateLimiter(session, default_delay=delay, checkpoint=cp)

            exclude_guilds = _merge_ids(args.exclude_guild, os.environ.get("EXCLUDE_GUILDS"))
            exclude_users = _merge_ids(args.exclude_user, os.environ.get("EXCLUDE_USERS"))
            if exclude_guilds:
                log.info("excluding %d guild(s): %s", len(exclude_guilds), ", ".join(sorted(exclude_guilds)))
            if exclude_users:
                log.info("excluding %d user(s): %s", len(exclude_users), ", ".join(sorted(exclude_users)))

            # Phase 1 runs in both modes — we need the channel list either way.
            if not batch:
                print("Authenticating and enumerating channels (this takes a minute)...", file=sys.stderr, flush=True)
            log.info("=== phase 1: discovery ===")
            self_id = await discover_all(
                rl, cp,
                exclude_guilds=exclude_guilds,
                exclude_users=exclude_users,
            )

            if batch:
                pending = list(cp.iter_pending_channels())
                log.info("=== phase 2: deletion (%d channels pending) ===", len(pending))
                sem = asyncio.Semaphore(args.concurrency)
                tasks = [
                    asyncio.create_task(process_channel(rl, cp, ch, self_id, args.dry_run, sem))
                    for ch in pending
                ]
                total_deleted = 0
                total_errors = 0
                for t in asyncio.as_completed(tasks):
                    d, e = await t
                    total_deleted += d
                    total_errors += e
                summary = cp.summary()
                log.info("=== SUMMARY ===")
                log.info("channels:         %s", summary["channels_by_status"])
                log.info("messages logged:  %s", summary["messages_by_status"])
                log.info("total deleted:    %d (this run: %d)", summary["total_deleted"], total_deleted)
                log.info("errors this run:  %d", total_errors)
                if args.dry_run:
                    log.info("(dry-run — no actual DELETE calls were made)")
            else:
                # TUI mode — hand off to the Textual app.
                from tui import run_tui
                print(f"Ready: {len(list(cp.iter_pending_channels()))} channels pending", file=sys.stderr, flush=True)
                await run_tui(
                    cp, rl, self_id, dry_run=args.dry_run,
                    exclude_guilds=exclude_guilds, exclude_users=exclude_users,
                )
                # After TUI exits, print a quick post-run summary to stderr.
                summary = cp.summary()
                print(file=sys.stderr)
                print(f"channels:       {summary['channels_by_status']}", file=sys.stderr)
                print(f"messages:       {summary['messages_by_status']}", file=sys.stderr)
                print(f"total deleted:  {summary['total_deleted']}", file=sys.stderr)
                if args.dry_run:
                    print("(dry-run — no actual DELETE calls were made)", file=sys.stderr)
    finally:
        if rl is not None:
            try:
                if rl._persistence_errors:
                    logging.getLogger("main").warning(
                        "rate-limiter persistence errors this run: %d "
                        "(check for save_bucket_model / save_route_bucket "
                        "exceptions in the log)",
                        rl._persistence_errors,
                    )
                await rl.close()
            except Exception:  # noqa: BLE001
                logging.getLogger("main").exception("rl.close() failed")
        cp.close()
    return 0


def parse_args():
    ap = argparse.ArgumentParser(
        description="Delete every Discord message you authored. "
                    "Default mode: interactive TUI (run with no args). "
                    "Pass --batch or any exclude/aggressive/concurrency flag for all-at-once mode.",
    )
    ap.add_argument("--batch", action="store_true",
                    help="force all-at-once batch mode (default is interactive TUI)")
    ap.add_argument("--dry-run", action="store_true", help="skip DELETE calls (works in both TUI and batch)")
    ap.add_argument("--concurrency", type=int, default=1, help="channels processed in parallel (batch mode; default 1)")
    ap.add_argument("--aggressive", action="store_true", help="halve per-request delay (faster, riskier)")
    ap.add_argument(
        "--exclude-guild", action="append", metavar="GUILD_ID",
        help="skip this guild (repeatable; drop the flag on a later run to re-include it)",
    )
    ap.add_argument(
        "--exclude-user", action="append", metavar="USER_ID",
        help="skip 1:1 DMs with this user and any group DM they're in (repeatable)",
    )
    ap.add_argument("--db", default=str(LOG_DIR / "checkpoint.db"), help="checkpoint DB path")
    ap.add_argument(
        "--reset-rate-limits", action="store_true",
        help="wipe persisted bucket / route rate-limit learning before starting "
             "(channels and message audit log are untouched)",
    )
    ap.add_argument("-v", "--verbose", action="store_true", help="debug-level console output (batch mode)")
    return ap.parse_args()


def main() -> int:
    try:
        return asyncio.run(main_async(parse_args()))
    except KeyboardInterrupt:
        print("\ninterrupted — checkpoint is safe, re-run to resume", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
