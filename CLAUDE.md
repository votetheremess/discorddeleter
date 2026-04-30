# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project purpose

One-shot, resumable tool to delete every message the authenticated user authored across Discord (guilds + DMs) before they delete their account. Targets 100k-message-scale accounts; smaller accounts work fine but the engineering choices (resumability, adaptive rate-limiting, persistent learning) are sized for the long tail.

**Design constraint — no ZIP/data-package path.** Discord's data-export feature can take weeks or never arrive. This tool is built entirely around live Discord API discovery so it works regardless of data-package availability. Don't reintroduce a ZIP-parsing branch — the live-API path is the contract.

Self-botting (user-token automation) violates Discord ToS. The intended use case is pre-account-deletion cleanup, where the ToS-flag risk is acceptable in exchange for clearing your message history before the account itself is gone. Tool defaults to conservative rates to reduce flag risk during the window between starting the run and finishing the account deletion.

## Commands

Setup (uses uv; `brew install uv` if you don't have it):
```bash
uv sync                         # creates .venv, installs aiohttp + textual, writes uv.lock
cp .env.example .env            # then edit .env; chmod 600 .env
```

Run:
```bash
uv run python src/deleter.py              # interactive TUI (default)
uv run python src/deleter.py --dry-run    # TUI with no DELETE calls
uv run python src/deleter.py --batch      # all-at-once batch mode
uv run python src/deleter.py --batch --concurrency 3 -v
uv run python src/deleter.py --reset-rate-limits  # wipe learned bucket params; keep channel/message state
```

`--reset-rate-limits` is a development / recovery tool: clears `rate_limit_buckets` and `route_buckets` only. Use it (a) when iterating on the limiter and you want a true cold start; (b) if you suspect Discord changed a bucket's shape and the warm-loaded model is wrong; (c) if `_persistence_errors > 0` at run-end and you want to drop possibly-corrupt rows. It does NOT touch `channels` or `messages`.

Stop the running tool with `Ctrl+C` (SIGINT). `kill <pid>` (SIGTERM) hard-kills — not data-loss because every DB write is autocommit, but the run-end summary doesn't fire and the in-flight channel stays `in_progress` until the next run's `reset_in_progress` flips it back to `pending`.

Checks (no formal test suite; each module self-tests in `__main__`):
```bash
uv run python -m py_compile src/*.py      # syntax check
uv run python src/checkpoint.py           # checkpoint self-test (channels + rate-limit tables)
uv run python src/ratelimit.py            # rate-limiter self-test (BucketModel + GlobalBudget)
```

Inspect state:
```bash
sqlite3 logs/checkpoint.db "SELECT status, COUNT(*) FROM channels GROUP BY status;"
sqlite3 logs/checkpoint.db "SELECT status, COUNT(*) FROM messages GROUP BY status;"
sqlite3 logs/checkpoint.db "SELECT bucket_key, limit_n, window_seconds, calibrated, sample_count FROM rate_limit_buckets ORDER BY sample_count DESC LIMIT 10;"
sqlite3 logs/checkpoint.db "SELECT route_key, bucket_key FROM route_buckets;"
```

Inspect the throughput ticker in a running log (every 5s):
```bash
grep "INFO ratelimit: rl: req=" logs/deleter-*.log | tail -10
# Sample line: rl: req=807 (+14, 2.8/s) 429s=0 global_429s=0 invalid_window=0 sf=[]
```

## Architecture

Two run modes share a two-phase pipeline:

**Phase 1 — discovery** (`discover_all` in `deleter.py`, runs in both modes): `GET /users/@me` → `/users/@me/guilds` → `/users/@me/channels`. One row per guild/DM/group-DM inserted into `channels`. Exclusions applied here.

**Phase 2 — per-channel clear** (`process_channel` dispatches by channel type):
- **Guilds**: `clear_via_search` (over `/guilds/{id}/messages/search?author_id=X`), then `_clear_guild_threads` to sweep active threads and forum-channel posts. After each successful DELETE, search results shift backward, so the loop re-queries from `offset=0`. On 202 ("index warming"), sleeps `retry_after` and retries. Search URL must include `include_nsfw=true&sort_by=timestamp&sort_order=asc` (mirrors Undiscord; without `include_nsfw=true`, NSFW-channel hits are silently dropped). Iterating every text channel via list isn't viable for guilds — too many channels — so the guild-level search is the bulk path and the thread sweep is the targeted-completeness path. **Threads must be swept explicitly** because `/guilds/{id}/messages/search` covers thread messages inconsistently and skips forum channels entirely (each forum post IS a thread; the forum channel itself has no listable messages). `_clear_guild_threads` calls `/guilds/{id}/threads/active` (paginated via `before`), then for each thread runs `clear_via_search` + `clear_via_list` (mirrors the group_dm strategy). Archived threads are out of scope of automatic sweep — re-run after manually un-archiving if a guild reports residual messages there.
- **Group DMs**: `clear_via_search` followed by `clear_via_list`, **always both**. Discord's search index has blind spots (lag, scattered un-deletable system messages tripping the no-progress abort), so the search pass is the fast bulk delete and the list pass is the thorough cleanup that walks raw channel history. Don't gate the list call on `d == 0 and e == 0` — that was a previous bug that left stragglers.
- **1:1 DMs**: `clear_via_list` only — iterate `/channels/{id}/messages?before=X` backward through history and filter client-side by `author.id == self_id`. Sidesteps the stricter search rate limit; with only 2 authors, filtering is trivial.

**Un-deletable system messages.** Discord attributes auto-events (call started, member joined, group renamed) to the user who triggered them, so search returns them as the user's. DELETE fails with `403 code:50021 "Cannot execute action on a system message"`. Recorded as `error` in the messages table. `cp.message_errored_before()` makes both search and list permanently skip them on subsequent runs — that's the intended behavior, not a bug to fix.

**Archived threads — unarchive-and-retry.** Discord rejects DELETE in archived threads with `400 code:50083 "Thread is archived"`. `_delete_with_unarchive` (used by both `clear_via_search` and `clear_via_list`) detects this code, PATCHes `/channels/{thread_id}` with `{"archived": False, "locked": False}`, and retries the DELETE once. A per-scan `unarchived_threads: set[str]` deduplicates so we only PATCH the same thread once per channel pass. Failures (e.g. private thread without membership, locked-by-mods) record the message as `error` and move on. **Side effect**: unarchiving sends a "Thread unarchived" event to thread members. Tradeoff is intentional — the alternative is leaving thread messages permanently un-deleted. Pre-account-deletion users typically don't care about the brief notification spam. **Don't bypass `_delete_with_unarchive` and call `_delete_one` directly** for any path that touches a thread or could touch one (search results from a guild include thread channel_ids).

**Total-results tracking.** `clear_via_search` accepts a `total_cb(remaining: int)` callback, called with `total_results` after each search response. Used by the TUI's per-channel progress display. `process_channel` forwards it to the search-using paths (guild, group_dm); `clear_via_list` doesn't take one because raw history doesn't know how many of the user's messages exist.

**Search-empty-with-total retry.** Discord's `/messages/search` index lags behind deletions: after a batch delete you frequently get back `msgs=[]` while `total_results` is still > 0. Treating that empty page as "channel done" was the cause of guilds being prematurely marked done after only ~25 messages — the long tail across other channels in the guild was abandoned. `clear_via_search` now retries empty-with-total-positive responses with exponential backoff (2s, 4s, 8s, 16s, 30s — total ~62s) before giving up. Resets the retry counter on any non-empty response. **Don't change the empty-and-total-zero break** to also retry — that path is the normal "truly empty, channel done" exit.

**Mode selection** (`_is_batch_mode` in `deleter.py`): batch mode is triggered by `--batch`, any `--exclude-*` value, `--aggressive`, or `--concurrency N>1`. Otherwise TUI mode (default). `--dry-run` is compatible with both. In TUI mode the stderr log handler is suppressed so logs don't corrupt the Textual rendering — they still go to `logs/deleter-*.log`.

**TUI layer** (`src/tui.py`): after discovery, `run_tui(cp, rl, self_id, dry_run, exclude_guilds, exclude_users)` starts a Textual `App`. Two tabs (Servers / DMs), each a `DataTable` from `cp.list_channels(ctypes=...)`. `Space` (or `Enter`) toggles a row in/out of `app.selected` (a `dict[id, channel]`), shown with a `*` mark. `a` toggles aggressive (mutates `rl.default_delay` live: 1.1s ↔ 0.55s). `h` toggles hiding `done` rows. `d` pushes a `ProgressScreen` modal that runs `process_channel` for every selected channel via `asyncio.gather` against `Semaphore(len(selected))` — so concurrency = selection size. **Done channels are NOT skipped on `d`** — re-running is the recovery path when search/list missed something the first time (idempotent: search/list naturally find whatever's left).

`r` does a **real Discord refresh**: re-runs `discover_all` (catches DMs/guilds opened since startup, re-applies exclusions) and then fires a search per `pending` channel; rows whose `total_results == 0` flip to `done` via `cp.mark_pending_done_if()` (guarded UPDATE — won't clobber `in_progress`/`excluded`/`error`). Runs in a background worker; `_refreshing` flag prevents stacking. Search failures (`msgs is None`) leave the row alone so a flaky 401 can't silently mark channels done.

`ProgressScreen` tracks per-channel `{deleted, missing, error, total}`. The total field starts None and is updated via `total_cb` using `max(deleted_so_far + remaining)` — Discord's `total_results` shrinks as we delete, so latching the first value would under-report under index lag. Display shows global `Deleted: X / Y` (with trailing `+` if any selected channel is a 1:1 DM, since list can't supply a total) and a per-channel breakdown padded to a fixed cell width.

**Markup gotcha.** Channel labels go through user-controlled names that often contain `[brackets]` (group DMs are formatted as `[name]`, recipients can have brackets in their display name). Two helpers:
- `_channel_label_plain(ch)` — raw string, no escape. Use with `rich.text.Text` (which doesn't parse markup).
- `_channel_label(ch)` — wraps the plain form in `rich.markup.escape`. Use with widgets that *do* parse markup (DataTable cells, `Static.update(str)`, error messages built into a count widget). Without escape, names like `[stream]` get silently swallowed as an unknown style tag.

The per-channel body in `ProgressScreen` is rendered via `rich.text.Text` with `no_wrap=True, overflow="ellipsis"` and labels padded to a fixed cell width via `Text.truncate(LABEL_WIDTH, overflow="ellipsis", pad=True)` so the count column aligns regardless of wide unicode / combining marks. Don't switch this back to a string passed through `Static.update` — Rich's markup parser inserts mystery padding around bare `]` in escaped labels.

Channel lifecycle: `pending → in_progress → done|error|excluded`. `iter_pending_channels()` yields only `pending`+`in_progress`, so `done`/`error`/`excluded` rows are naturally skipped on resume (in both modes). `apply_exclusion()` is careful to leave `done`/`error` untouched — exclusion toggling can never un-finish completed work. `reset_in_progress()` is called at startup to flip stranded `in_progress` rows (from a crashed previous run) back to `pending`.

The `messages` table is an audit log (one row per DELETE attempt, status `deleted`/`missing`/`error`), not a work queue.

**Throughput-visibility ticker.** `RateLimiter` spawns `_periodic_logger()` lazily on first `request()` call (constructing in `__init__` would fail outside an event loop). Emits one INFO line every 5s: `rl: req=N (+Δ, X.X/s) 429s=M global_429s=K invalid_window=W sf=[…]` — the sf list shows only buckets currently above 1.0. Without this, uncalibrated buckets emit no per-request DEBUG output and the console looks frozen even at 500+ msg/s. `RateLimiter.close()` cancels the ticker; `deleter.py`'s `main_async` finally block calls it.

### Request flow through `RateLimiter.request()`

When `await rl.request(method, path, bucket=...)` is called, the request travels through this gauntlet — each gate has a specific failure mode:

1. **Compute** `url`, `route_key = bucket or f"{method} {path}"`, `is_delete = method.upper() == "DELETE"`.
2. **`_ensure_ticker()`** — spawns the periodic logger task if not yet started (we now have a running loop).
3. **`_check_invalid_request_window()`** — evicts >600s-old entries from the Cloudflare counter, halves global budget at >1000 hits, recovers below 500.
4. **Loop** (continues forever on 429 / transport retry; only success returns):
   1. Honor `global_pause_until` — set by a prior GLOBAL 429. Sleep until clear.
   2. **`seq_before = global_budget.drain_seq`**, then `seq_at_acquire = await global_budget.acquire()` — proactive 40 req/s cap. `acquire()` may sleep (waiting for refill); `drain_seq` snapshots the value at entry.
   3. **Drain-race re-check**: if `drain_seq` advanced during `acquire()`'s sleep, a GLOBAL 429 was installed mid-flight. Re-check `global_pause_until` and sleep again.
   4. Acquire `_route_lock(route_key)` — per-route serialization, preserves channel ordering for DELETE.
   5. `model = _model_for(route_key)` — fetches via `route_index[route_key]` if known, else creates a placeholder keyed by `route_key`.
   6. `wait_bucket = model.time_until_token(now)` — sleep if > 0 (also covers `tokens_floor_until` post-429 boundary).
   7. **DELETE-only floor**: `floor_remaining = default_delay - (now - model.last_request_at)`; sleep if > 0. Anti-abuse margin, NOT a rate limit.
   8. `model.consume(now)` — decrement tokens, set `last_observation` and `last_request_at`.
   9. `await session.request(...)` — the actual network call. Held under route_lock so concurrent same-route requests serialize.
   10. **Transport error path**: `aiohttp.ClientError` increments `transport_failures`, sleeps `2*N`, continues. Capped at `MAX_TRANSPORT_ATTEMPTS=5` then raises `RuntimeError`.
5. **Read response** — body decoded via `_decode` (JSON-or-text); headers `X-RateLimit-Bucket/Limit/Remaining/Reset-After/Global/Scope`.
6. **First-response DEBUG dump** (one-shot per `route_key` per process via `_dumped_routes`) — for diagnosing what Discord actually sends on a route.
7. **Re-key path** — if `bucket_hash` from headers differs from `model.bucket_key`:
   - If `bucket_hash` not yet in `bucket_models`: rename (delete old, install new).
   - Else: pool — replace `model = existing`, take `max()` of `safety_factor` and `tokens_floor_until`, sum `sample_count`.
   - Walk `route_index` for ALL other routes pointing at `old_bucket_key` and migrate them too (persisting each).
   - Persist current route's mapping; delete old DB row via `_delete_bucket_model_db`.
8. **429 path** — bump `_stats["429s"]` (+`global_429s` if `X-RateLimit-Global=true`), log scope, set `global_pause_until` and `global_budget.drain()` if global, call `model.on_429(retry_after)` and `model.reconcile_from_headers(...)`, set `model.tokens_floor_until = max(...)` and `tokens = 0.0`, `_record_invalid_request(429, scope)`, persist, sleep `retry_after + 0.5`, **continue** the loop.
9. **Success path** — bump `_stats["requests"]`, `_record_invalid_request` for 401/403 (4xx still counts toward Cloudflare), `model.reconcile_from_headers(...)`, `model.on_success()` (which may trigger AIMD recovery), persist if dirty, return `(status, body)`.

### Token-bucket math (`BucketModel`)

Continuous-time token bucket with calibration flag and post-429 floor:

- **`_rate() = limit_n / window_seconds / safety_factor`** — tokens per second.
- **`refill(now)`** — if `now < tokens_floor_until`, only update `last_observation` and return (skip refill — the bucket is empty per Discord). Else `tokens = min(limit_n, tokens + (now - last_observation) * _rate())`.
- **`time_until_token(now)`** — three-tier gate:
  1. If `now < tokens_floor_until`: return `tokens_floor_until - now` (floor wins regardless of calibration).
  2. If `not calibrated`: return `0.0` (passthrough — only global budget gates).
  3. Else refill, return `0.0` if `tokens >= 1`, else `(1 - tokens) / _rate()`.
- **`consume(now)`** — refill first, then `tokens = max(0, tokens - 1)`, set `last_observation = last_request_at = now`. Never goes negative.
- **`reconcile_from_headers(now, remaining, reset_after, limit_seen)`** — pulls truth from headers:
  - Any non-None header → `calibrated = True`.
  - Update `limit_n` from `limit_seen`, OR from `remaining + 1` lower-bound if `limit_seen` is None.
  - Set `tokens = max(0, remaining)` if remaining is non-None.
  - Set `window_seconds = max(reset_after, 0.05)` if reset_after is non-None.
  - Always set `last_observation = now`.
- **`on_success()`** — increment `consecutive_ok`, `observed_burst_count`, `sample_count`. If `consecutive_ok >= 10` AND `safety_factor > 1.0`, AIMD recovery: `safety_factor = max(1.0, sf - 0.1)` and reset `consecutive_ok = 0`. Returns True iff sf changed (caller may want to persist).
- **`on_429(retry_after_seconds=0.0)`**:
  - AIMD bump: `safety_factor = sf * 1.5` if `consecutive_ok == 0` (back-to-back 429); else `max(sf, 1.5)` (interleaved with successes — fresh slowdown but no compounding spiral).
  - `consecutive_ok = 0`.
  - **Calibration from `Retry-After`** (the load-bearing fix): if `retry_after > 0`:
    - Uncalibrated: `limit_n = max(observed_burst_count, 1)`, `window_seconds = retry_after * limit_n`, `calibrated = True`. This derives token-bucket params from observed burst (a 429 after N successes with `Retry-After=T` implies capacity ≈ N refilling at 1 per T seconds).
    - Calibrated: `window_seconds = max(window, retry_after * limit_n)` — only nudges UP. Discord can mid-window-reset; shrinking would over-fire.
  - Reset `observed_burst_count = 0`.

### Rate-limiter state model (the three dicts)

Three related dicts hold rate-limit state, all keyed differently:

- **`route_index: dict[str, str]`** — caller-key (e.g. `search-c-{cid}`) → Discord bucket hash (`X-RateLimit-Bucket` value). Populated lazily as responses arrive. Persisted in the `route_buckets` table. Empty until a successful response actually carries the header (which user-API endpoints rarely do — see "User-API rate-limit reality" below).
- **`bucket_models: dict[str, BucketModel]`** — keyed by Discord bucket hash if known, else by caller-key as a cold-start placeholder (we install a placeholder under the route_key on first request, then re-key it under the real hash if/when Discord reveals one). Persisted in `rate_limit_buckets`.
- **`_route_locks: dict[str, asyncio.Lock]`** — keyed by caller-key always. Per-channel ordering (e.g. `del-c-{cid}` deletions in order) is preserved even when multiple route_keys map to one Discord bucket.

**Re-key invariants** (enforced in `request()`'s re-key block):
- When bucket_hash `H_new` arrives and current `model.bucket_key == K_old` (placeholder) where `K_old != H_new`:
  - If `H_new` not yet in `bucket_models`: rename (`pop(K_old)`, `model.bucket_key = H_new`, `bucket_models[H_new] = model`).
  - Else (pool): `model = existing`, take `max()` of `safety_factor` and `tokens_floor_until`, sum `sample_count`, drop placeholder.
  - Walk `route_index` for ALL keys whose value is `K_old`, update them to `H_new`, persist each. Without this, sibling routes warm-loaded with the old hash would silently cold-start when they fire.
  - Delete the old row at `K_old` from `rate_limit_buckets` via `_delete_bucket_model_db`.

**Persistence dirty-flagging**: `_persist_model` writes only when `(limit_n, window_seconds, safety_factor, calibrated)` differs from `BucketModel._last_persisted`. `sample_count` is diagnostic-only and flushes on a separate ~60s cadence per bucket via `_last_sample_flush_at`. Without dirty-flagging, the user-API endpoints (which never calibrate) would generate ~30ms/sec of pointless SQLite writes.

## Modules

| File | Role |
|---|---|
| `src/deleter.py` | CLI, orchestration, phase-1/phase-2 functions, `_is_batch_mode`, dotenv loader, `_merge_ids` for exclusion sources. `--reset-rate-limits` calls `cp.reset_rate_limit_state()` before constructing `RateLimiter`. `main_async` finally block calls `await rl.close()`. |
| `src/tui.py` | Textual `DeleterApp` + `ProgressScreen` modal. Entry point `run_tui()`. Keys: `Space`/`Enter`=toggle, `d`=delete, `a`=aggressive, `h`=hide done, `r`=refresh, `Tab`/`1`/`2`=switch tabs, `Esc`=cancel/close, `q`/`Ctrl+C`=quit. |
| `src/discovery.py` | Thin wrappers: `get_self`, `get_guilds`, `get_dm_channels`, `search_guild`, `search_channel`, `list_channel_messages`, `get_active_threads`. Each passes a fixed `bucket=` key to `rl.request()`. |
| `src/ratelimit.py` | `RateLimiter` — adaptive token-bucket client around aiohttp. See "Architecture > Request flow" and "Architecture > Token-bucket math" above for the algorithm. Fields on `BucketModel`: `bucket_key`, `limit_n`, `window_seconds`, `tokens`, `last_observation`, `last_request_at`, `safety_factor`, `consecutive_ok`, `sample_count`, `calibrated`, `observed_burst_count`, `tokens_floor_until`, `_last_persisted`, `_last_sample_flush_at`. `GlobalBudget` (~40 req/s, capacity = burst). Constants: `GLOBAL_CAPACITY=40`, `GLOBAL_REFILL_PER_SEC=40`, `SAFETY_MIN=1.0`, `SAFETY_MAX=4.0`, `SAFETY_MULT_DECREASE=1.5`, `SAFETY_ADD_DECREASE=0.1`, `SAFETY_DECREASE_AFTER_OK=10`, `COLD_START_LIMIT=1`, `COLD_START_WINDOW=1.0`, `CLOUDFLARE_INVALID_WINDOW_SECONDS=600`, `CLOUDFLARE_INVALID_THRESHOLD=1000`, `PROGRESS_LOG_INTERVAL_SECONDS=5.0`, `SAMPLE_COUNT_FLUSH_INTERVAL_SECONDS=60.0`, `MAX_TRANSPORT_ATTEMPTS=5`. |
| `src/checkpoint.py` | `Checkpoint` — SQLite WAL wrapper. Channel/message methods: `upsert_channel`, `set_channel_status`, `apply_exclusion`, `mark_pending_done_if` (guarded — refresh's "now empty" path), `record_message`, `message_errored_before` (perma-skip system messages + permanent failures), `iter_pending_channels`, `list_channels` (TUI full-list query), `bump_deleted_count`, `reset_in_progress`, `summary`. Rate-limit methods: `load_bucket_models`, `save_bucket_model`, `load_route_buckets`, `save_route_bucket`, `delete_bucket_model`, `reset_rate_limit_state`. Schema migrations live in `_migrate()` — always idempotent (`PRAGMA table_info` + `ALTER TABLE` only when column missing). |

`RateLimiter.request()` takes a caller-supplied `bucket` key for serialization: `search-g-{gid}`, `search-c-{cid}`, `del-c-{cid}`, `list-c-{cid}`, plus discovery keys `users-me`, `users-me-guilds`, `users-me-channels`. The caller key is also the cold-start placeholder for `BucketModel.bucket_key` until a response carries `X-RateLimit-Bucket`.

## Design red lines

- **No ZIP/data-package path.** See constraint above. Live API only.
- **Left servers stay unreachable.** `GET /users/@me/guilds` returns only current memberships; DELETE returns 403 for non-members. README documents rejoin → re-run as the only workaround. Do not build auto-rejoin.
- **Closed DMs stay unreachable.** `GET /users/@me/channels` returns only active DMs.
- **Delete only, no edit-then-delete.** User preference; halves API calls.
- **1.1s/DELETE floor per channel bucket — DELETE only.** Empirically safe (community reports 80k+ msgs without flags at 1000ms; reports of bans at 700ms). `--aggressive` halves it (0.55s). Lives in `RateLimiter.request()` and is applied **only** when `method == "DELETE"`, on top of whatever the adaptive bucket model already paced. Search/list/discovery routes pace purely from learned headers — they used to share this floor (and were artificially slow as a result); don't reintroduce that. Never hard-code shorter than `Retry-After` on 429. The `RateLimiter.default_delay` attribute stays mutable so `tui.py`'s aggressive-mode toggle keeps working.
- **Global request cap is proactive (`GlobalBudget`, ~40 req/s).** `RateLimiter` runs every request through `global_budget.acquire()` before sending so concurrent channels never breach Discord's documented 50 req/s ceiling. The 20% margin (40 vs 50) is insurance against the Cloudflare 10k-401/403/429-in-10min ban (24h IP lockout). On a real `X-RateLimit-Global=true` response we still pause via `global_pause_until` and drain the bucket; `acquire()` returns the pre-acquire `drain_seq` so post-acquire callers can detect a drain that raced their sleep and re-honor the pause boundary. A sliding 401/403/429 window guards against the Cloudflare ceiling — at >1000 hits in 10min it halves the global budget; `X-RateLimit-Scope: shared` 429s aren't counted (Cloudflare excludes them). Don't remove the proactive cap because "we already handle global 429s reactively".
- **Adaptive rate-limit model persists across runs.** Per-Discord-bucket `(limit, window, safety_factor, sample_count, calibrated)` lives in `rate_limit_buckets`; the caller-key→bucket-hash map lives in `route_buckets`. `RateLimiter.__init__` warm-loads both on construction (when given a `Checkpoint`). Persistence writes are dirty-flagged — only `(limit_n, window_seconds, safety_factor, calibrated)` changes trigger a save (sample_count flushes once per ~60s per bucket via `_last_sample_flush_at`). Don't wipe these tables when migrating other schema — the warm start is what avoids cold-start 429s on every run. Use `--reset-rate-limits` (calls `Checkpoint.reset_rate_limit_state`) for development resets — it touches only the rate-limit tables, not channels/messages.
- **`on_429` calibrates from `Retry-After` × observed burst.** User-API endpoints don't return `X-RateLimit-*` headers even on 429s — only `Retry-After`. So when the bucket is uncalibrated and we get a 429 with `Retry-After=T` after `observed_burst_count=N` successes, we set `limit_n=N, window_seconds=N*T` and flip `calibrated=True`. That gives the model the correct token-bucket parameters: capacity ≈ the burst we just saw, refill rate = 1 / T. After the post-429 sleep tokens auto-refill so the bucket can re-burst when Discord's window rolls over. On already-calibrated buckets the same path only nudges `window_seconds` UP (`max(current, T*limit_n)`) — never shortens it, since Discord can mid-window-reset and shrinking would over-fire. Don't replace this with magic-number force-calibration (`limit_n=5, window=5s`); that locked the bucket into headerless throttle math and was the root cause behind the original "way too slow" tail.
- **AIMD `safety_factor` is non-compounding when successes interleave 429s.** Compounds (`*= 1.5`) only when `consecutive_ok == 0` at the moment of 429 (truly back-to-back). With ≥1 success since the last 429 it does `max(current, 1.5)` instead — fresh slowdown, no spiral. Once at sf=3.38 you'd need 240 OKs to walk back at the standard recovery rate, so the compounding guard is load-bearing for production runs.
- **Post-429 `tokens_floor_until` is the bucket-empty signal across the Retry-After sleep.** `request()` sets `model.tokens_floor_until = max(model.tokens_floor_until, now + retry_after)` after a 429; `refill()` and `time_until_token()` honor it; `consume()` only writes `last_observation`, so a concurrent route sharing this pooled model can't erase the floor. Don't switch back to writing `last_observation` from the 429 path — pooled-bucket concurrency would silently re-introduce the "free tokens after sleep" bug.
- **Bucket-hash re-key migrates ALL pointing routes, not just the current one.** When `request()` learns a new `X-RateLimit-Bucket` for a route, it walks `route_index` for other routes pointing at the old hash and migrates them too (persisting each via `_persist_route`). It also drops the old DB row via `_delete_bucket_model_db`. Without this, warm-loaded sibling routes silently cold-start the next time they fire because their `route_index` entry pointed at a now-deleted bucket model.
- **Resumability is load-bearing.** Full runs take 10-30 hours for 100k msgs. Every DB write is autocommit (isolation_level=None + WAL). Don't batch commits or buffer outside SQLite.
- **X-Super-Properties header ships with every request** to mirror the Discord desktop client. The current value is base64-encoded JSON describing a Mac OS X / Discord Client / build 309876 / version 0.0.327 fingerprint. If Discord rotates the expected client-build-number format (login starts triggering CAPTCHAs or the account starts getting "scary login" emails), refresh the base64 value from a real client network capture (Network tab in the desktop app's DevTools).
- **Group DM clear = search + list, always.** Don't gate the list-sweep on `d == 0 and e == 0` from the search pass. Search has index blind spots; list is the completeness guarantee.
- **Guild clear = guild-search + thread-sweep, always.** `_clear_guild_threads` runs unconditionally after `clear_via_search` for guilds. Don't gate it on guild-search results — Discord's `/guilds/{id}/messages/search` covers thread messages inconsistently and skips forum channels entirely (forum posts are threads; the forum channel itself has no listable messages). Each active thread gets the full `clear_via_search` + `clear_via_list` treatment. Archived threads are out of scope (re-run after un-archiving).
- **Refresh writes are guarded.** Refresh's emptiness check uses `mark_pending_done_if()`, not `set_channel_status()`. A flaky search response or a concurrent worker must not silently re-mark channels.
- **Channel labels exposed to Rich must be escape-aware.** `_channel_label` (escape-wrapped) for markup-parsed widgets; `_channel_label_plain` for `rich.text.Text`. Mixing them up either corrupts brackets in names or strips the label entirely (`[stream]` → empty).
- **`_maybe_int` / `_maybe_float` swallow exotic header values.** `int(float("inf"))` raises `OverflowError`, which is caught alongside `ValueError`/`TypeError`. Same for `_maybe_float` and `nan`/`inf`. These helpers MUST return None for un-parseable headers — letting an exception propagate kills the worker and leaves the channel `in_progress`.

## Throughput expectations

Reality-anchored numbers for sizing runs:

| Phase | Per-channel rate | Total time for ~100k msgs (single channel) |
|---|---|---|
| Discovery (all 3 endpoints) | ~1 sec total | one-shot |
| List paginate (1:1 DM) | ~2-3 req/s × 100 msgs/page | ~6-10 min scan |
| Search paginate (guild / group) | bursts ~3-5 req/s × 25 msgs, then ~1 req per `Retry-After` | varies by guild density |
| DELETE (anti-abuse floor) | 1 msg/s default, 1.8 msg/s `--aggressive` | ~30 hr default, ~16 hr aggressive |

Concurrency arithmetic: each channel hits its own DELETE floor independently, so total throughput = N × per-channel rate. Global cap at 40 req/s only binds at N ≈ 44+ — for any practical concurrency (1-21) it's slack. **Practical sweet spot for multi-channel runs: 5-10**, picked via TUI multi-select. Total wall-clock is bounded by the largest channel — concurrency helps the rest finish in parallel, but a 90k-message DM still takes ~30 hours by itself.

What the rate-limiter rewrite (vs. the old reactive one) actually changed:
- Discovery: 3× faster (was gated by 1.1s floor on every bucket; now passthrough).
- List/search scan: 3× faster on list, 2-5× faster on search.
- Per-channel DELETE: **same** (1.1s floor preserved by design).
- Multi-channel concurrent: ~30-40% faster end-to-end due to no GLOBAL 429 cascades.
- Reliability: thousands of 429-related log lines per long run → near zero. No spiraling AIMD slowdown.
- Catastrophic-risk: Cloudflare 24h-ban guard now in place.

## User-API rate-limit reality

Critical context for anyone touching the limiter (per research summarized in the project's task history):

- **User-token auth deliberately omits `X-RateLimit-*` headers on 200/2xx responses.** Per the userdoccers docs: "User authorization usually only returns the Retry-After, X-RateLimit-Global, and X-RateLimit-Scope headers" — and those three are 429-only. So a successful user-auth request returns *no* rate-limit headers at all. This is the design, not a bug. Source: https://docs.discord.food/topics/rate-limits.
- **Even on 429**, `/channels/{id}/messages/search` (and most user-API routes) return only `Retry-After` in the JSON body — no `X-RateLimit-Bucket`/`Limit`/`Reset-After` headers. That's why `on_429` calibrates from `Retry-After × observed_burst_count` rather than from headers.
- **Search routes empirically behave like a token bucket of capacity ~50, refill ~1 token / 5s.** The "burst then 1-per-Retry-After" pattern users observe is consistent with this.
- **Cloudflare ban: 10k 401/403/429 in 10 minutes → 24h IP lockout.** `Scope: shared` 429s don't count. This is a catastrophic risk for multi-hour runs; the proactive guard is what protects against it.
- **Reference implementations are purely reactive.** Both Undiscord and Discrub sleep on `retry_after` (JSON body) and never read `X-RateLimit-*` headers. This codebase is more sophisticated (predictive token-bucket + persistent learning) but the underlying truth — "you can't see headers on success" — is the same.

## Self-test coverage

`uv run python src/checkpoint.py` exercises:
- Channel upsert / status flip / exclusion lifecycle
- Message recording + `message_errored_before` lookup
- `summary()` aggregation
- Rate-limit-buckets save/load with the calibrated flag
- Update-on-conflict (idempotent overwrite)
- Route-buckets save/load
- `reset_rate_limit_state` clears both rate-limit tables but leaves channels/messages

`uv run python src/ratelimit.py` exercises:
- `BucketModel` cold-start passthrough: uncalibrated → `time_until_token` returns 0 (relies on global budget alone)
- `reconcile_from_headers` calibration transition + implicit `limit_n` lower-bound from `remaining + 1` when `X-RateLimit-Limit` is absent
- AIMD non-compounding: single-429, back-to-back compounding, success-interleaved no-spiral, cap, decayed-sf bump-back-up
- Production-scenario replay (alternating 429/OK 20×, sf bounded near `SAFETY_MULT_DECREASE`)
- `consecutive_ok` zeroes on every `on_429` regardless of branch
- `on_429` calibration paths: no `Retry-After` (no calibration), with `Retry-After` uncalibrated (derives `limit_n=burst, window=N*T`), zero-burst floor (`limit_n=1`), calibrated nudge-up (only `max(window, T*limit_n)`)
- `observed_burst_count` increment / reset semantics
- `time_until_token` math at various `(limit, window, safety_factor)` combinations
- `tokens_floor_until` blocks `time_until_token`, blocks refill, expires correctly, `max()` preserves the longer of two floors, concurrent `consume()` during floor doesn't erase it
- `_maybe_int` / `_maybe_float` exotic inputs: `inf`, `-inf`, `nan`, scientific notation, empty string, missing key
- `GlobalBudget` burst → steady-state pacing
- `GlobalBudget` `drain_seq` race detection (drain during acquire's sleep returns the pre-drain seq)

**Not covered by self-tests** (would need mock aiohttp / TUI driver / multi-process harness):
- `RateLimiter.request()` end-to-end (re-key path under live response, persistence-write-fail handling)
- TUI rendering / keybindings
- Multi-process concurrency on the same checkpoint.db
- `discover_all` exclusion resolution
- `clear_via_search` no-progress-abort behavior

## Known rough edges

Small, file-local issues that aren't bugs but are worth knowing:

- **TUI selection-set drift.** `app.selected` is a single dict shared across both tabs and across `h`/`r` toggles. Selecting a row, toggling `h`, and selecting another keeps both in the set even though only one shows a `*`. The modal title says "Deleting from N channels" — that count is the source of truth. Recovery: `Esc` twice, check the `selected: N` subtitle in the header, deselect rogues. (Filed in the team's task list.)
- **SIGTERM not caught.** `main()` only catches `KeyboardInterrupt` (SIGINT). `kill <pid>` results in a hard kill — not data-loss because every DB write is autocommit + WAL, but the periodic ticker doesn't get a final summary line and `rl.close()` isn't awaited. Use `Ctrl+C`.
- **Concurrency=1 in batch mode means single-channel saturation.** `iter_pending_channels` orders by `(type, id)`, so 1:1 DMs come first alphabetically. If your largest channel is a DM and you're running batch mode without `--concurrency >1`, the smaller channels behind it never start until the big one finishes. Use the TUI for multi-select, or set `--concurrency 5+` in batch.
- **Dry-run still calls `record_message`** with `error_reason="DRY_RUN"`. Those rows persist to `messages` table. Subsequent real run replaces them via `INSERT OR REPLACE` (same `(channel_id, message_id)` PK). Not a bug; just non-obvious.
- **Two adjacent `--reset-rate-limits` runs back-to-back hit cold-start twice.** First fresh learn, second wipes it. Only matters if iterating quickly.

## Configuration

`.env` (git-ignored) holds:
- `DISCORD_TOKEN` — user token (not a bot token; has no `Bearer` prefix)
- `EXCLUDE_GUILDS` — comma/whitespace-separated guild IDs to skip
- `EXCLUDE_USERS` — comma/whitespace-separated user IDs; DMs with any of these users (1:1 or group) are skipped

CLI flags `--exclude-guild` / `--exclude-user` (repeatable) stack additively on top of `.env` values. Removing an ID from `.env` (or omitting `--exclude-*` on a later run) re-pends previously-excluded channels — `apply_exclusion(False)` flips `excluded` rows back to `pending`, leaving `done`/`error` alone.

## Dependencies

Managed by uv (`pyproject.toml` + `uv.lock`). Direct deps: `aiohttp` (HTTP), `textual` (TUI). Everything else is stdlib (sqlite3, asyncio, argparse, logging, collections, dataclasses). `uv.lock` is committed for reproducible builds; `.venv/` is git-ignored.

## Schema migration pattern

`Checkpoint._migrate()` is idempotent and runs on every construction. To add a new column:
1. Add it to the `CREATE TABLE` block in `SCHEMA` (so fresh DBs get it).
2. Add a `PRAGMA table_info(<table>)` check in `_migrate()` and an `ALTER TABLE … ADD COLUMN …` only if missing (so existing DBs get it).
3. Read/write the column in `load_*` / `save_*` methods, defaulting via `m.get(...)` so an old DB with NULL doesn't crash.

Don't drop columns or change types — sqlite's `ALTER TABLE` doesn't support that and we'd need a full table rebuild. Don't add foreign keys; the autocommit + WAL pattern doesn't tolerate the locking overhead.

## Not present

No formal test suite — each module's `__main__` is the test suite. No lint config. The repo ships without CI; users are expected to run the three self-test commands above before pushing changes.
