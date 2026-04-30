"""Adaptive rate-limit-aware async HTTP client for the Discord user API.

Per-bucket token-bucket model that learns each Discord bucket's
(limit, window) from `X-RateLimit-*` headers, persists those parameters
across runs via the `Checkpoint`, and predictively paces requests so we
saturate the budget without tripping 429s. A separate ~40 req/s global
token bucket keeps high-concurrency runs from clipping the documented
50 req/s ceiling, and a sliding 401/403/429 counter guards against the
Cloudflare 10k-in-10min invalid-request 24h IP ban.

Anti-abuse floor (`default_delay`, 1.1s default / 0.55s aggressive)
applies only to DELETE requests — search / list / discovery routes pace
purely from the learned bucket model.
"""
from __future__ import annotations

import asyncio
import collections
import json as jsonlib
import logging
import time
from dataclasses import dataclass, field

import aiohttp

log = logging.getLogger(__name__)

DISCORD_API = "https://discord.com/api/v9"

# Cap retries only for *transport* failures (connection reset, DNS, etc.).
# 429s are explicit "wait and try again" instructions from Discord — they
# don't count toward this cap, so persistent throttling just makes the
# channel slow, not fatal. The user can cancel via Esc if they want to bail.
MAX_TRANSPORT_ATTEMPTS = 5

# Discord documents 50 req/s as the user-API ceiling. 40 leaves a 20%
# margin — the Cloudflare-side cost of tripping the 10k 401/403/429 in
# 10-min ban is 24h of IP lockout, far worse than running 5 req/s
# slower. The cheap 5/s headroom is insurance against burst tolerance
# variance + multi-process Discord usage on the same IP.
GLOBAL_CAPACITY = 40.0
GLOBAL_REFILL_PER_SEC = 40.0

# AIMD knobs for the per-bucket safety_factor. >1.0 stretches the
# inter-request gap; AIMD multiplies on 429 and additively decreases
# back toward 1.0 after a streak of successes.
SAFETY_MIN = 1.0
SAFETY_MAX = 4.0
SAFETY_MULT_DECREASE = 1.5
SAFETY_ADD_DECREASE = 0.1
SAFETY_DECREASE_AFTER_OK = 10

# Cold-start defaults. The very first request to a never-seen bucket
# fires after ~0 wait; the first response then populates the real model.
COLD_START_LIMIT = 1
COLD_START_WINDOW = 1.0

# Cloudflare bans the source IP for 24h after 10,000 401/403/429
# responses in any rolling 10-minute window. We track 4xx-with-counted-
# scope responses ourselves and force the global budget down once we
# see >10% of that ceiling, leaving 10x headroom for the user's other
# Discord traffic on the same IP. `X-RateLimit-Scope: shared` 429s do
# NOT count against the Cloudflare ceiling so we exclude them.
CLOUDFLARE_INVALID_WINDOW_SECONDS = 600.0
CLOUDFLARE_INVALID_THRESHOLD = 1000

# How often the throughput-visibility ticker logs an INFO summary line.
PROGRESS_LOG_INTERVAL_SECONDS = 5.0

# Persist sample_count drift only this often per bucket. Flushing it on
# every response is wasted SQLite churn — it's diagnostic, not a resume
# input that must be exact.
SAMPLE_COUNT_FLUSH_INTERVAL_SECONDS = 60.0


@dataclass
class BucketModel:
    """Per-Discord-bucket model. Keyed by `X-RateLimit-Bucket` header.

    Continuous-time token bucket: tokens refill at
    `limit / window / safety_factor` per second up to `limit`. Each request
    consumes one token. After every response we reconcile against the
    server's `X-RateLimit-Remaining` (truth) and re-derive `window` from
    `X-RateLimit-Reset-After`.

    `calibrated` flips True the first time we see any rate-limit header on
    a response. Until then the model is a passthrough (zero per-bucket
    wait — only the global budget gates us). This matters because Discord
    returns no rate-limit headers at all on several user-API endpoints
    (`/users/@me*`, `/channels/{id}/messages`, etc.); without this flag,
    cold-start defaults clamp those routes at ~1 req/s indefinitely.
    """
    bucket_key: str
    limit_n: int = COLD_START_LIMIT
    window_seconds: float = COLD_START_WINDOW
    tokens: float = float(COLD_START_LIMIT)
    last_observation: float = field(default_factory=time.monotonic)
    last_request_at: float = 0.0
    safety_factor: float = 1.0
    consecutive_ok: int = 0
    sample_count: int = 0
    calibrated: bool = False
    # Successes since the last 429 (or since model creation, if no 429
    # yet). Used as the bucket-capacity estimate when we calibrate from
    # a Retry-After 429 — on the assumption that "we just got 429'd
    # after N successful requests" is a good proxy for "Discord's
    # bucket capacity is N."
    observed_burst_count: int = 0
    # When > now, refill is disabled and time_until_token gates on this
    # deadline. Set after a 429 to keep `tokens` at 0 across the
    # Retry-After sleep, even if a concurrent consume() on the same
    # pooled model also touches the bucket. Survives concurrent updates
    # because consume() never overwrites it (it only ever monotonically
    # rises via max()).
    tokens_floor_until: float = 0.0
    # Persistence dirty-flag state: snapshot of the last
    # (limit_n, window_seconds, safety_factor, calibrated) tuple we
    # wrote to disk. We skip the SQLite write entirely when nothing
    # changed — at 500+ responses/sec the user-API endpoints we hit
    # never return rate-limit headers, so this snapshot stays static
    # and we'd otherwise be doing ~30ms/sec of pointless fsync churn.
    _last_persisted: tuple | None = field(default=None, repr=False, compare=False)
    _last_sample_flush_at: float = field(default=0.0, repr=False, compare=False)

    def _rate(self) -> float:
        return self.limit_n / max(self.window_seconds, 0.001) / max(self.safety_factor, 0.001)

    def refill(self, now: float) -> None:
        # While in a post-429 floor window, tokens are pinned at 0 — don't
        # let concurrent consume() / time_until_token() refills erase the
        # bucket-empty signal. The caller is sleeping Retry-After before
        # the next request anyway.
        if now < self.tokens_floor_until:
            self.last_observation = now
            return
        elapsed = now - self.last_observation
        if elapsed <= 0:
            return
        self.tokens = min(float(self.limit_n), self.tokens + elapsed * self._rate())
        self.last_observation = now

    def time_until_token(self, now: float) -> float:
        """Seconds until the next token is available, or 0.0 if one is ready."""
        if now < self.tokens_floor_until:
            # Floor wins regardless of calibration: a concrete
            # Retry-After from Discord trumps the model's own pacing.
            return self.tokens_floor_until - now
        if not self.calibrated:
            # Passthrough: rely on the global budget alone until Discord
            # actually tells us this route has a per-bucket limit.
            return 0.0
        self.refill(now)
        if self.tokens >= 1.0:
            return 0.0
        deficit = 1.0 - self.tokens
        return deficit / max(self._rate(), 0.001)

    def consume(self, now: float) -> None:
        """Spend one token. Called immediately before sending the request."""
        self.refill(now)
        self.tokens = max(0.0, self.tokens - 1.0)
        self.last_observation = now
        self.last_request_at = now

    def reconcile_from_headers(
        self,
        now: float,
        remaining: int | None,
        reset_after: float | None,
        limit_seen: int | None,
    ) -> None:
        """Pull truth from the response. The server's view wins."""
        if remaining is not None or reset_after is not None or limit_seen is not None:
            self.calibrated = True
        if limit_seen is not None and limit_seen > 0:
            self.limit_n = limit_seen
        elif remaining is not None and remaining + 1 > self.limit_n:
            # Implicit lower bound: we just consumed one slot and `remaining`
            # are left, so the bucket has at least `remaining + 1` capacity.
            self.limit_n = remaining + 1
        if remaining is not None:
            self.tokens = float(max(0, remaining))
        if reset_after is not None and reset_after > 0:
            self.window_seconds = max(reset_after, 0.05)
        self.last_observation = now

    def on_success(self) -> bool:
        """Record a 2xx. Returns True if AIMD just decreased safety_factor
        (caller may want to persist).
        """
        self.consecutive_ok += 1
        self.observed_burst_count += 1
        self.sample_count += 1
        if (
            self.consecutive_ok >= SAFETY_DECREASE_AFTER_OK
            and self.safety_factor > SAFETY_MIN
        ):
            self.safety_factor = max(SAFETY_MIN, self.safety_factor - SAFETY_ADD_DECREASE)
            self.consecutive_ok = 0
            return True
        return False

    def on_429(self, retry_after_seconds: float = 0.0) -> None:
        """Record a 429. AIMD bumps the safety factor (non-compounding when
        successes happened since the last 429), and `Retry-After` is used
        as a calibration signal — a 429 after N successes with `Retry-After=T`
        implies a token-bucket of capacity ≈ N refilling at 1 token per
        `T` seconds (i.e. `window_seconds = N * T`).
        """
        # AIMD: compound only on TRULY back-to-back 429s (no successes
        # since the last 429). With ≥1 success in between, we're already
        # at roughly the right rate — bumping to a fresh 1.5x slowdown
        # is fine, but compounding past that spirals: once you're at
        # sf=3.38 you need 240 OKs to recover.
        if self.consecutive_ok == 0:
            self.safety_factor = min(SAFETY_MAX, self.safety_factor * SAFETY_MULT_DECREASE)
        else:
            self.safety_factor = min(SAFETY_MAX, max(self.safety_factor, SAFETY_MULT_DECREASE))
        self.consecutive_ok = 0

        if retry_after_seconds > 0:
            if not self.calibrated:
                # Derive token-bucket parameters from observed behavior:
                # we just hit 429 after `observed_burst_count` successes,
                # and Discord said the next slot is `retry_after_seconds`
                # away. That implies steady-state rate = 1 / retry_after,
                # with capacity = the burst we observed.
                burst = max(self.observed_burst_count, 1)
                self.limit_n = burst
                self.window_seconds = retry_after_seconds * burst
                self.calibrated = True
            else:
                # Already calibrated. Nudge window_seconds toward
                # retry_after * limit_n if it's drifted, but don't speed
                # up — Discord can mid-window-reset us, and shrinking
                # `window_seconds` would over-fire. Use max() so only
                # observed slowdowns bite.
                target = retry_after_seconds * self.limit_n
                self.window_seconds = max(self.window_seconds, target)

        # Reset the burst counter for the next observation cycle.
        self.observed_burst_count = 0


class GlobalBudget:
    """Aggregate request-rate cap (~40 req/s, burst up to capacity).

    A token-bucket: the first `capacity` acquires return immediately, then
    steady-state at `refill_per_sec`. On a real GLOBAL 429 we drain the
    bucket via `drain()` and the limiter pauses on `global_pause_until`.

    Pre-deduction (`tokens -= 1` even when going negative) plus
    `wait = deficit / rate` keeps the math fair under contention without
    holding the lock during sleep — the next caller sees the negative
    balance and queues behind.

    `drain_seq` increments each time `drain()` runs. Callers that slept
    inside `acquire()` re-check the seq when they wake up — if a drain
    happened during their sleep, they re-check `global_pause_until` in
    the request loop before sending. This closes the window where
    queued waiters could fire requests during a freshly-installed
    GLOBAL 429 pause.
    """
    def __init__(
        self,
        capacity: float = GLOBAL_CAPACITY,
        refill_per_sec: float = GLOBAL_REFILL_PER_SEC,
    ):
        self.capacity = capacity
        self.refill_per_sec = refill_per_sec
        self.tokens = capacity
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self.drain_seq: int = 0

    def _refill(self, now: float) -> None:
        elapsed = now - self.last_refill
        if elapsed > 0:
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_sec)
            self.last_refill = now

    async def acquire(self) -> int:
        """Acquire a token. Returns the `drain_seq` snapshot taken at
        entry. Callers that observed a different `drain_seq` after wake
        should re-check `global_pause_until` before sending."""
        async with self._lock:
            seq = self.drain_seq
            now = time.monotonic()
            self._refill(now)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return seq
            deficit = 1.0 - self.tokens
            wait = deficit / max(self.refill_per_sec, 0.001)
            self.tokens -= 1.0  # reserve; refill during sleep brings it to 0
        if wait > 0:
            log.debug("global budget wait=%.3fs (tokens=%.2f)", wait, self.tokens)
            await asyncio.sleep(wait)
        return seq

    def drain(self) -> None:
        self.tokens = 0.0
        self.last_refill = time.monotonic()
        self.drain_seq += 1


class RateLimiter:
    """Adaptive Discord rate limiter.

    Public surface:
      - `__init__(session, default_delay=1.1, checkpoint=None)`
      - `default_delay` mutable attribute — flipped live by the TUI's
        aggressive toggle (now means "DELETE-only floor").
      - `request(method, path, *, bucket=None, **kwargs) -> (status, body)`
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        default_delay: float = 1.1,
        checkpoint=None,  # checkpoint.Checkpoint | None
    ):
        self.session = session
        self.default_delay = default_delay
        self.checkpoint = checkpoint
        self.global_pause_until: float = 0.0
        self.global_budget = GlobalBudget()

        # caller-supplied route key (e.g. "search-c-{cid}") -> Discord
        # bucket hash. Until we've seen a response carrying
        # X-RateLimit-Bucket for the route, the route key doubles as the
        # model key (cold-start placeholder).
        self.route_index: dict[str, str] = {}

        # models keyed by Discord bucket hash (or route-key placeholder).
        self.bucket_models: dict[str, BucketModel] = {}

        # per-route serialization. Keyed by caller-supplied key so
        # per-channel ordering (e.g. del-c-{cid}) is preserved even if
        # several routes happen to share one Discord bucket.
        self._route_locks: dict[str, asyncio.Lock] = {}

        # routes for which we've already emitted the first-response header
        # dump (DEBUG diagnostic; one log line per route per process).
        self._dumped_routes: set[str] = set()

        # Throughput / health counters for the periodic ticker.
        self._stats = {"requests": 0, "429s": 0, "global_429s": 0}
        # Sliding window of timestamps for the Cloudflare invalid-request
        # guard (#23). Holds 401/403/429-non-shared response times in the
        # last CLOUDFLARE_INVALID_WINDOW_SECONDS.
        self._invalid_request_times: collections.deque[float] = collections.deque()
        self._cloudflare_throttle_active: bool = False
        # Counter for persistence failures, surfaced at run-end (#12).
        self._persistence_errors: int = 0
        # Spawned lazily by the first request — constructing in __init__
        # would fail outside an event loop, and the limiter is sometimes
        # built before the loop is running.
        self._ticker_task: asyncio.Task | None = None
        self._ticker_started_at: float | None = None
        self._closed: bool = False

        if checkpoint is not None:
            self._warm_load()

    def _warm_load(self) -> None:
        try:
            models = self.checkpoint.load_bucket_models()
            routes = self.checkpoint.load_route_buckets()
        except Exception:
            log.exception("warm-load from checkpoint failed; starting cold")
            return
        for bucket_key, m in models.items():
            bm = BucketModel(
                bucket_key=bucket_key,
                limit_n=m["limit_n"] or COLD_START_LIMIT,
                window_seconds=m["window_seconds"] or COLD_START_WINDOW,
                tokens=float(m["limit_n"] or COLD_START_LIMIT),
                safety_factor=m["safety_factor"] or 1.0,
                sample_count=m["sample_count"] or 0,
                calibrated=m.get("calibrated", False),
            )
            # Seed dirty-flag with the just-loaded values so we don't
            # immediately rewrite them on the first response.
            bm._last_persisted = (
                bm.limit_n, bm.window_seconds, bm.safety_factor, bm.calibrated,
            )
            bm._last_sample_flush_at = time.monotonic()
            self.bucket_models[bucket_key] = bm
        self.route_index.update(routes)
        if models or routes:
            log.info(
                "warm-loaded %d bucket model(s) and %d route mapping(s) from checkpoint",
                len(models), len(routes),
            )

    def _route_lock(self, route_key: str) -> asyncio.Lock:
        lock = self._route_locks.get(route_key)
        if lock is None:
            lock = asyncio.Lock()
            self._route_locks[route_key] = lock
        return lock

    def _model_for(self, route_key: str) -> BucketModel:
        bucket_key = self.route_index.get(route_key, route_key)
        m = self.bucket_models.get(bucket_key)
        if m is None:
            m = BucketModel(bucket_key=bucket_key)
            self.bucket_models[bucket_key] = m
        return m

    def _persist_model(self, model: BucketModel) -> None:
        if self.checkpoint is None:
            return
        # Dirty-flag (#1): only write when the persistable parameters
        # actually changed. sample_count drift gets a separate periodic
        # flush so it doesn't dominate disk I/O.
        snapshot = (
            model.limit_n,
            model.window_seconds,
            model.safety_factor,
            model.calibrated,
        )
        now = time.monotonic()
        sample_due = (
            now - model._last_sample_flush_at >= SAMPLE_COUNT_FLUSH_INTERVAL_SECONDS
        )
        if model._last_persisted == snapshot and not sample_due:
            return
        try:
            self.checkpoint.save_bucket_model(
                model.bucket_key,
                model.limit_n,
                model.window_seconds,
                model.safety_factor,
                model.sample_count,
                model.calibrated,
            )
        except Exception:
            self._persistence_errors += 1
            log.exception("save_bucket_model failed for %s", model.bucket_key)
            return
        model._last_persisted = snapshot
        if sample_due:
            model._last_sample_flush_at = now

    def _persist_route(self, route_key: str, bucket_key: str) -> None:
        if self.checkpoint is None:
            return
        try:
            self.checkpoint.save_route_bucket(route_key, bucket_key)
        except Exception:
            self._persistence_errors += 1
            log.exception("save_route_bucket failed for %s -> %s", route_key, bucket_key)

    def _delete_bucket_model_db(self, bucket_key: str) -> None:
        if self.checkpoint is None:
            return
        try:
            self.checkpoint.delete_bucket_model(bucket_key)
        except Exception:
            self._persistence_errors += 1
            log.exception("delete_bucket_model failed for %s", bucket_key)

    def _ensure_ticker(self) -> None:
        if self._ticker_task is not None or self._closed:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # no loop yet; first request inside one will spawn it
        self._ticker_started_at = time.monotonic()
        self._ticker_task = loop.create_task(self._periodic_logger())

    async def _periodic_logger(self) -> None:
        """Emit one INFO line every PROGRESS_LOG_INTERVAL_SECONDS so the
        user can see we're alive even when the limiter isn't producing
        any per-route DEBUG output (the no-headers user-API endpoints
        leave the model uncalibrated, so `bucket=... scheduled wait`
        never fires).
        """
        try:
            prev_requests = 0
            prev_t = time.monotonic()
            while not self._closed:
                await asyncio.sleep(PROGRESS_LOG_INTERVAL_SECONDS)
                now = time.monotonic()
                requests = self._stats["requests"]
                interval_requests = requests - prev_requests
                interval_secs = max(now - prev_t, 0.001)
                rate = interval_requests / interval_secs
                prev_requests = requests
                prev_t = now
                # Show only the buckets currently above sf=1.0 — those
                # are the throttled ones the user cares about. Anything
                # at sf=1.0 is irrelevant noise.
                live_sfs = sorted(
                    (m.safety_factor for m in self.bucket_models.values()
                     if m.safety_factor > 1.0 + 1e-6),
                    reverse=True,
                )[:5]
                sf_str = (
                    "[" + ", ".join(f"{s:.2f}" for s in live_sfs) + "]"
                    if live_sfs else "[]"
                )
                log.info(
                    "rl: req=%d (+%d, %.1f/s) 429s=%d global_429s=%d "
                    "invalid_window=%d sf=%s",
                    requests, interval_requests, rate,
                    self._stats["429s"], self._stats["global_429s"],
                    len(self._invalid_request_times), sf_str,
                )
        except asyncio.CancelledError:
            return

    def _record_invalid_request(self, status: int, scope: str | None) -> None:
        """Track 401/403/429 responses for the Cloudflare 10k/10min guard.

        `X-RateLimit-Scope: shared` 429s are excluded — Cloudflare doesn't
        count them. Anything else (user/global scope, or no scope at all)
        does count.
        """
        if status not in (401, 403, 429):
            return
        if status == 429 and (scope or "").lower() == "shared":
            return
        self._invalid_request_times.append(time.monotonic())

    def _check_invalid_request_window(self) -> None:
        """Evict timestamps older than the window and react if we're
        approaching Cloudflare's invalid-request ceiling.
        """
        if not self._invalid_request_times:
            if self._cloudflare_throttle_active:
                self._cloudflare_throttle_active = False
                self.global_budget.capacity = GLOBAL_CAPACITY
                self.global_budget.refill_per_sec = GLOBAL_REFILL_PER_SEC
                log.info(
                    "Cloudflare invalid-request window cleared, "
                    "global budget restored to %.0f req/s", GLOBAL_REFILL_PER_SEC,
                )
            return
        now = time.monotonic()
        cutoff = now - CLOUDFLARE_INVALID_WINDOW_SECONDS
        dq = self._invalid_request_times
        while dq and dq[0] < cutoff:
            dq.popleft()
        n = len(dq)
        if n > CLOUDFLARE_INVALID_THRESHOLD and not self._cloudflare_throttle_active:
            # Halve the global budget. Stays in effect until the deque
            # drops back below threshold (or fully drains as 4xx ages out).
            self._cloudflare_throttle_active = True
            self.global_budget.capacity = GLOBAL_CAPACITY * 0.5
            self.global_budget.refill_per_sec = GLOBAL_REFILL_PER_SEC * 0.5
            log.warning(
                "Cloudflare invalid-request guard tripped: %d 4xx in last "
                "%.0fs (threshold %d). Halving global budget to %.0f req/s.",
                n, CLOUDFLARE_INVALID_WINDOW_SECONDS,
                CLOUDFLARE_INVALID_THRESHOLD, self.global_budget.refill_per_sec,
            )
        elif n <= CLOUDFLARE_INVALID_THRESHOLD // 2 and self._cloudflare_throttle_active:
            self._cloudflare_throttle_active = False
            self.global_budget.capacity = GLOBAL_CAPACITY
            self.global_budget.refill_per_sec = GLOBAL_REFILL_PER_SEC
            log.info(
                "Cloudflare invalid-request guard recovered: %d 4xx in last "
                "%.0fs. Global budget restored to %.0f req/s.",
                n, CLOUDFLARE_INVALID_WINDOW_SECONDS, GLOBAL_REFILL_PER_SEC,
            )

    async def close(self) -> None:
        """Cancel the background ticker. Safe to call multiple times."""
        self._closed = True
        task = self._ticker_task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._ticker_task = None

    async def request(
        self,
        method: str,
        path: str,
        *,
        bucket: str | None = None,
        **kwargs,
    ) -> tuple[int, object]:
        """Returns (status_code, body). Body is json-decoded when possible."""
        url = path if path.startswith("http") else DISCORD_API + path
        route_key = bucket or f"{method} {path}"
        is_delete = method.upper() == "DELETE"

        # Spawn the periodic ticker on first use — we now have a running
        # loop guaranteed.
        self._ensure_ticker()
        # Evict expired entries from the Cloudflare invalid-request
        # window and react if we just crossed/uncrossed the threshold.
        self._check_invalid_request_window()

        transport_failures = 0
        while True:
            # Honor any in-effect global pause first (set by a prior GLOBAL 429).
            now = time.monotonic()
            if now < self.global_pause_until:
                wait = self.global_pause_until - now
                log.info("global rate-limit pause, sleeping %.2fs", wait)
                await asyncio.sleep(wait)

            # Proactive global cap (~40 req/s). Returns the drain_seq we
            # observed before our queued sleep — if it's bumped by the
            # time we wake up, a GLOBAL 429 was installed during our
            # acquire-sleep and we MUST re-check the pause boundary
            # before sending (#4).
            seq_before = self.global_budget.drain_seq
            seq_at_acquire = await self.global_budget.acquire()
            if (
                self.global_budget.drain_seq != seq_before
                or self.global_budget.drain_seq != seq_at_acquire
            ):
                now = time.monotonic()
                if now < self.global_pause_until:
                    wait = self.global_pause_until - now
                    log.info(
                        "global pause raced acquire(); sleeping %.2fs", wait,
                    )
                    await asyncio.sleep(wait)

            # Per-route serialization (preserves channel ordering).
            async with self._route_lock(route_key):
                model = self._model_for(route_key)
                now = time.monotonic()

                # Predictive bucket-token wait. With a calibrated model this
                # paces us just under the bucket's allowance — no 429.
                wait_bucket = model.time_until_token(now)
                if wait_bucket > 0:
                    log.debug(
                        "bucket=%s scheduled wait=%.2fs (tokens=%.2f sf=%.2f)",
                        model.bucket_key, wait_bucket, model.tokens, model.safety_factor,
                    )
                    await asyncio.sleep(wait_bucket)
                    now = time.monotonic()

                # DELETE-only anti-abuse floor.
                if is_delete:
                    floor_remaining = self.default_delay - (now - model.last_request_at)
                    if floor_remaining > 0:
                        await asyncio.sleep(floor_remaining)
                        now = time.monotonic()

                model.consume(now)

                try:
                    async with self.session.request(method, url, **kwargs) as resp:
                        status = resp.status
                        headers = resp.headers
                        body_text = await resp.text()
                except aiohttp.ClientError as ex:
                    transport_failures += 1
                    if transport_failures >= MAX_TRANSPORT_ATTEMPTS:
                        raise RuntimeError(
                            f"transport failed {MAX_TRANSPORT_ATTEMPTS}x on "
                            f"{method} {url}: {ex}"
                        ) from ex
                    log.warning(
                        "transport error on %s %s: %s — retry %d/%d",
                        method, url, ex, transport_failures, MAX_TRANSPORT_ATTEMPTS,
                    )
                    await asyncio.sleep(2.0 * transport_failures)
                    continue

            body = _decode(body_text)

            # Read every rate-limit header we care about (more than the
            # legacy code did — bucket hash + limit are new).
            bucket_hash = headers.get("X-RateLimit-Bucket")
            limit_seen = _maybe_int(headers, "X-RateLimit-Limit")
            remaining = _maybe_int(headers, "X-RateLimit-Remaining")
            reset_after = _maybe_float(headers, "X-RateLimit-Reset-After")

            if route_key not in self._dumped_routes:
                self._dumped_routes.add(route_key)
                log.debug(
                    "first response on route=%s status=%d "
                    "X-RateLimit-Limit=%r Remaining=%r Reset-After=%r "
                    "Bucket=%r Global=%r Scope=%r",
                    route_key, status,
                    headers.get("X-RateLimit-Limit"),
                    headers.get("X-RateLimit-Remaining"),
                    headers.get("X-RateLimit-Reset-After"),
                    headers.get("X-RateLimit-Bucket"),
                    headers.get("X-RateLimit-Global"),
                    headers.get("X-RateLimit-Scope"),
                )

            now = time.monotonic()

            # If we just learned the real Discord bucket hash, re-key the model.
            if bucket_hash and bucket_hash != model.bucket_key:
                old_bucket_key = model.bucket_key
                existing = self.bucket_models.get(bucket_hash)
                if existing is None:
                    self.bucket_models.pop(old_bucket_key, None)
                    model.bucket_key = bucket_hash
                    self.bucket_models[bucket_hash] = model
                else:
                    # A different route already populated this bucket; pool
                    # them. Reconcile-from-headers below will overwrite
                    # existing's tokens with the latest server count, so the
                    # placeholder's local consume() is harmlessly discarded.
                    existing.safety_factor = max(existing.safety_factor, model.safety_factor)
                    existing.sample_count += model.sample_count
                    # Floor must merge too — if either model was in a
                    # post-429 sleep, preserve the longer deadline.
                    existing.tokens_floor_until = max(
                        existing.tokens_floor_until, model.tokens_floor_until,
                    )
                    self.bucket_models.pop(old_bucket_key, None)
                    model = existing
                # Re-key any other routes that point at the old key (#6):
                # warm-loaded routes whose route_index entry still says
                # H_old would otherwise cold-start a fresh placeholder
                # next call.
                orphan_routes = [
                    rk for rk, bk in self.route_index.items()
                    if bk == old_bucket_key and rk != route_key
                ]
                for rk in orphan_routes:
                    self.route_index[rk] = bucket_hash
                    self._persist_route(rk, bucket_hash)
                self.route_index[route_key] = bucket_hash
                self._persist_route(route_key, bucket_hash)
                # And drop the now-orphaned DB row to keep
                # rate_limit_buckets from growing with rotation history.
                if old_bucket_key != bucket_hash:
                    self._delete_bucket_model_db(old_bucket_key)

            scope = headers.get("X-RateLimit-Scope")

            if status == 429:
                retry_after = _retry_after(body, headers)
                is_global = str(headers.get("X-RateLimit-Global", "")).lower() == "true"
                self._stats["429s"] += 1
                if is_global:
                    self._stats["global_429s"] += 1
                    self.global_pause_until = time.monotonic() + retry_after + 0.5
                    self.global_budget.drain()
                    log.warning(
                        "GLOBAL 429, pausing all buckets for %.2fs (scope=%r)",
                        retry_after, scope,
                    )
                else:
                    log.info(
                        "bucket 429 on %s, sleeping %.2fs (sf %.2f -> %.2f scope=%r)",
                        route_key, retry_after, model.safety_factor,
                        min(SAFETY_MAX, model.safety_factor * SAFETY_MULT_DECREASE),
                        scope,
                    )
                self._record_invalid_request(status, scope)
                model.on_429(retry_after_seconds=retry_after)
                model.reconcile_from_headers(now, remaining, reset_after, limit_seen)
                # Don't let token refill think the upcoming sleep is "free
                # time" — the bucket is genuinely empty on Discord's side.
                # `tokens_floor_until` is the post-sleep boundary; refill()
                # and time_until_token() honor it, and consume() on a
                # concurrent route sharing this pooled model can't erase
                # it (consume only writes `last_observation`, not the
                # floor). Use max() so a longer pre-existing floor wins.
                model.tokens_floor_until = max(
                    model.tokens_floor_until, now + retry_after
                )
                model.tokens = 0.0
                self._persist_model(model)
                await asyncio.sleep(retry_after + 0.5)
                continue

            # Success path.
            self._stats["requests"] += 1
            if status in (401, 403):
                self._record_invalid_request(status, scope)
            model.reconcile_from_headers(now, remaining, reset_after, limit_seen)
            sf_changed = model.on_success()
            self._persist_model(model)
            if sf_changed:
                log.debug(
                    "bucket=%s AIMD recovered, sf=%.2f",
                    model.bucket_key, model.safety_factor,
                )

            return status, body


def _decode(body_text: str):
    if not body_text:
        return None
    try:
        return jsonlib.loads(body_text)
    except jsonlib.JSONDecodeError:
        return body_text


def _retry_after(body, headers) -> float:
    if isinstance(body, dict) and "retry_after" in body:
        try:
            return float(body["retry_after"])
        except (TypeError, ValueError):
            pass
    try:
        return float(headers.get("Retry-After", "1"))
    except (TypeError, ValueError):
        return 1.0


def _maybe_int(headers, key):
    if key not in headers:
        return None
    raw = headers[key]
    # Discord sometimes ships ints as decimal-formatted strings ("5.0"),
    # so go through float() to be safe. OverflowError is here for the
    # vanishingly-rare "inf" case (int(float("inf")) raises it) — we'd
    # rather treat exotic header values as missing than crash the worker.
    try:
        return int(float(raw))
    except (ValueError, TypeError, OverflowError):
        return None


def _maybe_float(headers, key):
    if key not in headers:
        return None
    try:
        return float(headers[key])
    except (ValueError, TypeError, OverflowError):
        return None


# --- self-test ----------------------------------------------------

if __name__ == "__main__":
    # Exercises BucketModel + GlobalBudget in isolation. No aiohttp, no
    # network. Same convention as checkpoint.py's __main__ block.
    import math

    # ---- BucketModel: cold-start passthrough until calibrated ----
    bm = BucketModel("test")
    assert bm.calibrated is False
    # Uncalibrated: time_until_token always 0 (rely on global budget).
    bm.consume(0.0)
    assert bm.time_until_token(0.0) == 0.0, "uncalibrated should be passthrough"
    assert bm.time_until_token(0.001) == 0.0
    # 100 imaginary requests, still no headers seen -> still passthrough.
    for _ in range(100):
        bm.consume(0.0)
        assert bm.time_until_token(0.0) == 0.0

    # First response with headers calibrates and switches to bucket math.
    bm.reconcile_from_headers(0.1, remaining=4, reset_after=4.5, limit_seen=5)
    assert bm.calibrated is True
    assert bm.limit_n == 5, bm.limit_n
    assert bm.tokens == 4.0, bm.tokens
    assert math.isclose(bm.window_seconds, 4.5, rel_tol=0.01), bm.window_seconds
    # 4 tokens left, no wait
    assert bm.time_until_token(0.1) == 0.0

    # Implicit limit_n inference when X-RateLimit-Limit absent
    bm_imp = BucketModel("imp")
    bm_imp.reconcile_from_headers(0.0, remaining=7, reset_after=3.0, limit_seen=None)
    assert bm_imp.calibrated is True
    assert bm_imp.limit_n == 8, bm_imp.limit_n  # remaining + 1

    # AIMD: 429 multiplies safety_factor by 1.5 only when consecutive_ok==0
    bm.on_429()
    assert math.isclose(bm.safety_factor, 1.5), bm.safety_factor
    # Back-to-back 429 (no successes between) compounds: 1.5 -> 2.25
    bm.on_429()
    assert math.isclose(bm.safety_factor, 2.25), bm.safety_factor
    # 1 success then 429 should NOT compound past the existing 2.25
    # (we just got a success — we're at a workable rate already).
    bm.on_success()
    bm.on_429()
    assert math.isclose(bm.safety_factor, 2.25), \
        f"non-consecutive 429 should not compound; got {bm.safety_factor}"
    # 10 successes additively decrease back by 0.1
    for _ in range(10):
        bm.on_success()
    assert math.isclose(bm.safety_factor, 2.15, rel_tol=0.01), bm.safety_factor

    # Production-scenario replay (#28): alternating 429/OK on a calibrated
    # bucket must stay bounded near SAFETY_MULT_DECREASE rather than spiraling.
    bm_alt = BucketModel("alt", calibrated=True, limit_n=5, window_seconds=5.0)
    for _ in range(20):
        bm_alt.on_429()
        bm_alt.on_success()
    assert bm_alt.safety_factor <= SAFETY_MULT_DECREASE + 1e-6, bm_alt.safety_factor

    # consecutive_ok zeroes on every on_429 regardless of branch taken (#28).
    bm_co = BucketModel("co", calibrated=True, limit_n=5, window_seconds=5.0)
    for _ in range(7):
        bm_co.on_success()
    assert bm_co.consecutive_ok == 7
    bm_co.on_429()
    assert bm_co.consecutive_ok == 0
    bm_co.on_429()
    assert bm_co.consecutive_ok == 0

    # Cap interaction with the else-branch (#28): sf at SAFETY_MAX, then
    # success-then-429 -> stays at cap (else: max(SAFETY_MAX, 1.5) = SAFETY_MAX).
    bm_cap = BucketModel("cap", calibrated=True, limit_n=5, window_seconds=5.0,
                         safety_factor=SAFETY_MAX)
    bm_cap.on_success()
    bm_cap.on_429()
    assert math.isclose(bm_cap.safety_factor, SAFETY_MAX), bm_cap.safety_factor

    # Decayed-sf branch coverage (#28): sf already below SAFETY_MULT_DECREASE,
    # success-then-429 must bump back up to the floor.
    bm_dec = BucketModel("dec", calibrated=True, limit_n=5, window_seconds=5.0,
                         safety_factor=1.2)
    bm_dec.on_success()
    bm_dec.on_429()
    assert math.isclose(bm_dec.safety_factor, SAFETY_MULT_DECREASE), bm_dec.safety_factor

    # on_429 calibration paths (#25 revised):
    #
    # (a) on_429 with no Retry-After: AIMD only, no calibration. The
    #     model stays uncalibrated until we get a Retry-After.
    bm_no_ra = BucketModel("no-ra")
    assert not bm_no_ra.calibrated
    bm_no_ra.on_429()  # default retry_after=0.0
    assert not bm_no_ra.calibrated, "no Retry-After must not calibrate"
    assert bm_no_ra.limit_n == COLD_START_LIMIT
    assert bm_no_ra.window_seconds == COLD_START_WINDOW

    # (b) on_429 WITH Retry-After on uncalibrated bucket — derive
    #     (limit_n, window_seconds) from the observed burst + Retry-After.
    #     A 429 after 52 successes with retry_after=4.59s implies a
    #     token bucket of capacity 52 refilling at 1 token / 4.59s, i.e.
    #     window_seconds = 52 * 4.59 ≈ 238.7s.
    bm_cal = BucketModel("cal-from-ra")
    for _ in range(52):
        bm_cal.on_success()
    assert bm_cal.observed_burst_count == 52
    assert not bm_cal.calibrated
    bm_cal.on_429(retry_after_seconds=4.59)
    assert bm_cal.calibrated, "Retry-After must calibrate uncalibrated bucket"
    assert bm_cal.limit_n == 52, bm_cal.limit_n
    assert math.isclose(bm_cal.window_seconds, 52 * 4.59, rel_tol=0.001), bm_cal.window_seconds
    # observed_burst_count resets so the next cycle starts fresh.
    assert bm_cal.observed_burst_count == 0
    # Steady-state pacing now matches Discord's actual cadence: with
    # tokens=0 (set by the request loop), time_until_token at sf=1.5
    # (post-AIMD bump) gives ~retry_after * 1.5.
    bm_cal.tokens = 0.0
    bm_cal.last_observation = 0.0
    # rate = 52 / (52*4.59) / 1.5 = 1 / (4.59 * 1.5)
    expected_wait = 4.59 * 1.5
    actual_wait = bm_cal.time_until_token(0.0)
    assert math.isclose(actual_wait, expected_wait, rel_tol=0.01), actual_wait

    # (c) Even with Retry-After but zero burst observed, capacity floors
    #     at 1 (we still saw at least the request that just got 429'd).
    bm_zero = BucketModel("zero-burst")
    bm_zero.on_429(retry_after_seconds=5.0)
    assert bm_zero.calibrated
    assert bm_zero.limit_n == 1, bm_zero.limit_n
    assert math.isclose(bm_zero.window_seconds, 5.0), bm_zero.window_seconds

    # (d) Calibrated path: on_429 with Retry-After only NUDGES UP, never
    #     shortens window_seconds. Discord can mid-window-reset us, and
    #     shrinking would over-fire.
    bm_nudge = BucketModel(
        "nudge", calibrated=True, limit_n=10, window_seconds=100.0,
    )
    # target = retry_after * limit_n = 1.0 * 10 = 10.0; current=100.0 wins.
    bm_nudge.on_429(retry_after_seconds=1.0)
    assert math.isclose(bm_nudge.window_seconds, 100.0), bm_nudge.window_seconds
    # target = 20.0 * 10 = 200.0; bumps up.
    bm_nudge.on_429(retry_after_seconds=20.0)
    assert math.isclose(bm_nudge.window_seconds, 200.0), bm_nudge.window_seconds

    # (e) observed_burst_count increments on every on_success and resets
    #     on every on_429.
    bm_burst = BucketModel("burst")
    for _ in range(7):
        bm_burst.on_success()
    assert bm_burst.observed_burst_count == 7
    bm_burst.on_429()
    assert bm_burst.observed_burst_count == 0
    bm_burst.on_success()
    assert bm_burst.observed_burst_count == 1

    # time_until_token: calibrated empty 2/2s bucket needs 1s for 1 token
    bm2 = BucketModel("t2", limit_n=2, window_seconds=2.0, tokens=0.0,
                     last_observation=0.0, calibrated=True)
    wait = bm2.time_until_token(0.0)
    assert math.isclose(wait, 1.0, rel_tol=0.01), wait
    bm2.refill(2.0)
    assert math.isclose(bm2.tokens, 2.0, rel_tol=0.01), bm2.tokens

    # safety_factor stretches the gap
    bm3 = BucketModel("t3", limit_n=2, window_seconds=2.0, tokens=0.0,
                     safety_factor=2.0, last_observation=0.0, calibrated=True)
    # rate = 2/2/2 = 0.5 tok/s; 1 deficit -> wait 2.0s
    assert math.isclose(bm3.time_until_token(0.0), 2.0, rel_tol=0.01)

    # ---- tokens_floor_until (#22, #28) ----
    # Floor blocks time_until_token regardless of calibration.
    bm_floor = BucketModel(
        "floor", calibrated=True, limit_n=5, window_seconds=5.0, tokens=5.0,
    )
    bm_floor.tokens_floor_until = 100.0
    # At t=99.5 we still have 0.5s of floor remaining.
    assert math.isclose(bm_floor.time_until_token(99.5), 0.5, abs_tol=0.01)
    # Floor also overrides passthrough on uncalibrated models.
    bm_uf = BucketModel("uf")
    assert not bm_uf.calibrated
    bm_uf.tokens_floor_until = 100.0
    assert math.isclose(bm_uf.time_until_token(99.0), 1.0, abs_tol=0.01)

    # Refill respects the floor: while floor is in effect, calling consume()
    # / refill() doesn't move tokens above 0 even if elapsed time piles up.
    bm_f2 = BucketModel(
        "floor2", calibrated=True, limit_n=10, window_seconds=1.0, tokens=0.0,
        last_observation=50.0,
    )
    bm_f2.tokens_floor_until = 100.0
    # Concurrent route's consume() lands during floor — must NOT erase floor
    # or accidentally refill tokens.
    bm_f2.consume(60.0)
    assert bm_f2.tokens == 0.0, bm_f2.tokens
    assert bm_f2.tokens_floor_until == 100.0
    # time_until_token still gates on the floor, not the model's rate.
    assert math.isclose(bm_f2.time_until_token(60.0), 40.0, abs_tol=0.01)

    # Floor expires correctly: once we're past it, refill happens normally.
    bm_f3 = BucketModel(
        "floor3", calibrated=True, limit_n=2, window_seconds=2.0, tokens=0.0,
        last_observation=99.0,
    )
    bm_f3.tokens_floor_until = 100.0
    # Just past the floor; rate=2/2=1 tok/s; elapsed since last_observation=2s
    # so refill brings tokens up to 2.0 (capped).
    bm_f3.refill(101.0)
    assert math.isclose(bm_f3.tokens, 2.0, rel_tol=0.05), bm_f3.tokens

    # max() in the request loop preserves a longer pre-existing floor.
    bm_f4 = BucketModel("floor4", calibrated=True, limit_n=5, window_seconds=5.0)
    bm_f4.tokens_floor_until = 50.0
    bm_f4.tokens_floor_until = max(bm_f4.tokens_floor_until, 30.0)  # shorter
    assert bm_f4.tokens_floor_until == 50.0
    bm_f4.tokens_floor_until = max(bm_f4.tokens_floor_until, 70.0)  # longer
    assert bm_f4.tokens_floor_until == 70.0

    # ---- _maybe_int / _maybe_float exotic inputs (#14) ----
    assert _maybe_int({"k": "inf"}, "k") is None, "inf must not crash"
    assert _maybe_int({"k": "-inf"}, "k") is None, "-inf must not crash"
    assert _maybe_int({"k": "nan"}, "k") is None, "nan must not crash"
    assert _maybe_int({"k": ""}, "k") is None
    assert _maybe_int({"k": "5.0"}, "k") == 5
    assert _maybe_int({"k": "1e3"}, "k") == 1000
    assert _maybe_int({}, "k") is None
    assert _maybe_float({"k": "nan"}, "k") != _maybe_float({"k": "nan"}, "k")  # NaN != NaN
    assert _maybe_float({"k": "inf"}, "k") == float("inf")
    assert _maybe_float({"k": ""}, "k") is None

    # ---- GlobalBudget: burst then steady-state ----
    async def _gb_test():
        gb = GlobalBudget(capacity=3, refill_per_sec=10.0)
        t0 = time.monotonic()
        for _ in range(3):
            await gb.acquire()
        burst_elapsed = time.monotonic() - t0
        assert burst_elapsed < 0.05, f"first 3 acquires should burst, took {burst_elapsed:.3f}s"

        # 4th must wait ~1/refill_per_sec = 0.1s for one token to refill
        t1 = time.monotonic()
        await gb.acquire()
        elapsed = time.monotonic() - t1
        assert 0.05 < elapsed < 0.25, f"4th acquire took {elapsed:.3f}s, expected ~0.1s"

        # drain() bumps drain_seq so callers can detect a race (#4).
        seq_before = gb.drain_seq
        gb.drain()
        assert gb.drain_seq == seq_before + 1
        # acquire() returns the seq snapshot it observed at entry.
        seq_at_acq = await gb.acquire()
        assert seq_at_acq >= gb.drain_seq - 1
        # Concurrent drain races: acquire returns seq from BEFORE drain;
        # request() compares to current and re-checks pause_until.
        gb2 = GlobalBudget(capacity=1, refill_per_sec=10.0)
        await gb2.acquire()  # capacity exhausted
        # Next acquire will sleep ~0.1s waiting for refill.
        async def _drain_during_sleep():
            await asyncio.sleep(0.02)
            gb2.drain()
        seq_pre = gb2.drain_seq
        drain_task = asyncio.create_task(_drain_during_sleep())
        seq_obs = await gb2.acquire()
        await drain_task
        # acquire() snapshotted seq before sleeping; the post-acquire
        # seq is now bumped, so request() will detect the race.
        assert seq_obs == seq_pre, (seq_obs, seq_pre)
        assert gb2.drain_seq == seq_pre + 1

    asyncio.run(_gb_test())

    print("ratelimit self-test: ok")
