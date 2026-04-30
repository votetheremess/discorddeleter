"""Discord resource enumeration: self, guilds, DMs, and author-message search."""
from __future__ import annotations

import logging

from ratelimit import RateLimiter

log = logging.getLogger(__name__)


async def get_self(rl: RateLimiter) -> dict:
    status, body = await rl.request("GET", "/users/@me", bucket="users-me")
    if status != 200:
        raise RuntimeError(f"/users/@me failed: {status} {body}")
    return body


async def get_guilds(rl: RateLimiter) -> list[dict]:
    """All guilds the account is currently a member of."""
    out: list[dict] = []
    after: str | None = None
    while True:
        path = "/users/@me/guilds?limit=200"
        if after:
            path += f"&after={after}"
        status, body = await rl.request("GET", path, bucket="users-me-guilds")
        if status != 200:
            raise RuntimeError(f"/users/@me/guilds failed: {status} {body}")
        if not body:
            break
        out.extend(body)
        if len(body) < 200:
            break
        after = body[-1]["id"]
    return out


async def get_dm_channels(rl: RateLimiter) -> list[dict]:
    """Currently-open DMs and group DMs in the sidebar."""
    status, body = await rl.request("GET", "/users/@me/channels", bucket="users-me-channels")
    if status != 200:
        raise RuntimeError(f"/users/@me/channels failed: {status} {body}")
    return body or []


# Mirrors the exact query Undiscord uses. `include_nsfw=true` is important:
# without it, messages in NSFW-flagged channels are silently excluded.
_SEARCH_EXTRA = "&include_nsfw=true&sort_by=timestamp&sort_order=asc"


async def search_guild(rl: RateLimiter, guild_id: str, self_id: str, offset: int = 0):
    """Search authored messages inside a guild. Returns (messages, total, retry_after).

    retry_after is non-None iff Discord returned 202 (index warming) — caller
    should sleep and retry. messages is None on permanent failure.
    """
    path = f"/guilds/{guild_id}/messages/search?author_id={self_id}&offset={offset}" + _SEARCH_EXTRA
    status, body = await rl.request("GET", path, bucket=f"search-g-{guild_id}")
    return _parse_search(status, body)


async def search_channel(rl: RateLimiter, channel_id: str, self_id: str, offset: int = 0):
    """Same but scoped to a single channel (used for DMs and group DMs)."""
    path = f"/channels/{channel_id}/messages/search?author_id={self_id}&offset={offset}" + _SEARCH_EXTRA
    status, body = await rl.request("GET", path, bucket=f"search-c-{channel_id}")
    return _parse_search(status, body)


def _parse_search(status, body):
    if status == 202:
        ra = 5.0
        if isinstance(body, dict):
            try:
                ra = float(body.get("retry_after", 5.0))
            except (TypeError, ValueError):
                pass
        return None, None, ra
    if status != 200 or not isinstance(body, dict):
        return None, None, None
    msgs: list[dict] = []
    for group in body.get("messages", []):
        if group:
            msgs.append(group[0])  # first of the context list is the hit
    return msgs, body.get("total_results", 0), None


async def list_channel_messages(
    rl: RateLimiter,
    channel_id: str,
    before: str | None = None,
    limit: int = 100,
) -> list[dict] | None:
    """Fallback for channels where search isn't available — raw message history."""
    path = f"/channels/{channel_id}/messages?limit={limit}"
    if before:
        path += f"&before={before}"
    status, body = await rl.request("GET", path, bucket=f"list-c-{channel_id}")
    if status != 200:
        return None
    return body or []


async def get_active_threads(rl: RateLimiter, guild_id: str) -> list[dict]:
    """Active threads (incl. forum-channel threads) the user has access to in a guild.

    Returns a list of channel dicts (Discord channel objects with `id`,
    `type`, `name`, `parent_id`). Empty list on failure or no access.

    Why this is needed: `/guilds/{id}/messages/search?author_id=X` doesn't
    reliably cover messages inside threads (and skips forum-channel posts
    entirely, since each forum post IS a thread). Without enumerating
    threads explicitly the guild-search pass would leave thread messages
    behind. This is the active-threads-only path; archived threads need
    a per-channel `/channels/{cid}/threads/archived/...` enumeration that
    we don't do automatically (re-run the tool after manually un-archiving
    if needed).
    """
    out: list[dict] = []
    seen: set[str] = set()
    before: str | None = None
    # Discord's active-threads endpoint can paginate via `before`
    # (timestamp/id of the oldest thread on the previous page) when
    # has_more is true. In practice most guilds fit in one page.
    for _ in range(20):  # cap iterations defensively
        path = f"/guilds/{guild_id}/threads/active"
        if before:
            path += f"?before={before}"
        status, body = await rl.request(
            "GET", path, bucket=f"threads-g-{guild_id}",
        )
        if status != 200 or not isinstance(body, dict):
            break
        threads = body.get("threads") or []
        for t in threads:
            tid = t.get("id")
            if tid and tid not in seen:
                seen.add(tid)
                out.append(t)
        if not body.get("has_more"):
            break
        if not threads:
            break
        before = threads[-1]["id"]
    return out
