# discord-deleter

Mass-delete every message you authored across Discord (guilds + DMs) before
deleting your account. Uses Discord's user-facing HTTP API via your own token.

## What it does

1. Enumerates every guild and DM/group-DM the account has access to.
2. Uses Discord's search API to locate messages you authored.
3. Deletes each one via `DELETE /channels/{channel_id}/messages/{message_id}`.
4. Records every attempt in a SQLite checkpoint so crashes/interruptions are resumable.

## What it can't do

- **Servers you've left.** Discord's API only returns guilds you're currently
  in, and DELETE returns 403 Missing Access on channels where you're no longer
  a member. The only workaround:
  - **Rejoin** each server (via saved invite or public listing), re-run the
    tool, then leave again. Rejoined servers are picked up automatically on
    the next run — the checkpoint handles resumability.
  - **GDPR erasure ticket** to Discord support — slow (40+ days), inconsistent
    results, but the only remaining lever for servers where you're banned or
    that no longer accept you.
  - If neither works, those messages remain as "Deleted User" after account
    deletion.
- **Closed DMs.** Only DMs currently open in your sidebar are discoverable.
  Open the DM (reply to anyone once) to bring it back before running.
- **Other users' messages.** You can only delete your own.
- **Protect against internal backups.** If Discord retains logs beyond the
  user-facing database, this tool can't touch them. GDPR/CCPA erasure is the
  only lever there — and France's CNIL fined Discord €800K in 2022 for this
  exact class of retention failure, so the concern isn't hypothetical.

## Zero-code alternatives

If you'd rather not run a Python script:

- **[Discrub](https://github.com/prathercc/discrub-ext)** — Chrome/Firefox extension,
  actively maintained as of late 2025. Adds bulk delete UI to Discord itself. Easiest
  path if you just want to click through it.
- **[Undiscord](https://github.com/victornpb/undiscord)** — browser-console script.
  Older, intermittent breakages, but well-known.

This tool exists because your 100k+ scale is painful in a browser extension
(easy to lose progress on a page reload) and because we have explicit
resume-from-crash via SQLite.

## Search endpoint caveats

Discord's `/messages/search` API is known to be flaky:
- Sometimes returns empty pages despite more results existing.
- Occasional spurious 401s mid-run.
- For very large or rarely-searched guilds, returns 202 "index warming" — the
  tool sleeps and retries automatically.

If a guild finishes with residual messages, re-run the tool; discovery is
idempotent and will pick up missed ones.

## ToS warning

Automating with a user token ("self-botting") violates Discord's Terms of
Service and can result in an account ban. Because the account will be deleted
anyway, this is an acceptable pre-deletion trade-off — but note: a ban cancels
further access, so already-deleted messages stay deleted but pending ones don't.

The tool defaults to conservative rates (one DELETE per ~1.1s per channel) and
no parallelism, to reduce flag risk. `--aggressive` and `--concurrency N` are
opt-in.

## Installation

Uses [uv](https://github.com/astral-sh/uv). Install it first if you don't have it:
`brew install uv` (macOS) or see the uv README for other platforms.

```bash
uv sync      # creates .venv and installs aiohttp + textual
```

That's it — `uv sync` creates a virtualenv at `.venv`, installs dependencies,
and produces `uv.lock` for reproducible builds.

## Getting your user token

1. Open Discord in a browser, log in.
2. DevTools (F12) → Network tab.
3. Send any message (or refresh the page).
4. Find a request to `discord.com/api/v9/...` and copy the `authorization`
   header value (it does NOT start with `Bearer`).
5. **Never share this token.** Anyone with it can log in as you. Revoke it by
   changing your Discord password when you're done.

## Usage

The tool has two modes:

- **Interactive TUI (default)** — launched with no flags. Browse servers and DMs, pick one, delete it. Repeat.
- **Batch mode** — triggered by any of `--batch`, `--exclude-guild`, `--exclude-user`, `--aggressive`, or `--concurrency N>1`. Deletes across all (non-excluded) channels in a single long run.

`--dry-run` works in both modes.

### Setup

```bash
cp .env.example .env
# edit .env, paste your token after DISCORD_TOKEN=
chmod 600 .env
```

### Interactive TUI

```bash
uv run python src/deleter.py              # live
uv run python src/deleter.py --dry-run    # no DELETE calls
```

On launch the tool authenticates, enumerates all your guilds and open DMs
(takes a minute), then opens the TUI:

```
┌─ discord-deleter ─── selected: <none> ──┐
│ [Servers]  DMs                          │
├─────────────────────────────────────────┤
│ ✓ Server A                  12345...    │
│ > Server B                  67890...    │
│   Server C                  11111...    │
└─────────────────────────────────────────┘
```

Keys:
- `↑` / `↓` — navigate rows
- `Tab` / `Shift+Tab` — switch between Servers and DMs
- `1` / `2` — jump to Servers / DMs
- `Space` (or `Enter`) — toggle the current row in/out of the selection (multi-select)
- `d` — delete messages from every selected channel (opens a progress modal). Concurrency = number of channels selected. Re-running an already-`done` channel is allowed and is the recovery path if the first pass left stragglers.
- `a` — toggle aggressive mode (halves per-request delay live: 1.1s ↔ 0.55s)
- `h` — hide channels already marked `done`
- `r` — refresh from Discord. Re-discovers channels (catches DMs/guilds you've opened since startup) and queries each pending channel via search; any whose Discord-side count is 0 get marked `done`. This is how the tool picks up DMs you cleared manually outside the tool. Runs in the background; press `r` again to no-op while one is in flight.
- `Esc` — cancel the active deletion (or close the modal if it's already finished)
- `q` / `Ctrl+C` — quit

Marks: `✓` = completed, `✗` = errored, `–` = excluded, `…` = in progress, `*` = currently selected

The progress modal shows a global `Deleted: X / Y` count plus a per-channel breakdown (`name  deleted / total`). Totals come from Discord's search index and only display for guild + group-DM channels — 1:1 DMs use raw history pagination which can't supply a count. The status lives in `logs/checkpoint.db` so finished channels keep their `✓` on the next run.

### Batch mode

```bash
uv run python src/deleter.py --batch                 # delete everything
uv run python src/deleter.py --dry-run --batch       # batch dry-run
uv run python src/deleter.py --exclude-guild 12345   # batch + exclusion
```

Provides the token two ways — inline `DISCORD_TOKEN='...' .venv/...` overrides the `.env` file for one-off runs.

Review `logs/checkpoint.db` and the log file under `logs/`, then:

```bash
DISCORD_TOKEN='your_token_here' python src/deleter.py
```

Interrupt with Ctrl-C at any time. Re-run the same command to resume — the
checkpoint DB is the source of truth for what's done.

## Flags

- `--dry-run` — discover + record only, no DELETE calls
- `--concurrency N` — process N channels in parallel (default: 1)
- `--aggressive` — halve the per-request delay (faster, higher flag risk)
- `--exclude-guild GUILD_ID` — skip this guild (repeatable: pass the flag once per ID)
- `--exclude-user USER_ID` — skip 1:1 DMs with this user and any group DM they're in (repeatable)
- `--db PATH` — override checkpoint DB path (default: `logs/checkpoint.db`)
- `-v` / `--verbose` — debug-level console output

### Exclusion semantics

Two ways to specify exclusions (they're additive — both sources combine):

1. **In `.env`** (persistent across runs):
   ```
   EXCLUDE_GUILDS=123456789012345678, 987654321098765432
   EXCLUDE_USERS=555666777888999000
   ```
   Separator can be commas or whitespace.

2. **On the command line** (repeatable, for ad-hoc overrides):
   ```
   --exclude-guild 123456789012345678 --exclude-user 555666777888999000
   ```

Rules:
- Excluded channels get status `excluded` in the checkpoint DB and are skipped by the delete loop.
- Dropping an ID from `.env` (or not passing `--exclude-*` on a later run) re-pends the channel so it gets processed next run.
- Channels already in `done` or `error` status are never touched by exclusion toggling.

To get IDs: enable Settings → Advanced → Developer Mode, then right-click a
server or user → "Copy ID".

## Output

- Log file: `logs/deleter-<UTC timestamp>.log`
- Checkpoint: `logs/checkpoint.db` (SQLite; inspect with `sqlite3`)
- Final summary printed on exit.

## Final step

After the tool reports a clean summary, go to Discord Settings → My Account →
Delete Account. Your messages are already gone; the account itself gets
disabled and scheduled for deletion.
