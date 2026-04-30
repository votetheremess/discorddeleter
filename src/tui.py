"""Textual TUI front-end for discord-deleter.

Launched by deleter.py when no batch-mode flags are given. Browses guilds
and DMs via the checkpoint, lets the user multi-select with Space, and
runs the existing process_channel() pipeline inside a progress modal.
Selection size = concurrency: pick N channels and they run in parallel.
"""
from __future__ import annotations

import asyncio

from rich.markup import escape
from rich.text import Text

from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Static,
    TabbedContent,
    TabPane,
)

from checkpoint import Checkpoint
from ratelimit import RateLimiter


def _channel_label_plain(ch: dict) -> str:
    """Plain-text label for a channel — no markup escaping.

    Use when handing the label to a `rich.text.Text` (which doesn't parse
    markup) or to anything that won't be interpreted by Rich.
    """
    if ch["type"] == "guild":
        return ch.get("guild_name") or ch.get("name") or "<unnamed guild>"
    if ch["type"] == "group_dm":
        return f"[{ch.get('name') or '<unnamed group>'}]"
    return ch.get("name") or "<unnamed DM>"


def _channel_label(ch: dict) -> str:
    """Markup-safe label, for widgets that parse Rich markup (DataTable
    cells, Static.update with a string). The escape prevents `[stream]`
    style names from being silently swallowed as an unknown style tag.
    """
    return escape(_channel_label_plain(ch))


def _status_mark(ch: dict) -> str:
    s = ch.get("status")
    if s == "done":
        return "✓"
    if s == "error":
        return "✗"
    if s == "excluded":
        return "–"
    if s == "in_progress":
        return "…"
    return " "


def _row_mark(ch: dict, selected: bool) -> str:
    # selection wins over status display so the user can see what they picked
    return "*" if selected else _status_mark(ch)


class ProgressScreen(ModalScreen):
    """Live-updating modal shown while one or more channels are being cleared.

    Concurrency = len(channels): each selected channel runs as its own task
    against a shared semaphore sized to the selection count, so picking N
    channels is equivalent to --batch --concurrency N for that subset.
    """

    CSS = """
    ProgressScreen {
        align: center middle;
    }
    #modal {
        width: 78;
        min-height: 19;
        max-height: 90%;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }
    #title { content-align: center middle; height: 2; text-style: bold; }
    #counts { height: auto; padding: 1 0 0 0; }
    #per-channel-header { height: 1; padding-top: 1; color: $text-muted; }
    #per-channel { height: auto; max-height: 20; padding: 0 0 1 0; }
    #hint { dock: bottom; content-align: center middle; color: $text-muted; }
    """

    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("q", "close", "Close"),
    ]

    def __init__(
        self,
        channels: list[dict],
        cp: Checkpoint,
        rl: RateLimiter,
        self_id: str,
        dry_run: bool,
    ):
        super().__init__()
        self.channels = channels
        self.cp = cp
        self.rl = rl
        self.self_id = self_id
        self.dry_run = dry_run
        self.counts = {"deleted": 0, "missing": 0, "error": 0}
        self.completed = 0
        self.done = False
        self.cancelling = False
        self.errors: list[str] = []
        self._worker = None
        # Per-channel state for the breakdown rows. `total` stays None until
        # the search endpoint reports total_results (1:1 DMs go via list, so
        # they have no total to show — rendered as "deleted" without "/N").
        # Label is the plain (un-escaped) form because the per-channel body
        # is rendered via a rich.text.Text — no markup parsing in that path.
        self.per_channel: dict[str, dict] = {
            ch["id"]: {
                "label": _channel_label_plain(ch),
                "type": ch["type"],
                "deleted": 0,
                "missing": 0,
                "error": 0,
                "total": None,
            }
            for ch in channels
        }

    def compose(self) -> ComposeResult:
        with Container(id="modal"):
            n = len(self.channels)
            if n == 1:
                ch = self.channels[0]
                label = _channel_label(ch)
                kind = {"guild": "server", "dm": "DM", "group_dm": "group DM"}[ch["type"]]
                title = f"Deleting from {kind}: {label}"
            else:
                title = f"Deleting from {n} channels (concurrency={n})"
            yield Static(title, id="title")
            yield Static("", id="counts")
            yield Static("Per channel:", id="per-channel-header")
            with VerticalScroll(id="per-channel"):
                yield Static("", id="per-channel-body")
            yield Static("Esc to cancel · Esc again to close", id="hint")

    def on_mount(self) -> None:
        self._refresh_counts()
        self.set_interval(0.3, self._refresh_counts)
        self._worker = self.run_worker(
            self._do_delete_all(), exclusive=True, group="delete"
        )

    async def _do_delete_all(self) -> None:
        # Import lazily to avoid a cycle at module load time.
        from deleter import process_channel

        def make_progress_cb(ch_id: str):
            pc = self.per_channel[ch_id]
            def _cb(status: str) -> None:
                self.counts[status] = self.counts.get(status, 0) + 1
                pc[status] = pc.get(status, 0) + 1
            return _cb

        def make_total_cb(ch_id: str):
            pc = self.per_channel[ch_id]
            # Discord's `total_results` shrinks as we delete, so we can't just
            # latch the first value (search-index lag can under-report it
            # initially). Track `deleted_so_far + remaining` and keep the max
            # we've ever seen — converges on the true initial total.
            def _cb(remaining: int) -> None:
                candidate = pc["deleted"] + remaining
                if pc["total"] is None or candidate > pc["total"]:
                    pc["total"] = candidate
            return _cb

        sem = asyncio.Semaphore(len(self.channels))

        async def run_one(ch: dict) -> None:
            try:
                await process_channel(
                    self.rl, self.cp, ch, self.self_id, self.dry_run, sem,
                    progress_cb=make_progress_cb(ch["id"]),
                    total_cb=make_total_cb(ch["id"]),
                )
            except asyncio.CancelledError:
                # Don't count cancelled channels as completed; just bail.
                raise
            except Exception as ex:  # noqa: BLE001
                self.errors.append(f"{_channel_label(ch)}: {type(ex).__name__}: {ex}")
            self.completed += 1

        try:
            await asyncio.gather(*(run_one(ch) for ch in self.channels))
        except asyncio.CancelledError:
            # Expected when the user hits Esc; cleanup happens in finally.
            pass
        finally:
            # process_channel sets in_progress before each channel and only
            # clears it on the success/error/dry-run paths — cancel skips
            # those, leaving stranded rows. Reset so the TUI doesn't show
            # stale "…" marks and the next run resumes cleanly.
            self.cp.reset_in_progress()
            self.done = True
            self._refresh_counts()

    def _refresh_counts(self) -> None:
        widget = self.query_one("#counts", Static)
        c = self.counts
        n = len(self.channels)
        remaining = n - self.completed
        if self.done and self.cancelling:
            tail = f" — {remaining} will resume on next run" if remaining else ""
            state = f"[b yellow]CANCELLED[/]  ({self.completed}/{n} finished{tail})"
        elif self.done and self.errors:
            shown = "\n  ".join(self.errors[:2])
            extra = f"\n  (+{len(self.errors) - 2} more — see log)" if len(self.errors) > 2 else ""
            state = f"[b red]{len(self.errors)} CHANNEL ERROR(S)[/]\n  {shown}{extra}"
        elif self.done:
            state = "[b green]✓ COMPLETE[/]"
        elif self.cancelling:
            state = "[b yellow]Cancelling…[/] waiting for in-flight requests"
        elif self.dry_run:
            state = f"[b yellow]DRY-RUN[/] working… ({self.completed}/{n} channels)"
        else:
            state = f"working… ({self.completed}/{n} channels)"

        # Global total = sum of known per-channel totals. If any channel has
        # no known total (1:1 DMs go via list, no search count), suffix "+"
        # so the user knows the displayed total is a lower bound.
        known = [pc["total"] for pc in self.per_channel.values() if pc["total"] is not None]
        unknown = sum(1 for pc in self.per_channel.values() if pc["total"] is None)
        if known:
            total_sum = sum(known)
            suffix = "+" if unknown else ""
            deleted_str = f"{c['deleted']} / {total_sum}{suffix}"
        else:
            deleted_str = f"{c['deleted']}"

        widget.update(
            f"{state}\n\n"
            f"  Deleted:  {deleted_str}\n"
            f"  Missing:  {c['missing']}  (already gone on server)\n"
            f"  Errors:   {c['error']}"
        )

        # Build per-channel body as a Text (no_wrap so a long label can't
        # wrap and dump the count onto an empty next line, and no markup
        # parsing so we don't have to escape brackets / styles in names).
        # Each label is padded/truncated to LABEL_WIDTH *cells* via Rich
        # (which knows about wide unicode + combining marks) so the count
        # column lines up across rows regardless of name composition.
        LABEL_WIDTH = 50
        body = self.query_one("#per-channel-body", Static)
        rows = Text(no_wrap=True, overflow="ellipsis")
        for i, ch in enumerate(self.channels):
            pc = self.per_channel[ch["id"]]
            if i > 0:
                rows.append("\n")
            rows.append("  ")
            label = Text(pc["label"], no_wrap=True)
            label.truncate(LABEL_WIDTH, overflow="ellipsis", pad=True)
            rows.append(label)
            if pc["total"] is not None:
                rows.append(f" {pc['deleted']} / {pc['total']}")
            else:
                rows.append(f" {pc['deleted']}")
        body.update(rows)

    def action_close(self) -> None:
        if self.done:
            self.dismiss(None)
            return
        if self.cancelling:
            # Already cancelling — re-press is a no-op until cleanup finishes.
            return
        self.cancelling = True
        self._refresh_counts()
        if self._worker is not None:
            self._worker.cancel()


class DeleterApp(App):
    """Multi-select browser for the discord-deleter checkpoint."""

    CSS = """
    DataTable { height: 1fr; }
    TabbedContent { height: 1fr; }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("ctrl+c", "quit", "Quit"),
        # priority=True so Tab wins over DataTable's focus handling
        Binding("tab", "next_tab", "Next tab", show=True, priority=True),
        Binding("shift+tab", "prev_tab", "Prev tab", show=False, priority=True),
        Binding("1", "tab_servers", "Servers", show=False),
        Binding("2", "tab_dms", "DMs", show=False),
        # priority=True so Space toggles selection instead of falling through
        # to DataTable's default key handling
        Binding("space", "toggle_select", "Select", priority=True),
        Binding("d", "delete_selected", "Delete"),
        Binding("a", "toggle_aggressive", "Aggressive"),
        Binding("h", "toggle_hide", "Hide empty"),
        Binding("r", "refresh", "Refresh"),
    ]

    TITLE = "discord-deleter"

    def __init__(
        self,
        cp: Checkpoint,
        rl: RateLimiter,
        self_id: str,
        dry_run: bool = False,
        exclude_guilds: set[str] | None = None,
        exclude_users: set[str] | None = None,
    ):
        super().__init__()
        self.cp = cp
        self.rl = rl
        self.self_id = self_id
        self.dry_run = dry_run
        # Carried so refresh can re-run discovery with the same exclusions
        # the user launched with (.env values + CLI flags merged upstream).
        self.exclude_guilds = exclude_guilds or set()
        self.exclude_users = exclude_users or set()
        # multi-select: channel_id -> channel dict; concurrency = len(selected)
        self.selected: dict[str, dict] = {}
        self.aggressive: bool = self.rl.default_delay <= 0.6
        # When True, hide channels we've already cleared (status == 'done').
        # Pending/in_progress/error/excluded stay visible since we don't have
        # a confirmed count for those.
        self.hide_empty: bool = False
        self.channels_by_id: dict[str, dict] = {}
        self._hidden_count: int = 0
        self._refreshing: bool = False

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        with TabbedContent(initial="tab-servers"):
            with TabPane("Servers", id="tab-servers"):
                yield DataTable(id="servers-table", cursor_type="row", zebra_stripes=True)
            with TabPane("DMs", id="tab-dms"):
                yield DataTable(id="dms-table", cursor_type="row", zebra_stripes=True)
        yield Footer()

    def on_mount(self) -> None:
        self.sub_title = self._selection_text()
        self._populate()
        # put initial focus on the servers table so arrows work immediately
        self.query_one("#servers-table", DataTable).focus()

    def _populate(self) -> None:
        self.channels_by_id = {}
        hidden = 0

        def _show(ch: dict) -> bool:
            return not (self.hide_empty and ch.get("status") == "done")

        servers_tbl = self.query_one("#servers-table", DataTable)
        servers_tbl.clear(columns=True)
        servers_tbl.add_column("", width=2, key="mark")
        servers_tbl.add_column("Name", width=40, key="name")
        servers_tbl.add_column("ID", key="id")
        for ch in self.cp.list_channels(ctypes=("guild",)):
            if not _show(ch):
                hidden += 1
                continue
            self.channels_by_id[ch["id"]] = ch
            mark = _row_mark(ch, ch["id"] in self.selected)
            servers_tbl.add_row(mark, _channel_label(ch), ch["id"], key=ch["id"])

        dms_tbl = self.query_one("#dms-table", DataTable)
        dms_tbl.clear(columns=True)
        dms_tbl.add_column("", width=2, key="mark")
        dms_tbl.add_column("Name", width=40, key="name")
        dms_tbl.add_column("ID", key="id")
        for ch in self.cp.list_channels(ctypes=("dm", "group_dm")):
            if not _show(ch):
                hidden += 1
                continue
            self.channels_by_id[ch["id"]] = ch
            mark = _row_mark(ch, ch["id"] in self.selected)
            dms_tbl.add_row(mark, _channel_label(ch), ch["id"], key=ch["id"])

        self._hidden_count = hidden

        # drop selections that no longer exist (refresh OR hide filter removed them).
        # Keep hidden-but-still-in-DB selections so toggling 'h' off restores them.
        for stale_id in list(self.selected):
            if stale_id not in self.channels_by_id and not self._exists(stale_id):
                del self.selected[stale_id]

    def _exists(self, channel_id: str) -> bool:
        cur = self.cp.db.execute("SELECT 1 FROM channels WHERE id=?", (channel_id,))
        return cur.fetchone() is not None

    def _selection_text(self) -> str:
        flags = []
        if self.dry_run:
            flags.append("DRY-RUN")
        if self.aggressive:
            flags.append("AGGRESSIVE")
        suffix = f" [{' '.join(flags)}]" if flags else ""
        n = len(self.selected)
        if n == 0:
            return f"selected: <none>{suffix}"
        if n == 1:
            ch = next(iter(self.selected.values()))
            return f"selected: {_channel_label(ch)}{suffix}"
        return f"selected: {n} channels (concurrency={n}){suffix}"

    def _table_for(self, ch: dict) -> DataTable:
        table_id = "#servers-table" if ch["type"] == "guild" else "#dms-table"
        return self.query_one(table_id, DataTable)

    def _toggle_row(self, key: str | None) -> None:
        if key is None:
            return
        ch = self.channels_by_id.get(key)
        if not ch:
            return
        if key in self.selected:
            del self.selected[key]
            now_selected = False
        else:
            self.selected[key] = ch
            now_selected = True
        try:
            self._table_for(ch).update_cell(key, "mark", _row_mark(ch, now_selected))
        except Exception:
            # best-effort; cell layout may have changed (e.g. mid-refresh)
            pass
        self.sub_title = self._selection_text()

    # --- event handlers ------------------------------------------------

    @on(DataTable.RowSelected)
    def _on_row_selected(self, event: DataTable.RowSelected) -> None:
        # Enter mirrors Space — toggles the row in/out of the selection set.
        key = event.row_key.value
        self._toggle_row(key)

    # --- actions -------------------------------------------------------

    def action_next_tab(self) -> None:
        tabs = self.query_one(TabbedContent)
        tabs.active = "tab-dms" if tabs.active == "tab-servers" else "tab-servers"
        active_table = "#dms-table" if tabs.active == "tab-dms" else "#servers-table"
        self.query_one(active_table, DataTable).focus()

    def action_prev_tab(self) -> None:
        # Only two tabs so this is identical to next_tab.
        self.action_next_tab()

    def action_tab_servers(self) -> None:
        self.query_one(TabbedContent).active = "tab-servers"
        self.query_one("#servers-table", DataTable).focus()

    def action_tab_dms(self) -> None:
        self.query_one(TabbedContent).active = "tab-dms"
        self.query_one("#dms-table", DataTable).focus()

    def action_refresh(self) -> None:
        if self._refreshing:
            self.notify("Refresh already in progress…", severity="warning")
            return
        self.run_worker(self._refresh_from_discord(), exclusive=True, group="refresh")

    async def _refresh_from_discord(self) -> None:
        """Re-discover channels and check each pending one against Discord.

        Pending channels whose search returns total_results=0 are flipped to
        'done' — that's how the user picks up DMs they cleared manually
        outside the tool. Re-discovery also catches DMs/guilds opened since
        startup. Skips in_progress rows so a concurrent worker isn't stomped.
        """
        from deleter import discover_all
        from discovery import search_channel, search_guild

        self._refreshing = True
        try:
            self.notify("Refreshing from Discord…")
            await discover_all(
                self.rl, self.cp,
                exclude_guilds=self.exclude_guilds,
                exclude_users=self.exclude_users,
            )

            # Skip in_progress rows so we don't race a concurrent worker.
            pending = [ch for ch in self.cp.list_channels() if ch["status"] == "pending"]

            newly_done = 0

            async def check_one(ch: dict) -> None:
                nonlocal newly_done
                if ch["type"] == "guild":
                    msgs, total, _ = await search_guild(self.rl, ch["id"], self.self_id, 0)
                else:
                    msgs, total, _ = await search_channel(self.rl, ch["id"], self.self_id, 0)
                # msgs is None on a search failure (e.g. 403); leave the row
                # alone in that case so a real failure can't silently mark
                # channels done. total==0 with msgs==[] is the empty signal.
                if msgs is not None and total == 0:
                    if self.cp.mark_pending_done_if(ch["id"]):
                        newly_done += 1

            if pending:
                await asyncio.gather(*(check_one(ch) for ch in pending))

            self._populate()
            still_pending = sum(
                1 for ch in self.cp.list_channels() if ch["status"] == "pending"
            )
            self.notify(
                f"Refreshed: {newly_done} now empty, {still_pending} still pending"
            )
        except Exception as ex:  # noqa: BLE001
            self.notify(f"Refresh failed: {type(ex).__name__}: {ex}", severity="error")
        finally:
            self._refreshing = False

    def action_toggle_hide(self) -> None:
        self.hide_empty = not self.hide_empty
        self._populate()
        if self.hide_empty:
            self.notify(f"Hiding {self._hidden_count} cleared channel(s)")
        else:
            self.notify("Showing all channels")

    def action_toggle_select(self) -> None:
        tabs = self.query_one(TabbedContent)
        table_id = "#dms-table" if tabs.active == "tab-dms" else "#servers-table"
        table = self.query_one(table_id, DataTable)
        if table.row_count == 0:
            return
        try:
            row_key, _ = table.coordinate_to_cell_key(table.cursor_coordinate)
        except Exception:
            return
        self._toggle_row(row_key.value)

    def action_toggle_aggressive(self) -> None:
        self.aggressive = not self.aggressive
        # default_delay is read live on every request, so this takes effect
        # for the next call without restarting the rate limiter
        self.rl.default_delay = 0.55 if self.aggressive else 1.1
        self.sub_title = self._selection_text()
        self.notify(
            f"Aggressive {'ON' if self.aggressive else 'OFF'} "
            f"(delay {self.rl.default_delay:.2f}s)",
            severity="warning" if self.aggressive else "information",
        )

    def action_delete_selected(self) -> None:
        if not self.selected:
            self.notify("Nothing selected — press Space on a row first", severity="warning")
            return

        # Process whatever the user explicitly selected, including 'done'
        # channels — re-running is idempotent (search/list pick up wherever
        # things stand) and is the recovery path when the first pass missed
        # messages (search-index lag, etc.).
        valid = list(self.selected.values())

        def _on_close(_result) -> None:
            # Clear selection and refresh row marks after the modal closes
            # so ✓ / ✗ replaces the * markers.
            self.selected = {}
            self._populate()

        self.push_screen(
            ProgressScreen(valid, self.cp, self.rl, self.self_id, self.dry_run),
            _on_close,
        )


async def run_tui(
    cp: Checkpoint,
    rl: RateLimiter,
    self_id: str,
    dry_run: bool = False,
    exclude_guilds: set[str] | None = None,
    exclude_users: set[str] | None = None,
) -> None:
    app = DeleterApp(
        cp, rl, self_id, dry_run=dry_run,
        exclude_guilds=exclude_guilds, exclude_users=exclude_users,
    )
    await app.run_async()
