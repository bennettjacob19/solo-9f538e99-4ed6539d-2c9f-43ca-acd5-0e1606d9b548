(function () {
  "use strict";

  const panel = document.getElementById("sync-panel");
  if (!panel) return;

  const syncId = panel.dataset.syncId;
  const initialStatus = panel.dataset.status;
  const TERMINAL = ["synced", "cancelled", "error", "expired", "ended"];
  let stopped = TERMINAL.includes(initialStatus);

  function setText(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  }

  function setHtml(id, html) {
    const el = document.getElementById(id);
    if (el) el.innerHTML = html;
  }

  function render(snap) {
    setHtml(
      "replay-state",
      !snap.standby_configured
        ? '<span class="tag tag-bad">无法确认</span>'
        : snap.standby_paused
        ? '<span class="tag tag-warn">已暂停</span>'
        : '<span class="tag tag-ok">回放中</span>'
    );

    const session = snap.commit_session;
    setText("commit-state", session ? session.state : "已结束");
    const waitEl = document.getElementById("commit-wait");
    if (waitEl) {
      if (session && session.wait_event_type) {
        waitEl.innerHTML =
          '<span class="tag tag-warn">' +
          session.wait_event_type + " / " + session.wait_event +
          "</span>";
      } else if (session) {
        waitEl.textContent = "不在等待";
      } else {
        waitEl.textContent = "—";
      }
    }

    const confirmedEl = document.getElementById("waiting-confirmed");
    if (confirmedEl) {
      confirmedEl.innerHTML = snap.waiting_confirmed
        ? '<span class="tag tag-ok">已确认正在等待同步复制（IPC/SyncRep）</span>'
        : '<span class="tag tag-bad">尚未确认 · 无法取消</span>';
    }

    const badge = document.getElementById("status-badge");
    if (badge) {
      badge.className = "status status-" + snap.status;
      badge.textContent = snap.status;
    }
  }

  async function poll() {
    if (stopped) return;
    try {
      const res = await fetch(
        "/api/sync/state?sync_id=" + encodeURIComponent(syncId),
        { headers: { Accept: "application/json" }, cache: "no-store" }
      );
      if (!res.ok) {
        stopped = true;
        return;
      }
      const data = await res.json();
      const snap = data.sync;
      if (!snap) {
        stopped = true;
        const wrap = document.createElement("div");
        wrap.className = "banner banner-error";
        wrap.textContent =
          "该演练记录已不存在（服务可能已重启），本页面不再可操作。";
        panel.prepend(wrap);
        return;
      }
      if (snap.status !== panel.dataset.status) {
        // Status transitions change which controls are valid (e.g. the cancel
        // button only renders once "waiting" is confirmed): reload so the page
        // never shows an action the server would reject.
        stopped = true;
        window.location.reload();
        return;
      }
      render(snap);
    } catch (err) {
      /* transient network error: keep polling */
    }
    if (!stopped) setTimeout(poll, 1000);
  }

  if (!stopped) setTimeout(poll, 800);
})();
