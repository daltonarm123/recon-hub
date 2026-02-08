import { useEffect, useState } from "react";

export default function AdminHealth() {
  const [overview, setOverview] = useState(null);
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(true);
  const [notes, setNotes] = useState([]);
  const [notesErr, setNotesErr] = useState("");
  const [notesLoading, setNotesLoading] = useState(true);
  const [noteText, setNoteText] = useState("");
  const [savingNote, setSavingNote] = useState(false);
  const [noteMsg, setNoteMsg] = useState("");

  async function load() {
    setLoading(true);
    setErr("");
    try {
      const r = await fetch("/api/admin/overview", {
        cache: "no-store",
        credentials: "include",
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(j?.detail || `HTTP ${r.status}`);
      setOverview(j);
    } catch (e) {
      setOverview(null);
      setErr(e?.message || "Failed to load");
    } finally {
      setLoading(false);
    }
  }

  async function loadNotes() {
    setNotesLoading(true);
    setNotesErr("");
    try {
      const r = await fetch("/api/admin/notes?limit=200", {
        cache: "no-store",
        credentials: "include",
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(j?.detail || `HTTP ${r.status}`);
      setNotes(Array.isArray(j?.notes) ? j.notes : []);
    } catch (e) {
      setNotes([]);
      setNotesErr(e?.message || "Failed to load notes");
    } finally {
      setNotesLoading(false);
    }
  }

  async function saveNote() {
    const clean = noteText.trim();
    if (!clean || savingNote) return;

    setSavingNote(true);
    setNoteMsg("");
    try {
      const r = await fetch("/api/admin/notes", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ note: clean }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(j?.detail || `HTTP ${r.status}`);

      if (j?.note) {
        setNotes((prev) => [j.note, ...prev]);
      } else {
        await loadNotes();
      }
      setNoteText("");
      setNoteMsg("Note saved.");
    } catch (e) {
      setNoteMsg(e?.message || "Failed to save note");
    } finally {
      setSavingNote(false);
    }
  }

  useEffect(() => {
    load();
    loadNotes();
  }, []);

  function Metric({ label, value }) {
    return (
      <div style={metricCard}>
        <div style={metricLabel}>{label}</div>
        <div style={metricValue}>{value}</div>
      </div>
    );
  }

  return (
    <div style={{ display: "grid", gap: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
        <h2 style={{ margin: 0, fontSize: 18 }}>Admin Operations</h2>
        <button onClick={load} disabled={loading} style={btn}>
          {loading ? "Refreshing..." : "Refresh"}
        </button>
      </div>

      {err ? <div style={{ color: "#ff8b8b", fontSize: 13 }}>Error: {err}</div> : null}
      {!overview && !err ? <div style={{ fontSize: 13, opacity: 0.8 }}>Loading admin overview...</div> : null}

      {overview ? (
        <>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 10 }}>
            <Metric label="Spy Reports" value={overview?.counts?.spy_reports ?? 0} />
            <Metric label="NW History Points" value={overview?.counts?.nw_history ?? 0} />
            <Metric label="Top Kingdom Rows" value={overview?.counts?.kg_top_kingdoms ?? 0} />
            <Metric label="KG Connections" value={overview?.counts?.kg_connections ?? 0} />
            <Metric label="Rankings Age (s)" value={overview?.health?.rankings_age_seconds ?? "-"} />
            <Metric label="NW Tick Age (s)" value={overview?.health?.nw_tick_age_seconds ?? "-"} />
          </div>

          <div style={panel}>
            <div style={{ fontWeight: 700, marginBottom: 6 }}>Admin Context</div>
            <div style={line}>User: {overview?.admin?.discord_username} ({overview?.admin?.discord_user_id})</div>
            <div style={line}>DB: {overview?.database?.name || "-"}</div>
            <div style={line}>Last Rankings Fetch: {overview?.latest?.rankings_fetch_at || "-"}</div>
            <div style={line}>Last NW Tick: {overview?.latest?.nw_tick_at || "-"}</div>
          </div>

          <div style={panel}>
            <div style={{ fontWeight: 700, marginBottom: 8 }}>Top NW Snapshot</div>
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                <thead>
                  <tr>
                    <th style={th}>Rank</th>
                    <th style={th}>Kingdom</th>
                    <th style={th}>Networth</th>
                    <th style={th}>Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {(overview?.top_nw_latest || []).map((r) => (
                    <tr key={`${r.kingdom}:${r.rank}`}>
                      <td style={td}>{r.rank}</td>
                      <td style={td}>{r.kingdom}</td>
                      <td style={td}>{Number(r.networth || 0).toLocaleString()}</td>
                      <td style={td}>{r.updated_at ? new Date(r.updated_at).toLocaleString() : "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div style={panel}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 8, flexWrap: "wrap", marginBottom: 8 }}>
              <div style={{ fontWeight: 700 }}>Admin Feedback Notes</div>
              <button onClick={loadNotes} disabled={notesLoading || savingNote} style={btn}>
                {notesLoading ? "Refreshing..." : "Refresh Notes"}
              </button>
            </div>

            <textarea
              style={textarea}
              placeholder="Write bug reports, follow-up tasks, or fixes for later..."
              value={noteText}
              onChange={(e) => setNoteText(e.target.value)}
            />

            <div style={{ display: "flex", gap: 8, marginTop: 8, alignItems: "center", flexWrap: "wrap" }}>
              <button onClick={saveNote} disabled={!noteText.trim() || savingNote} style={btn}>
                {savingNote ? "Saving..." : "Save Note"}
              </button>
              {noteMsg ? <div style={{ fontSize: 12, color: noteMsg === "Note saved." ? "#8be28b" : "#ff8b8b" }}>{noteMsg}</div> : null}
              {notesErr ? <div style={{ fontSize: 12, color: "#ff8b8b" }}>Error: {notesErr}</div> : null}
            </div>

            <div style={{ marginTop: 12, display: "grid", gap: 8 }}>
              {notesLoading ? <div style={{ fontSize: 12, opacity: 0.8 }}>Loading notes...</div> : null}
              {!notesLoading && notes.length === 0 ? (
                <div style={{ fontSize: 12, opacity: 0.75 }}>No notes yet.</div>
              ) : null}
              {notes.map((n) => (
                <div key={n.id} style={noteItem}>
                  <div style={{ fontSize: 11, opacity: 0.72, marginBottom: 4 }}>
                    {n.created_at ? new Date(n.created_at).toLocaleString() : "-"} • {n.created_by_discord_username || n.created_by_discord_user_id}
                  </div>
                  <div style={{ fontSize: 13, whiteSpace: "pre-wrap" }}>{n.note_text}</div>
                </div>
              ))}
            </div>
          </div>
        </>
      ) : null}
    </div>
  );
}

const btn = {
  background: "rgba(255,255,255,.06)",
  border: "1px solid rgba(255,255,255,.15)",
  color: "#e7ecff",
  borderRadius: 10,
  padding: "8px 10px",
  cursor: "pointer",
  fontSize: 12,
};

const metricCard = {
  border: "1px solid rgba(255,255,255,.10)",
  borderRadius: 10,
  padding: 10,
  background: "rgba(0,0,0,.20)",
};

const metricLabel = {
  fontSize: 11,
  opacity: 0.7,
  textTransform: "uppercase",
  letterSpacing: 0.3,
};

const metricValue = {
  marginTop: 6,
  fontSize: 18,
  fontWeight: 800,
};

const panel = {
  border: "1px solid rgba(255,255,255,.10)",
  borderRadius: 12,
  padding: 12,
  background: "rgba(255,255,255,.03)",
};

const textarea = {
  width: "100%",
  minHeight: 88,
  resize: "vertical",
  background: "rgba(0,0,0,.22)",
  border: "1px solid rgba(255,255,255,.14)",
  borderRadius: 10,
  color: "#e7ecff",
  padding: "10px 12px",
  fontSize: 13,
  outline: "none",
  boxSizing: "border-box",
};

const noteItem = {
  border: "1px solid rgba(255,255,255,.10)",
  borderRadius: 10,
  background: "rgba(0,0,0,.18)",
  padding: "10px 12px",
};

const line = {
  fontSize: 13,
  opacity: 0.8,
};

const th = {
  textAlign: "left",
  padding: "8px 6px",
  borderBottom: "1px solid rgba(255,255,255,.12)",
  color: "rgba(231,236,255,.75)",
};

const td = {
  padding: "8px 6px",
  borderBottom: "1px solid rgba(255,255,255,.08)",
};
