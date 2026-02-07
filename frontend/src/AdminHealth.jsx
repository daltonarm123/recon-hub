import { useEffect, useState } from "react";

export default function AdminHealth() {
  const [overview, setOverview] = useState(null);
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(true);

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

  useEffect(() => {
    load();
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
