import React, { useEffect, useMemo, useState } from "react";
import { BrowserRouter, Routes, Route, Link, Navigate, useNavigate, useParams } from "react-router-dom";

const API_BASE = ""; // same-origin

function useFetchJson(url, deps = []) {
  const [data, setData] = useState(null);
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setErr("");
    fetch(url)
      .then(async (r) => {
        const j = await r.json().catch(() => ({}));
        if (!r.ok) throw new Error(j?.detail || `HTTP ${r.status}`);
        return j;
      })
      .then((j) => alive && setData(j))
      .catch((e) => alive && setErr(String(e.message || e)))
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  return { data, err, loading };
}

const navLink = {
  color: "#e7ecff",
  textDecoration: "none",
  padding: "8px 10px",
  borderRadius: 10,
  border: "1px solid rgba(255,255,255,.10)",
  background: "rgba(255,255,255,.04)",
  fontSize: 12,
};

function Layout({ children }) {
  return (
    <div style={{ minHeight: "100vh", background: "#0b1020", color: "#e7ecff" }}>
      <div style={{ maxWidth: 1100, margin: "0 auto", padding: 16 }}>
        <header style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
          <div>
            <div style={{ fontWeight: 800, letterSpacing: 0.2 }}>Recon Hub</div>
            <div style={{ fontSize: 12, color: "rgba(231,236,255,.65)" }}>KG tools + recon database views</div>
          </div>

          <nav style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
            <Link style={navLink} to="/">Dashboard</Link>
            <Link style={navLink} to="/kingdoms">Kingdoms</Link>
            <Link style={navLink} to="/reports">Reports</Link>
            <Link style={navLink} to="/research">Research</Link>
            <a style={navLink} href="/kg-calc.html">Calc</a>
          </nav>
        </header>

        <div style={{ height: 1, background: "rgba(255,255,255,.10)", margin: "14px 0" }} />
        {children}
      </div>
    </div>
  );
}

function Card({ title, subtitle, children, right }) {
  return (
    <div style={{ border: "1px solid rgba(255,255,255,.10)", borderRadius: 14, overflow: "hidden", background: "rgba(255,255,255,.03)", boxShadow: "0 10px 30px rgba(0,0,0,.25)" }}>
      <div style={{ padding: 12, borderBottom: "1px solid rgba(255,255,255,.10)", display: "flex", justifyContent: "space-between", gap: 10, alignItems: "center" }}>
        <div>
          <div style={{ fontWeight: 800, fontSize: 13 }}>{title}</div>
          {subtitle ? <div style={{ fontSize: 12, color: "rgba(231,236,255,.65)" }}>{subtitle}</div> : null}
        </div>
        {right}
      </div>
      <div style={{ padding: 12 }}>{children}</div>
    </div>
  );
}

function Dashboard() {
  const { data, err, loading } = useFetchJson(`${API_BASE}/api/status`, []);
  return (
    <Layout>
      <Card title="Status" subtitle="Backend health check">
        {loading ? <div>Loading…</div> : null}
        {err ? <div style={{ color: "#ff6b6b" }}>{err}</div> : null}
        {data ? <pre style={pre}>{JSON.stringify(data, null, 2)}</pre> : null}
      </Card>
    </Layout>
  );
}

function Kingdoms() {
  const [search, setSearch] = useState("");
  const query = useMemo(() => `${API_BASE}/api/kingdoms?search=${encodeURIComponent(search)}&limit=500`, [search]);
  const { data, err, loading } = useFetchJson(query, [query]);
  const nav = useNavigate();

  const grouped = useMemo(() => {
    const list = data?.kingdoms || [];
    const map = new Map();
    for (const k of list) {
      const a = (k.alliance || "—").trim() || "—";
      if (!map.has(a)) map.set(a, []);
      map.get(a).push(k);
    }
    const alliances = Array.from(map.keys()).sort((a, b) => a.localeCompare(b));
    return alliances.map((a) => [a, map.get(a).sort((x, y) => String(x.name).localeCompare(String(y.name)))]);
  }, [data]);

  return (
    <Layout>
      <div style={{ display: "grid", gap: 14 }}>
        <Card
          title="Kingdoms"
          subtitle="Pulled from Postgres Recon Hub tables (rh_kingdoms + rh_spy_reports). If empty, use Reports to ingest spy reports."
          right={
            <input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search kingdom or alliance…"
              style={input}
            />
          }
        >
          {loading ? <div>Loading…</div> : null}
          {err ? <div style={{ color: "#ff6b6b" }}>{err}</div> : null}
          {data?.note ? <div style={{ color: "rgba(231,236,255,.65)", fontSize: 12 }}>{data.note}</div> : null}

          {grouped.length === 0 && !loading ? (
            <div style={{ color: "rgba(231,236,255,.65)", fontSize: 12 }}>
              No kingdoms yet. Paste a spy report in <b>Reports</b> to start building the list.
            </div>
          ) : null}

          {grouped.map(([alliance, items]) => (
            <div key={alliance} style={{ marginTop: 10 }}>
              <div style={{ fontSize: 12, color: "rgba(231,236,255,.75)", marginBottom: 6, textTransform: "uppercase", letterSpacing: 0.3 }}>
                Alliance: {alliance}
              </div>
              <div style={{ overflowX: "auto" }}>
                <table style={table}>
                  <thead>
                    <tr>
                      <th style={th}>Kingdom</th>
                      <th style={th}>Reports</th>
                      <th style={th}>Latest</th>
                    </tr>
                  </thead>
                  <tbody>
                    {items.map((k) => (
                      <tr key={`${alliance}:${k.name}`}>
                        <td style={td}>
                          <button
                            style={linkBtn}
                            onClick={() => nav(`/kingdoms/${encodeURIComponent(k.name)}`)}
                            title="Open reports"
                          >
                            {k.name}
                          </button>
                        </td>
                        <td style={td}>{k.report_count ?? 0}</td>
                        <td style={td}>{k.latest_report_at ? new Date(k.latest_report_at).toLocaleString() : "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </Card>

        <Card title="How this connects to your DB" subtitle="Safe by default (no touching existing bot tables)">
          <ul style={{ margin: 0, paddingLeft: 18, color: "rgba(231,236,255,.8)", fontSize: 12, lineHeight: 1.6 }}>
            <li>Recon Hub uses its own Postgres tables: <code>rh_kingdoms</code> and <code>rh_spy_reports</code>.</li>
            <li>Nothing in your existing bot schema is modified.</li>
            <li>To populate the list, paste spy reports on the Reports page (it stores + indexes them).</li>
          </ul>
        </Card>
      </div>
    </Layout>
  );
}

function KingdomDetail() {
  const { name } = useParams();
  const decoded = decodeURIComponent(name || "");
  const url = `${API_BASE}/api/kingdoms/${encodeURIComponent(decoded)}/spy-reports?limit=100`;
  const { data, err, loading } = useFetchJson(url, [url]);
  const nav = useNavigate();

  return (
    <Layout>
      <div style={{ display: "grid", gap: 14 }}>
        <Card
          title={`Spy Reports: ${decoded}`}
          subtitle="Latest spy reports stored in rh_spy_reports"
          right={<button style={btnGhost} onClick={() => nav("/kingdoms")}>Back</button>}
        >
          {loading ? <div>Loading…</div> : null}
          {err ? <div style={{ color: "#ff6b6b" }}>{err}</div> : null}

          <div style={{ overflowX: "auto" }}>
            <table style={table}>
              <thead>
                <tr>
                  <th style={th}>Date</th>
                  <th style={th}>Alliance</th>
                  <th style={th}>Defender DP</th>
                  <th style={th}>Castles</th>
                  <th style={th}>Troops keys</th>
                  <th style={th}>Raw</th>
                </tr>
              </thead>
              <tbody>
                {(data?.reports || []).map((r) => (
                  <tr key={r.id}>
                    <td style={td}>{r.created_at ? new Date(r.created_at).toLocaleString() : "—"}</td>
                    <td style={td}>{r.alliance || "—"}</td>
                    <td style={td}>{r.defender_dp ? Number(r.defender_dp).toLocaleString() : "—"}</td>
                    <td style={td}>{r.castles ?? "—"}</td>
                    <td style={td}>{r.troops ? Object.keys(r.troops).length : 0}</td>
                    <td style={td}>
                      <a style={{ color: "#5aa0ff" }} href={`/api/spy-reports/${r.id}`} target="_blank" rel="noreferrer">
                        view
                      </a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {(data?.reports || []).length === 0 && !loading ? (
            <div style={{ color: "rgba(231,236,255,.65)", fontSize: 12 }}>
              No spy reports stored yet for this kingdom. Paste one in the Reports page.
            </div>
          ) : null}
        </Card>
      </div>
    </Layout>
  );
}

function Reports() {
  const [raw, setRaw] = useState("");
  const [msg, setMsg] = useState("");
  const [busy, setBusy] = useState(false);

  async function ingest() {
    setBusy(true);
    setMsg("");
    try {
      const r = await fetch(`${API_BASE}/api/reports/spy`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ raw_text: raw }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(j?.detail || `HTTP ${r.status}`);
      setMsg(`Stored report #${j?.stored?.id} for ${j?.parsed?.target || "?"}`);
      setRaw("");
    } catch (e) {
      setMsg(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Layout>
      <div style={{ display: "grid", gap: 14 }}>
        <Card title="Reports" subtitle="Paste a KG spy report to store + index it (Postgres rh_* tables)">
          <textarea
            value={raw}
            onChange={(e) => setRaw(e.target.value)}
            placeholder="Paste the full KG Spy Report text here…"
            style={{ ...input, height: 220, fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, Liberation Mono, Courier New, monospace", fontSize: 12, lineHeight: 1.35 }}
          />
          <div style={{ display: "flex", gap: 10, marginTop: 10, alignItems: "center", flexWrap: "wrap" }}>
            <button style={btn} onClick={ingest} disabled={busy || !raw.trim()}>
              {busy ? "Saving…" : "Parse + Save"}
            </button>
            {msg ? <div style={{ fontSize: 12, color: msg.startsWith("Stored") ? "#58d68d" : "#ff6b6b" }}>{msg}</div> : null}
          </div>
          <div style={{ marginTop: 10, fontSize: 12, color: "rgba(231,236,255,.65)" }}>
            Tip: After saving, go to <b>Kingdoms</b> to see it listed by alliance.
          </div>
        </Card>
      </div>
    </Layout>
  );
}

function Research() {
  return (
    <Layout>
      <Card title="Research" subtitle="Next tab after Kingdoms / Reports is solid">
        <div style={{ color: "rgba(231,236,255,.65)", fontSize: 12 }}>
          Placeholder for now.
        </div>
      </Card>
    </Layout>
  );
}

const pre = {
  background: "rgba(0,0,0,.25)",
  border: "1px solid rgba(255,255,255,.10)",
  borderRadius: 12,
  padding: 12,
  overflow: "auto",
  margin: 0,
  fontSize: 12,
  color: "rgba(231,236,255,.85)",
};

const input = {
  width: "100%",
  background: "rgba(0,0,0,.25)",
  border: "1px solid rgba(255,255,255,.10)",
  borderRadius: 10,
  padding: "10px 12px",
  color: "#e7ecff",
  outline: "none",
};

const table = {
  width: "100%",
  borderCollapse: "collapse",
  fontSize: 12,
};

const th = {
  textAlign: "left",
  padding: "10px 8px",
  borderBottom: "1px solid rgba(255,255,255,.10)",
  color: "rgba(231,236,255,.65)",
  whiteSpace: "nowrap",
};

const td = {
  padding: "10px 8px",
  borderBottom: "1px solid rgba(255,255,255,.08)",
  whiteSpace: "nowrap",
};

const btn = {
  background: "rgba(90,160,255,.16)",
  border: "1px solid rgba(90,160,255,.35)",
  color: "#e7ecff",
  padding: "8px 10px",
  borderRadius: 10,
  cursor: "pointer",
  fontSize: 12,
};

const btnGhost = {
  ...btn,
  background: "rgba(255,255,255,.06)",
  border: "1px solid rgba(255,255,255,.10)",
  color: "rgba(231,236,255,.8)",
};

const linkBtn = {
  background: "transparent",
  border: "none",
  padding: 0,
  margin: 0,
  color: "#5aa0ff",
  cursor: "pointer",
  fontSize: 12,
};

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/kingdoms" element={<Kingdoms />} />
        <Route path="/kingdoms/:name" element={<KingdomDetail />} />
        <Route path="/reports" element={<Reports />} />
        <Route path="/research" element={<Research />} />
        <Route path="/calc" element={<Navigate to="/kg-calc.html" replace />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  );
}
