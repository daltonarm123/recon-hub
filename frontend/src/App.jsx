import React, { useEffect, useMemo, useState } from "react";
import {
  BrowserRouter,
  Routes,
  Route,
  Link,
  Navigate,
  useNavigate,
  useParams,
} from "react-router-dom";

import BackendBadge from "./BackendBadge";
import AdminHealth from "./AdminHealth";

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
        <header
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 12,
          }}
        >
          <div>
            <div style={{ fontWeight: 800, letterSpacing: 0.2 }}>Recon Hub</div>
            <div style={{ fontSize: 12, color: "rgba(231,236,255,.65)" }}>
              KG tools + recon database views
            </div>
          </div>

          <nav style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
            <Link style={navLink} to="/">Dashboard</Link>
            <Link style={navLink} to="/kingdoms">Kingdoms</Link>
            <Link style={navLink} to="/reports">Reports</Link>
            <Link style={navLink} to="/research">Research</Link>
            <Link style={navLink} to="/admin/health">Admin</Link>
            <a style={navLink} href="/kg-calc.html">Calc</a>
          </nav>
        </header>

        <div
          style={{
            height: 1,
            background: "rgba(255,255,255,.10)",
            margin: "14px 0",
          }}
        />
        {children}
      </div>
    </div>
  );
}

function Card({ title, subtitle, children, right }) {
  return (
    <div
      style={{
        border: "1px solid rgba(255,255,255,.10)",
        borderRadius: 14,
        overflow: "hidden",
        background: "rgba(255,255,255,.03)",
        boxShadow: "0 10px 30px rgba(0,0,0,.25)",
      }}
    >
      <div
        style={{
          padding: 12,
          borderBottom: "1px solid rgba(255,255,255,.10)",
          display: "flex",
          justifyContent: "space-between",
          gap: 10,
          alignItems: "center",
        }}
      >
        <div>
          <div style={{ fontWeight: 800, fontSize: 13 }}>{title}</div>
          {subtitle ? (
            <div style={{ fontSize: 12, color: "rgba(231,236,255,.65)" }}>
              {subtitle}
            </div>
          ) : null}
        </div>
        {right}
      </div>
      <div style={{ padding: 12 }}>{children}</div>
    </div>
  );
}

/* ---------------- Dashboard ---------------- */

function Dashboard() {
  return (
    <Layout>
      <Card title="Status" subtitle="Service availability">
        <BackendBadge />
      </Card>
    </Layout>
  );
}

/* ---------------- Kingdoms ---------------- */

function Kingdoms() {
  const [search, setSearch] = useState("");
  const query = useMemo(
    () => `${API_BASE}/api/kingdoms?search=${encodeURIComponent(search)}&limit=500`,
    [search]
  );
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
    const alliances = Array.from(map.keys()).sort((a, b) =>
      a.localeCompare(b)
    );
    return alliances.map((a) => [
      a,
      map.get(a).sort((x, y) => String(x.name).localeCompare(String(y.name))),
    ]);
  }, [data]);

  return (
    <Layout>
      <div style={{ display: "grid", gap: 14 }}>
        <Card
          title="Kingdoms"
          subtitle="Pulled from Postgres Recon Hub tables (rh_kingdoms + rh_spy_reports)."
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

          {grouped.map(([alliance, items]) => (
            <div key={alliance} style={{ marginTop: 10 }}>
              <div
                style={{
                  fontSize: 12,
                  color: "rgba(231,236,255,.75)",
                  marginBottom: 6,
                  textTransform: "uppercase",
                }}
              >
                Alliance: {alliance}
              </div>
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
                          onClick={() =>
                            nav(`/kingdoms/${encodeURIComponent(k.name)}`)
                          }
                        >
                          {k.name}
                        </button>
                      </td>
                      <td style={td}>{k.report_count ?? 0}</td>
                      <td style={td}>
                        {k.latest_report_at
                          ? new Date(k.latest_report_at).toLocaleString()
                          : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ))}
        </Card>
      </div>
    </Layout>
  );
}

/* ---------------- Admin ---------------- */

function Admin() {
  return (
    <Layout>
      <AdminHealth />
    </Layout>
  );
}

/* ---------------- Router ---------------- */

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/kingdoms" element={<Kingdoms />} />
        <Route path="/kingdoms/:name" element={<KingdomDetail />} />
        <Route path="/reports" element={<Reports />} />
        <Route path="/research" element={<Research />} />
        <Route path="/admin/health" element={<Admin />} />
        <Route path="/calc" element={<Navigate to="/kg-calc.html" replace />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  );
}

/* ---------------- Styles ---------------- */

const input = {
  width: "100%",
  background: "rgba(0,0,0,.25)",
  border: "1px solid rgba(255,255,255,.10)",
  borderRadius: 10,
  padding: "10px 12px",
  color: "#e7ecff",
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
};

const td = {
  padding: "10px 8px",
  borderBottom: "1px solid rgba(255,255,255,.08)",
};

const linkBtn = {
  background: "transparent",
  border: "none",
  color: "#5aa0ff",
  cursor: "pointer",
  fontSize: 12,
};
