import React, { Suspense, lazy, useDeferredValue, useEffect, useMemo, useState } from "react";
import {
    BrowserRouter,
    Routes,
    Route,
    Link,
    NavLink,
    Navigate,
    useNavigate,
    useParams,
    useLocation,
} from "react-router-dom";

import BackendBadge from "./BackendBadge";
import "./App.css";

const AdminHealth = lazy(() => import("./AdminHealth"));
const NWChart = lazy(() => import("./NWChart"));

const API_BASE = ""; // same-origin

function timeAgo(dateString) {
    if (!dateString) return "—";
    const d = new Date(dateString);
    const now = new Date();
    const diff = Math.floor((now - d) / 1000); // seconds
    if (diff < 60) return "Just now";
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    return `${Math.floor(diff / 86400)}d ago`;
}

function useFetchJson(url, deps = []) {
    const [data, setData] = useState(null);
    const [err, setErr] = useState("");
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        if (!url) {
            setData(null);
            setErr("");
            setLoading(false);
            return undefined;
        }

        let alive = true;
        const controller = new AbortController();
        setLoading(true);
        setErr("");

        fetch(url, { credentials: "include", signal: controller.signal })
            .then(async (r) => {
                const j = await r.json().catch(() => ({}));
                if (!r.ok) throw new Error(j?.detail || `HTTP ${r.status}`);
                return j;
            })
            .then((j) => alive && setData(j))
            .catch((e) => {
                if (!alive || e?.name === "AbortError") return;
                setErr(String(e.message || e));
            })
            .finally(() => alive && setLoading(false));

        return () => {
            alive = false;
            controller.abort();
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, deps);

    return { data, err, loading };
}

function useFetchText(url, deps = []) {
    const [data, setData] = useState("");
    const [err, setErr] = useState("");
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        if (!url) {
            setData("");
            setErr("");
            setLoading(false);
            return undefined;
        }

        let alive = true;
        const controller = new AbortController();
        setLoading(true);
        setErr("");

        fetch(url, {
            headers: { Accept: "text/plain" },
            credentials: "include",
            signal: controller.signal,
        })
            .then(async (r) => {
                const t = await r.text();
                if (!r.ok) throw new Error(t || `HTTP ${r.status}`);
                return t;
            })
            .then((t) => alive && setData(t))
            .catch((e) => {
                if (!alive || e?.name === "AbortError") return;
                setErr(String(e.message || e));
            })
            .finally(() => alive && setLoading(false));

        return () => {
            alive = false;
            controller.abort();
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, deps);

    return { data, err, loading };
}

function useAuthMe(refreshKey = 0) {
    const [data, setData] = useState({ authenticated: false });
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        let alive = true;
        fetch(`${API_BASE}/auth/me`, { credentials: "include" })
            .then((r) => r.json().catch(() => ({ authenticated: false })))
            .then((j) => {
                if (!alive) return;
                setData({
                    authenticated: Boolean(j?.authenticated),
                    user: j?.user || null,
                });
            })
            .catch(() => {
                if (!alive) return;
                setData({ authenticated: false, user: null });
            })
            .finally(() => alive && setLoading(false));

        return () => {
            alive = false;
        };
    }, [refreshKey]);

    return { data, loading };
}

function KVTable({ obj, formatValue = (value) => value }) {
    const entries = Object.entries(obj || {});
    if (entries.length === 0) {
        return <div style={{ color: "rgba(231,236,255,.65)", fontSize: 12 }}>—</div>;
    }

    return (
        <div style={{ overflowX: "auto" }}>
            <table style={{ ...table, minWidth: 360 }}>
                <thead>
                    <tr>
                        <th style={th}>Name</th>
                        <th style={th}>Value</th>
                    </tr>
                </thead>
                <tbody>
                    {entries
                        .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
                        .map(([k, v]) => (
                            <tr key={k}>
                                <td style={td}>{k}</td>
                                <td style={td}>{formatValue(v)}</td>
                            </tr>
                        ))}
                </tbody>
            </table>
        </div>
    );
}

const navLink = {
    color: "var(--rh-text)",
    textDecoration: "none",
    padding: "8px 12px",
    borderRadius: 10,
    border: "1px solid var(--rh-border)",
    background: "linear-gradient(180deg, rgba(173,132,72,.30), rgba(111,82,42,.20))",
    fontSize: 12,
    fontWeight: 700,
    letterSpacing: ".2px",
};

const navLinkActive = {
    border: "1px solid rgba(223,188,127,.68)",
    background: "linear-gradient(180deg, rgba(197,154,82,.44), rgba(132,96,47,.26))",
};

const navGroup = {
    border: "1px solid var(--rh-border)",
    borderRadius: 10,
    padding: 8,
    background: "rgba(60,45,25,.25)",
    minWidth: 210,
};

const navGroupLabel = {
    fontSize: 10,
    textTransform: "uppercase",
    letterSpacing: 0.6,
    color: "var(--rh-muted)",
    marginBottom: 6,
    fontWeight: 700,
    fontFamily: "var(--rh-head)",
};

function Layout({ children }) {
    const [authRefresh, setAuthRefresh] = useState(0);
    const auth = useAuthMe(authRefresh);
    const location = useLocation();

    async function logout() {
        await fetch(`${API_BASE}/auth/logout`, {
            method: "POST",
            credentials: "include",
        }).catch(() => null);
        setAuthRefresh((x) => x + 1);
    }

    function isActivePath(to) {
        if (to === "/") return location.pathname === "/";
        return location.pathname === to || location.pathname.startsWith(`${to}/`);
    }

    function navStyle(to) {
        return isActivePath(to) ? { ...navLink, ...navLinkActive } : navLink;
    }

    return (
        <div style={{ minHeight: "100vh", background: "transparent", color: "var(--rh-text)" }}>
            <div style={{ maxWidth: 1240, margin: "0 auto", padding: 20 }}>
                <a className="skip-link" href="#main-content">
                    Skip to content
                </a>
                <header
                    style={{
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "space-between",
                        gap: 12,
                        flexWrap: "wrap",
                        background: "linear-gradient(180deg, rgba(153,116,62,.26), rgba(89,66,36,.20))",
                        border: "1px solid var(--rh-border)",
                        borderRadius: 14,
                        padding: 12,
                        boxShadow: "0 14px 36px rgba(0,0,0,.34)",
                    }}
                >
                    <div>
                        <div style={{ fontWeight: 800, letterSpacing: 0.4, fontFamily: "var(--rh-head)", textTransform: "uppercase" }}>Recon Hub</div>
                        <div style={{ fontSize: 12, color: "var(--rh-muted)" }}>
                            KG tools + recon database views
                        </div>
                    </div>

                    <nav style={{ display: "grid", gap: 8 }}>
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                            <NavLink style={navStyle("/")} to="/">Dashboard</NavLink>
                            <NavLink style={navStyle("/reports")} to="/reports">Reports</NavLink>
                            <NavLink style={navStyle("/kingdoms")} to="/kingdoms">Kingdoms</NavLink>
                            {auth.data?.user?.is_admin ? (
                                <NavLink style={navStyle("/admin/health")} to="/admin/health">Admin</NavLink>
                            ) : null}
                            {auth.loading ? null : auth.data?.authenticated ? (
                                <button style={navBtn} onClick={logout} title="Logout Discord session">
                                    {auth.data?.user?.discord_username || "Discord"} Logout
                                </button>
                            ) : (
                                <a style={navLink} href="/login">Login</a>
                            )}
                        </div>

                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                            <div style={navGroup}>
                                <div style={navGroupLabel}>Intel</div>
                                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                                    <NavLink style={navStyle("/nwot")} to="/nwot">NWOT</NavLink>
                                </div>
                            </div>

                            <div style={navGroup}>
                                <div style={navGroupLabel}>Settlements</div>
                                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                                    <NavLink style={navStyle("/settlements")} to="/settlements">Overview</NavLink>
                                    <NavLink style={navStyle("/tracked-settlements")} to="/tracked-settlements">Tracking</NavLink>
                                    <NavLink style={navStyle("/settlement-effects")} to="/settlement-effects">Effects</NavLink>
                                </div>
                            </div>

                            <div style={navGroup}>
                                <div style={navGroupLabel}>Tools</div>
                                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                                    <NavLink style={navStyle("/research")} to="/research">Research</NavLink>
                                    <a style={navLink} href="/kg-calc.html">Calc</a>
                                    <a style={navLink} href="/kg-calc.html?tool=sim">Simulator</a>
                                    <a style={navLink} href="/kg-calc.html?tool=return">Return Time</a>
                                </div>
                            </div>

                            {auth.data?.user?.is_admin ? (
                                <div style={navGroup}>
                                    <div style={navGroupLabel}>Alliance</div>
                                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                                        <a style={navLink} href="/admin/health#alliance-access-control">Alliance Access</a>
                                    </div>
                                </div>
                            ) : null}
                        </div>
                    </nav>
                </header>

                <div
                    style={{
                        height: 1,
                        background: "var(--rh-border)",
                        margin: "14px 0",
                    }}
                />
                <main id="main-content">{children}</main>
            </div>
        </div>
    );
}

function Card({ title, subtitle, children, right }) {
    return (
        <div
            style={{
                border: "1px solid var(--rh-border)",
                borderRadius: 14,
                overflow: "hidden",
                background: "linear-gradient(180deg, rgba(255,236,201,.08), rgba(255,236,201,.04))",
                boxShadow: "0 16px 40px rgba(0,0,0,.36)",
            }}
        >
            <div
                style={{
                    padding: 12,
                    borderBottom: "1px solid var(--rh-border)",
                    display: "flex",
                    justifyContent: "space-between",
                    gap: 10,
                    alignItems: "center",
                    flexWrap: "wrap",
                }}
            >
                <div>
                    <div style={{ fontWeight: 800, fontSize: 13, fontFamily: "var(--rh-head)", letterSpacing: ".35px" }}>{title}</div>
                    {subtitle ? (
                        <div style={{ fontSize: 12, color: "var(--rh-muted)" }}>
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

function EmptyState({ title, body, action }) {
    return (
        <div className="empty-state">
            <div style={{ fontWeight: 800, fontFamily: "var(--rh-head)", letterSpacing: 0.2 }}>{title}</div>
            <div style={{ fontSize: 13, color: "var(--rh-muted)", lineHeight: 1.45 }}>{body}</div>
            {action ? <div>{action}</div> : null}
        </div>
    );
}

function QuickLinkCard({ to, title, description, meta }) {
    return (
        <Link className="quick-link-card" to={to}>
            <div style={{ fontWeight: 800, fontFamily: "var(--rh-head)", letterSpacing: 0.25 }}>{title}</div>
            <div style={{ fontSize: 13, color: "var(--rh-muted)", lineHeight: 1.45 }}>{description}</div>
            <div style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 0.45, color: "var(--rh-accent)" }}>{meta}</div>
        </Link>
    );
}

/* ---------------- Dashboard ---------------- */

function Dashboard() {
    const [statusRefresh, setStatusRefresh] = useState(0);
    const nwStatus = useFetchJson(`${API_BASE}/api/nw/status?r=${encodeURIComponent(statusRefresh)}`, [statusRefresh]);

    useEffect(() => {
        const id = window.setInterval(() => setStatusRefresh((v) => v + 1), 60000);
        return () => window.clearInterval(id);
    }, []);

    const nwTickAgeSeconds = Number(nwStatus.data?.nw_tick_age_seconds);
    const nwTrackerHealthy = Number.isFinite(nwTickAgeSeconds) && nwTickAgeSeconds >= 0 && nwTickAgeSeconds < 900;

    return (
        <Layout>
            <div style={{ display: "grid", gap: 14 }}>
                <Card title="Command Center" subtitle="Fast paths into the data views and KG tooling you use the most.">
                    <div className="dashboard-hero">
                        <div style={{ display: "grid", gap: 12 }}>
                            <div>
                                <div style={{ fontSize: 28, fontFamily: "var(--rh-head)", fontWeight: 800, lineHeight: 1.1 }}>
                                    Recon Hub keeps the recon loop tight.
                                </div>
                                <div style={{ marginTop: 8, fontSize: 14, color: "var(--rh-muted)", lineHeight: 1.5, maxWidth: 680 }}>
                                    Ingest reports, inspect kingdom history, monitor settlement drift, and jump into KG utilities without hunting through raw endpoints.
                                </div>
                            </div>

                            <div className="dashboard-actions">
                                <Link style={{ ...btn, textDecoration: "none" }} to="/reports">
                                    Paste a Report
                                </Link>
                                <Link style={{ ...btnGhost, textDecoration: "none" }} to="/nwot">
                                    Open NWOT
                                </Link>
                                <Link style={{ ...btnGhost, textDecoration: "none" }} to="/settlements">
                                    Connect KG
                                </Link>
                            </div>
                        </div>

                        <div className="dashboard-status-panel">
                            <div style={{ fontSize: 12, textTransform: "uppercase", letterSpacing: 0.5, color: "var(--rh-muted)" }}>
                                Service Health
                            </div>
                            <BackendBadge />
                            <div
                                style={{
                                    border: "1px solid var(--rh-border)",
                                    borderRadius: 10,
                                    padding: "8px 10px",
                                    background: "rgba(255,236,201,.05)",
                                    display: "grid",
                                    gap: 4,
                                }}
                            >
                                <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center" }}>
                                    <span style={{ fontSize: 11, color: "var(--rh-muted)", textTransform: "uppercase", letterSpacing: 0.35 }}>
                                        NW Tracker
                                    </span>
                                    <span
                                        style={{
                                            fontSize: 11,
                                            fontWeight: 700,
                                            color: nwTrackerHealthy ? "#7ad8a2" : "#ffc56a",
                                        }}
                                    >
                                        {nwStatus.loading ? "Checking..." : nwTrackerHealthy ? "Healthy" : "Waiting"}
                                    </span>
                                </div>
                                <div style={{ fontSize: 12, color: "var(--rh-muted)", lineHeight: 1.4 }}>
                                    Tick: {nwStatus.data?.last_nw_tick ? timeAgo(nwStatus.data.last_nw_tick) : "not available"}
                                </div>
                                <div style={{ fontSize: 12, color: "var(--rh-muted)", lineHeight: 1.4 }}>
                                    Rankings: {nwStatus.data?.last_rankings_fetch ? timeAgo(nwStatus.data.last_rankings_fetch) : "not available"}
                                </div>
                            </div>
                            <div style={{ fontSize: 12, color: "var(--rh-muted)", lineHeight: 1.45 }}>
                                Use this as the landing page to verify backend reachability before opening heavier data views.
                            </div>
                        </div>
                    </div>
                </Card>

                <div className="quick-link-grid">
                    <QuickLinkCard
                        to="/reports"
                        title="Reports"
                        description="Paste spy or attack reports and turn them into searchable kingdom and settlement data."
                        meta="Ingest + parse"
                    />
                    <QuickLinkCard
                        to="/kingdoms"
                        title="Kingdoms"
                        description="Search alliance-grouped kingdoms, inspect freshness, and jump directly to stored report history."
                        meta="Search + drill in"
                    />
                    <QuickLinkCard
                        to="/nwot"
                        title="NWOT"
                        description="Filter kingdoms quickly and view networth history without reloading the page on every keystroke."
                        meta="Trend view"
                    />
                    <QuickLinkCard
                        to="/tracked-settlements"
                        title="Tracked Settlements"
                        description="Watch sightings, failed takes, captures, and the latest observed levels in one place."
                        meta="Live tracking"
                    />
                </div>
            </div>
        </Layout>
    );
}

/* ---------------- Kingdoms ---------------- */

function Kingdoms() {
    const [search, setSearch] = useState("");
    const deferredSearch = useDeferredValue(search);
    const query = useMemo(
        () =>
            `${API_BASE}/api/kingdoms?search=${encodeURIComponent(
                deferredSearch
            )}&limit=500`,
        [deferredSearch]
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
        const alliances = Array.from(map.keys()).sort((a, b) => a.localeCompare(b));
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
                    {data?.note ? (
                        <div style={{ color: "rgba(231,236,255,.65)", fontSize: 12 }}>
                            {data.note}
                        </div>
                    ) : null}

                    {grouped.length === 0 && !loading ? (
                        <div style={{ color: "rgba(231,236,255,.65)", fontSize: 12 }}>
                            No kingdoms yet. Paste a spy report in <b>Reports</b> to start
                            building the list.
                        </div>
                    ) : null}

                    {grouped.map(([alliance, items]) => (
                        <div key={alliance} style={{ marginTop: 10 }}>
                            <div
                                style={{
                                    fontSize: 12,
                                    color: "rgba(231,236,255,.75)",
                                    marginBottom: 6,
                                    textTransform: "uppercase",
                                    letterSpacing: 0.3,
                                }}
                            >
                                Alliance: {alliance}
                            </div>

                            <div className="table-wrap">
                                <table className="data-table" style={table}>
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
                                                <td data-label="Kingdom" style={td}>
                                                    <button
                                                        style={linkBtn}
                                                        onClick={() =>
                                                            nav(`/kingdoms/${encodeURIComponent(k.name)}`)
                                                        }
                                                        title="Open reports"
                                                    >
                                                        {k.name}
                                                    </button>
                                                </td>
                                                <td data-label="Reports" style={td}>{k.report_count ?? 0}</td>
                                                <td data-label="Latest" style={td} title={k.latest_report_at ? new Date(k.latest_report_at).toLocaleString() : ""}>
                                                    {timeAgo(k.latest_report_at)}
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    ))}
                </Card>
            </div>
        </Layout>
    );
}

/* ---------------- Kingdom Detail ---------------- */

function KingdomDetail() {
    const { name } = useParams();
    const decoded = decodeURIComponent(name || "");
    const url = `${API_BASE}/api/kingdoms/${encodeURIComponent(
        decoded
    )}/spy-reports?limit=100`;
    const { data, err, loading } = useFetchJson(url, [url]);
    const nav = useNavigate();

    return (
        <Layout>
            <div style={{ display: "grid", gap: 14 }}>
                <Card
                    title={`Spy Reports: ${decoded}`}
                    subtitle="Latest spy reports stored in rh_spy_reports"
                    right={
                        <button style={btnGhost} onClick={() => nav("/kingdoms")}>
                            Back
                        </button>
                    }
                >
                    {loading ? <div>Loading…</div> : null}
                    {err ? <div style={{ color: "#ff6b6b" }}>{err}</div> : null}

                    <div className="table-wrap">
                        <table className="data-table" style={table}>
                            <thead>
                                <tr>
                                    <th style={th}>Date</th>
                                    <th style={th}>Alliance</th>
                                    <th style={th}>Defender DP</th>
                                    <th style={th}>Castles</th>
                                    <th style={th}>Troops keys</th>
                                    <th style={th}>View</th>
                                </tr>
                            </thead>
                            <tbody>
                                {(data?.reports || []).map((r) => (
                                    <tr key={r.id}>
                                        <td data-label="Date" style={td} title={r.created_at ? new Date(r.created_at).toLocaleString() : ""}>
                                            {timeAgo(r.created_at)}
                                        </td>
                                        <td data-label="Alliance" style={td}>{r.alliance || "—"}</td>
                                        <td data-label="Defender DP" style={td}>
                                            {r.defender_dp
                                                ? Number(r.defender_dp).toLocaleString()
                                                : "—"}
                                        </td>
                                        <td data-label="Castles" style={td}>{r.castles ?? "—"}</td>
                                        <td data-label="Troops Keys" style={td}>
                                            {r.troops ? Object.keys(r.troops).length : 0}
                                        </td>
                                        <td data-label="View" style={td}>
                                            <button
                                                style={linkBtn}
                                                onClick={() => nav(`/spy-reports/${r.id}`)}
                                                title="View spy report"
                                            >
                                                view
                                            </button>
                                            <span style={{ opacity: 0.5 }}>{" · "}</span>
                                            <a
                                                style={{ color: "#5aa0ff" }}
                                                href={`/api/spy-reports/${r.id}/raw`}
                                                target="_blank"
                                                rel="noreferrer"
                                                title="Open raw in new tab"
                                            >
                                                raw
                                            </a>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>

                    {(data?.reports || []).length === 0 && !loading ? (
                        <div style={{ color: "rgba(231,236,255,.65)", fontSize: 12 }}>
                            No spy reports stored yet for this kingdom. Paste one in the Reports
                            page.
                        </div>
                    ) : null}
                </Card>
            </div>
        </Layout>
    );
}

/* ---------------- Spy Report View (Readable) ---------------- */

function SpyReportView() {
    const { id } = useParams();
    const nav = useNavigate();

    const metaUrl = `${API_BASE}/api/spy-reports/${id}`;
    const rawUrl = `${API_BASE}/api/spy-reports/${id}/raw`;

    const meta = useFetchJson(metaUrl, [metaUrl]);
    const raw = useFetchText(rawUrl, [rawUrl]);

    const r = meta.data?.report || null;

    const title = r?.kingdom_name
        ? `Latest Spy Report — ${r.kingdom_name}`
        : `Spy Report #${id}`;
    const subtitle = r?.created_at
        ? `Stored: ${new Date(r.created_at).toLocaleString()}`
        : "Readable spy report view";

    const troops = r?.troops || {};
    const resources = r?.resources || {};

    function fmtNum(x) {
        if (x === null || x === undefined || x === "") return "—";
        const n = Number(x);
        return Number.isFinite(n) ? n.toLocaleString() : String(x);
    }

    return (
        <Layout>
            <div style={{ display: "grid", gap: 14 }}>
                <Card
                    title={title}
                    subtitle={subtitle}
                    right={
                        <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                            <a
                                style={{ ...btnGhost, textDecoration: "none", display: "inline-block" }}
                                href={`/api/spy-reports/${id}/raw`}
                                target="_blank"
                                rel="noreferrer"
                            >
                                Open raw
                            </a>
                            <button style={btnGhost} onClick={() => nav(-1)}>
                                Back
                            </button>
                        </div>
                    }
                >
                    {meta.loading || raw.loading ? <div>Loading…</div> : null}
                    {meta.err ? <div style={{ color: "#ff6b6b" }}>{meta.err}</div> : null}
                    {raw.err ? <div style={{ color: "#ff6b6b" }}>{raw.err}</div> : null}

                    <div
                        style={{
                            display: "grid",
                            gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))",
                            gap: 10,
                            marginBottom: 12,
                        }}
                    >
                        <div style={{ ...pill }}>
                            <div style={pillLabel}>Result</div>
                            <div style={pillValue}>{r?.result_level || "—"}</div>
                        </div>
                        <div style={{ ...pill }}>
                            <div style={pillLabel}>Spies</div>
                            <div style={pillValue}>
                                Sent {fmtNum(r?.spies_sent)} | Lost {fmtNum(r?.spies_lost)}
                            </div>
                        </div>
                        <div style={{ ...pill }}>
                            <div style={pillLabel}>Defender DP</div>
                            <div style={pillValue}>{fmtNum(r?.defender_dp)}</div>
                        </div>
                        <div style={{ ...pill }}>
                            <div style={pillLabel}>Castles</div>
                            <div style={pillValue}>{fmtNum(r?.castles)}</div>
                        </div>
                        <div style={{ ...pill }}>
                            <div style={pillLabel}>Honour / Rank</div>
                            <div style={pillValue}>
                                {r?.honour ?? "—"} / {fmtNum(r?.ranking)}
                            </div>
                        </div>
                        <div style={{ ...pill }}>
                            <div style={pillLabel}>Networth</div>
                            <div style={pillValue}>{fmtNum(r?.networth)}</div>
                        </div>
                    </div>

                    <div
                        style={{
                            display: "grid",
                            gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
                            gap: 12,
                        }}
                    >
                        <div>
                            <div style={{ fontWeight: 800, marginBottom: 8 }}>Troops</div>
                            <KVTable obj={troops} formatValue={fmtNum} />
                        </div>
                        <div>
                            <div style={{ fontWeight: 800, marginBottom: 8 }}>Resources</div>
                            <KVTable obj={resources} formatValue={fmtNum} />
                        </div>
                    </div>

                    <div style={{ marginTop: 12 }}>
                        <details>
                            <summary style={{ cursor: "pointer", color: "rgba(231,236,255,.85)" }}>
                                Raw report text
                            </summary>
                            <pre
                                style={{
                                    whiteSpace: "pre-wrap",
                                    wordBreak: "break-word",
                                    background: "rgba(0,0,0,.25)",
                                    border: "1px solid rgba(255,255,255,.10)",
                                    borderRadius: 12,
                                    padding: 12,
                                    marginTop: 10,
                                    fontSize: 12,
                                    lineHeight: 1.35,
                                    fontFamily:
                                        "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, Liberation Mono, Courier New, monospace",
                                }}
                            >
                                {raw.data || ""}
                            </pre>
                        </details>
                    </div>
                </Card>
            </div>
        </Layout>
    );
}

/* ---------------- Reports ---------------- */

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
            const kind = j?.report_type || "report";
            const target = j?.parsed?.target || "?";
            const ev = Number(j?.settlement_events || 0);
            setMsg(`Stored ${kind} report #${j?.stored?.id} for ${target} (${ev} settlement event${ev === 1 ? "" : "s"})`);
            setRaw("");
        } catch (e) {
            setMsg(String(e.message || e));
        } finally {
            setBusy(false);
        }
    }

    function handleRawKeyDown(e) {
        if ((e.metaKey || e.ctrlKey) && e.key === "Enter" && raw.trim() && !busy) {
            e.preventDefault();
            ingest();
        }
    }

    return (
        <Layout>
            <div style={{ display: "grid", gap: 14 }}>
                <Card
                    title="Reports"
                    subtitle="Paste a KG spy report or attack report to store + track settlements."
                >
                    <textarea
                        value={raw}
                        onChange={(e) => setRaw(e.target.value)}
                        onKeyDown={handleRawKeyDown}
                        placeholder="Paste the full KG Spy Report text here…"
                        style={{
                            ...input,
                            height: 220,
                            fontFamily:
                                "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, Liberation Mono, Courier New, monospace",
                            fontSize: 12,
                            lineHeight: 1.35,
                        }}
                    />
                    <div
                        style={{
                            display: "flex",
                            gap: 10,
                            marginTop: 10,
                            alignItems: "center",
                            flexWrap: "wrap",
                        }}
                    >
                        <button style={btn} onClick={ingest} disabled={busy || !raw.trim()}>
                            {busy ? "Saving…" : "Parse + Save"}
                        </button>
                        {msg ? (
                            <div
                                style={{
                                    fontSize: 12,
                                    color: msg.startsWith("Stored") ? "#58d68d" : "#ff6b6b",
                                }}
                            >
                                {msg}
                            </div>
                        ) : null}
                    </div>

                    <div style={{ marginTop: 10, fontSize: 12, color: "rgba(231,236,255,.65)" }}>
                        Shortcut: press Ctrl/Cmd + Enter to parse and save.
                    </div>

                    <div style={{ marginTop: 10, fontSize: 12, color: "rgba(231,236,255,.65)" }}>
                        Tip: Spy reports show in <b>Kingdoms</b>. Settlement observations are at{" "}
                        <code>/api/settlements/tracked</code>.
                    </div>
                </Card>
            </div>
        </Layout>
    );
}

/* ---------------- Research ---------------- */

function Settlements() {
    const [refresh, setRefresh] = useState(0);
    const [form, setForm] = useState({ account_id: "", kingdom_id: "", token: "" });
    const [snippet, setSnippet] = useState("");
    const [busy, setBusy] = useState(false);
    const [msg, setMsg] = useState("");

    const conn = useFetchJson(`${API_BASE}/api/kg/connection?r=${refresh}`, [refresh]);
    const settlements = useFetchJson(`${API_BASE}/api/kg/settlements?r=${refresh}`, [refresh]);

    async function connectKg() {
        setBusy(true);
        setMsg("");
        try {
            const r = await fetch(`${API_BASE}/api/kg/connect`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                credentials: "include",
                body: JSON.stringify({
                    account_id: Number(form.account_id),
                    kingdom_id: Number(form.kingdom_id),
                    token: form.token.trim(),
                }),
            });
            const j = await r.json().catch(() => ({}));
            if (!r.ok) throw new Error(j?.detail || `HTTP ${r.status}`);
            setMsg("KG account connected.");
            setForm((f) => ({ ...f, token: "" }));
            setRefresh((x) => x + 1);
        } catch (e) {
            setMsg(String(e.message || e));
        } finally {
            setBusy(false);
        }
    }

    function parseKgSnippet(raw) {
        const txt = String(raw || "");
        const pick = (patterns) => {
            for (const p of patterns) {
                const m = txt.match(p);
                if (m && m[1]) return String(m[1]).trim();
            }
            return "";
        };

        const accountId = pick([
            /"accountId"\s*:\s*"?(\d+)"?/i,
            /\baccountId\s*:\s*"?(\d+)"?/i,
            /"account_id"\s*:\s*"?(\d+)"?/i,
        ]);
        const kingdomId = pick([
            /"kingdomId"\s*:\s*"?(\d+)"?/i,
            /\bkingdomId\s*:\s*"?(\d+)"?/i,
            /"kingdom_id"\s*:\s*"?(\d+)"?/i,
        ]);
        const token = pick([
            /"token"\s*:\s*"([A-Za-z0-9-]{16,})"/i,
            /\btoken\s*:\s*"([A-Za-z0-9-]{16,})"/i,
            /"token"\s*:\s*"?([A-Za-z0-9-]{16,})"?/i,
        ]);

        return { accountId, kingdomId, token };
    }

    function applySnippet() {
        const parsed = parseKgSnippet(snippet);
        if (!parsed.accountId || !parsed.kingdomId || !parsed.token) {
            setMsg("Could not parse accountId/kingdomId/token from pasted text.");
            return;
        }
        setForm({
            account_id: parsed.accountId,
            kingdom_id: parsed.kingdomId,
            token: parsed.token,
        });
        setMsg("Detected account and token from pasted snippet.");
    }

    async function disconnectKg() {
        setBusy(true);
        setMsg("");
        try {
            const r = await fetch(`${API_BASE}/api/kg/connection`, {
                method: "DELETE",
                credentials: "include",
            });
            const j = await r.json().catch(() => ({}));
            if (!r.ok) throw new Error(j?.detail || `HTTP ${r.status}`);
            setMsg("KG account disconnected.");
            setRefresh((x) => x + 1);
        } catch (e) {
            setMsg(String(e.message || e));
        } finally {
            setBusy(false);
        }
    }

    const connected = Boolean(conn.data?.connected);

    return (
        <Layout>
            <div style={{ display: "grid", gap: 14 }}>
                <Card
                    title="Settlements"
                    subtitle="Log In, connect KG token, then load your settlements."
                >
                    {conn.err ? <div style={{ color: "#ff6b6b", marginBottom: 10 }}>{conn.err}</div> : null}
                    {!connected ? (
                        <div style={{ display: "grid", gap: 10, maxWidth: 560 }}>
                            <a style={{ ...btn, textDecoration: "none", width: "fit-content" }} href="/login">
                                Log In
                            </a>
                            <div style={{ fontSize: 12, color: "rgba(231,236,255,.75)" }}>
                                Easy connect: paste the KG request snippet (from `GetKingdomDetails` or `GetSettlements`) and click Detect.
                            </div>
                            <textarea
                                style={{ ...input, minHeight: 100, resize: "vertical" }}
                                placeholder='Paste text containing {"accountId":"...","token":"...","kingdomId":...}'
                                value={snippet}
                                onChange={(e) => setSnippet(e.target.value)}
                            />
                            <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                                <button style={btnGhost} onClick={applySnippet} disabled={busy || !snippet.trim()}>
                                    Detect from Paste
                                </button>
                                <button
                                    style={btn}
                                    disabled={busy || !form.account_id || !form.kingdom_id || !form.token}
                                    onClick={connectKg}
                                >
                                    {busy ? "Connecting..." : "Connect KG"}
                                </button>
                            </div>
                            <div style={{ fontSize: 12, color: "rgba(231,236,255,.55)" }}>
                                Manual fallback (only if auto-detect misses):
                            </div>
                            <input
                                style={input}
                                placeholder="KG accountId"
                                value={form.account_id}
                                onChange={(e) => setForm((f) => ({ ...f, account_id: e.target.value }))}
                            />
                            <input
                                style={input}
                                placeholder="KG kingdomId"
                                value={form.kingdom_id}
                                onChange={(e) => setForm((f) => ({ ...f, kingdom_id: e.target.value }))}
                            />
                            <input
                                style={input}
                                placeholder="KG token"
                                value={form.token}
                                onChange={(e) => setForm((f) => ({ ...f, token: e.target.value }))}
                            />
                        </div>
                    ) : (
                        <div style={{ display: "grid", gap: 10 }}>
                            <div style={{ fontSize: 12, color: "rgba(231,236,255,.75)" }}>
                                Connected KG account {conn.data?.connection?.account_id} / kingdom {conn.data?.connection?.kingdom_id}
                            </div>
                            <div>
                                <button style={btnGhost} onClick={() => setRefresh((x) => x + 1)} disabled={busy}>
                                    Refresh
                                </button>
                                <span style={{ marginRight: 8 }} />
                                <button style={btnGhost} onClick={disconnectKg} disabled={busy}>
                                    Disconnect KG
                                </button>
                            </div>
                        </div>
                    )}
                    {msg ? (
                        <div style={{ marginTop: 10, color: msg.includes("connected.") ? "#58d68d" : "#ff6b6b" }}>
                            {msg}
                        </div>
                    ) : null}
                </Card>

                <Card title="Settlement List" subtitle="Live data fetched from KG using your connected token.">
                    {settlements.loading ? <div>Loading settlements...</div> : null}
                    {settlements.err ? <div style={{ color: "#ff6b6b" }}>{settlements.err}</div> : null}

                    {Array.isArray(settlements.data?.settlements) && settlements.data.settlements.length > 0 ? (
                        settlements.data.settlements.map((s) => (
                            <div
                                key={s.settlement_id}
                                style={{
                                    border: "1px solid rgba(255,255,255,.10)",
                                    borderRadius: 12,
                                    padding: 10,
                                    marginBottom: 10,
                                }}
                            >
                                <div style={{ fontWeight: 800 }}>
                                    {s.name} #{s.settlement_id}
                                </div>
                                <div style={{ fontSize: 12, opacity: 0.8, margin: "6px 0" }}>
                                    Buildings: {Array.isArray(s.buildings) ? s.buildings.length : 0}
                                </div>
                                <div className="table-wrap">
                                    <table className="data-table" style={table}>
                                        <thead>
                                            <tr>
                                                <th style={th}>Building Type</th>
                                                <th style={th}>Level</th>
                                                <th style={th}>Effect</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {(s.buildings || []).map((b, idx) => (
                                                <tr key={`${s.settlement_id}:${idx}`}>
                                                    <td data-label="Building Type" style={td}>{b.building_type}</td>
                                                    <td data-label="Level" style={td}>{b.level}</td>
                                                    <td data-label="Effect" style={td}>{b.effect_text || "-"}</td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        ))
                    ) : (
                        <div style={{ fontSize: 12, color: "rgba(231,236,255,.65)" }}>
                            No settlements loaded yet.
                        </div>
                    )}
                </Card>
            </div>
        </Layout>
    );
}

function SettlementEffects() {
    const effects = useFetchJson(`${API_BASE}/api/kg/settlement-effects`, []);

    return (
        <Layout>
            <Card title="Settlement Effects" subtitle="Aggregated effects across all your settlements.">
                {effects.loading ? <div>Loading effects...</div> : null}
                {effects.err ? <div style={{ color: "#ff6b6b" }}>{effects.err}</div> : null}
                {Array.isArray(effects.data?.effects) && effects.data.effects.length > 0 ? (
                    <div className="table-wrap">
                        <table className="data-table" style={table}>
                            <thead>
                                <tr>
                                    <th style={th}>Effect</th>
                                    <th style={th}>Total %</th>
                                    <th style={th}>Cap %</th>
                                    <th style={th}>Applied %</th>
                                    <th style={th}>Cap Hit</th>
                                    <th style={th}>Buildings</th>
                                </tr>
                            </thead>
                            <tbody>
                                {effects.data.effects.map((e) => (
                                    <tr key={e.effect_key}>
                                        <td data-label="Effect" style={td}>{e.label}</td>
                                        <td data-label="Total %" style={td}>{e.total_pct}</td>
                                        <td data-label="Cap %" style={td}>{e.cap_pct ?? "-"}</td>
                                        <td data-label="Applied %" style={td}>{e.applied_pct}</td>
                                        <td data-label="Cap Hit" style={td}>{e.cap_reached ? "Yes" : "No"}</td>
                                        <td data-label="Buildings" style={td}>{e.building_count}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                ) : (
                    <div style={{ fontSize: 12, color: "rgba(231,236,255,.65)" }}>
                        No effects available yet. Connect KG first.
                    </div>
                )}
            </Card>
        </Layout>
    );
}

function TrackedSettlements() {
    const [search, setSearch] = useState("");
    const [tick, setTick] = useState(0);
    const deferredSearch = useDeferredValue(search);

    useEffect(() => {
        const id = setInterval(() => setTick((x) => x + 1), 10000);
        return () => clearInterval(id);
    }, []);

    const url = useMemo(
        () =>
            `${API_BASE}/api/settlements/tracked?kingdom=${encodeURIComponent(
                deferredSearch
            )}&limit=1000&r=${tick}`,
        [deferredSearch, tick]
    );
    const tracked = useFetchJson(url, [url]);

    return (
        <Layout>
            <div style={{ display: "grid", gap: 14 }}>
                <Card
                    title="Tracked Settlements"
                    subtitle="Auto-refreshes every 10 seconds as new reports are ingested."
                    right={
                        <input
                            value={search}
                            onChange={(e) => setSearch(e.target.value)}
                            placeholder="Filter by kingdom…"
                            style={input}
                        />
                    }
                >
                    {tracked.loading ? <div>Loading…</div> : null}
                    {tracked.err ? <div style={{ color: "#ff6b6b" }}>{tracked.err}</div> : null}

                    <div className="table-wrap">
                        <table className="data-table" style={table}>
                            <thead>
                                <tr>
                                    <th style={th}>Kingdom</th>
                                    <th style={th}>Settlement</th>
                                    <th style={th}>Latest Lvl</th>
                                    <th style={th}>Sightings</th>
                                    <th style={th}>Failed Takes</th>
                                    <th style={th}>Captures</th>
                                    <th style={th}>Last Seen</th>
                                </tr>
                            </thead>
                            <tbody>
                                {(tracked.data?.items || []).map((r) => (
                                    <tr key={`${r.kingdom}:${r.settlement_name}`}>
                                        <td data-label="Kingdom" style={td}>{r.kingdom}</td>
                                        <td data-label="Settlement" style={td}>{r.settlement_name}</td>
                                        <td data-label="Latest Lvl" style={td}>{r.latest_level ?? "—"}</td>
                                        <td data-label="Sightings" style={td}>{r.sightings ?? 0}</td>
                                        <td data-label="Failed Takes" style={td}>{r.failed_take_attempts ?? 0}</td>
                                        <td data-label="Captures" style={td}>{r.captures ?? 0}</td>
                                        <td data-label="Last Seen" style={td} title={r.last_seen_at ? new Date(r.last_seen_at).toLocaleString() : ""}>
                                            {timeAgo(r.last_seen_at)}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>

                    {(tracked.data?.items || []).length === 0 && !tracked.loading ? (
                        <div style={{ fontSize: 12, color: "rgba(231,236,255,.65)" }}>
                            No tracked settlements yet.
                        </div>
                    ) : null}
                </Card>
            </div>
        </Layout>
    );
}

function Research() {
    return (
        <Layout>
            <Card title="Research" subtitle="Placeholder for now">
                <div style={{ color: "rgba(231,236,255,.65)", fontSize: 12 }}>Coming soon.</div>
            </Card>
        </Layout>
    );
}

/* ---------------- Admin ---------------- */

function Admin() {
    const auth = useAuthMe(0);

    if (auth.loading) {
        return (
            <Layout>
                <Card title="Admin" subtitle="Checking access">
                    <div style={{ fontSize: 12, color: "rgba(231,236,255,.7)" }}>Loading...</div>
                </Card>
            </Layout>
        );
    }

    if (!auth.data?.authenticated) {
        return (
            <Layout>
                <Card title="Admin" subtitle="Authentication required">
                    <div style={{ fontSize: 12, color: "rgba(231,236,255,.75)" }}>
                        You need to login with Discord to access admin tools.
                    </div>
                    <div style={{ marginTop: 10 }}>
                        <a style={{ ...btn, textDecoration: "none" }} href="/login">
                            Log In
                        </a>
                    </div>
                </Card>
            </Layout>
        );
    }

    if (!auth.data?.user?.is_admin) {
        return (
            <Layout>
                <Card title="Admin" subtitle="Access denied">
                    <div style={{ fontSize: 12, color: "#ff8b8b" }}>
                        Your Discord account is not in the admin allow-list (`DEV_USER_IDS`).
                    </div>
                </Card>
            </Layout>
        );
    }

    return (
        <Layout>
            <AdminHealth />
        </Layout>
    );
}

/* ---------------- NWOT ---------------- */

function NWOT() {
    const [search, setSearch] = useState("");
    const [selected, setSelected] = useState("Galileo");
    const [hours, setHours] = useState(24);
    const [refresh, setRefresh] = useState(0);
    const deferredSearch = useDeferredValue(search);

    useEffect(() => {
        const id = window.setInterval(() => setRefresh((v) => v + 1), 60000);
        return () => window.clearInterval(id);
    }, []);

    const kingdomsUrl = useMemo(
        () => `${API_BASE}/api/nw/kingdoms?limit=300&r=${encodeURIComponent(refresh)}`,
        [refresh]
    );
    const kingdoms = useFetchJson(kingdomsUrl, [kingdomsUrl]);
    const status = useFetchJson(`${API_BASE}/api/nw/status?r=${encodeURIComponent(refresh)}`, [refresh]);

    const filtered = useMemo(() => {
        const list = kingdoms.data?.kingdoms || [];
        const s = deferredSearch.trim().toLowerCase();
        if (!s) return list;
        return list.filter((k) => String(k.kingdom || "").toLowerCase().includes(s));
    }, [deferredSearch, kingdoms.data]);

    const historyUrl = useMemo(() => {
        if (!selected) return "";
        return `${API_BASE}/api/nw/history/${encodeURIComponent(
            selected
        )}?hours=${encodeURIComponent(hours)}&r=${encodeURIComponent(refresh)}`;
    }, [selected, hours, refresh]);

    const history = useFetchJson(historyUrl, [historyUrl]);

    useEffect(() => {
        const list = kingdoms.data?.kingdoms || [];
        if (!Array.isArray(list) || list.length === 0) return;
        const selectedExists = list.some((k) => k?.kingdom === selected);
        if (selectedExists) return;

        const withHistory = list.find((k) => Number(k?.points || 0) > 0);
        setSelected(withHistory?.kingdom || list[0]?.kingdom || "");
    }, [kingdoms.data, selected]);

    return (
        <Layout>
            <div style={{ display: "grid", gap: 14 }}>
                <Card
                    title="Networth Over Time"
                    subtitle="Select a kingdom to view NWOT (from nw_history)."
                    right={
                        <div className="nwot-controls">
                            <input
                                value={search}
                                onChange={(e) => setSearch(e.target.value)}
                                placeholder="Search kingdom…"
                                style={input}
                            />
                            <select
                                value={String(hours)}
                                onChange={(e) => setHours(Number(e.target.value))}
                                style={{ ...input, cursor: "pointer" }}
                            >
                                <option value={6}>6h</option>
                                <option value={12}>12h</option>
                                <option value={24}>24h</option>
                                <option value={48}>48h</option>
                                <option value={72}>72h</option>
                            </select>
                            <button
                                type="button"
                                onClick={() => setRefresh((v) => v + 1)}
                                style={{ ...btnGhost, whiteSpace: "nowrap" }}
                                title="Refresh kingdoms, status, and history now"
                            >
                                Refresh
                            </button>
                        </div>
                    }
                >
                    {kingdoms.loading ? <div>Loading kingdoms…</div> : null}
                    {kingdoms.err ? <div style={{ color: "#ff6b6b" }}>{kingdoms.err}</div> : null}
                    {!kingdoms.err ? (
                        <div style={{ marginBottom: 8, fontSize: 11, color: "rgba(231,236,255,.70)" }}>
                            Last rankings sync: {status.data?.last_rankings_fetch ? timeAgo(status.data.last_rankings_fetch) : "not available"}
                            {" • "}
                            Last NW tick: {status.data?.last_nw_tick ? timeAgo(status.data.last_nw_tick) : "not available"}
                        </div>
                    ) : null}

                    <div className="nwot-grid">
                        {/* Left: list */}
                        <div
                            className="nwot-sidebar"
                            style={{
                                border: "1px solid rgba(255,255,255,.10)",
                                borderRadius: 12,
                                overflow: "hidden",
                                background: "rgba(0,0,0,.20)",
                            }}
                        >
                            <div style={{ maxHeight: 520, overflowY: "auto" }}>
                                {filtered.length === 0 && !kingdoms.loading ? (
                                    <div style={{ padding: 12, fontSize: 12, color: "rgba(231,236,255,.65)" }}>
                                        {kingdoms.data?.note || "No matches."}
                                    </div>
                                ) : null}

                                {filtered.map((k) => {
                                    const name = k.kingdom;
                                    const active = name === selected;
                                    return (
                                        <button
                                            key={name}
                                            onClick={() => setSelected(name)}
                                            style={{
                                                width: "100%",
                                                textAlign: "left",
                                                padding: "10px 12px",
                                                border: "none",
                                                borderBottom: "1px solid rgba(255,255,255,.08)",
                                                background: active ? "rgba(90,160,255,.18)" : "transparent",
                                                color: "#e7ecff",
                                                cursor: "pointer",
                                                fontSize: 12,
                                            }}
                                            title={`Last tick: ${k.last_tick || "—"} • Points: ${k.points ?? "—"}`}
                                        >
                                            <div style={{ fontWeight: 800 }}>{name}</div>
                                            <div style={{ opacity: 0.7, fontSize: 11, marginTop: 2 }}>
                                                {k.points ?? 0} pts •{" "}
                                                {k.last_tick ? new Date(k.last_tick).toLocaleString() : "—"}
                                            </div>
                                        </button>
                                    );
                                })}
                            </div>
                        </div>

                        {/* Right: chart */}
                        <div className="nwot-chart">
                            <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginBottom: 10 }}>
                                <div style={{ fontWeight: 800 }}>{selected || "—"}</div>
                                {history.loading ? (
                                    <div style={{ fontSize: 12, opacity: 0.7 }}>Loading history…</div>
                                ) : null}
                                {history.err ? (
                                    <div style={{ fontSize: 12, color: "#ff6b6b" }}>{history.err}</div>
                                ) : null}
                            </div>

                            {Array.isArray(history.data) && history.data.length > 0 ? (
                                <Suspense fallback={<div style={{ fontSize: 12, color: "var(--rh-muted)" }}>Loading chart…</div>}>
                                    <NWChart data={history.data} />
                                </Suspense>
                            ) : (
                                <div style={{ fontSize: 12, color: "rgba(231,236,255,.65)" }}>
                                    No history points yet for this kingdom/time range.
                                </div>
                            )}
                        </div>
                    </div>
                </Card>
            </div>
        </Layout>
    );
}


function Login() {
    const [isReg, setIsReg] = useState(false);
    const [form, setForm] = useState({ username: "", password: "" });
    const [msg, setMsg] = useState("");
    const [busy, setBusy] = useState(false);

    async function submit(e) {
        e.preventDefault();
        if (!form.username || !form.password) {
            setMsg("Username & password required.");
            return;
        }
                    <Suspense fallback={<div style={{ fontSize: 12, color: "var(--rh-muted)" }}>Loading admin tools…</div>}>
                        <AdminHealth />
                    </Suspense>
        setBusy(true);

        const url = isReg ? `${API_BASE}/auth/register` : `${API_BASE}/auth/login`;

        try {
            const r = await fetch(url, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                credentials: "include",
                body: JSON.stringify(form)
            });
            const j = await r.json().catch(() => ({}));
            if (!r.ok) throw new Error(j.detail || `HTTP ${r.status}`);
            window.location.href = "/";
        } catch (err) {
            setMsg(String(err.message || err));
            setBusy(false);
        }
    }

    return (
        <Layout>
            <div style={{ maxWidth: 400, margin: "40px auto" }}>
                <Card title={isReg ? "Create Account" : "Login"} subtitle="Access recon-hub features">
                    <form onSubmit={submit} style={{ display: "grid", gap: 14 }}>
                        <input
                            style={input}
                            placeholder="Username"
                            value={form.username}
                            onChange={e => setForm({...form, username: e.target.value})}
                        />
                        <input
                            style={input}
                            type="password"
                            placeholder="Password"
                            value={form.password}
                            onChange={e => setForm({...form, password: e.target.value})}
                        />
                        {msg && <div style={{ color: "#ff6b6b", fontSize: 12 }}>{msg}</div>}
                        <button type="submit" style={btn} disabled={busy}>
                            {busy ? "Wait..." : (isReg ? "Sign Up" : "Log In")}
                        </button>
                        <div style={{ textAlign: "center", fontSize: 12, marginTop: 10 }}>
                            <a href="#" style={{ color: "#5aa0ff" }} onClick={(e) => { e.preventDefault(); setIsReg(!isReg); setMsg(""); setForm({username: "", password: ""}); }}>
                                {isReg ? "Already have an account? Log In" : "Need an account? Sign Up"}
                            </a>
                        </div>
                    </form>
                </Card>
            </div>
        </Layout>
    );
}

function NotFound() {
    return (
        <Layout>
            <Card title="Page not found" subtitle="This route does not exist in Recon Hub.">
                <EmptyState
                    title="Nothing is mapped here yet."
                    body="Use the dashboard to jump back into the core views instead of being redirected without context."
                    action={
                        <Link style={{ ...btn, textDecoration: "none" }} to="/">
                            Return to Dashboard
                        </Link>
                    }
                />
            </Card>
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
                <Route path="/nwot" element={<NWOT />} />
                <Route path="/settlements" element={<Settlements />} />
                <Route path="/tracked-settlements" element={<TrackedSettlements />} />
                <Route path="/settlement-effects" element={<SettlementEffects />} />
                <Route path="/kingdoms/:name" element={<KingdomDetail />} />
                <Route path="/spy-reports/:id" element={<SpyReportView />} />
                <Route path="/reports" element={<Reports />} />
                <Route path="/research" element={<Research />} />
                <Route path="/admin/health" element={<Admin />} />
                <Route path="/login" element={<Login />} />
                <Route path="/calc" element={<Navigate to="/kg-calc.html" replace />} />
                <Route path="*" element={<NotFound />} />
            </Routes>
        </BrowserRouter>
    );
}

/* ---------------- Styles ---------------- */

const input = {
    width: "100%",
    background: "rgba(20,15,10,.70)",
    border: "1px solid var(--rh-border)",
    borderRadius: 10,
    padding: "10px 12px",
    color: "var(--rh-text)",
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
    borderBottom: "1px solid var(--rh-border)",
    color: "var(--rh-muted)",
    whiteSpace: "nowrap",
};

const td = {
    padding: "10px 8px",
    borderBottom: "1px solid rgba(205,172,111,.20)",
    whiteSpace: "nowrap",
};

const btn = {
    background: "linear-gradient(180deg, rgba(173,132,72,.35), rgba(111,82,42,.22))",
    border: "1px solid rgba(223,188,127,.46)",
    color: "var(--rh-text)",
    padding: "8px 10px",
    borderRadius: 10,
    cursor: "pointer",
    fontSize: 12,
};

const btnGhost = {
    ...btn,
    background: "rgba(120,89,47,.22)",
    border: "1px solid var(--rh-border)",
    color: "var(--rh-muted)",
};

const linkBtn = {
    background: "transparent",
    border: "none",
    padding: 0,
    margin: 0,
    color: "var(--rh-accent)",
    cursor: "pointer",
    fontSize: 12,
};

const navBtn = {
    ...navLink,
    cursor: "pointer",
};

const pill = {
    background: "rgba(67,50,27,.26)",
    border: "1px solid var(--rh-border)",
    borderRadius: 12,
    padding: "10px 12px",
};

const pillLabel = {
    fontSize: 11,
    color: "var(--rh-muted)",
    letterSpacing: 0.25,
    textTransform: "uppercase",
    marginBottom: 6,
};

const pillValue = {
    fontSize: 13,
    fontWeight: 800,
    color: "var(--rh-text)",
};
