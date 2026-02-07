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
import NWChart from "./NWChart";
import "./App.css";

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

function useFetchText(url, deps = []) {
    const [data, setData] = useState("");
    const [err, setErr] = useState("");
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        let alive = true;
        setLoading(true);
        setErr("");

        fetch(url, { headers: { Accept: "text/plain" } })
            .then(async (r) => {
                const t = await r.text();
                if (!r.ok) throw new Error(t || `HTTP ${r.status}`);
                return t;
            })
            .then((t) => alive && setData(t))
            .catch((e) => alive && setErr(String(e.message || e)))
            .finally(() => alive && setLoading(false));

        return () => {
            alive = false;
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
        setLoading(true);
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

const navLink = {
    color: "#e7ecff",
    textDecoration: "none",
    padding: "8px 12px",
    borderRadius: 10,
    border: "1px solid rgba(255,255,255,.14)",
    background: "linear-gradient(180deg, rgba(255,255,255,.09), rgba(255,255,255,.04))",
    fontSize: 12,
    fontWeight: 700,
};

function Layout({ children }) {
    const [authRefresh, setAuthRefresh] = useState(0);
    const auth = useAuthMe(authRefresh);

    async function logout() {
        await fetch(`${API_BASE}/auth/logout`, {
            method: "POST",
            credentials: "include",
        }).catch(() => null);
        setAuthRefresh((x) => x + 1);
    }

    return (
        <div style={{ minHeight: "100vh", background: "transparent", color: "#e7ecff" }}>
            <div style={{ maxWidth: 1240, margin: "0 auto", padding: 20 }}>
                <header
                    style={{
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "space-between",
                        gap: 12,
                        flexWrap: "wrap",
                        background: "rgba(255,255,255,.03)",
                        border: "1px solid rgba(255,255,255,.11)",
                        borderRadius: 14,
                        padding: 12,
                        boxShadow: "0 14px 36px rgba(0,0,0,.2)",
                    }}
                >
                    <div>
                        <div style={{ fontWeight: 800, letterSpacing: 0.2 }}>Recon Hub</div>
                        <div style={{ fontSize: 12, color: "rgba(231,236,255,.65)" }}>
                            KG tools + recon database views
                        </div>
                    </div>

                    <nav style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                        <Link style={navLink} to="/">
                            Dashboard
                        </Link>
                        <Link style={navLink} to="/kingdoms">
                            Kingdoms
                        </Link>
                        <Link style={navLink} to="/nwot">
                            NWOT
                        </Link>
                        <Link style={navLink} to="/settlements">
                            Settlements
                        </Link>
                        <Link style={navLink} to="/settlement-effects">
                            Settlement Effects
                        </Link>
                        <Link style={navLink} to="/reports">
                            Reports
                        </Link>
                        <Link style={navLink} to="/research">
                            Research
                        </Link>
                        {auth.data?.user?.is_admin ? (
                            <Link style={navLink} to="/admin/health">
                                Admin
                            </Link>
                        ) : null}
                        <a style={navLink} href="/kg-calc.html">
                            Calc
                        </a>
                        {auth.loading ? null : auth.data?.authenticated ? (
                            <button style={navBtn} onClick={logout} title="Logout Discord session">
                                {auth.data?.user?.discord_username || "Discord"} Logout
                            </button>
                        ) : (
                            <a style={navLink} href="/auth/discord/login">
                                Discord Login
                            </a>
                        )}
                    </nav>
                </header>

                <div
                    style={{
                        height: 1,
                        background: "rgba(255,255,255,.12)",
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
                border: "1px solid rgba(255,255,255,.13)",
                borderRadius: 14,
                overflow: "hidden",
                background: "linear-gradient(180deg, rgba(255,255,255,.08), rgba(255,255,255,.03))",
                boxShadow: "0 16px 40px rgba(0,0,0,.25)",
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
                    flexWrap: "wrap",
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
        () =>
            `${API_BASE}/api/kingdoms?search=${encodeURIComponent(
                search
            )}&limit=500`,
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
                                                        onClick={() =>
                                                            nav(`/kingdoms/${encodeURIComponent(k.name)}`)
                                                        }
                                                        title="Open reports"
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

                    <div style={{ overflowX: "auto" }}>
                        <table style={table}>
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
                                        <td style={td}>
                                            {r.created_at
                                                ? new Date(r.created_at).toLocaleString()
                                                : "—"}
                                        </td>
                                        <td style={td}>{r.alliance || "—"}</td>
                                        <td style={td}>
                                            {r.defender_dp
                                                ? Number(r.defender_dp).toLocaleString()
                                                : "—"}
                                        </td>
                                        <td style={td}>{r.castles ?? "—"}</td>
                                        <td style={td}>
                                            {r.troops ? Object.keys(r.troops).length : 0}
                                        </td>
                                        <td style={td}>
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

    function KVTable({ obj }) {
        const entries = Object.entries(obj || {});
        if (entries.length === 0) {
            return (
                <div style={{ color: "rgba(231,236,255,.65)", fontSize: 12 }}>—</div>
            );
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
                                    <td style={td}>{fmtNum(v)}</td>
                                </tr>
                            ))}
                    </tbody>
                </table>
            </div>
        );
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
                            <KVTable obj={troops} />
                        </div>
                        <div>
                            <div style={{ fontWeight: 800, marginBottom: 8 }}>Resources</div>
                            <KVTable obj={resources} />
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
                <Card
                    title="Reports"
                    subtitle="Paste a KG spy report to store + index it (Postgres rh_* tables)"
                >
                    <textarea
                        value={raw}
                        onChange={(e) => setRaw(e.target.value)}
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
                        Tip: After saving, go to <b>Kingdoms</b> to see it listed by alliance.
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
    const [kgLogin, setKgLogin] = useState({ email: "", password: "", kingdom_id: "" });
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

    async function loginKg() {
        setBusy(true);
        setMsg("");
        try {
            const body = {
                email: kgLogin.email.trim(),
                password: kgLogin.password,
            };
            if (kgLogin.kingdom_id.trim()) {
                body.kingdom_id = Number(kgLogin.kingdom_id);
            }
            const r = await fetch(`${API_BASE}/api/kg/login`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                credentials: "include",
                body: JSON.stringify(body),
            });
            const j = await r.json().catch(() => ({}));
            if (!r.ok) throw new Error(j?.detail || `HTTP ${r.status}`);
            const policy = j?.password_policy || "one-time-only-not-stored";
            setMsg(`KG login connected successfully. Password policy: ${policy}.`);
            setKgLogin((x) => ({ ...x, password: "" }));
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
                    subtitle="Login with Discord, connect KG token, then load your settlements."
                >
                    {conn.err ? <div style={{ color: "#ff6b6b", marginBottom: 10 }}>{conn.err}</div> : null}
                    {!connected ? (
                        <div style={{ display: "grid", gap: 10, maxWidth: 560 }}>
                            <a style={{ ...btn, textDecoration: "none", width: "fit-content" }} href="/auth/discord/login">
                                Login with Discord
                            </a>
                            <div style={{ marginTop: 4, fontSize: 12, color: "rgba(231,236,255,.82)", fontWeight: 700 }}>
                                Quick connect for phone users
                            </div>
                            <input
                                style={input}
                                placeholder="KG email"
                                value={kgLogin.email}
                                onChange={(e) => setKgLogin((x) => ({ ...x, email: e.target.value }))}
                            />
                            <input
                                style={input}
                                placeholder="KG password"
                                type="password"
                                value={kgLogin.password}
                                onChange={(e) => setKgLogin((x) => ({ ...x, password: e.target.value }))}
                            />
                            <input
                                style={input}
                                placeholder="Kingdom ID (optional, for first-time connect)"
                                value={kgLogin.kingdom_id}
                                onChange={(e) => setKgLogin((x) => ({ ...x, kingdom_id: e.target.value }))}
                            />
                            <div>
                                <button
                                    style={btn}
                                    disabled={busy || !kgLogin.email.trim() || !kgLogin.password}
                                    onClick={loginKg}
                                >
                                    {busy ? "Connecting..." : "Login with KG"}
                                </button>
                            </div>
                            <div style={{ fontSize: 12, color: "rgba(231,236,255,.55)" }}>
                                Password is used one-time only for KG token exchange and is not stored.
                            </div>
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
                                <div style={{ overflowX: "auto" }}>
                                    <table style={table}>
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
                                                    <td style={td}>{b.building_type}</td>
                                                    <td style={td}>{b.level}</td>
                                                    <td style={td}>{b.effect_text || "-"}</td>
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
                    <div style={{ overflowX: "auto" }}>
                        <table style={table}>
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
                                        <td style={td}>{e.label}</td>
                                        <td style={td}>{e.total_pct}</td>
                                        <td style={td}>{e.cap_pct ?? "-"}</td>
                                        <td style={td}>{e.applied_pct}</td>
                                        <td style={td}>{e.cap_reached ? "Yes" : "No"}</td>
                                        <td style={td}>{e.building_count}</td>
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
                        <a style={{ ...btn, textDecoration: "none" }} href="/auth/discord/login">
                            Login with Discord
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

    const kingdomsUrl = useMemo(() => `${API_BASE}/api/nw/kingdoms?limit=300`, []);
    const kingdoms = useFetchJson(kingdomsUrl, [kingdomsUrl]);

    const filtered = useMemo(() => {
        const list = kingdoms.data?.kingdoms || [];
        const s = search.trim().toLowerCase();
        if (!s) return list;
        return list.filter((k) => String(k.kingdom || "").toLowerCase().includes(s));
    }, [kingdoms.data, search]);

    const historyUrl = useMemo(() => {
        if (!selected) return "";
        return `${API_BASE}/api/nw/history/${encodeURIComponent(
            selected
        )}?hours=${encodeURIComponent(hours)}`;
    }, [selected, hours]);

    const history = useFetchJson(historyUrl, [historyUrl]);

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
                        </div>
                    }
                >
                    {kingdoms.loading ? <div>Loading kingdoms…</div> : null}
                    {kingdoms.err ? <div style={{ color: "#ff6b6b" }}>{kingdoms.err}</div> : null}

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
                                        No matches.
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
                                <NWChart data={history.data} />
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

/* ---------------- Router ---------------- */

export default function App() {
    return (
        <BrowserRouter>
            <Routes>
                <Route path="/" element={<Dashboard />} />
                <Route path="/kingdoms" element={<Kingdoms />} />
                <Route path="/nwot" element={<NWOT />} />
                <Route path="/settlements" element={<Settlements />} />
                <Route path="/settlement-effects" element={<SettlementEffects />} />
                <Route path="/kingdoms/:name" element={<KingdomDetail />} />
                <Route path="/spy-reports/:id" element={<SpyReportView />} />
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

const navBtn = {
    ...navLink,
    cursor: "pointer",
};

const pill = {
    background: "rgba(0,0,0,.20)",
    border: "1px solid rgba(255,255,255,.10)",
    borderRadius: 12,
    padding: "10px 12px",
};

const pillLabel = {
    fontSize: 11,
    color: "rgba(231,236,255,.65)",
    letterSpacing: 0.25,
    textTransform: "uppercase",
    marginBottom: 6,
};

const pillValue = {
    fontSize: 13,
    fontWeight: 800,
    color: "#e7ecff",
};
