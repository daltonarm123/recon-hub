import { BrowserRouter, Routes, Route, NavLink, Navigate } from "react-router-dom";
import { useEffect, useState } from "react";
import { Shield, Calculator, LayoutDashboard, ScrollText, FlaskConical, Crown, LogOut } from "lucide-react";

function cx(...classes) {
    return classes.filter(Boolean).join(" ");
}

function CalcRedirect() {
    useEffect(() => {
        // Hard redirect to the static v2 calculator (served by backend/static or frontend/public)
        window.location.replace("/kg-calc.html");
    }, []);
    return null;
}


async function fetchMe() {
    const r = await fetch("/me", { credentials: "include" });
    return r.json();
}

async function logout() {
    await fetch("/auth/logout", { method: "POST", credentials: "include" });
}

function AppShell({ me, setMe }) {
    const authed = me?.authenticated;

    return (
        <div className="min-h-screen bg-slate-950 text-slate-100">
            <header className="sticky top-0 z-10 border-b border-slate-800 bg-slate-950/80 backdrop-blur">
                <div className="mx-auto flex max-w-6xl items-center gap-4 px-4 py-3">
                    <div className="flex items-center gap-2 font-semibold tracking-wide">
                        <Shield className="h-5 w-5" />
                        Recon Hub
                    </div>

                    <nav className="ml-4 flex flex-wrap gap-1">
                        <Tab to="/dashboard" icon={<LayoutDashboard className="h-4 w-4" />} label="Dashboard" />
                        <Tab to="/kingdoms" icon={<ScrollText className="h-4 w-4" />} label="Kingdoms" />
                        <Tab to="/reports" icon={<FlaskConical className="h-4 w-4" />} label="Reports" />
                        <Tab to="/research" icon={<ScrollText className="h-4 w-4" />} label="Research" />

                        {/* Calc tab -> static page */}
                        <TabHref href="/kg-calc.html" icon={<Calculator className="h-4 w-4" />} label="Calc" />

                        {me?.is_admin && <Tab to="/admin" icon={<Crown className="h-4 w-4" />} label="Admin" />}
                    </nav>

                    <div className="ml-auto flex items-center gap-2">
                        {!authed ? (
                            <a
                                href="/auth/discord/login"
                                className="rounded-lg bg-indigo-600 px-3 py-2 text-sm font-semibold hover:bg-indigo-500"
                            >
                                Login with Discord
                            </a>
                        ) : (
                            <>
                                <div className="hidden sm:flex items-center gap-2 rounded-lg border border-slate-800 bg-slate-900 px-3 py-2 text-sm">
                                    <span className="font-medium">{me.global_name || me.username}</span>
                                    {me.is_admin && <span className="text-xs text-amber-300">ADMIN</span>}
                                </div>
                                <button
                                    onClick={async () => {
                                        await logout();
                                        setMe({ authenticated: false });
                                    }}
                                    className="inline-flex items-center gap-2 rounded-lg border border-slate-800 bg-slate-900 px-3 py-2 text-sm hover:bg-slate-800"
                                >
                                    <LogOut className="h-4 w-4" />
                                    Logout
                                </button>
                            </>
                        )}
                    </div>
                </div>
            </header>

            <main className="mx-auto max-w-6xl px-4 py-6">
                <Routes>
                    <Route path="/" element={<Navigate to="/dashboard" replace />} />
                    <Route path="/dashboard" element={<Dashboard me={me} />} />
                    <Route path="/kingdoms" element={<Kingdoms />} />
                    <Route path="/reports" element={<Reports />} />
                    <Route path="/research" element={<Research />} />

                    {/* /calc now redirects to static v2; keep old in-app calc at /calc-old (optional) */}
                    <Route path="/calc" element={<CalcRedirect />} />

                    <Route path="/admin" element={me?.is_admin ? <Admin /> : <NoAccess />} />
                    <Route path="*" element={<NotFound />} />
                </Routes>
            </main>
        </div>
    );
}
                    <Route path="/calc-old" element={<Calc />} />
function Tab({ to, icon, label }) {
    return (
        <NavLink
            to={to}
            className={({ isActive }) =>
                cx(
                    "inline-flex items-center gap-2 rounded-lg px-3 py-2 text-sm",
                    isActive ? "bg-slate-800 text-white" : "text-slate-300 hover:bg-slate-900 hover:text-white"
                )
            }
        >
            {icon}
            {label}
        </NavLink>
    );
}

function TabHref({ href, icon, label }) {
    return (
        <a
            href={href}
            className={cx(
                "inline-flex items-center gap-2 rounded-lg px-3 py-2 text-sm",
                "text-slate-300 hover:bg-slate-900 hover:text-white"
            )}
        >
            {icon}
            {label}
        </a>
    );
}

function Card({ title, children }) {
    return (
        <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5 shadow">
            <div className="mb-3 text-sm font-semibold text-slate-200">{title}</div>
            {children}
        </div>
    );
}

function NumBox({ value, onChange }) {
    return (
        <input
            inputMode="numeric"
            value={value ?? ""}
            onChange={(e) => {
                const raw = e.target.value;
                const cleaned = raw.replace(/[^\d,]/g, "");
                onChange(cleaned);
            }}
            placeholder="0"
            className="w-full rounded-xl border border-slate-800 bg-slate-950 px-3 py-2 text-sm text-slate-100 outline-none focus:border-slate-700"
        />
    );
}

function Toggle({ label, checked, onChange }) {
    return (
        <label className="flex items-center justify-between gap-3 rounded-xl border border-slate-800 bg-slate-950 px-3 py-2">
            <span className="text-sm text-slate-200">{label}</span>
            <button
                type="button"
                onClick={() => onChange(!checked)}
                className={[
                    "relative h-6 w-11 rounded-full border transition",
                    checked ? "border-indigo-500 bg-indigo-600" : "border-slate-700 bg-slate-900",
                ].join(" ")}
            >
                <span
                    className={[
                        "absolute top-0.5 h-5 w-5 rounded-full bg-white transition",
                        checked ? "left-5" : "left-0.5",
                    ].join(" ")}
                />
            </button>
        </label>
    );
}

function Dashboard({ me }) {
    return (
        <div className="grid gap-4 md:grid-cols-3">
            <Card title="Status">
                <div className="text-sm text-slate-300">
                    {me?.authenticated ? "Logged in ✅" : "Not logged in — use Login with Discord"}
                </div>
            </Card>
            <Card title="Recon Data">
                <div className="text-sm text-slate-300">Next: wire DB + spy report ingestion.</div>
            </Card>
            <Card title="Admin">
                <div className="text-sm text-slate-300">{me?.is_admin ? "Admin enabled ✅" : "Admin not enabled"}</div>
            </Card>
        </div>
    );
}

function Kingdoms() {
    return <Card title="Kingdoms">Coming next: list/search kingdoms from DB.</Card>;
}
function Reports() {
    return <Card title="Reports">Coming next: paste/upload spy reports and parse them.</Card>;
}
function Research() {
    return <Card title="Research">Coming next: research history per kingdom.</Card>;
}

function Calc() {
    const UNITS = [
        { key: "peasants", label: "Peasants" },
        { key: "foot", label: "Foot Soldiers" },
        { key: "pike", label: "Pikeman" },
        { key: "elite", label: "Elite" },
        { key: "archers", label: "Archers" },
        { key: "crossbow", label: "Crossbow" },
        { key: "light_cav", label: "Light Cavalry", mounted: true },
        { key: "heavy_cav", label: "Heavy Cavalry", mounted: true },
        { key: "knights", label: "Knights", mounted: true },
        { key: "castles", label: "Castles" },
    ];

    const empty = () => Object.fromEntries(UNITS.map((u) => [u.key, ""]));

    const [atk, setAtk] = useState(empty());
    const [def, setDef] = useState(empty());

    const [armor, setArmor] = useState(false);
    const [warDip, setWarDip] = useState(false);
    const [wrath, setWrath] = useState(false);
    const [steeds, setSteeds] = useState(false);

    const [spyText, setSpyText] = useState("");
    const [parseMsg, setParseMsg] = useState("");

    const resetAll = () => {
        setAtk(empty());
        setDef(empty());
        setArmor(false);
        setWarDip(false);
        setWrath(false);
        setSteeds(false);
        setSpyText("");
        setParseMsg("");
    };

    function parseSpyReportToCounts(text) {
        const alias = [
            ["peasants", ["peasants", "peasant"]],
            ["foot", ["foot soldiers", "footmen", "foot soldier"]],
            ["pike", ["pikeman", "pikes", "pike men"]],
            ["elite", ["elite", "elites"]],
            ["archers", ["archers", "archer"]],
            ["crossbow", ["crossbow", "crossbows", "crossbowmen", "crossbow men"]],
            ["light_cav", ["light cavalry", "light cav", "lcav"]],
            ["heavy_cav", ["heavy cavalry", "heavy cav", "hcav"]],
            ["knights", ["knights", "knight"]],
            ["castles", ["castles", "castle"]],
        ];

        const out = {};
        for (const [key] of alias) out[key] = "";

        const lines = text.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);

        const numFromLine = (line) => {
            const m = line.match(/(\d[\d,]*)\s*$/);
            if (!m) return null;
            return m[1].replace(/,/g, "");
        };

        for (const line of lines) {
            const lower = line.toLowerCase();

            for (const [key, names] of alias) {
                for (const name of names) {
                    if (lower.startsWith(name + ":") || lower.startsWith(name + " ")) {
                        const n = numFromLine(line);
                        if (n !== null) out[key] = n;
                    }
                }
            }
        }

        return out;
    }

    const parseAndFill = () => {
        const counts = parseSpyReportToCounts(spyText);
        const filled = Object.values(counts).some((v) => String(v).trim() !== "");
        if (!filled) {
            setParseMsg("Couldn’t find troop lines. Paste the section that lists troops (e.g., “Archers: 12345”).");
            return;
        }
        setDef((prev) => ({ ...prev, ...counts }));
        setParseMsg("Filled defending kingdom counts from spy report ✅");
    };

    return (
        <div className="space-y-4">
            <Card title="Calculator">
                <div className="grid gap-4 lg:grid-cols-[1fr,340px] lg:items-start">
                    <div className="overflow-hidden rounded-2xl border border-slate-800 bg-slate-950">
                        <div className="grid grid-cols-[1fr,160px,160px] gap-3 border-b border-slate-800 px-4 py-3 text-sm font-semibold text-slate-200">
                            <div className="flex items-center gap-3">
                                <button
                                    onClick={resetAll}
                                    className="rounded-lg border border-slate-800 bg-slate-900 px-3 py-2 text-xs font-semibold text-slate-100 hover:bg-slate-800"
                                >
                                    Reset Info
                                </button>
                            </div>
                            <div className="text-center">Attacking Force</div>
                            <div className="text-center">Defending Kingdom</div>
                        </div>

                        <div className="divide-y divide-slate-900">
                            {UNITS.map((u) => (
                                <div key={u.key} className="grid grid-cols-[1fr,160px,160px] items-center gap-3 px-4 py-2">
                                    <div className="text-sm text-slate-200">{u.label}</div>

                                    <NumBox value={atk[u.key]} onChange={(v) => setAtk((p) => ({ ...p, [u.key]: v }))} />
                                    <NumBox value={def[u.key]} onChange={(v) => setDef((p) => ({ ...p, [u.key]: v }))} />
                                </div>
                            ))}
                        </div>

                        <div className="border-t border-slate-800 px-4 py-3">
                            <div className="grid gap-2 sm:grid-cols-2">
                                <Toggle label="Armor?" checked={armor} onChange={setArmor} />
                                <Toggle label="War Dip?" checked={warDip} onChange={setWarDip} />
                                <Toggle label="Attacking Wrath (+5%)" checked={wrath} onChange={setWrath} />
                                <Toggle label="Steed’s Fury (+5% mounted)" checked={steeds} onChange={setSteeds} />
                            </div>
                            <div className="mt-2 text-xs text-slate-400">Note: Home Defense removed.</div>
                        </div>
                    </div>

                    <div className="space-y-4">
                        <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5 shadow">
                            <div className="mb-2 text-sm font-semibold text-slate-200">Paste Spy Report</div>
                            <div className="text-xs text-slate-400 mb-3">
                                Paste the troop section. We’ll auto-fill the defending kingdom.
                            </div>

                            <textarea
                                value={spyText}
                                onChange={(e) => setSpyText(e.target.value)}
                                rows={10}
                                className="w-full rounded-xl border border-slate-800 bg-slate-950 px-3 py-2 text-sm text-slate-100 outline-none focus:border-slate-700"
                                placeholder={`Example:
Archers: 12345
Crossbow: 456
Knights: 789
...`}
                            />

                            <div className="mt-3 flex items-center gap-2">
                                <button
                                    onClick={parseAndFill}
                                    className="rounded-lg bg-indigo-600 px-3 py-2 text-sm font-semibold hover:bg-indigo-500"
                                >
                                    Parse & Fill
                                </button>
                                {parseMsg && <div className="text-xs text-slate-300">{parseMsg}</div>}
                            </div>
                        </div>

                        <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5 shadow">
                            <div className="mb-2 text-sm font-semibold text-slate-200">Results</div>
                            <div className="text-sm text-slate-300">Next step: calculate attack/defense points + show outcome tier.</div>
                        </div>
                    </div>
                </div>
            </Card>
        </div>
    );
}

function Admin() {
    return <Card title="Admin Panel">Coming next: admin tools (reindex, imports, manage kingdoms).</Card>;
}
function NoAccess() {
    return <Card title="No Access">You don’t have permission to view this page.</Card>;
}
function NotFound() {
    return <Card title="Not Found">That page doesn’t exist.</Card>;
}

export default function App() {
    const [me, setMe] = useState(null);

    useEffect(() => {
        (async () => {
            try {
                const data = await fetchMe();
                setMe(data);
            } catch {
                setMe({ authenticated: false });
            }
        })();
    }, []);

    return (
        <BrowserRouter>
            <AppShell me={me} setMe={setMe} />
        </BrowserRouter>
    );
}