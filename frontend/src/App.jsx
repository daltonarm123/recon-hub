import { BrowserRouter, Routes, Route, NavLink, Navigate } from "react-router-dom";
import { useEffect, useState } from "react";
import { Shield, Calculator, LayoutDashboard, ScrollText, FlaskConical, Crown, LogOut } from "lucide-react";

function cx(...classes) {
    return classes.filter(Boolean).join(" ");
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
                        <Tab to="/calc" icon={<Calculator className="h-4 w-4" />} label="Calc" />
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
                    <Route path="/calc" element={<Calc />} />
                    <Route path="/admin" element={me?.is_admin ? <Admin /> : <NoAccess />} />
                    <Route path="*" element={<NotFound />} />
                </Routes>
            </main>
        </div>
    );
}

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

function Card({ title, children }) {
    return (
        <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5 shadow">
            <div className="mb-3 text-sm font-semibold text-slate-200">{title}</div>
            {children}
        </div>
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
    return <Card title="Calculator">Next: build the exact calculator.</Card>;
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
