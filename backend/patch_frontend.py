with open("frontend/src/App.jsx", "r") as f:
    text = f.read()

import re

# 1. Add Login Component
login_comp = """
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
        setMsg("");
        setBusy(true);

        const url = isReg ? `${API_BASE}/auth/register` : `${API_BASE}/auth/login`;

        try {
            const r = await fetch(url, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
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
                            <a href="#" style={{ color: "var(--rh-accent)" }} onClick={(e) => { e.preventDefault(); setIsReg(!isReg); setMsg(""); setForm({username: "", password: ""}); }}>
                                {isReg ? "Already have an account? Log In" : "Need an account? Sign Up"}
                            </a>
                        </div>
                    </form>
                </Card>
            </div>
        </Layout>
    );
}

/* ---------------- Router ---------------- */
"""

text = text.replace("/* ---------------- Router ---------------- */", login_comp)

# 2. Add Login Route
text = text.replace(
    '<Route path="/admin/health" element={<Admin />} />',
    '<Route path="/admin/health" element={<Admin />} />\n                <Route path="/login" element={<Login />} />'
)

# 3. Replace all discord login links
text = text.replace('href="/auth/discord/login"', 'href="/login"')
text = text.replace('Discord Login', 'Login')
text = text.replace('Login with Discord', 'Log In')
text = text.replace('href="/auth/discord/login"', 'href="/login"')
text = text.replace('href="/auth/discord/login"', 'href="/login"')

# 4. Remove discord_user_id checks if we replaced it
text = text.replace('auth.data?.user?.discord_username', 'auth.data?.user?.discord_username') # keeping it for now

with open("frontend/src/App.jsx", "w") as f:
    f.write(text)

