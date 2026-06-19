import { useEffect, useState } from "react";

export default function BackendBadge() {
  const [status, setStatus] = useState(null);
  const [err, setErr] = useState(null);
  const [loading, setLoading] = useState(true);

  async function run() {
    setLoading(true);
    try {
      const res = await fetch("/api/status", { cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setStatus(data);
      setErr(null);
    } catch (e) {
      setStatus(null);
      setErr(e?.message || "offline");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    run();
    const t = setInterval(run, 15000);
    return () => clearInterval(t);
  }, []);

  const online = !!status?.ok && !err;

  return (
    <div
      aria-live="polite"
      style={{
        display: "grid",
        gap: 10,
        padding: 12,
        borderRadius: 14,
        border: "1px solid var(--rh-border)",
        background: "linear-gradient(180deg, rgba(255,236,201,.10), rgba(255,236,201,.05))",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
        <span
          aria-hidden="true"
          style={{
            width: 11,
            height: 11,
            borderRadius: 999,
            background: online ? "#58d68d" : "#ff6b6b",
            boxShadow: online ? "0 0 12px rgba(88,214,141,.55)" : "0 0 12px rgba(255,107,107,.45)",
          }}
        />
        <span style={{ fontWeight: 800 }}>Backend</span>
        <span style={{ color: online ? "#58d68d" : "#ff9a9a" }}>
          {loading ? "Checking..." : online ? "Online" : "Offline"}
        </span>
        <button onClick={run} style={{ marginLeft: "auto", fontSize: 12, cursor: "pointer", background: "transparent", color: "var(--rh-accent)", border: "none", padding: 0 }}>
          Refresh now
        </button>
      </div>
      {online && status?.ts ? (
        <div style={{ fontSize: 12, color: "var(--rh-muted)" }}>
          Last heartbeat: {new Date(status.ts).toLocaleString()}
        </div>
      ) : null}
      {!online && err ? <div style={{ fontSize: 12, color: "#ff9a9a" }}>{err}</div> : null}
    </div>
  );
}
