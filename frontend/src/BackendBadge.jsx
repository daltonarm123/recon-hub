import { useEffect, useState } from "react";

export default function BackendBadge() {
  const [status, setStatus] = useState(null);
  const [err, setErr] = useState(null);

  useEffect(() => {
    let cancelled = false;

    async function run() {
      try {
        const res = await fetch("/api/status", { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (!cancelled) {
          setStatus(data);
          setErr(null);
        }
      } catch (e) {
        if (!cancelled) {
          setStatus(null);
          setErr(e?.message || "offline");
        }
      }
    }

    run();
    const t = setInterval(run, 15000);
    return () => {
      cancelled = true;
      clearInterval(t);
    };
  }, []);

  const online = !!status?.ok && !err;

  return (
    <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-sm">
      <span className={`h-2.5 w-2.5 rounded-full ${online ? "bg-green-500" : "bg-red-500"}`} />
      <span className="font-medium">Backend:</span>
      <span>{online ? "Online" : "Offline"}</span>
      {online && status?.ts ? (
        <span className="text-xs opacity-70">
          ({new Date(status.ts).toLocaleString()})
        </span>
      ) : null}
    </div>
  );
}
