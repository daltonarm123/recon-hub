import { useEffect, useState } from "react";

export default function AdminHealth() {
  const [status, setStatus] = useState(null);
  const [healthz, setHealthz] = useState(null);
  const [err, setErr] = useState(null);

  useEffect(() => {
    async function run() {
      try {
        const s = await fetch("/api/status", { cache: "no-store" }).then((r) => r.json());
        const h = await fetch("/healthz", { cache: "no-store" }).then((r) => r.json());
        setStatus(s);
        setHealthz(h);
        setErr(null);
      } catch (e) {
        setErr(e?.message || "Failed to load");
      }
    }
    run();
  }, []);

  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold">Admin • Health</h2>

      {err ? (
        <div className="rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-red-200">
          Error: {err}
        </div>
      ) : null}

      <div className="rounded-lg border border-white/10 bg-white/5 p-3">
        <div className="mb-2 font-medium">/api/status</div>
        <pre className="text-xs overflow-auto">{JSON.stringify(status, null, 2)}</pre>
      </div>

      <div className="rounded-lg border border-white/10 bg-white/5 p-3">
        <div className="mb-2 font-medium">/healthz</div>
        <pre className="text-xs overflow-auto">{JSON.stringify(healthz, null, 2)}</pre>
      </div>
    </div>
  );
}
