import type { Freshness, Health } from "../types";

const COMPONENTS = ["streaming", "ml", "backend", "database", "broker"] as const;

function tone(status?: string) {
  if (status === "ok") return "text-emerald-400";
  if (status === "degraded" || status === "unknown") return "text-amber-400";
  return "text-rose-400";
}

export default function SystemHealth({
  health,
  freshness,
  loading,
  error,
}: {
  health: Health | null;
  freshness: Freshness | null;
  loading: boolean;
  error: string | null;
}) {
  if (loading && !health) return <p className="text-slate-400">Loading system health…</p>;
  if (error && !health) return <p className="text-rose-300">Could not load system health.</p>;
  if (!health) return <p className="text-slate-400">Health snapshot not available.</p>;

  return (
    <div className="space-y-6">
      <h2 className="text-2xl font-semibold">System Health</h2>
      <p className="text-slate-400 text-sm">
        Overall status <span className={tone(health.status)}>{health.status}</span> · checked {health.checked_at}
      </p>
      <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {COMPONENTS.map((key) => (
          <div key={key} className="rounded-xl border border-slate-800 bg-slate-900 p-4">
            <div className="text-xs uppercase text-slate-400">{key}</div>
            <div className={`text-xl font-semibold ${tone((health as any)[key])}`}>{(health as any)[key]}</div>
          </div>
        ))}
        <div className="rounded-xl border border-slate-800 bg-slate-900 p-4">
          <div className="text-xs uppercase text-slate-400">Consumer lag</div>
          <div className="text-xl font-semibold">{health.consumer_lag ?? "—"}</div>
        </div>
        <div className="rounded-xl border border-slate-800 bg-slate-900 p-4">
          <div className="text-xs uppercase text-slate-400">Data freshness</div>
          <div className="text-xl font-semibold">
            {freshness?.freshness_seconds == null ? "—" : `${freshness.freshness_seconds.toFixed(1)}s`}
          </div>
          <div className="text-xs text-slate-500">
            {freshness?.freshness_status} · P95 {freshness?.freshness_p95_seconds ?? "—"}s
          </div>
        </div>
      </div>
      <p className="text-sm text-slate-500">
        Stop streaming or backend in Compose to see this page degrade, then start the service again to recover.
      </p>
    </div>
  );
}
