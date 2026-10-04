import { Area, AreaChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { DashboardKpi, Freshness } from "../types";

function Card({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900 p-4">
      <div className="text-xs uppercase tracking-wide text-slate-400">{label}</div>
      <div className="mt-1 text-2xl font-semibold">{value}</div>
      {hint ? <div className="text-xs text-slate-500 mt-1">{hint}</div> : null}
    </div>
  );
}

export default function Overview({
  dashboard,
  freshness,
  loading,
  error,
  emptyOrgs,
}: {
  dashboard: DashboardKpi | null;
  freshness: Freshness | null;
  loading: boolean;
  error: string | null;
  emptyOrgs: boolean;
}) {
  if (loading && !dashboard) return <p className="text-slate-400">Loading overview…</p>;
  if (error && !dashboard) return <p className="text-rose-300">Could not load overview.</p>;
  if (emptyOrgs) return <p className="text-slate-400">No organizations configured.</p>;
  if (!dashboard) return <p className="text-slate-400">No KPI snapshot yet. Waiting for live events.</p>;

  const spark = [
    { t: "t-2", v: Math.max(0, dashboard.events_per_minute * 0.7) },
    { t: "t-1", v: Math.max(0, dashboard.events_per_minute * 0.9) },
    { t: "now", v: dashboard.events_per_minute },
  ];

  return (
    <div className="space-y-6">
      <header>
        <h2 className="text-2xl font-semibold">Overview</h2>
        <p className="text-slate-400 text-sm">Live scores for the selected organization. Detection is useful only while data is still fresh.</p>
      </header>
      <div className="grid grid-cols-2 xl:grid-cols-4 gap-4">
        <Card label="Events / min" value={dashboard.events_per_minute.toFixed(1)} hint={`last ${dashboard.window_seconds}s`} />
        <Card label="Events total" value={String(dashboard.events_total)} />
        <Card label="Anomalies" value={String(dashboard.anomalies_total)} />
        <Card label="Critical" value={String(dashboard.critical_total)} />
        <Card
          label="Average score"
          value={dashboard.anomaly_score_avg == null ? "—" : `${Math.round(dashboard.anomaly_score_avg * 100)}%`}
        />
        <Card
          label="Data freshness"
          value={dashboard.freshness_seconds == null ? "—" : `${dashboard.freshness_seconds.toFixed(1)}s`}
          hint={dashboard.freshness_status}
        />
        <Card label="Pipeline" value={dashboard.pipeline_status} />
        <Card
          label="P95 freshness"
          value={freshness?.freshness_p95_seconds == null ? "—" : `${freshness.freshness_p95_seconds.toFixed(1)}s`}
          hint={`SLA ${freshness?.sla_seconds ?? 5}s`}
        />
      </div>
      <div className="h-48 rounded-xl border border-slate-800 bg-slate-900 p-4">
        <div className="text-xs uppercase text-slate-400 mb-2">Events / min</div>
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={spark}>
            <XAxis dataKey="t" stroke="#64748b" />
            <YAxis stroke="#64748b" />
            <Tooltip />
            <Area type="monotone" dataKey="v" stroke="#38bdf8" fill="#0ea5e9" fillOpacity={0.2} />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
