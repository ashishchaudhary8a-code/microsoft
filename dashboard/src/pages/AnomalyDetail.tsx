import { useEffect, useState } from "react";
import { getAnomalyDetail } from "../api";

export default function AnomalyDetail({
  orgId,
  eventId,
  loading,
  error,
}: {
  orgId: string;
  eventId: string;
  loading: boolean;
  error: string | null;
}) {
  const [detail, setDetail] = useState<any>(null);
  const [localError, setLocalError] = useState<string | null>(null);
  const [busy, setBusy] = useState(true);

  useEffect(() => {
    if (!orgId || !eventId) return;
    setBusy(true);
    getAnomalyDetail(orgId, eventId)
      .then(setDetail)
      .catch((err) => setLocalError(err?.response?.data?.error?.message || err.message || "Not found"))
      .finally(() => setBusy(false));
  }, [orgId, eventId]);

  if (busy || loading) return <p className="text-slate-400">Loading anomaly detail…</p>;
  if (localError || error) return <p className="text-rose-300">{localError || error}</p>;
  if (!detail) return <p className="text-slate-400">No detail available.</p>;

  const scorePct = detail.anomaly_score == null ? "—" : `${Math.round(detail.anomaly_score * 100)}%`;

  return (
    <div className="space-y-6">
      <header>
        <h2 className="text-2xl font-semibold">Anomaly Detail</h2>
        <p className="text-slate-400 text-sm">
          {detail.event_id} · {detail.severity || "unscored"} · {scorePct}
        </p>
      </header>
      <section className="rounded-xl border border-slate-800 bg-slate-900 p-4">
        <h3 className="font-medium mb-2">Reasons</h3>
        {!detail.reasons?.length ? (
          <p className="text-slate-400 text-sm">No explanation reasons on this event.</p>
        ) : (
          <ul className="space-y-2">
            {detail.reasons.map((r: any, i: number) => (
              <li key={i} className="text-sm">
                <span className="font-mono text-sky-300">{r.code}</span> — {r.detail}{" "}
                <span className="text-slate-500">({Math.round((r.contribution || 0) * 100)}%)</span>
              </li>
            ))}
          </ul>
        )}
      </section>
      <section className="rounded-xl border border-slate-800 bg-slate-900 p-4">
        <h3 className="font-medium mb-2">Linked alerts</h3>
        {!detail.alerts?.length ? (
          <p className="text-slate-400 text-sm">No alerts linked to this event.</p>
        ) : (
          <ul className="text-sm space-y-1">
            {detail.alerts.map((a: any) => (
              <li key={a.alert_id}>
                {a.alert_type} · {a.severity} · {a.alert_id}
              </li>
            ))}
          </ul>
        )}
      </section>
      <div className="grid md:grid-cols-2 gap-4">
        <pre className="rounded-xl border border-slate-800 bg-slate-950 p-4 text-xs overflow-auto">
          {JSON.stringify(detail.event, null, 2)}
        </pre>
        <pre className="rounded-xl border border-slate-800 bg-slate-950 p-4 text-xs overflow-auto">
          {JSON.stringify(detail.features, null, 2)}
        </pre>
      </div>
    </div>
  );
}
