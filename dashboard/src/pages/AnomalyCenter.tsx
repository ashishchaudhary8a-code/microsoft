import { Link } from "react-router-dom";
import type { AlertItem, AnomalyItem } from "../types";

export default function AnomalyCenter({
  anomalies,
  alerts,
  loading,
  error,
}: {
  anomalies: AnomalyItem[];
  alerts: AlertItem[];
  loading: boolean;
  error: string | null;
}) {
  if (loading && anomalies.length === 0 && alerts.length === 0) {
    return <p className="text-slate-400">Loading anomalies…</p>;
  }
  if (error && anomalies.length === 0) return <p className="text-rose-300">Could not load anomaly center.</p>;

  return (
    <div className="space-y-8">
      <h2 className="text-2xl font-semibold">Anomaly Center</h2>
      <section>
        <h3 className="text-lg font-medium mb-2">Anomalies</h3>
        {anomalies.length === 0 ? (
          <p className="text-slate-400">No anomalous events for this organization.</p>
        ) : (
          <div className="overflow-auto rounded-xl border border-slate-800">
            <table className="w-full text-sm">
              <thead className="bg-slate-900 text-slate-400 text-left">
                <tr>
                  <th className="px-3 py-2">Event</th>
                  <th className="px-3 py-2">User</th>
                  <th className="px-3 py-2">Score</th>
                  <th className="px-3 py-2">Severity</th>
                  <th className="px-3 py-2">Processed</th>
                </tr>
              </thead>
              <tbody>
                {anomalies.map((a) => (
                  <tr key={a.event_id} className="odd:bg-slate-900/40">
                    <td className="px-3 py-2">
                      <Link className="text-sky-300 underline" to={`/anomalies/${a.event_id}`}>
                        {a.event_id}
                      </Link>
                    </td>
                    <td className="px-3 py-2">{a.user_id || "—"}</td>
                    <td className="px-3 py-2">{Math.round(a.anomaly_score * 100)}%</td>
                    <td className="px-3 py-2">{a.severity}</td>
                    <td className="px-3 py-2">{a.processed_at}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
      <section>
        <h3 className="text-lg font-medium mb-2">Alerts</h3>
        {alerts.length === 0 ? (
          <p className="text-slate-400">No alerts yet.</p>
        ) : (
          <ul className="space-y-2">
            {alerts.map((al) => (
              <li key={al.alert_id} className="rounded-xl border border-slate-800 bg-slate-900 p-4">
                <div className="flex justify-between gap-4">
                  <div>
                    <div className="font-medium">{al.title}</div>
                    <div className="text-sm text-slate-400">{al.summary}</div>
                  </div>
                  <div className="text-xs text-slate-500 whitespace-nowrap">
                    {al.alert_type} · {al.severity}
                  </div>
                </div>
                <div className="mt-2 text-xs text-sky-300">
                  {al.event_ids.map((id) => (
                    <Link key={id} className="mr-2 underline" to={`/anomalies/${id}`}>
                      {id}
                    </Link>
                  ))}
                </div>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
