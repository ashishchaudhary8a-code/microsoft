import { Link } from "react-router-dom";
import type { EventItem } from "../types";

export default function LiveEvents({
  events,
  loading,
  error,
}: {
  events: EventItem[];
  loading: boolean;
  error: string | null;
}) {
  if (loading && events.length === 0) return <p className="text-slate-400">Loading live events…</p>;
  if (error && events.length === 0) return <p className="text-rose-300">Could not load events.</p>;
  if (events.length === 0) return <p className="text-slate-400">No events yet for this organization.</p>;

  return (
    <div>
      <h2 className="text-2xl font-semibold mb-4">Live Events</h2>
      <div className="overflow-auto rounded-xl border border-slate-800">
        <table className="w-full text-sm">
          <thead className="bg-slate-900 text-slate-400 text-left">
            <tr>
              <th className="px-3 py-2">Time</th>
              <th className="px-3 py-2">Event</th>
              <th className="px-3 py-2">User</th>
              <th className="px-3 py-2">Location</th>
              <th className="px-3 py-2">Amount</th>
              <th className="px-3 py-2">Score</th>
              <th className="px-3 py-2">Severity</th>
            </tr>
          </thead>
          <tbody>
            {events.map((ev) => {
              const anomalous = Boolean(ev.is_anomaly);
              return (
                <tr
                  key={ev.event_id}
                  className={anomalous ? "bg-rose-950/40" : "odd:bg-slate-900/40"}
                >
                  <td className="px-3 py-2 whitespace-nowrap">{ev.timestamp}</td>
                  <td className="px-3 py-2">
                    {anomalous ? (
                      <Link className="text-sky-300 underline" to={`/anomalies/${ev.event_id}`}>
                        {ev.event_id}
                      </Link>
                    ) : (
                      ev.event_id
                    )}
                  </td>
                  <td className="px-3 py-2">{ev.user_id || "—"}</td>
                  <td className="px-3 py-2">{ev.location || "—"}</td>
                  <td className="px-3 py-2">{ev.amount ?? "—"}</td>
                  <td className="px-3 py-2">
                    {ev.anomaly_score == null ? "—" : `${Math.round(ev.anomaly_score * 100)}%`}
                  </td>
                  <td className="px-3 py-2">{ev.severity || "—"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
