import { NavLink, Outlet } from "react-router-dom";
import { Activity, ShieldAlert, HeartPulse, LayoutDashboard, Radio } from "lucide-react";
import type { Organization } from "./types";
import type { Freshness } from "./types";

const links = [
  { to: "/", label: "Overview", icon: LayoutDashboard },
  { to: "/events", label: "Live Events", icon: Radio },
  { to: "/anomalies", label: "Anomaly Center", icon: ShieldAlert },
  { to: "/health", label: "System Health", icon: HeartPulse },
];

function freshnessColor(status?: string) {
  if (status === "GREEN") return "text-emerald-400";
  if (status === "WARNING") return "text-amber-400";
  return "text-rose-400";
}

export default function Layout({
  orgs,
  orgId,
  setOrgId,
  freshness,
  error,
}: {
  orgs: Organization[];
  orgId: string;
  setOrgId: (id: string) => void;
  freshness: Freshness | null;
  error: string | null;
}) {
  return (
    <div className="min-h-screen flex">
      <aside className="w-60 shrink-0 border-r border-slate-800 bg-slate-900/80 p-4 flex flex-col gap-6">
        <div>
          <div className="flex items-center gap-2 text-sm font-semibold tracking-wide uppercase text-slate-400">
            <Activity size={16} /> Live detection
          </div>
          <h1 className="mt-1 text-lg font-semibold">Anomaly platform</h1>
        </div>
        <label className="text-xs text-slate-400">
          Organization
          <select
            className="mt-1 w-full rounded-md bg-slate-800 border border-slate-700 px-2 py-2 text-sm"
            value={orgId}
            onChange={(e) => setOrgId(e.target.value)}
          >
            {orgs.map((o) => (
              <option key={o.organization_id} value={o.organization_id}>
                {o.organization_name}
              </option>
            ))}
          </select>
        </label>
        <nav className="flex flex-col gap-1">
          {links.map(({ to, label, icon: Icon }) => (
            <NavLink
              key={to}
              to={to}
              end={to === "/"}
              className={({ isActive }) =>
                `flex items-center gap-2 rounded-md px-3 py-2 text-sm ${
                  isActive ? "bg-sky-900/60 text-sky-100" : "text-slate-300 hover:bg-slate-800"
                }`
              }
            >
              <Icon size={16} /> {label}
            </NavLink>
          ))}
        </nav>
        <div className="mt-auto text-sm">
          <div className="text-slate-400">Data freshness</div>
          <div className={`text-xl font-semibold ${freshnessColor(freshness?.freshness_status)}`}>
            {freshness?.freshness_seconds == null ? "—" : `${freshness.freshness_seconds.toFixed(1)}s`}
          </div>
          <div className="text-xs text-slate-500">{freshness?.freshness_status ?? "unknown"}</div>
        </div>
      </aside>
      <main className="flex-1 p-6">
        {error ? (
          <div className="mb-4 rounded border border-rose-700 bg-rose-950/40 px-3 py-2 text-sm text-rose-200">
            {error}
          </div>
        ) : null}
        <Outlet />
      </main>
    </div>
  );
}
