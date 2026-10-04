import { useEffect, useMemo, useState } from "react";
import { Navigate, Route, Routes, useParams } from "react-router-dom";
import Layout from "./Layout";
import Overview from "./pages/Overview";
import LiveEvents from "./pages/LiveEvents";
import AnomalyCenter from "./pages/AnomalyCenter";
import AnomalyDetail from "./pages/AnomalyDetail";
import SystemHealth from "./pages/SystemHealth";
import { getAlerts, getAnomalies, getDashboard, getEvents, getFreshness, getHealth, getOrganizations, wsUrl } from "./api";
import type { AlertItem, AnomalyItem, DashboardKpi, EventItem, Freshness, Health, Organization, WsEnvelope } from "./types";

function DetailRoute({
  orgId,
  loading,
  error,
}: {
  orgId: string;
  loading: boolean;
  error: string | null;
}) {
  const { eventId } = useParams();
  return <AnomalyDetail orgId={orgId} eventId={eventId || ""} loading={loading} error={error} />;
}

export default function App() {
  const [orgs, setOrgs] = useState<Organization[]>([]);
  const [orgId, setOrgId] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [dashboard, setDashboard] = useState<DashboardKpi | null>(null);
  const [events, setEvents] = useState<EventItem[]>([]);
  const [anomalies, setAnomalies] = useState<AnomalyItem[]>([]);
  const [alerts, setAlerts] = useState<AlertItem[]>([]);
  const [freshness, setFreshness] = useState<Freshness | null>(null);
  const [health, setHealth] = useState<Health | null>(null);

  useEffect(() => {
    getOrganizations()
      .then((list) => {
        setOrgs(list);
        setOrgId((prev) => prev || list[0]?.organization_id || "");
      })
      .catch((err) => setError(err?.message || "Failed to load organizations"));
  }, []);

  async function refresh(id: string) {
    if (!id) return;
    setLoading(true);
    setError(null);
    try {
      const [d, e, a, al, f, h] = await Promise.all([
        getDashboard(id),
        getEvents(id),
        getAnomalies(id),
        getAlerts(id),
        getFreshness(id),
        getHealth(),
      ]);
      setDashboard(d);
      setEvents(e);
      setAnomalies(a);
      setAlerts(al);
      setFreshness(f);
      setHealth(h);
    } catch (err: any) {
      setError(err?.message || "Failed to load dashboard data");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (orgId) refresh(orgId);
  }, [orgId]);

  useEffect(() => {
    if (!orgId) return;
    let ws: WebSocket | null = null;
    let closed = false;
    function connect() {
      ws = new WebSocket(wsUrl(orgId));
      ws.onmessage = (ev) => {
        const msg = JSON.parse(ev.data) as WsEnvelope;
        if (msg.organization_id !== orgId) return;
        if (msg.type === "event") {
          const item = msg.payload as EventItem;
          setEvents((prev) => [item, ...prev.filter((p) => p.event_id !== item.event_id)].slice(0, 200));
        } else if (msg.type === "anomaly") {
          const item = msg.payload as AnomalyItem;
          setAnomalies((prev) => [item, ...prev.filter((p) => p.event_id !== item.event_id)].slice(0, 200));
        } else if (msg.type === "alert") {
          const item = msg.payload as AlertItem;
          setAlerts((prev) => [item, ...prev.filter((p) => p.alert_id !== item.alert_id)].slice(0, 200));
        } else if (msg.type === "dashboard") {
          setDashboard(msg.payload as DashboardKpi);
        } else if (msg.type === "freshness") {
          setFreshness(msg.payload as Freshness);
        } else if (msg.type === "health") {
          setHealth(msg.payload as Health);
        }
      };
      ws.onclose = () => {
        if (!closed) setTimeout(connect, 2000);
      };
    }
    connect();
    const poll = setInterval(() => refresh(orgId), 15000);
    return () => {
      closed = true;
      ws?.close();
      clearInterval(poll);
    };
  }, [orgId]);

  const emptyOrgs = useMemo(() => orgs.length === 0 && !loading, [orgs, loading]);

  return (
    <Routes>
      <Route
        element={
          <Layout orgs={orgs} orgId={orgId} setOrgId={setOrgId} freshness={freshness} error={error} />
        }
      >
        <Route
          path="/"
          element={
            <Overview
              dashboard={dashboard}
              freshness={freshness}
              loading={loading}
              error={error}
              emptyOrgs={emptyOrgs}
            />
          }
        />
        <Route path="/events" element={<LiveEvents events={events} loading={loading} error={error} />} />
        <Route
          path="/anomalies"
          element={<AnomalyCenter anomalies={anomalies} alerts={alerts} loading={loading} error={error} />}
        />
        <Route path="/anomalies/:eventId" element={<DetailRoute orgId={orgId} loading={loading} error={error} />} />
        <Route
          path="/health"
          element={<SystemHealth health={health} freshness={freshness} loading={loading} error={error} />}
        />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}
