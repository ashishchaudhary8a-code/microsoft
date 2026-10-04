export type Organization = {
  organization_id: string;
  organization_name: string;
  domain: string;
  timezone: string;
  currency: string | null;
};

export type DashboardKpi = {
  organization_id: string;
  events_per_minute: number;
  events_total: number;
  anomalies_total: number;
  critical_total: number;
  anomaly_score_avg: number | null;
  freshness_seconds: number | null;
  freshness_status: "GREEN" | "WARNING" | "CRITICAL";
  pipeline_status: string;
  window_seconds: number;
};

export type EventItem = {
  event_id: string;
  organization_id: string;
  domain: string;
  event_type: string;
  timestamp: string;
  user_id: string | null;
  username: string | null;
  location: string | null;
  device_id: string | null;
  ip_address: string | null;
  action: string | null;
  status: string | null;
  amount: number | null;
  currency: string | null;
  anomaly_score: number | null;
  severity: string | null;
  is_anomaly: boolean | null;
};

export type AnomalyItem = {
  event_id: string;
  organization_id: string;
  timestamp: string | null;
  user_id: string | null;
  anomaly_score: number;
  severity: string;
  is_anomaly: boolean;
  model_version: string;
  processed_at: string;
  reasons?: { code: string; feature: string; detail: string; contribution: number }[];
};

export type AlertItem = {
  alert_id: string;
  organization_id: string;
  alert_type: string;
  severity: string;
  user_id: string | null;
  event_ids: string[];
  title: string;
  summary: string;
  created_at: string;
};

export type Freshness = {
  organization_id: string;
  freshness_seconds: number | null;
  freshness_p95_seconds: number | null;
  freshness_status: "GREEN" | "WARNING" | "CRITICAL";
  sla_seconds: number;
  last_event_timestamp: string | null;
  last_dashboard_visible_at: string | null;
  pipeline_status: string;
};

export type Health = {
  status: string;
  backend: string;
  database: string;
  broker: string;
  streaming: string;
  ml: string;
  checked_at: string;
  consumer_lag?: number;
};

export type WsEnvelope = {
  type: "event" | "anomaly" | "alert" | "dashboard" | "freshness" | "health";
  organization_id: string;
  emitted_at: string;
  payload: unknown;
};
