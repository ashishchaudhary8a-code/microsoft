import axios from "axios";
import type {
  AlertItem,
  AnomalyItem,
  DashboardKpi,
  EventItem,
  Freshness,
  Health,
  Organization,
} from "./types";

const api = axios.create({ baseURL: "/api" });

export async function getOrganizations(): Promise<Organization[]> {
  const { data } = await api.get("/organizations");
  return data.organizations;
}

export async function getDashboard(org: string): Promise<DashboardKpi> {
  const { data } = await api.get("/dashboard", { params: { organization_id: org } });
  return data;
}

export async function getEvents(org: string, extra?: Record<string, string>): Promise<EventItem[]> {
  const { data } = await api.get("/events", { params: { organization_id: org, limit: 100, ...extra } });
  return data.items;
}

export async function getAnomalies(org: string, extra?: Record<string, string>): Promise<AnomalyItem[]> {
  const { data } = await api.get("/anomalies", { params: { organization_id: org, limit: 100, ...extra } });
  return data.items;
}

export async function getAnomalyDetail(org: string, eventId: string) {
  const { data } = await api.get(`/anomalies/${eventId}`, { params: { organization_id: org } });
  return data;
}

export async function getAlerts(org: string): Promise<AlertItem[]> {
  const { data } = await api.get("/alerts", { params: { organization_id: org, limit: 100 } });
  return data.items;
}

export async function getFreshness(org: string): Promise<Freshness> {
  const { data } = await api.get("/pipeline/freshness", { params: { organization_id: org } });
  return data;
}

export async function getHealth(): Promise<Health> {
  const { data } = await api.get("/health");
  return data;
}

export function wsUrl(org: string): string {
  const proto = window.location.protocol === "https:" ? "wss" : "ws";
  return `${proto}://${window.location.host}/ws/live?organization_id=${encodeURIComponent(org)}`;
}
