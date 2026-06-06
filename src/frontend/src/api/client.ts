import type { Table, TableDetail, LineageNode } from '../types';

const BASE_URL = 'http://localhost:8000';

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) {
    throw new Error(`API error ${res.status}: ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  getTables: (): Promise<Table[]> =>
    apiFetch<Table[]>('/tables'),

  getTableDetail: (tableKey: string): Promise<TableDetail> =>
    apiFetch<TableDetail>(`/tables/${encodeURIComponent(tableKey)}`),

  getUpstreamLineage: (columnKey: string): Promise<LineageNode[]> =>
    apiFetch<LineageNode[]>(`/lineage/upstream/${encodeURIComponent(columnKey)}`),

  getDownstreamLineage: (columnKey: string): Promise<LineageNode[]> =>
    apiFetch<LineageNode[]>(`/lineage/downstream/${encodeURIComponent(columnKey)}`),
};
