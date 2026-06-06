import { useEffect, useMemo, useState } from 'react';
import { ReactFlow, Background, Controls, MiniMap, BackgroundVariant } from '@xyflow/react';
import type { Edge, Node } from '@xyflow/react';

import { useTables } from '../../hooks/useTables';
import { api } from '../../api/client';
import type { SchemaTree, Table, TableDetail, LineageNode } from '../../types';
import { useQueries } from '@tanstack/react-query';

import { TableNodeComponent } from '../TableLineage/TableNode';

type RepoKey = { database: string; schema: string };

function parseTableFromColumnKey(key: string): string {
  const parts = key.split('.');
  return parts.slice(0, -1).join('.');
}

export function RepoGraph() {
  const { data: tables, isLoading: tablesLoading, isError: tablesError } = useTables();

  const [repoKey, setRepoKey] = useState<RepoKey>({ database: 'default', schema: 'public' });

  const schemaTree: SchemaTree = useMemo(() => {
    const tree: SchemaTree = {};
    if (!tables) return tree;
    for (const table of tables) {
      const parts = table.key.split('.');
      const db = parts.length >= 3 ? parts[0] : 'default';
      const schema = parts.length >= 3 ? parts[1] : parts.length === 2 ? parts[0] : 'public';
      if (!tree[db]) tree[db] = {};
      if (!tree[db][schema]) tree[db][schema] = [];
      tree[db][schema].push(table);
    }
    return tree;
  }, [tables]);

  const databases = useMemo(() => Object.keys(schemaTree), [schemaTree]);
  const schemas = useMemo(() => (schemaTree[repoKey.database] ? Object.keys(schemaTree[repoKey.database]) : []), [schemaTree, repoKey.database]);

  const availableRepos: RepoKey[] = useMemo(() => {
    const repos: RepoKey[] = [];
    for (const db of Object.keys(schemaTree)) {
      for (const s of Object.keys(schemaTree[db] || {})) {
        repos.push({ database: db, schema: s });
      }
    }
    return repos;
  }, [schemaTree]);

  // Auto-pick a real repo if defaults aren't present.
  useEffect(() => {
    const hasDefault = availableRepos.some(
      (r) => r.database === repoKey.database && r.schema === repoKey.schema,
    );
    if (!hasDefault && availableRepos.length > 0) {
      setRepoKey(availableRepos[0]);
    }
  }, [availableRepos, repoKey.database, repoKey.schema]);

  const repoTables: Table[] = useMemo(() => {
    if (!tables) return [];
    const prefix = `${repoKey.database}.${repoKey.schema}.`;
    return tables.filter((t) => t.key.startsWith(prefix));
  }, [tables, repoKey.database, repoKey.schema]);

  // Limit to keep the UI responsive.
  const maxTables = 60;
  const selectedTables = repoTables.slice(0, maxTables);

  const tableDetailQueries = useQueries({
    queries: selectedTables.map((t) => ({
      queryKey: ['table', 'detail', t.key],
      queryFn: () => api.getTableDetail(t.key),
      staleTime: 30_000,
    })),
  });

  const representativeColumns = useMemo(() => {
    const reps: { tableKey: string; columnKey: string }[] = [];
    for (let i = 0; i < selectedTables.length; i++) {
      const detail = tableDetailQueries[i]?.data as TableDetail | undefined;
      const firstCol = detail?.columns?.[0];
      if (firstCol) reps.push({ tableKey: selectedTables[i].key, columnKey: firstCol.key });
    }
    return reps;
  }, [selectedTables, tableDetailQueries]);

  const lineageQueryResults = useQueries({
    queries: representativeColumns.flatMap((r) => [
      {
        queryKey: ['lineage', 'upstream', r.columnKey],
        queryFn: () => api.getUpstreamLineage(r.columnKey),
        staleTime: 30_000,
      },
      {
        queryKey: ['lineage', 'downstream', r.columnKey],
        queryFn: () => api.getDownstreamLineage(r.columnKey),
        staleTime: 30_000,
      },
    ]),
  });

  const isLoading =
    tablesLoading ||
    tableDetailQueries.some((q) => q.isLoading) ||
    lineageQueryResults.some((q) => q.isLoading);

  const isError =
    tablesError ||
    tableDetailQueries.some((q) => q.isError) ||
    lineageQueryResults.some((q) => q.isError);

  const graph = useMemo(() => {
    if (!representativeColumns.length) return { nodes: [] as Node[], edges: [] as Edge[] };

    const nodesByKey = new Map<string, Node>();
    const edgesById = new Map<string, Edge>();

    for (const t of selectedTables) {
      nodesByKey.set(t.key, {
        id: t.key,
        type: 'tableNode',
        position: { x: 0, y: 0 },
        data: { label: t.name, fullKey: t.key, role: 'center' },
      });
    }

    // Simple layout: place tables in a grid.
    const cols = 5;
    const cellW = 420;
    const cellH = 140;
    let idx = 0;
    for (const t of selectedTables) {
      const node = nodesByKey.get(t.key);
      if (!node) continue;
      node.position = { x: (idx % cols) * cellW - ((cols - 1) * cellW) / 2, y: Math.floor(idx / cols) * cellH - 80 };
      idx++;
    }

    // Each representative column produces two query results in order: upstream then downstream.
    for (let i = 0; i < representativeColumns.length; i++) {
      const rep = representativeColumns[i];
      const upstreamRes = lineageQueryResults[i * 2]?.data as LineageNode[] | undefined;
      const downstreamRes = lineageQueryResults[i * 2 + 1]?.data as LineageNode[] | undefined;

      const upList = upstreamRes ?? [];
      const downList = downstreamRes ?? [];

      // Upstream: upstream tables -> this table
      for (const col of upList) {
        const srcTable = parseTableFromColumnKey(col.key);
        if (!nodesByKey.has(srcTable)) continue;
        const edgeId = `${srcTable}→${rep.tableKey}`;
        if (!edgesById.has(edgeId)) {
          edgesById.set(edgeId, {
            id: edgeId,
            source: srcTable,
            target: rep.tableKey,
            animated: true,
            style: { stroke: '#6366f1', strokeWidth: 1.5 },
          });
        }
      }

      // Downstream: this table -> downstream tables
      for (const col of downList) {
        const dstTable = parseTableFromColumnKey(col.key);
        if (!nodesByKey.has(dstTable)) continue;
        const edgeId = `${rep.tableKey}→${dstTable}`;
        if (!edgesById.has(edgeId)) {
          edgesById.set(edgeId, {
            id: edgeId,
            source: rep.tableKey,
            target: dstTable,
            animated: true,
            style: { stroke: '#10b981', strokeWidth: 1.5 },
          });
        }
      }
    }

    return { nodes: Array.from(nodesByKey.values()), edges: Array.from(edgesById.values()) };
  }, [representativeColumns, selectedTables, lineageQueryResults]);

  const nodeTypes = useMemo(() => ({ tableNode: TableNodeComponent }), []);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <div className="px-6 py-4 border-b border-slate-700 shrink-0 flex items-center gap-4">
        <div>
          <h2 className="text-lg font-bold text-white">Repository graph</h2>
          <p className="text-xs text-slate-500 font-mono">Approximate table-level connectivity from column lineage</p>
        </div>

        <div className="ml-auto flex items-center gap-2">
          {databases.length > 0 && (
            <>
              <select
                className="text-xs bg-slate-800 border border-slate-700 rounded px-2 py-1 text-slate-200"
                value={repoKey.database}
                onChange={(e) => setRepoKey((rk) => ({ ...rk, database: e.target.value }))}
              >
                {databases.map((db) => (
                  <option key={db} value={db}>
                    {db}
                  </option>
                ))}
              </select>
              <select
                className="text-xs bg-slate-800 border border-slate-700 rounded px-2 py-1 text-slate-200"
                value={repoKey.schema}
                onChange={(e) => setRepoKey((rk) => ({ ...rk, schema: e.target.value }))}
              >
                {schemas.map((s) => (
                  <option key={s} value={s}>
                    {s}
                  </option>
                ))}
              </select>
            </>
          )}
        </div>
      </div>

      <div className="flex-1 relative">
        {isLoading && (
          <div className="absolute inset-0 flex items-center justify-center text-slate-500 z-10 bg-slate-900/50">
            Loading repository graph…
          </div>
        )}

        {isError && (
          <div className="absolute inset-0 flex flex-col items-center justify-center text-red-400 z-10 bg-slate-900/50 p-4">
            Failed to load graph. Is the API running and Neo4j reachable?
          </div>
        )}

        {!isLoading && !isError && graph.nodes.length === 0 && (
          <div className="absolute inset-0 flex flex-col items-center justify-center text-slate-500 z-10 p-4">
            <p className="text-4xl mb-2">⬢</p>
            <p>No data found for `{repoKey.database}.{repoKey.schema}`.</p>
          </div>
        )}

        <ReactFlow nodes={graph.nodes.map((n) => ({
          ...n,
          // Make sure TableNodeComponent navigation works.
          // It treats non-center nodes as clickable; we want all table nodes clickable.
          data: { ...(n.data as any), role: 'upstream' },
        }))} edges={graph.edges} nodeTypes={nodeTypes} fitView fitViewOptions={{ padding: 0.3 }} proOptions={{ hideAttribution: true }}>
          <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="#334155" />
          <Controls style={{ background: '#1e293b', border: '1px solid #334155' }} />
          <MiniMap style={{ background: '#0f172a', border: '1px solid #334155' }} nodeColor="#6366f1" />
        </ReactFlow>
      </div>
    </div>
  );
}
