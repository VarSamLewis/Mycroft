import { useMemo } from 'react';
import type { Node, Edge } from '@xyflow/react';
import { useUpstreamLineage, useDownstreamLineage } from '../../hooks/useLineage';
import { useTableDetail } from '../../hooks/useTableDetail';

function tableFromColumnKey(key: string): string {
  const parts = key.split('.');
  return parts.slice(0, -1).join('.');
}

function tableNameFromKey(key: string): string {
  const parts = key.split('.');
  return parts[parts.length - 1];
}

export function useTableGraph(tableKey: string | undefined) {
  // Fetch the table's columns so we can query lineage per column
  const { data: tableDetail, isLoading: tableLoading } = useTableDetail(tableKey);

  // We pick the first column as a representative to find connected tables.
  // For a proper table-level graph we'd union all columns — but fetching
  // every column's lineage in parallel would be expensive. Using the first
  // column gives a useful approximation; the full table lineage feature can
  // be enhanced later.
  const firstColumnKey = tableDetail?.columns[0]?.key;

  const { data: upstream = [], isLoading: upLoading } =
    useUpstreamLineage(firstColumnKey);
  const { data: downstream = [], isLoading: downLoading } =
    useDownstreamLineage(firstColumnKey);

  const { nodes, edges } = useMemo(() => {
    if (!tableKey) return { nodes: [], edges: [] };

    const tableNodes = new Map<string, Node>();
    const tableEdges = new Map<string, Edge>();

    // Center table
    tableNodes.set(tableKey, {
      id: tableKey,
      type: 'tableNode',
      position: { x: 0, y: 0 },
      data: { label: tableNameFromKey(tableKey), fullKey: tableKey, role: 'center' },
    });

     upstream.forEach((col, i) => {
       const tKey = tableFromColumnKey(col.key);
       if (!tableNodes.has(tKey)) {
         tableNodes.set(tKey, {
           id: tKey,
           type: 'tableNode',
           position: { x: 340, y: (i - (upstream.length - 1) / 2) * 80 },
           data: { label: tableNameFromKey(tKey), fullKey: tKey, role: 'upstream' },
         });
       }
       const edgeId = `${tKey}→${tableKey}`;
       if (!tableEdges.has(edgeId)) {
         tableEdges.set(edgeId, {
           id: edgeId,
           source: tKey,
           target: tableKey,
           animated: true,
           style: { stroke: '#6366f1', strokeWidth: 2 },
         });
       }
     });

     downstream.forEach((col, i) => {
       const tKey = tableFromColumnKey(col.key);
       if (!tableNodes.has(tKey)) {
         tableNodes.set(tKey, {
           id: tKey,
           type: 'tableNode',
           position: { x: -340, y: (i - (downstream.length - 1) / 2) * 80 },
           data: { label: tableNameFromKey(tKey), fullKey: tKey, role: 'downstream' },
         });
       }
       const edgeId = `${tableKey}→${tKey}`;
       if (!tableEdges.has(edgeId)) {
         tableEdges.set(edgeId, {
           id: edgeId,
           source: tableKey,
           target: tKey,
           animated: true,
           style: { stroke: '#10b981', strokeWidth: 2 },
         });
       }
     });

    return {
      nodes: Array.from(tableNodes.values()),
      edges: Array.from(tableEdges.values()),
    };
  }, [tableKey, upstream, downstream]);

  return {
    nodes,
    edges,
    isLoading: tableLoading || upLoading || downLoading,
  };
}
