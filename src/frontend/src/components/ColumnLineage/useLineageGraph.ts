import { useMemo } from 'react';
import type { Node, Edge } from '@xyflow/react';
import { useUpstreamLineage, useDownstreamLineage } from '../../hooks/useLineage';
import type { LineageNode } from '../../types';

export type Direction = 'all' | 'upstream' | 'downstream';

function parseTableFromKey(key: string): string {
  const parts = key.split('.');
  // key = db.schema.table.column  → return db.schema.table
  return parts.slice(0, -1).join('.');
}

function buildNodes(
  centerKey: string,
  upstream: LineageNode[],
  downstream: LineageNode[],
  direction: Direction,
): Node[] {
  const nodes: Node[] = [];

  // Center node
  const centerParts = centerKey.split('.');
  nodes.push({
    id: centerKey,
    type: 'columnNode',
    position: { x: 0, y: 0 },
    data: {
      label: centerParts[centerParts.length - 1],
      tableKey: parseTableFromKey(centerKey),
      fullKey: centerKey,
      role: 'center',
    },
  });

   if (direction !== 'downstream') {
     upstream.forEach((n, i) => {
       const parts = n.key.split('.');
       nodes.push({
         id: n.key,
         type: 'columnNode',
         position: { x: 320, y: (i - (upstream.length - 1) / 2) * 70 },
         data: {
           label: parts[parts.length - 1],
           tableKey: parseTableFromKey(n.key),
           fullKey: n.key,
           dataType: n.data_type,
           role: 'upstream',
         },
       });
     });
   }

   if (direction !== 'upstream') {
     downstream.forEach((n, i) => {
       const parts = n.key.split('.');
       nodes.push({
         id: n.key,
         type: 'columnNode',
         position: { x: -320, y: (i - (downstream.length - 1) / 2) * 70 },
         data: {
           label: parts[parts.length - 1],
           tableKey: parseTableFromKey(n.key),
           fullKey: n.key,
           dataType: n.data_type,
           role: 'downstream',
         },
       });
     });
   }

  return nodes;
}

function buildEdges(
  centerKey: string,
  upstream: LineageNode[],
  downstream: LineageNode[],
  direction: Direction,
): Edge[] {
  const edges: Edge[] = [];

  if (direction !== 'downstream') {
    upstream.forEach((n) => {
      edges.push({
        id: `${n.key}→${centerKey}`,
        source: n.key,
        target: centerKey,
        animated: true,
        style: { stroke: '#6366f1', strokeWidth: 1.5 },
        type: 'transformationEdge',
        data: { transformation: n.transformation, tone: 'upstream' },
      });
    });
  }

  if (direction !== 'upstream') {
    downstream.forEach((n) => {
      edges.push({
        id: `${centerKey}→${n.key}`,
        source: centerKey,
        target: n.key,
        animated: true,
        style: { stroke: '#10b981', strokeWidth: 1.5 },
        type: 'transformationEdge',
        data: { transformation: n.transformation, tone: 'downstream' },
      });
    });
  }

  return edges;
}

export function useLineageGraph(columnKey: string | undefined, direction: Direction) {
  const {
    data: upstream = [],
    isLoading: upLoading,
    isError: upError,
  } = useUpstreamLineage(columnKey);
  const {
    data: downstream = [],
    isLoading: downLoading,
    isError: downError,
  } = useDownstreamLineage(columnKey);

  const nodes = useMemo(() => {
    if (!columnKey) return [];
    return buildNodes(columnKey, upstream, downstream, direction);
  }, [columnKey, upstream, downstream, direction]);

  const edges = useMemo(() => {
    if (!columnKey) return [];
    return buildEdges(columnKey, upstream, downstream, direction);
  }, [columnKey, upstream, downstream, direction]);

  return {
    nodes,
    edges,
    isLoading: upLoading || downLoading,
    isError: upError || downError,
    upstreamCount: upstream.length,
    downstreamCount: downstream.length,
  };
}
