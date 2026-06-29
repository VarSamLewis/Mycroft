import { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  ReactFlow,
  Background,
  Controls,
  BackgroundVariant,
} from '@xyflow/react';
import { ColumnNodeComponent } from './ColumnNode';
import { useLineageGraph, type Direction } from './useLineageGraph';
import { TransformationEdge } from './TransformationEdge';

const nodeTypes = { columnNode: ColumnNodeComponent };
const edgeTypes = { transformationEdge: TransformationEdge };

const DIRECTION_OPTIONS: { value: Direction; label: string }[] = [
  { value: 'all', label: 'Both' },
  { value: 'upstream', label: 'Upstream only' },
  { value: 'downstream', label: 'Downstream only' },
];

export function ColumnLineage() {
  const { columnKey } = useParams<{ columnKey: string }>();
  const navigate = useNavigate();
  const decodedKey = columnKey ? decodeURIComponent(columnKey) : undefined;
  const [direction, setDirection] = useState<Direction>('all');

  const { nodes, edges, isLoading, isError, upstreamCount, downstreamCount } =
    useLineageGraph(decodedKey, direction);

  const keyParts = decodedKey?.split('.') ?? [];
  const columnName = keyParts[keyParts.length - 1];
  const tableParts = keyParts.slice(0, -1);
  const tableName = tableParts[tableParts.length - 1];

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Header */}
      <div className="px-6 py-4 border-b border-slate-700 flex items-center gap-4 shrink-0">
        <div>
          <h2 className="text-lg font-bold text-white">
            <span className="text-slate-400 font-normal text-sm">{tableName} / </span>
            {columnName}
          </h2>
          <p className="text-xs text-slate-500 font-mono">{decodedKey}</p>
        </div>

        <div className="ml-auto flex items-center gap-2">
          <span className="text-xs text-slate-500">
            {upstreamCount} upstream · {downstreamCount} downstream
          </span>

          <button
            onClick={() => navigate(`/columns/${encodeURIComponent(decodedKey!)}/table`)}
            className="px-3 py-1 text-xs bg-slate-700 hover:bg-slate-600 text-slate-200 rounded transition-colors"
          >
            Table view
          </button>

          <div className="flex rounded overflow-hidden border border-slate-600">
            {DIRECTION_OPTIONS.map((opt) => (
              <button
                key={opt.value}
                onClick={() => setDirection(opt.value)}
                className={`px-3 py-1 text-xs transition-colors
                  ${direction === opt.value
                    ? 'bg-indigo-600 text-white'
                    : 'bg-slate-800 text-slate-400 hover:text-slate-200'}`}
              >
                {opt.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Graph */}
      <div className="flex-1 relative">
        {isLoading && (
          <div className="absolute inset-0 flex items-center justify-center text-slate-500 z-10 bg-slate-900/50">
            Loading lineage…
          </div>
        )}

        {isError && (
          <div className="absolute inset-0 flex items-center justify-center text-red-400 z-10 bg-slate-900/50 p-4">
            Failed to load lineage. Is the API running and Neo4j reachable?
          </div>
        )}

        {!isLoading && !isError && nodes.length <= 1 && (
          <div className="absolute inset-0 flex flex-col items-center justify-center text-slate-500 z-10">
            <p className="text-4xl mb-3">⬡</p>
            <p>No lineage found for this column.</p>
          </div>
        )}

         <ReactFlow
           nodes={nodes}
           edges={edges}
           nodeTypes={nodeTypes}
           edgeTypes={edgeTypes}
           fitView
           fitViewOptions={{ padding: 0.3 }}
           proOptions={{ hideAttribution: true }}
         >
          <Background
            variant={BackgroundVariant.Dots}
            gap={20}
            size={1}
            color="#334155"
          />
          <Controls
            style={{ background: '#1e293b', border: '1px solid #334155' }}
          />
        </ReactFlow>
      </div>
    </div>
  );
}
