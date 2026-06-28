import { useParams } from 'react-router-dom';
import {
  ReactFlow,
  Background,
  Controls,
  BackgroundVariant,
} from '@xyflow/react';
import { TableNodeComponent } from './TableNode';
import { useTableGraph } from './useTableGraph';

const nodeTypes = { tableNode: TableNodeComponent };

export function TableLineage() {
  const { tableKey } = useParams<{ tableKey: string }>();
  const decodedKey = tableKey ? decodeURIComponent(tableKey) : undefined;

  const { nodes, edges, isLoading } = useTableGraph(decodedKey);

  const parts = decodedKey?.split('.') ?? [];
  const tableName = parts[parts.length - 1];

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Header */}
      <div className="px-6 py-4 border-b border-slate-700 shrink-0">
        <h2 className="text-lg font-bold text-white">
          <span className="text-slate-400 font-normal text-sm">Table lineage / </span>
          {tableName}
        </h2>
        <p className="text-xs text-slate-500 font-mono">{decodedKey}</p>
      </div>

      {/* Graph */}
      <div className="flex-1 relative">
        {isLoading && (
          <div className="absolute inset-0 flex items-center justify-center text-slate-500 z-10 bg-slate-900/50">
            Loading lineage…
          </div>
        )}

        {!isLoading && nodes.length <= 1 && (
          <div className="absolute inset-0 flex flex-col items-center justify-center text-slate-500 z-10">
            <p className="text-4xl mb-3">⬢</p>
            <p>No table-level lineage found.</p>
            <p className="text-xs mt-1 text-slate-600">
              This table may have no columns with traced lineage.
            </p>
          </div>
        )}

        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
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
