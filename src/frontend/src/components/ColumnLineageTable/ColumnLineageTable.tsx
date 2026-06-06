import { useNavigate, useParams } from 'react-router-dom';
import { useUpstreamLineage, useDownstreamLineage } from '../../hooks/useLineage';
import type { LineageNode } from '../../types';

function parseTableKey(key: string): string {
  const parts = key.split('.');
  return parts.slice(0, -1).join('.');
}

function parseTableName(key: string): string {
  const parts = key.split('.');
  return parts[parts.length - 2] ?? '';
}

function parseColumnName(key: string): string {
  const parts = key.split('.');
  return parts[parts.length - 1];
}

function LineageTable({
  title,
  items,
  direction,
  columnKey,
}: {
  title: string;
  items: LineageNode[];
  direction: 'upstream' | 'downstream';
  columnKey: string;
}) {
  const navigate = useNavigate();

  return (
    <div className="rounded-lg overflow-hidden border border-slate-700">
      <div className="bg-slate-800 px-4 py-2 text-sm font-semibold text-slate-300 flex items-center gap-2">
        <span className={direction === 'upstream' ? 'text-indigo-400' : 'text-emerald-400'}>
          {direction === 'upstream' ? '↑' : '↓'}
        </span>
        {title}
        <span className="ml-auto text-xs font-normal text-slate-500">{items.length} columns</span>
      </div>
      <table className="w-full text-left">
        <thead className="bg-slate-850">
          <tr>
            <th className="px-4 py-2 text-xs font-semibold text-slate-400 uppercase tracking-wider">Table</th>
            <th className="px-4 py-2 text-xs font-semibold text-slate-400 uppercase tracking-wider">Column</th>
            <th className="px-4 py-2 text-xs font-semibold text-slate-400 uppercase tracking-wider">Type</th>
            <th className="px-4 py-2 text-xs font-semibold text-slate-400 uppercase tracking-wider">Transformation</th>
          </tr>
        </thead>
        <tbody>
          {items.length === 0 && (
            <tr>
              <td colSpan={4} className="px-4 py-6 text-center text-slate-500 text-sm">
                No {direction} lineage found.
              </td>
            </tr>
          )}
          {items.map((node) => (
            <tr
              key={node.key}
              className="border-b border-slate-700 hover:bg-slate-800 group cursor-pointer"
              onClick={() => navigate(`/columns/${encodeURIComponent(node.key)}/table`)}
            >
              <td className="px-4 py-2.5 text-sm text-slate-300 font-mono">
                {parseTableName(node.key)}
              </td>
              <td className="px-4 py-2.5 text-sm text-slate-200 font-mono">
                {parseColumnName(node.key)}
              </td>
              <td className="px-4 py-2.5 text-xs text-slate-400 font-mono">
                {node.data_type ?? <span className="text-slate-600">—</span>}
              </td>
              <td className="px-4 py-2.5 text-xs font-mono max-w-md">
                {node.transformation ? (
                  <code className="text-amber-300 bg-slate-850 px-1.5 py-0.5 rounded text-[11px] break-all">
                    {node.transformation}
                  </code>
                ) : (
                  <span className="text-slate-600">—</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function ColumnLineageTable() {
  const { columnKey } = useParams<{ columnKey: string }>();
  const navigate = useNavigate();
  const decodedKey = columnKey ? decodeURIComponent(columnKey) : undefined;

  const {
    data: upstream = [],
    isLoading: upLoading,
    isError: upError,
  } = useUpstreamLineage(decodedKey);
  const {
    data: downstream = [],
    isLoading: downLoading,
    isError: downError,
  } = useDownstreamLineage(decodedKey);

  const isLoading = upLoading || downLoading;
  const isError = upError || downError;

  const keyParts = decodedKey?.split('.') ?? [];
  const colName = keyParts[keyParts.length - 1];
  const tableName = keyParts.length >= 2 ? keyParts[keyParts.length - 2] : '';

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center text-slate-500">
        Loading lineage…
      </div>
    );
  }

  if (isError) {
    return (
      <div className="flex-1 flex items-center justify-center text-red-400">
        Failed to load lineage. Is the API running and Neo4j reachable?
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto p-6">
      <div className="mb-6">
        <div className="flex items-center gap-3 mb-1">
          <h2 className="text-2xl font-bold text-white">
            <span className="text-slate-400 font-normal text-base">{tableName} / </span>
            {colName}
          </h2>
        </div>
        <p className="text-sm text-slate-500 font-mono mb-4">{decodedKey}</p>

        <div className="flex gap-2">
          <button
            onClick={() => navigate(`/columns/${encodeURIComponent(decodedKey!)}`)}
            className="px-3 py-1.5 text-sm bg-indigo-600 hover:bg-indigo-500 text-white rounded transition-colors"
          >
            Graph view
          </button>
        </div>
      </div>

      <div className="space-y-6">
        <LineageTable
          title="Upstream lineage"
          items={upstream}
          direction="upstream"
          columnKey={decodedKey!}
        />

        <LineageTable
          title="Downstream lineage"
          items={downstream}
          direction="downstream"
          columnKey={decodedKey!}
        />
      </div>
    </div>
  );
}
