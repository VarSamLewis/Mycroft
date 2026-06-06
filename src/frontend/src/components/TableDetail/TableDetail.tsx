import { useNavigate, useParams } from 'react-router-dom';
import { useTableDetail } from '../../hooks/useTableDetail';
import type { Column, Transformation } from '../../types';

const TRANSFORM_TYPE_COLORS: Record<string, string> = {
  filter: 'text-amber-300',
  join: 'text-sky-300',
  distinct: 'text-purple-300',
  group_by: 'text-emerald-300',
  having: 'text-rose-300',
  order_by: 'text-cyan-300',
  limit: 'text-orange-300',
  set_op: 'text-fuchsia-300',
};

function TransformationRow({ transformation }: { transformation: Transformation }) {
  const color = TRANSFORM_TYPE_COLORS[transformation.type] ?? 'text-slate-300';
  return (
    <div className="flex items-start gap-3 bg-slate-800/50 rounded-lg px-4 py-3 border border-slate-700">
      <span className={`text-xs font-mono font-semibold uppercase shrink-0 mt-0.5 ${color}`}>
        {transformation.type.replace('_', ' ')}
      </span>
      {transformation.expression && (
        <code className="text-xs text-slate-300 font-mono break-all leading-relaxed">
          {transformation.expression}
        </code>
      )}
    </div>
  );
}

const NULL_COLORS: Record<string, string> = {
  true: 'text-yellow-500',
  false: 'text-green-500',
};

function ColumnRow({ col }: { col: Column }) {
  const navigate = useNavigate();
  const columnKey = encodeURIComponent(col.key);

  return (
    <tr className="border-b border-slate-700 hover:bg-slate-800 group">
      <td className="px-4 py-2.5 font-mono text-sm text-slate-200">{col.name}</td>
      <td className="px-4 py-2.5 font-mono text-xs text-slate-400">
        {col.data_type ?? <span className="text-slate-600">—</span>}
      </td>
      <td className="px-4 py-2.5 text-xs">
        {col.is_nullable === null ? (
          <span className="text-slate-600">—</span>
        ) : (
          <span className={NULL_COLORS[String(col.is_nullable)]}>
            {col.is_nullable ? 'nullable' : 'not null'}
          </span>
        )}
      </td>
      <td className="px-4 py-2.5 text-right flex gap-2 justify-end">
        <button
          onClick={() => navigate(`/columns/${columnKey}/table`)}
          className="text-xs text-slate-400 hover:text-slate-300 opacity-0 group-hover:opacity-100 transition-opacity"
        >
          Table
        </button>
        <button
          onClick={() => navigate(`/columns/${columnKey}`)}
          className="text-xs text-indigo-400 hover:text-indigo-300 opacity-0 group-hover:opacity-100 transition-opacity"
        >
          Graph →
        </button>
      </td>
    </tr>
  );
}

export function TableDetail() {
  const { tableKey } = useParams<{ tableKey: string }>();
  const navigate = useNavigate();
  const decodedKey = tableKey ? decodeURIComponent(tableKey) : undefined;
  const { data, isLoading, isError } = useTableDetail(decodedKey);

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center text-slate-500">
        Loading table…
      </div>
    );
  }

  if (isError || !data) {
    return (
      <div className="flex-1 flex items-center justify-center text-red-400">
        Failed to load table.
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto p-6">
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center gap-3 mb-1">
          <h2 className="text-2xl font-bold text-white">{data.name}</h2>
          {data.type && (
            <span className="text-xs px-2 py-1 rounded bg-slate-700 text-slate-300 font-mono">
              {data.type}
            </span>
          )}
        </div>
        <p className="text-sm text-slate-500 font-mono">{data.key}</p>
        {data.source_file && (
          <p className="text-xs text-slate-600 mt-1">
            Source: <span className="font-mono text-slate-500">{data.source_file}</span>
          </p>
        )}
      </div>

      {/* Actions */}
      <div className="flex gap-2 mb-6">
        <button
          onClick={() =>
            navigate(`/tables/${encodeURIComponent(data.key)}/lineage`)
          }
          className="px-3 py-1.5 text-sm bg-indigo-600 hover:bg-indigo-500 text-white rounded transition-colors"
        >
          Table lineage graph
        </button>
      </div>

      {/* Columns table */}
      <div className="rounded-lg overflow-hidden border border-slate-700">
        <table className="w-full text-left">
          <thead className="bg-slate-800">
            <tr>
              <th className="px-4 py-2.5 text-xs font-semibold text-slate-400 uppercase tracking-wider">
                Column
              </th>
              <th className="px-4 py-2.5 text-xs font-semibold text-slate-400 uppercase tracking-wider">
                Type
              </th>
              <th className="px-4 py-2.5 text-xs font-semibold text-slate-400 uppercase tracking-wider">
                Nullable
              </th>
              <th className="px-4 py-2.5" />
            </tr>
          </thead>
          <tbody>
            {data.columns.length === 0 && (
              <tr>
                <td colSpan={4} className="px-4 py-6 text-center text-slate-500 text-sm">
                  No columns found.
                </td>
              </tr>
            )}
            {data.columns.map((col) => (
              <ColumnRow key={col.key} col={col} />
            ))}
          </tbody>
        </table>
      </div>

      <p className="text-xs text-slate-600 mt-3">{data.columns.length} columns</p>

      {/* Transformations */}
      {data.transformations.length > 0 && (
        <div className="mt-8">
          <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-3">
            Table transformations
          </h3>
          <div className="space-y-2">
            {data.transformations.map((tr) => (
              <TransformationRow key={tr.key} transformation={tr} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
