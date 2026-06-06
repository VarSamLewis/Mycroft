import { useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useSchemaTree } from '../../hooks/useTables';
import { useTableDetail } from '../../hooks/useTableDetail';
import type { Table } from '../../types';

// ---- type badge ----
const TYPE_COLORS: Record<string, string> = {
  physical: 'bg-blue-900 text-blue-300',
  cte: 'bg-purple-900 text-purple-300',
  view: 'bg-teal-900 text-teal-300',
  dataframe: 'bg-orange-900 text-orange-300',
};

function TypeBadge({ type }: { type: string | null }) {
  if (!type) return null;
  const cls = TYPE_COLORS[type] ?? 'bg-slate-700 text-slate-300';
  return (
    <span className={`text-[10px] px-1.5 py-0.5 rounded font-mono ${cls}`}>
      {type}
    </span>
  );
}

// ---- expandable table row (lazy loads columns) ----
function TableRow({ table }: { table: Table }) {
  const navigate = useNavigate();
  const { tableKey, columnKey } = useParams();
  const [open, setOpen] = useState(false);
  const { data: detail } = useTableDetail(open ? table.key : undefined);

  const isActive = tableKey === encodeURIComponent(table.key);

  return (
    <div>
      <div
        className={`flex items-center gap-1 px-3 py-1.5 cursor-pointer rounded text-sm group
          ${isActive ? 'bg-indigo-600 text-white' : 'text-slate-300 hover:bg-slate-700'}`}
        onClick={() => {
          navigate(`/tables/${encodeURIComponent(table.key)}`);
          setOpen((o) => !o);
        }}
      >
        <span className="text-slate-500 group-hover:text-slate-300 w-3 shrink-0 select-none">
          {open ? '▾' : '▸'}
        </span>
        <span className="truncate flex-1">{table.name}</span>
        <TypeBadge type={table.type} />
      </div>

      {open && (
        <div className="ml-4 border-l border-slate-700 pl-2">
          {!detail && (
            <p className="text-xs text-slate-500 px-2 py-1">Loading…</p>
          )}
          {detail?.columns.map((col) => {
            const colKey = encodeURIComponent(col.key);
            const isColActive = columnKey === colKey;
            return (
              <div
                key={col.key}
                onClick={() => navigate(`/columns/${colKey}`)}
                className={`flex items-center gap-1.5 px-2 py-1 rounded cursor-pointer text-xs
                  ${isColActive ? 'bg-indigo-700 text-white' : 'text-slate-400 hover:bg-slate-700 hover:text-slate-200'}`}
              >
                <span className="shrink-0 text-slate-600">⬡</span>
                <span className="truncate flex-1">{col.name}</span>
                {col.data_type && (
                  <span className="text-[10px] text-slate-500 font-mono shrink-0">
                    {col.data_type}
                  </span>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// ---- schema section ----
function SchemaSection({
  schemaName,
  tables,
}: {
  schemaName: string;
  tables: Table[];
}) {
  const [open, setOpen] = useState(true);
  return (
    <div className="mb-1">
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-1 w-full px-2 py-1 text-xs font-semibold text-slate-400 uppercase tracking-wider hover:text-slate-200"
      >
        <span className="w-3">{open ? '▾' : '▸'}</span>
        {schemaName}
        <span className="ml-auto font-normal normal-case tracking-normal text-slate-600">
          {tables.length}
        </span>
      </button>
      {open && (
        <div className="space-y-0.5">
          {tables.map((t) => (
            <TableRow key={t.key} table={t} />
          ))}
        </div>
      )}
    </div>
  );
}

// ---- database section ----
function DatabaseSection({
  dbName,
  schemas,
}: {
  dbName: string;
  schemas: Record<string, Table[]>;
}) {
  const [open, setOpen] = useState(true);
  return (
    <div className="mb-3">
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-1.5 w-full px-2 py-1.5 text-sm font-semibold text-slate-200 hover:text-white"
      >
        <span className="text-slate-500">
          {open ? '▾' : '▸'}
        </span>
        <span className="text-indigo-400">⬢</span>
        {dbName}
      </button>
      {open && (
        <div className="ml-2">
          {Object.entries(schemas).map(([schemaName, tables]) => (
            <SchemaSection key={schemaName} schemaName={schemaName} tables={tables} />
          ))}
        </div>
      )}
    </div>
  );
}

// ---- main export ----
export function SchemaExplorer() {
  const { tree, isLoading, isError } = useSchemaTree();
  const [collapsed, setCollapsed] = useState(false);

  return (
    <aside
      className={`shrink-0 bg-slate-900 border-r border-slate-700 flex flex-col overflow-hidden transition-all duration-200 ${
        collapsed ? 'w-14' : 'w-72'
      }`}
    >
      <div className="px-2 py-3 border-b border-slate-700 flex items-center gap-2">
        <button
          onClick={() => setCollapsed((c) => !c)}
          className="w-10 h-10 rounded-md bg-slate-800 border border-slate-700 text-slate-200 hover:text-white hover:bg-slate-700 flex items-center justify-center"
          aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          {collapsed ? '⟫' : '⟪'}
        </button>

        {!collapsed && (
          <div className="leading-tight">
            <h1 className="text-lg font-bold text-white tracking-tight">Mycroft</h1>
            <p className="text-xs text-slate-500 mt-0.5">Data Lineage Explorer</p>
          </div>
        )}
      </div>

      {!collapsed && (
        <div className="overflow-y-auto flex-1 py-2 px-1">
        {isLoading && (
          <p className="text-sm text-slate-500 px-4 py-3">Loading schema…</p>
        )}
        {isError && (
          <p className="text-sm text-red-400 px-4 py-3">
            Could not connect to API. Is the backend running?
          </p>
        )}
        {!isLoading && !isError && Object.keys(tree).length === 0 && (
          <p className="text-sm text-slate-500 px-4 py-3">
            No tables found. Run an ingestion first.
          </p>
        )}
        {Object.entries(tree).map(([db, schemas]) => (
          <DatabaseSection key={db} dbName={db} schemas={schemas} />
        ))}
        </div>
      )}
    </aside>
  );
}
