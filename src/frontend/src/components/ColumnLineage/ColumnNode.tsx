import { memo } from 'react';
import { Handle, Position } from '@xyflow/react';
import { useNavigate } from 'react-router-dom';

interface ColumnNodeData {
  label: string;
  tableKey: string;
  fullKey: string;
  dataType?: string;
  role: 'center' | 'upstream' | 'downstream';
}

const ROLE_STYLES: Record<string, string> = {
  center: 'bg-indigo-600 border-indigo-400 text-white shadow-lg shadow-indigo-900',
  upstream: 'bg-slate-800 border-slate-600 text-slate-200 hover:border-indigo-500',
  downstream: 'bg-slate-800 border-slate-600 text-slate-200 hover:border-emerald-500',
};

export const ColumnNodeComponent = memo(function ColumnNodeComponent({
  data,
}: {
  data: ColumnNodeData;
}) {
  const navigate = useNavigate();
  const tableParts = data.tableKey.split('.');
  const tableName = tableParts[tableParts.length - 1];

  const isCenter = data.role === 'center';

  return (
    <div
      onClick={() => {
        if (!isCenter) {
          navigate(`/columns/${encodeURIComponent(data.fullKey)}`);
        }
      }}
      className={`px-3 py-2 rounded-lg border min-w-[160px] max-w-[220px] transition-colors
        ${ROLE_STYLES[data.role]}
        ${!isCenter ? 'cursor-pointer' : 'cursor-default'}`}
    >
      <Handle type="target" position={Position.Left} style={{ background: '#6366f1' }} />

      <p className="text-[10px] text-slate-400 truncate mb-0.5">{tableName}</p>
      <p className={`text-sm font-semibold truncate ${isCenter ? 'text-white' : ''}`}>
        {data.label}
      </p>
      {data.dataType && (
        <p className="text-[10px] font-mono text-slate-500 mt-0.5 truncate">{data.dataType}</p>
      )}

      <Handle type="source" position={Position.Right} style={{ background: '#10b981' }} />
    </div>
  );
});
