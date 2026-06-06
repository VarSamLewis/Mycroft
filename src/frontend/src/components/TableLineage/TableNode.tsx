import { memo } from 'react';
import { Handle, Position } from '@xyflow/react';
import { useNavigate } from 'react-router-dom';

interface TableNodeData {
  label: string;
  fullKey: string;
  role: 'center' | 'upstream' | 'downstream';
}

const ROLE_STYLES: Record<string, string> = {
  center: 'bg-indigo-600 border-indigo-400 text-white shadow-lg shadow-indigo-900',
  upstream: 'bg-slate-800 border-slate-600 text-slate-200 hover:border-indigo-500',
  downstream: 'bg-slate-800 border-slate-600 text-slate-200 hover:border-emerald-500',
};

export const TableNodeComponent = memo(function TableNodeComponent({
  data,
}: {
  data: TableNodeData;
}) {
  const navigate = useNavigate();
  const isCenter = data.role === 'center';

  return (
    <div
      onClick={() => {
        if (!isCenter) {
          navigate(`/tables/${encodeURIComponent(data.fullKey)}`);
        }
      }}
      className={`px-4 py-3 rounded-lg border min-w-[160px] max-w-[220px] transition-colors
        ${ROLE_STYLES[data.role]}
        ${!isCenter ? 'cursor-pointer' : 'cursor-default'}`}
    >
      <Handle type="target" position={Position.Left} style={{ background: '#6366f1' }} />

      <div className="flex items-center gap-2">
        <span className={isCenter ? 'text-indigo-300' : 'text-slate-500'}>⬢</span>
        <p className="text-sm font-semibold truncate">{data.label}</p>
      </div>
      <p className="text-[10px] font-mono text-slate-500 mt-1 truncate">{data.fullKey}</p>

      <Handle type="source" position={Position.Right} style={{ background: '#10b981' }} />
    </div>
  );
});
