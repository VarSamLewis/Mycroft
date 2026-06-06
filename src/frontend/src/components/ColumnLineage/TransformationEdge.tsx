import { memo, useMemo, useState } from 'react';
import { BaseEdge, EdgeLabelRenderer, getBezierPath } from '@xyflow/react';
import type { EdgeProps } from '@xyflow/react';

type TransformationEdgeData = {
  transformation?: string | null;
  tone?: 'upstream' | 'downstream';
};

function formatTransformation(t: string | null | undefined): string {
  if (!t) return '';
  const normalized = t.replace(/\s+/g, ' ').trim();
  if (!normalized) return '';
  const max = 32;
  if (normalized.length <= max) return normalized;
  return `${normalized.slice(0, max)}…`;
}

export const TransformationEdge = memo(function TransformationEdge(
  props: EdgeProps,
) {
  const [hovered, setHovered] = useState(false);

  const data = (props.data ?? {}) as TransformationEdgeData;
  const transformation = formatTransformation(data.transformation);

  const toneColor = useMemo(() => {
    if (data.tone === 'downstream') return '#a7f3d0';
    return '#c7d2fe';
  }, [data.tone]);

  const [edgePath, labelX, labelY] = getBezierPath(props);

  if (!transformation) {
    return <BaseEdge path={edgePath} {...props} />;
  }

  return (
    <>
      <BaseEdge path={edgePath} {...props} />
      <EdgeLabelRenderer>
        <div
          className="nodrag nopan"
          style={{
            position: 'absolute',
            transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
            pointerEvents: 'all',
            padding: 0,
            border: 'none',
            background: 'transparent',
            color: toneColor,
            fontSize: 10,
            fontWeight: 500,
            whiteSpace: 'nowrap',
            maxWidth: 320,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            opacity: hovered ? 1 : 0,
            transition: 'opacity 120ms ease',
            userSelect: 'none',
          }}
          onMouseEnter={() => setHovered(true)}
          onMouseLeave={() => setHovered(false)}
        >
          {transformation}
        </div>
      </EdgeLabelRenderer>
    </>
  );
});
