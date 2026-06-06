import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';

export function useUpstreamLineage(columnKey: string | undefined) {
  return useQuery({
    queryKey: ['lineage', 'upstream', columnKey],
    queryFn: () => api.getUpstreamLineage(columnKey!),
    enabled: !!columnKey,
    staleTime: 30_000,
  });
}

export function useDownstreamLineage(columnKey: string | undefined) {
  return useQuery({
    queryKey: ['lineage', 'downstream', columnKey],
    queryFn: () => api.getDownstreamLineage(columnKey!),
    enabled: !!columnKey,
    staleTime: 30_000,
  });
}
