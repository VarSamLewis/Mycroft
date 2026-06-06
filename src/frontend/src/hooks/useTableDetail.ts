import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';

export function useTableDetail(tableKey: string | undefined) {
  return useQuery({
    queryKey: ['table', tableKey],
    queryFn: () => api.getTableDetail(tableKey!),
    enabled: !!tableKey,
    staleTime: 30_000,
  });
}
