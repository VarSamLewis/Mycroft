import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';
import type { SchemaTree } from '../types';

export function useTables() {
  return useQuery({
    queryKey: ['tables'],
    queryFn: api.getTables,
    staleTime: 30_000,
  });
}

export function useSchemaTree() {
  const { data: tables, ...rest } = useTables();

  const tree: SchemaTree = {};
  if (tables) {
    for (const table of tables) {
      const parts = table.key.split('.');
      // key format: db.schema.table  (or just schema.table, or just table)
      const db = parts.length >= 3 ? parts[0] : 'default';
      const schema = parts.length >= 3 ? parts[1] : parts.length === 2 ? parts[0] : 'public';
      if (!tree[db]) tree[db] = {};
      if (!tree[db][schema]) tree[db][schema] = [];
      tree[db][schema].push(table);
    }
  }

  return { tree, ...rest };
}
