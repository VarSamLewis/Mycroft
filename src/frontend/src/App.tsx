import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { SchemaExplorer } from './components/SchemaExplorer/SchemaExplorer';
import { TableDetail } from './components/TableDetail/TableDetail';
import { ColumnLineage } from './components/ColumnLineage/ColumnLineage';
import { ColumnLineageTable } from './components/ColumnLineageTable/ColumnLineageTable';
import { TableLineage } from './components/TableLineage/TableLineage';
import { RepoGraph } from './components/RepoGraph/RepoGraph';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

function EmptyPanel() {
  return (
    <div className="flex-1 flex flex-col items-center justify-center text-slate-600">
      <p className="text-5xl mb-4">⬢</p>
      <p className="text-lg font-medium text-slate-500">Select a table or column</p>
      <p className="text-sm mt-1">Use the schema explorer on the left to get started.</p>
    </div>
  );
}

function Shell({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex h-screen overflow-hidden bg-slate-900">
      <SchemaExplorer />
      <main className="flex-1 flex overflow-hidden">{children}</main>
    </div>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route
            path="/"
            element={
              <Shell>
                <RepoGraph />
              </Shell>
            }
          />
          <Route
            path="/tables"
            element={
              <Shell>
                <EmptyPanel />
              </Shell>
            }
          />
          <Route
            path="/tables/:tableKey"
            element={
              <Shell>
                <TableDetail />
              </Shell>
            }
          />
          <Route
            path="/tables/:tableKey/lineage"
            element={
              <Shell>
                <TableLineage />
              </Shell>
            }
          />
          <Route
            path="/columns/:columnKey"
            element={
              <Shell>
                <ColumnLineage />
              </Shell>
            }
          />
          <Route
            path="/columns/:columnKey/table"
            element={
              <Shell>
                <ColumnLineageTable />
              </Shell>
            }
          />
          <Route path="*" element={<Navigate to="/tables" replace />} />
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
