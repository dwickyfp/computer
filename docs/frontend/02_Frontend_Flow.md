# FE-02 — Frontend: Application Flow

## 1. Application Bootstrap

```
index.html
  └─ <script type="module" src="/src/main.tsx">
        │
        main.tsx
          ├─ QueryClient = new QueryClient({
          │     defaultOptions: {
          │       queries: { staleTime: 30_000, retry: 1 },
          │     }
          │   })
          ├─ router = createRouter({ routeTree, context: { queryClient } })
          │
          └─ ReactDOM.createRoot().render(
               <QueryClientProvider client={queryClient}>
                 <RouterProvider router={router} />
               </QueryClientProvider>
             )
```

---

## 2. Route Resolution Flow

```
Browser navigates to /_authenticated/pipelines/42
    │
    TanStack Router
      ├─ Match route: /_authenticated → PipelinesLayout (sidebar + header)
      │     ├─ Auth check (redirects to /login if unauthenticated)
      │     └─ Render <Outlet />
      │
      └─ Match child route: /pipelines/$id → PipelineDetailsPage
            │  router.params.id = "42"
            │
            PipelineDetailsPage mounts
              ├─ useQuery({ queryKey: ['pipeline', 42], queryFn: () => pipelinesRepo.get(42) })
              ├─ useQuery({ queryKey: ['sources'], queryFn: sourcesRepo.list })
              └─ render layout when data resolves
```

Route components are **code-split automatically** by the TanStack Router Vite plugin (`autoCodeSplitting: true`), so each route's bundle is lazy-loaded on first navigation.

---

## 3. Data Fetching Pattern

```typescript
// Standard query (read)
const { data, isLoading, error } = useQuery({
  queryKey: ["pipelines"],
  queryFn: () => pipelinesRepo.list().then((r) => r.data),
  staleTime: 30_000,
});

// Mutation (write) with 300ms invalidation delay
const mutation = useMutation({
  mutationFn: () => pipelinesRepo.start(pipelineId),
  onSuccess: () => {
    setTimeout(() => {
      queryClient.invalidateQueries({ queryKey: ["pipelines"] });
      queryClient.invalidateQueries({ queryKey: ["pipeline", pipelineId] });
    }, 300); // ← REQUIRED: allow DB commit to propagate before refetch
  },
  onError: (err) => toast.error("Failed to start pipeline"),
});
```

**The 300ms delay is mandatory** on all mutations that modify server state. Without it, the immediate refetch may return pre-commit data from the Backend, causing the UI to briefly show stale state.

---

## 4. Form Submission Flow

```typescript
// 1. Define schema with Zod
const createPipelineSchema = z.object({
  name: z.string().min(1).max(255),
  source_id: z.number().positive(),
});

// 2. Connect react-hook-form
const form = useForm<z.infer<typeof createPipelineSchema>>({
  resolver: zodResolver(createPipelineSchema),
  defaultValues: { name: "", source_id: undefined },
});

// 3. Submit handler calls mutation
const onSubmit = form.handleSubmit((data) => {
  createMutation.mutate(data);
});
```

Validation errors from Zod surface inline via `form.formState.errors`. Server errors (non-2xx) are caught in `onError` and displayed via `toast.error()`.

---

## 5. Pipeline Details Page Flow

The most complex page in the application:

```
PipelineDetailsPage mounts
  │
  ├─ Parallel queries:
  │     useQuery(['pipeline', id])         → pipeline config + metadata
  │     useQuery(['sources'])              → all sources list
  │
  ├─ Render top bar:
  │     PipelineStatusSwitch              → START / PAUSE toggle
  │     RestartButton                     → triggers refresh mutation
  │
  ├─ Custom tabs (PipelineTabs):
  │     Tab: Overview
  │       PipelineDataFlow               → animated flow diagram (XY Flow)
  │       LineageFlowDiagram             → SQL lineage graph
  │     Tab: Table Sync
  │       per-table sync config cards
  │       TableSyncDetails drawer
  │     Tab: Backfill
  │       BackfillDataTab                → backfill job queue table
  │     Tab: Flow
  │       PipelineFlowTab                → flow task steps
  │
  └─ Mutations:
       start/pause: PATCH + 300ms invalidate
       restart:     PATCH /refresh + 300ms invalidate
       add table:   POST  /table-sync + 300ms invalidate
```

---

## 6. Error Handling Flow

```
API call fails (axios rejects)
    │
    ├─ TanStack Query:
    │     retry once (retry: 1 in defaultOptions)
    │     if still failing → error state
    │
    ├─ Component renders error state:
    │     isLoading: <Skeleton />
    │     isError:   <ErrorCard message={error.message} />
    │
    └─ Mutation errors:
          onError: toast.error('Human-readable message')
          form.setError() for field-level errors (422 responses)

Route-level error boundary (errorComponent: GeneralError in router):
    Catches unhandled errors in route component tree
    Displays full-page error UI with "Try Again" button
```

---

## 7. Real-Time Status Updates

The Dashboard and Pipeline list use **polling** (not WebSockets) for live status:

```typescript
useQuery({
  queryKey: ["pipeline-health"],
  queryFn: () => dashboardRepo.getHealth(),
  refetchInterval: 5_000, // refetch every 5 s
  refetchIntervalInBackground: false, // pause when tab hidden
});
```

The `/ws/pipeline-status` WebSocket endpoint exists on the Backend but is consumed selectively for live streaming updates in high-frequency views.

---

## 8. Navigation & Layout Flow

```
AppShell
  ├─ Sidebar (collapsible, Zustand state)
  │     Navigation links (TanStack Router <Link>)
  │     Active route highlighted via useMatch()
  │
  ├─ Header
  │     Search command bar (cmd+K → CommandMenu)
  │     ThemeSwitch (dark/light, Zustand)
  │     ProfileDropdown
  │
  └─ Main  (scrollable content area)
        <Outlet /> → current route's page component
```

Breadcrumbs are rendered inside each page component using the `<Breadcrumb>` shadcn component with TanStack Router `<Link>`.

---

## 9. Table Data Flow

All data tables use the generic `DataTable<TData, TValue>` component:

```
repo call → useQuery → data: TData[]
    │
    │  column definitions (src/features/<feat>/data/columns.tsx)
    │
    ▼
DataTable<TData, TValue>
    ├─ useReactTable({ data, columns, getCoreRowModel, ... })
    ├─ DataTableToolbar  (global search, column filter)
    ├─ DataTableBody     (rows from table.getRowModel())
    └─ DataTablePagination (page size selector, prev/next)
```

Column definitions live in `features/<feature>/data/` and are kept separate from component logic for testability.
