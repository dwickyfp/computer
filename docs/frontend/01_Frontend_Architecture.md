# FE-01 — Frontend: Architecture

## 1. Overview

The Web frontend is a **React 19 Single-Page Application** (SPA) that serves as the admin dashboard for the Rosetta ETL platform. It is built with Vite and TypeScript and uses the full **TanStack** ecosystem for routing, server-state management, and data tables.

Users interact with the frontend to configure sources, destinations, and pipelines; monitor live replication health; preview source data with custom SQL; manage backfill jobs; define flow tasks and schedules; and administer Rosetta Chain inter-instance connections.

---

## 2. Technology Stack

| Layer           | Library / Tool                    | Version |
| --------------- | --------------------------------- | ------- |
| Core framework  | React                             | 19      |
| Language        | TypeScript                        | 5.x     |
| Build tool      | Vite + `@vitejs/plugin-react-swc` | 5.x     |
| Routing         | TanStack Router                   | latest  |
| Server state    | TanStack Query (React Query)      | v5      |
| Tables          | TanStack Table                    | v8      |
| UI components   | shadcn/ui + Radix UI primitives   | —       |
| Styling         | Tailwind CSS v4                   | latest  |
| Form management | react-hook-form + Zod             | —       |
| HTTP client     | fetch-based `api` client (`@/repo/client.ts`) | —       |
| Charts          | Recharts                          | —       |
| Flow diagrams   | @xyflow/react                     | —       |
| Notifications   | Sonner                            | —       |
| Icons           | Lucide React                      | —       |

---

## 3. High-Level Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Browser                                                       │
│                                                                │
│  TanStack Router                                               │
│     ├─ Route tree (auto-generated: routeTree.gen.ts)          │
│     ├─ Code splitting (autoCodeSplitting: true)               │
│     └─ Route-level error boundaries                           │
│                                                                │
│  TanStack Query (QueryClientProvider)                          │
│     ├─ Server state cache (dedupe, refetch, stale-while-reuse)│
│     └─ Mutations with 300ms invalidation delay                │
│                                                                │
│  Feature Modules (src/features/<feature>/)                    │
│     ├─ pages/         ← route components                      │
│     ├─ components/    ← feature-scoped UI components          │
│     ├─ data/          ← Zod schemas + table column configs    │
│     └─ context/       ← feature-scoped React context          │
│                                                                │
│  Shared UI (src/components/)                                   │
│     ├─ ui/            ← shadcn/ui primitives                  │
│     ├─ layout/        ← Header, Main, Sidebar                 │
│     └─ data-table/    ← Generic DataTable                     │
│                                                                │
│  API Layer (src/repo/)                                         │
│     └─ 19 repo files + client.ts                              │
└──────────────────────────────────────────────────────────────┘
         │  fetch  │
         ▼         ▼
   Backend API  :8000/api/v1
```

---

## 4. Folder Structure

```
web/src/
├── main.tsx              # App entrypoint (QueryClient + Router provider)
├── routeTree.gen.ts      # Auto-generated route tree (DO NOT EDIT MANUALLY)
├── assets/               # Static assets (images, fonts)
├── components/
│   ├── ui/               # shadcn/ui components (Button, Card, Dialog, …)
│   ├── layout/           # Header, Main, Sidebar, AppShell
│   ├── data-table/       # Generic DataTable<TData, TValue>
│   ├── command-menu.tsx  # Global keyboard command palette
│   ├── confirm-dialog.tsx
│   └── theme-switch.tsx
├── features/
│   ├── dashboard/        # KPI cards, pipeline health summary
│   ├── pipelines/        # 27+ components — largest feature
│   ├── sources/          # Source CRUD
│   ├── destinations/     # Destination CRUD
│   ├── schedules/        # Pipeline scheduling
│   ├── smart-tags/       # Tag management
│   ├── rosetta-chain/    # Chain key, clients, table sync
│   ├── flow-tasks/       # Flow task builder
│   ├── linked-tasks/     # Linked task config
│   ├── settings/         # Runtime configuration
│   └── errors/           # 404, 500, general error pages
├── repo/
│   ├── client.ts         # Fetch-based client with base URL resolution
│   ├── pipelines.ts      # Pipeline API calls + TypeScript interfaces
│   ├── sources.ts
│   ├── destinations.ts
│   └── … (19 total)
├── hooks/                # Custom React hooks (useDebounce, useMediaQuery, …)
├── lib/                  # Utility functions (cn(), formatDate(), …)
├── stores/               # Zustand stores (theme, sidebar state)
├── context/              # App-level React contexts
├── routes/               # File-based route definitions
│   ├── __root.tsx        # Root layout (Toaster, NavigationProgress)
│   ├── _authenticated/   # Auth-gated routes
│   └── (auth)/           # Login routes
└── styles/               # Global CSS (Tailwind config)
```

---

## 5. Feature Module Anatomy

Each feature follows a standard structure:

```
features/<feature>/
├── pages/           # Full-page route components (imported by routes/)
├── components/      # Feature-specific components (forms, tables, cards)
├── data/            # Zod validation schemas + TanStack Table column defs
└── context/         # Feature-scoped React Contexts (if needed)
```

Example — `features/pipelines/`:

```
pages/
  pipeline-details-page.tsx     ← /pipelines/:id
  pipeline-flow-page.tsx        ← /pipelines/:id/flow
  table-sync-details-page.tsx   ← /pipelines/:id/table-sync/:tableId
components/
  pipeline-status-switch.tsx    ← Start/Pause toggle
  pipeline-data-flow.tsx        ← Animated source→destination diagram
  lineage-flow-diagram.tsx      ← XY Flow lineage graph
  backfill-data-tab.tsx         ← Backfill job management
  restart-button.tsx
  … (27+ components)
context/
  pipeline-selection-context.tsx
```

---

## 6. Routing

TanStack Router uses **file-based routing**. Route files live in `src/routes/`. The route tree is auto-generated into `src/routeTree.gen.ts` by the Vite plugin on every save.

Key routes:

| Path                            | Feature         |
| ------------------------------- | --------------- |
| `/`                             | Dashboard       |
| `/_authenticated/sources`       | Sources list    |
| `/_authenticated/destinations`  | Destinations    |
| `/_authenticated/pipelines`     | Pipelines list  |
| `/_authenticated/pipelines/$id` | Pipeline detail |
| `/_authenticated/rosetta-chain` | Rosetta Chain   |
| `/_authenticated/schedules`     | Schedules       |
| `/_authenticated/settings`      | Settings        |

`_authenticated/` is a layout route that handles auth-gating.

---

## 7. API Layer

All repo HTTP calls must go through `src/repo/client.ts`:

```typescript
// client.ts — single fetch-based client
export const api = {
  get: <T>(path: string, options?: ApiRequestOptions) => request<T>('GET', path, undefined, options),
  post: <T>(path: string, body?: unknown, options?: ApiRequestOptions) => request<T>('POST', path, body, options),
}
```

Repo files wrap `api` calls with typed return values:

```typescript
// repo/pipelines.ts
export const pipelinesRepo = {
  getAll: async () => {
    const { data } = await api.get<Pipeline[]>('/pipelines')
    return { pipelines: data, total: data.length }
  },
  get: async (id: number) => {
    const { data } = await api.get<Pipeline>(`/pipelines/${id}`)
    return data
  },
};
```

**Rule:** Components never import `axios` or `@/repo/client`. Only repo files talk to the shared client.

---

## 8. State Management Strategy

| State Type       | Solution              | Example                                       |
| ---------------- | --------------------- | --------------------------------------------- |
| Server state     | TanStack Query        | Pipeline list, source configs, health metrics |
| Form state       | react-hook-form + Zod | Create/Edit forms with validation             |
| UI / local state | React `useState`      | Dialog open/close, tab selection              |
| Global UI state  | Zustand               | Theme (dark/light), sidebar collapsed         |
| Route state      | TanStack Router       | Current pipeline ID, query params             |
