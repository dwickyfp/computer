# FE-03 — Frontend: Coding Standards & Style

## 1. File & Folder Naming

| Construct          | Convention                 | Example                                   |
| ------------------ | -------------------------- | ----------------------------------------- |
| React components   | `kebab-case.tsx`           | `pipeline-status-switch.tsx`              |
| Pages              | `<name>-page.tsx`          | `pipeline-details-page.tsx`               |
| Hooks              | `use-<name>.ts`            | `use-pipeline-selection.ts`               |
| Repo files         | `<resource>.ts`            | `pipelines.ts`, `sources.ts`              |
| Zod schemas        | `<name>-schema.ts`         | `pipeline-schema.ts`                      |
| Column definitions | `columns.tsx`              | always named `columns.tsx` inside `data/` |
| Types / interfaces | `<name>.types.ts`          | `pipeline.types.ts` (or colocated)        |
| Route files        | TanStack Router convention | `_authenticated.pipelines.$id.tsx`        |

---

## 2. Component Conventions

### Anatomy of a Page Component

```tsx
// pipeline-details-page.tsx

// 1. Imports: react, tanstack, repo, components (in this order)
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useParams } from '@tanstack/react-router'
import { pipelinesRepo } from '@/repo/pipelines'
import { Button } from '@/components/ui/button'
import { Header } from '@/components/layout/header'

// 2. Types / interfaces colocated with component if small
interface TabState { ... }

// 3. Component function — named export
export function PipelineDetailsPage() {
  // a. Route params / search
  const { id } = useParams({ from: '/_authenticated/pipelines/$id' })

  // b. Queries
  const { data: pipeline, isLoading } = useQuery({ ... })

  // c. Mutations
  const queryClient = useQueryClient()
  const startMutation = useMutation({ ... })

  // d. Local state
  const [activeTab, setActiveTab] = useState<string>('overview')

  // e. Derived values / computed
  const isRunning = pipeline?.status === 'START'

  // f. Render
  if (isLoading) return <LoadingSkeleton />
  return (
    <>
      <Header />
      <Main>{/* ... */}</Main>
    </>
  )
}
```

### Small / Reusable Components

```tsx
// Prefer named exports over default exports for components
export function PipelineStatusBadge({ status }: { status: PipelineStatus }) {
  const variant = status === "START" ? "success" : "secondary";
  return <Badge variant={variant}>{status}</Badge>;
}
```

---

## 3. API Call Conventions

```typescript
// CORRECT — always use the `api` instance from repo/client
import { api } from '@/repo/client'
const response = await api.get<Pipeline[]>('/pipelines')

// WRONG — never import axios directly in components or repo files
import axios from 'axios'
const response = await axios.get(...)  // ❌ breaks base URL resolution
```

All HTTP calls are centralised in `src/repo/*.ts` files. Components never call `api` directly — they call repo functions imported from `@/repo/<resource>`.

---

## 4. React Query Conventions

```typescript
// Query key conventions:
// ─ list:   ['<resource>']                e.g. ['pipelines']
// ─ detail: ['<resource>', id]            e.g. ['pipeline', 42]
// ─ nested: ['<resource>', id, '<sub>']   e.g. ['pipeline', 42, 'table-syncs']

// staleTime on actively-polled queries: 0
// staleTime on static config queries: 30_000 ms (30 s)

// ALWAYS add 300ms delay before invalidateQueries on mutations:
onSuccess: () => {
  setTimeout(() => {
    queryClient.invalidateQueries({ queryKey: ["pipelines"] });
  }, 300);
};
```

---

## 5. Form Conventions

```typescript
// Always pair Zod with react-hook-form:
import { z } from "zod";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";

const schema = z.object({
  name: z.string().min(1, "Name is required").max(255),
  source_id: z.number({ required_error: "Source is required" }),
});

const form = useForm<z.infer<typeof schema>>({
  resolver: zodResolver(schema),
});
```

- Never use uncontrolled inputs — always register with `form.register()` or Controller.
- Display server-side errors using `form.setError('root', { message: error })`.
- Reset forms on drawer/modal close: `form.reset()` in `onOpenChange`.

---

## 6. TypeScript Conventions

| Rule                                                      | Example                                                 |
| --------------------------------------------------------- | ------------------------------------------------------- |
| Prefer `interface` for object shapes that may be extended | `interface Pipeline { ... }`                            |
| Use `type` for unions, intersections, and aliases         | `type PipelineStatus = 'START' \| 'PAUSE' \| 'REFRESH'` |
| No `any` — use `unknown` + type guards                    | `const data: unknown = response.data`                   |
| Explicit return types on exported functions               | `function getStatus(): PipelineStatus { ... }`          |
| Generic DataTable props always typed                      | `DataTable<Pipeline, PipelineColumn>`                   |
| Optional chaining liberally for API response fields       | `pipeline?.metadata?.last_error`                        |

---

## 7. Styling / Tailwind Conventions

```tsx
// Use the `cn()` utility for conditional class merging:
import { cn } from '@/lib/utils'

<div className={cn(
  'base-classes',
  isActive && 'active-classes',
  variant === 'danger' && 'text-red-500',
)}>

// Prefer Tailwind utility classes over inline styles
// Use shadcn/ui variants for standard UI states (primary, secondary, destructive)
// Dark mode: use `dark:` prefix — ThemeSwitch sets `dark` class on <html>
```

---

## 8. Route File Conventions

```tsx
// src/routes/_authenticated/pipelines.$id.tsx

import { createFileRoute } from "@tanstack/react-router";
import { PipelineDetailsPage } from "@/features/pipelines/pages/pipeline-details-page";

// Route definition — keep thin, just wire params and component
export const Route = createFileRoute("/_authenticated/pipelines/$id")({
  component: PipelineDetailsPage,
  // Optional: loader for prefetching
  loader: ({ context: { queryClient }, params }) =>
    queryClient.ensureQueryData({
      queryKey: ["pipeline", Number(params.id)],
      queryFn: () => pipelinesRepo.get(Number(params.id)).then((r) => r.data),
    }),
});
```

- Route files are **thin wrappers** — no business logic or JSX markup.
- All UI lives in `features/<feature>/pages/`.

---

## 9. Linting & Formatting

| Tool       | Config              | Purpose                              |
| ---------- | ------------------- | ------------------------------------ |
| ESLint     | `eslint.config.js`  | TypeScript + React rules             |
| Prettier   | (via ESLint plugin) | Consistent formatting                |
| TypeScript | `tsconfig.app.json` | Strict mode enabled                  |
| knip       | `knip.config.ts`    | Dead code detection (unused exports) |

```bash
# Lint
pnpm lint

# Format
pnpm format

# Type check
pnpm tsc --noEmit
```
