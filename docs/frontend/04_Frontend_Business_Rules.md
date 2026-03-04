# FE-04 — Frontend: Business Rules & Constraints

## 1. API Client Rules

| Rule                                      | Description                                                                                                                                            |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Always use `api` from `@/repo/client`** | Direct `axios` imports break base URL resolution. All HTTP calls must go through `import { api } from '@/repo/client'`.                                |
| **Base URL resolution**                   | `VITE_API_URL` env var takes priority. In dev mode, defaults to `http://localhost:8000/api/v1`. In production, uses `window.location.origin + '/api'`. |
| **No hardcoded API paths**                | All API paths must be defined in `src/repo/*.ts` files. Never hardcode `/api/v1/...` strings in components.                                            |

---

## 2. Mutation & Query Invalidation Rules

| Rule                                               | Description                                                                                                                                                                          |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **300ms delay mandatory**                          | All `onSuccess` handlers that call `queryClient.invalidateQueries()` must delay for 300ms. This is non-negotiable; without it, the refetch returns pre-commit data from the Backend. |
| **Invalidate parent + detail**                     | Mutations that change a resource must invalidate both the list query (`['pipelines']`) and the detail query (`['pipeline', id]`) to keep all views consistent.                       |
| **Optimistic updates only for non-critical state** | Avoid optimistic updates for pipeline status changes — the status machine is authoritative on the Backend; premature optimism misleads operators.                                    |

---

## 3. Pipeline Status Rules

| Rule                                               | Description                                                                                                                                                                                                                                                                                                |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Display status from `pipeline_metadata.status`** | The runtime status is in `pipeline_metadata.status` (`RUNNING` / `PAUSED` / `ERROR`), not `pipeline.status` (`START` / `PAUSE` / `REFRESH`). The difference: `status='START'` means Compute _should_ run it; `metadata.status='RUNNING'` means it _is_ running. Display the metadata status for operators. |
| **Cannot start without a destination**             | The "Start Pipeline" button must be disabled and show a tooltip if `pipeline.destinations?.length === 0`. Pipelines without destinations will fail immediately if started.                                                                                                                                 |
| **REFRESH state is transient**                     | `status='REFRESH'` is an intermediate state set by the "Restart" action and auto-transitions back to `START` once Compute restarts the process. The UI should show a loading indicator during REFRESH and not allow another restart while in this state.                                                   |

---

## 4. Rosetta Chain UI Rules

| Rule                                | Description                                                                                                                                                                                            |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **`chain_key` never shown in list** | `ChainClientResponse` does not include `chain_key`. The client list table must never attempt to display a key column.                                                                                  |
| **Edit form clears `chain_key`**    | When editing a chain client, the `chain_key` field is always empty on form open. An empty value means "do not update the key". Only a filled value triggers a key update.                              |
| **One-time key reveal**             | After regenerating the chain key, the raw key is shown in a dialog exactly once. The dialog copy button should be emphasized and the dialog should warn the user it cannot be retrieved again.         |
| **Masked key display**              | `GET /chain/key` returns `chain_key_masked` (e.g., `ab12****5678`). Display this in the `ChainKeyCard` — never display a "reveal" link that refetches the masked key; the reveal is a separate action. |

---

## 5. Form Validation Rules

| Rule                                                | Description                                                                                                                                                                                             |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Zod schemas are the source of truth**             | No manual validation logic in components — all validation is expressed in the Zod schema.                                                                                                               |
| **Server errors displayed at root level**           | API 4xx/5xx errors that are not field-specific (e.g., "name already in use" for a field Zod allows) must call `form.setError('root', { message: ... })` and render `<FormMessage />` outside the field. |
| **Required fields use `.min(1)` not `.nonempty()`** | Prefer `z.string().min(1, 'Required')` for string fields. This produces clearer error messages.                                                                                                         |
| **Credential fields**                               | Password and key fields must use `<PasswordInput>` (shows/hides toggle). Never use a plain `<Input type="text">` for credentials.                                                                       |

---

## 6. Data Table Rules

| Rule                                      | Description                                                                                                                                                                                       |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Use the generic `DataTable` component** | Never build custom inline `<table>` elements for resource lists. Always use `DataTable<TData, TValue>` from `@/components/data-table/`.                                                           |
| **Column defs in `data/` folder**         | Column definitions belong in `features/<feature>/data/columns.tsx`, not inside page components.                                                                                                   |
| **Row actions via `DropdownMenu`**        | Each table row's action menu must use a 3-dot `DropdownMenu`. Action items: Edit, Delete, and any status-changing actions. Destructive actions (Delete) must have `className="text-destructive"`. |
| **Pagination is required**                | Every resource table must use `DataTablePagination`. No infinite-scroll tables without explicit design decision.                                                                                  |

---

## 7. Error Presentation Rules

| Rule                         | Description                                                                                                                                                  |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Loading → `<Skeleton />`** | Use `<Skeleton>` components during loading states — never show empty content.                                                                                |
| **Error → `<ErrorCard />`**  | When a query errors, show an `ErrorCard` with the error message and a retry button.                                                                          |
| **Toast for mutations**      | Mutation success/failure feedback goes through `toast.success()` / `toast.error()` (Sonner). Toast duration is globally set to 5000ms in `__root.tsx`.       |
| **Route error boundary**     | Unhandled errors in route components are caught by the TanStack Router `errorComponent: GeneralError` boundary. Never suppress errors that should bubble up. |

---

## 8. Theme & Accessibility Rules

| Rule                                       | Description                                                                                                                         |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| **Dark mode supported**                    | The app supports dark mode via Tailwind `dark:` classes. All new components must include dark mode variants.                        |
| **No hardcoded colors**                    | Use Tailwind semantic tokens (`bg-background`, `text-foreground`, `border`) instead of hardcoded colors (`bg-white`, `text-black`). |
| **Interactive elements need `aria-label`** | Icon-only buttons must have `aria-label` or `<span className="sr-only">`.                                                           |
| **Tab order**                              | Form fields must have logical tab order. Avoid `tabIndex > 0`. Use natural DOM order.                                               |

---

## 9. Performance Rules

| Rule                                      | Description                                                                                                                                                                                |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Code splitting per route**              | TanStack Router code-splits routes automatically. Do not disable `autoCodeSplitting`.                                                                                                      |
| **Manual chunk splitting in Vite config** | Heavy libraries (TanStack, Recharts, XY Flow, Radix UI) are split into named chunks in `vite.config.ts` to prevent over-bundled initial load. Do not move these imports to inline bundles. |
| **Memoize expensive computations**        | Use `useMemo` for column definitions and derived data in large table components. Use `useCallback` for stable callback references passed to child components.                              |
| **`refetchIntervalInBackground: false`**  | Polling queries must not poll while the browser tab is hidden. This avoids unnecessary API load when the operator has the dashboard open in a background tab.                              |
