import { createFileRoute, Outlet } from '@tanstack/react-router'

export const Route = createFileRoute('/_authenticated/flow-tasks')({
  component: () => <Outlet />,
})
