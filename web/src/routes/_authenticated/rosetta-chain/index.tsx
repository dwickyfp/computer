import { createFileRoute, redirect } from '@tanstack/react-router'

export const Route = createFileRoute('/_authenticated/rosetta-chain/')({
  beforeLoad: () => {
    throw redirect({ to: '/pipelines' })
  },
})
