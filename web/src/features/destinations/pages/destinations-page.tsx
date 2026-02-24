import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { DestinationsDialogs } from '../components/destinations-dialogs'
import { DestinationsPrimaryButtons } from '../components/destinations-primary-buttons'
import { DestinationsProvider } from '../components/destinations-provider'
import { DestinationsTable } from '../components/destinations-table'
import { destinationsRepo } from '@/repo/destinations'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useState } from 'react'

export function DestinationsPage() {
    const queryClient = useQueryClient()
    const [lastRefreshedAt, setLastRefreshedAt] = useState<Date | null>(null)

    const { data } = useQuery({
        queryKey: ['destinations'],
        queryFn: destinationsRepo.getAll,
        refetchInterval: 10_000,
    })

    const refreshAllMutation = useMutation({
        mutationFn: async () => {
            const destinations = data?.destinations ?? []
            await Promise.allSettled(
                destinations.map((d) => destinationsRepo.refreshTableList(d.id))
            )
        },
        onSuccess: async () => {
            setLastRefreshedAt(new Date())
            // Wait 300ms for DB commit then refresh queries
            await new Promise((r) => setTimeout(r, 300))
            await queryClient.refetchQueries({ queryKey: ['destinations'] })
            // Also invalidate destination-tables so Flow Task input nodes pick up fresh data
            queryClient.invalidateQueries({ queryKey: ['destination-tables'] })
        },
    })

    useEffect(() => {
        document.title = 'Destinations'
        return () => {
            document.title = 'Rosetta'
        }
    }, [])

    const destinations = (data?.destinations ?? []).map((d) => ({
        ...d,
        total_tables: d.total_tables ?? 0,
    }))

    return (
        <DestinationsProvider>
            <Header fixed>
                <Search />
                <div className='ms-auto flex items-center space-x-4'>
                    <ThemeSwitch />
                    
                </div>
            </Header>

            <Main className='flex flex-1 flex-col gap-4 sm:gap-6'>
                <div className='flex flex-wrap items-end justify-between gap-2'>
                    <div>
                        <h2 className='text-2xl font-bold tracking-tight'>Destinations</h2>
                        <p className='text-muted-foreground'>
                            Manage your Snowflake data destinations.
                        </p>
                    </div>
                    <DestinationsPrimaryButtons
                        onRefreshAll={() => refreshAllMutation.mutate()}
                        isRefreshing={refreshAllMutation.isPending}
                        lastRefreshedAt={lastRefreshedAt}
                    />
                </div>
                <DestinationsTable data={destinations} />
            </Main>

            <DestinationsDialogs />
        </DestinationsProvider>
    )
}
