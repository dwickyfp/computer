import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { SourcesDialogs } from './components/sources-dialogs'
import { SourcesPrimaryButtons } from './components/sources-primary-buttons'
import { SourcesProvider } from './components/sources-provider'
import { SourcesTable } from './components/sources-table'
import { sourcesRepo } from '@/repo/sources'
import { useQuery } from '@tanstack/react-query'
import { useEffect } from 'react'

export function Sources() {
    const { data } = useQuery({
        queryKey: ['sources'], // Consistent query key
        queryFn: sourcesRepo.getAll,
        refetchInterval: 5000,
    })

    useEffect(() => {
        document.title = 'Sources'
        return () => {
            document.title = 'Rosetta'
        }
    }, [])

    const sources = data?.sources || []

    return (
        <SourcesProvider>
            <Header fixed>
                <Search />
                <div className='ms-auto flex items-center space-x-4'>
                    <ThemeSwitch />
                    
                </div>
            </Header>

            <Main className='flex flex-1 flex-col gap-4 sm:gap-6'>
                <div className='flex flex-wrap items-end justify-between gap-2'>
                    <div>
                        <h2 className='text-2xl font-bold tracking-tight'>Sources</h2>
                        <p className='text-muted-foreground'>
                            Manage your PostgreSQL data sources.
                        </p>
                    </div>
                    <SourcesPrimaryButtons />
                </div>
                <SourcesTable data={sources} />
            </Main>

            <SourcesDialogs />
        </SourcesProvider>
    )
}
