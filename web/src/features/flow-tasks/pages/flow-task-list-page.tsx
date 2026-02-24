import { useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { Button } from '@/components/ui/button'
import { Plus, GitBranch } from 'lucide-react'
import { flowTasksRepo } from '@/repo/flow-tasks'
import { FlowTasksProvider, useFlowTasks } from '../components/flow-tasks-provider'
import { FlowTasksTable } from '../components/flow-tasks-table'
import { FlowTasksDialogs } from '../components/flow-tasks-dialogs'

function FlowTasksPrimaryButtons() {
    const { setOpen } = useFlowTasks()
    return (
        <Button onClick={() => setOpen('create')}>
            <Plus className="h-4 w-4 mr-2" />
            New Flow Task
        </Button>
    )
}

export default function FlowTaskListPage() {
    useEffect(() => {
        document.title = 'Flow Tasks'
        return () => { document.title = 'Rosetta' }
    }, [])

    const { data } = useQuery({
        queryKey: ['flow-tasks'],
        queryFn: () => flowTasksRepo.list(1, 100),
        refetchInterval: 10_000,
        refetchOnWindowFocus: true,
    })

    const flowTasks = data?.data.items ?? []

    return (
        <FlowTasksProvider>
            <Header fixed>
                <Search />
                <div className="ms-auto flex items-center space-x-4">
                    <ThemeSwitch />
                </div>
            </Header>

            <Main className="flex flex-1 flex-col gap-4 sm:gap-6">
                <div className="flex flex-wrap items-end justify-between gap-2">
                    <div>
                        <div className="flex items-center gap-2">
                            <GitBranch className="h-5 w-5 text-muted-foreground" />
                            <h2 className="text-2xl font-bold tracking-tight">Flow Tasks</h2>
                        </div>
                        <p className="text-muted-foreground mt-1">
                            Visual ETL transformation flows powered by DuckDB.
                        </p>
                    </div>
                    <FlowTasksPrimaryButtons />
                </div>

                <FlowTasksTable data={flowTasks} />
            </Main>
            <FlowTasksDialogs />
        </FlowTasksProvider>
    )
}
