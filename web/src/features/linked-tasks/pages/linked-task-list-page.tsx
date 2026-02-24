import { useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { Button } from '@/components/ui/button'
import { Plus, Link2 } from 'lucide-react'
import { linkedTasksRepo } from '@/repo/linked-tasks'
import { LinkedTasksProvider, useLinkedTasks } from '../components/linked-tasks-provider'
import { LinkedTasksTable } from '../components/linked-tasks-table'
import { LinkedTasksDialogs } from '../components/linked-tasks-dialogs'

function LinkedTasksPrimaryButtons() {
    const { setOpen } = useLinkedTasks()
    return (
        <Button onClick={() => setOpen('create')}>
            <Plus className="h-4 w-4 mr-2" />
            New Linked Task
        </Button>
    )
}

export default function LinkedTaskListPage() {
    useEffect(() => {
        document.title = 'Linked Tasks'
        return () => { document.title = 'Rosetta' }
    }, [])

    const { data } = useQuery({
        queryKey: ['linked-tasks'],
        queryFn: () => linkedTasksRepo.list(1, 100),
        refetchInterval: 10_000,
        refetchOnWindowFocus: true,
    })

    const linkedTasks = (data?.data as any)?.items ?? []

    return (
        <LinkedTasksProvider>
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
                            <Link2 className="h-5 w-5 text-muted-foreground" />
                            <h2 className="text-2xl font-bold tracking-tight">Linked Tasks</h2>
                        </div>
                        <p className="text-muted-foreground mt-1">
                            Orchestrate multiple flow tasks in sequential and parallel patterns.
                        </p>
                    </div>
                    <LinkedTasksPrimaryButtons />
                </div>

                <LinkedTasksTable data={linkedTasks} />
            </Main>
            <LinkedTasksDialogs />
        </LinkedTasksProvider>
    )
}
