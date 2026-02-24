import { useQuery } from '@tanstack/react-query'
import { pipelinesRepo } from '@/repo/pipelines'
import { pipelineColumns } from './pipeline-columns'
import { PipelinesTable } from './pipeline-table'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { Button } from '@/components/ui/button'
import { Plus } from 'lucide-react'
import { useState, useEffect } from 'react'
import { PipelineCreateDrawer } from './pipeline-create-drawer.tsx'

export default function PipelineList() {
    const { data: pipelines } = useQuery({
        queryKey: ['pipelines'],
        queryFn: pipelinesRepo.getAll,
        refetchInterval: 5000, // Refetch every 5 seconds
    })

    useEffect(() => {
        document.title = 'Pipelines'
        return () => {
            document.title = 'Rosetta'
        }
    }, [])

    const [open, setOpen] = useState(false)

    // Remove blocking loading state to allow layout rendering
    // if (isLoading) return <div>Loading...</div>
    // if (error) return <div>Error loading pipelines</div>

    return (
        <>
            <Header>
                <Search />
                <div className='ml-auto flex items-center space-x-4'>
                    <ThemeSwitch />
                    
                </div>
            </Header>
            <Main>
                <div className='mb-2 flex items-center justify-between space-y-2'>
                    <div>
                        <h2 className='text-2xl font-bold tracking-tight'>Pipelines</h2>
                        <p className='text-muted-foreground'>
                            Manage your data pipelines.
                        </p>
                    </div>
                    <div className='flex items-center space-x-2'>
                        <Button onClick={() => setOpen(true)}>
                            <Plus className='mr-2 h-4 w-4' /> New Pipeline
                        </Button>
                    </div>
                </div>
                <div className='-mx-4 flex-1 overflow-auto px-4 py-1 lg:flex-row lg:space-x-12 lg:space-y-0'>
                    <PipelinesTable data={pipelines?.pipelines || []} columns={pipelineColumns} />
                </div>
            </Main>

            <PipelineCreateDrawer open={open} setOpen={setOpen} />
        </>
    )
}
