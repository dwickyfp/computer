import { useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from '@tanstack/react-router'
import { schedulesRepo } from '@/repo/schedules'
import { Calendar, Plus } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { SchedulesDialogs } from './components/schedules-dialogs'
import { SchedulesProvider } from './components/schedules-provider'
import { SchedulesList } from './components/schedules-list'

function SchedulesPrimaryButton() {
  const navigate = useNavigate()
  return (
    <Button
      onClick={() =>
        navigate({
          to: '/schedules/$scheduleId',
          params: { scheduleId: 'new' },
        })
      }
    >
      <Plus className='mr-2 h-4 w-4' />
      Add Schedule
    </Button>
  )
}

export function Schedules() {
  useEffect(() => {
    document.title = 'Schedules'
    return () => {
      document.title = 'Rosetta'
    }
  }, [])

  const { data } = useQuery({
    queryKey: ['schedules'],
    queryFn: () => schedulesRepo.getAll(),
    refetchInterval: 15_000,
    refetchOnWindowFocus: true,
  })

  const schedules = data ?? []

  return (
    <SchedulesProvider>
      <Header fixed>
        <Search />
        <div className='ms-auto flex items-center space-x-4'>
          <ThemeSwitch />
        </div>
      </Header>

      <Main className='flex flex-1 flex-col gap-4 sm:gap-6'>
        <div className='flex flex-wrap items-end justify-between gap-2'>
          <div>
            <div className='flex items-center gap-2'>
              <Calendar className='h-5 w-5 text-muted-foreground' />
              <h2 className='text-2xl font-bold tracking-tight'>Schedules</h2>
            </div>
            <p className='mt-1 text-muted-foreground'>
              Automate flow tasks and linked tasks with cron-based schedules.
            </p>
          </div>
          <SchedulesPrimaryButton />
        </div>

        <SchedulesList data={schedules} />
      </Main>

      <SchedulesDialogs />
    </SchedulesProvider>
  )
}
