import { useNavigate } from '@tanstack/react-router'
import { type ScheduleListItem } from '@/repo/schedules'
import { MoreHorizontal, Pencil, Pause, Play, Trash2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { useSchedules } from './schedules-provider'

interface Props {
  schedule: ScheduleListItem
}

export function SchedulesRowActions({ schedule }: Props) {
  const { setOpen, setCurrentRow } = useSchedules()
  const navigate = useNavigate()

  function handleEdit() {
    navigate({
      to: '/schedules/$scheduleId',
      params: { scheduleId: String(schedule.id) },
    })
  }

  function handlePause() {
    setCurrentRow(schedule)
    setOpen('pause')
  }

  function handleResume() {
    setCurrentRow(schedule)
    setOpen('resume')
  }

  function handleDelete() {
    setCurrentRow(schedule)
    setOpen('delete')
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant='ghost' size='icon' className='h-8 w-8 p-0'>
          <span className='sr-only'>Open menu</span>
          <MoreHorizontal className='h-4 w-4' />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align='end' className='w-[160px]'>
        <DropdownMenuItem onClick={handleEdit}>
          <Pencil className='mr-2 h-4 w-4' />
          Edit
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        {schedule.status === 'ACTIVE' ? (
          <DropdownMenuItem onClick={handlePause}>
            <Pause className='mr-2 h-4 w-4' />
            Pause
          </DropdownMenuItem>
        ) : (
          <DropdownMenuItem onClick={handleResume}>
            <Play className='mr-2 h-4 w-4' />
            Resume
          </DropdownMenuItem>
        )}
        <DropdownMenuSeparator />
        <DropdownMenuItem
          onClick={handleDelete}
          className='text-destructive focus:text-destructive'
        >
          <Trash2 className='mr-2 h-4 w-4' />
          Delete
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
