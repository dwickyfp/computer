import { createFileRoute } from '@tanstack/react-router'
import ScheduleDetailPage from '@/features/schedules/pages/schedule-detail-page'

export const Route = createFileRoute('/_authenticated/schedules/$scheduleId')({
  component: ScheduleDetailPage,
})
