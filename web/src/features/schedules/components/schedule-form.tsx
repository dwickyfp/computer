import { useEffect, useState } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useQuery } from '@tanstack/react-query'
import { flowTasksRepo } from '@/repo/flow-tasks'
import { linkedTasksRepo } from '@/repo/linked-tasks'
import {
    type ScheduleListItem,
} from '@/repo/schedules'
import cronstrue from 'cronstrue'
import {
    Loader2,
    Save,
    Clock,
    Calendar,
    Info,
    Sliders
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import {
    Form,
    FormControl,
    FormDescription,
    FormField,
    FormItem,
    FormLabel,
    FormMessage,
} from '@/components/ui/form'
import { Input } from '@/components/ui/input'
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group'
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from '@/components/ui/select'
import { Textarea } from '@/components/ui/textarea'
import { Separator } from '@/components/ui/separator'
import { scheduleFormSchema, type ScheduleFormValues } from '../data/schema'
import { Switch } from '@/components/ui/switch'

// ─── Cron presets ─────────────────────────────────────────────────────────────

const CRON_PRESETS = [
    { label: 'Every 5m', value: '*/5 * * * *', desc: 'Run every 5 minutes' },
    { label: 'Hourly', value: '0 * * * *', desc: 'At the start of every hour' },
    { label: 'Daily', value: 'DAILY', desc: 'Run once a day' },
    { label: 'Weekly', value: 'WEEKLY', desc: 'Run once a week' },
    { label: 'Monthly', value: 'MONTHLY', desc: 'Run once a month' },
]

const HOURS = Array.from({ length: 24 }, (_, i) => ({
    label: `${String(i).padStart(2, '0')}:00`,
    value: String(i),
}))
const DAYS_OF_MONTH = Array.from({ length: 31 }, (_, i) => ({
    label: String(i + 1),
    value: String(i + 1),
}))
const DAYS_OF_WEEK = [
    { label: 'Sunday', value: '0' },
    { label: 'Monday', value: '1' },
    { label: 'Tuesday', value: '2' },
    { label: 'Wednesday', value: '3' },
    { label: 'Thursday', value: '4' },
    { label: 'Friday', value: '5' },
    { label: 'Saturday', value: '6' },
]

interface ScheduleFormProps {
    schedule?: ScheduleListItem | null
    onSubmit: (values: ScheduleFormValues) => void
    isSubmitting: boolean
}

export function ScheduleForm({ schedule, onSubmit, isSubmitting }: ScheduleFormProps) {
    const [cronMode, setCronMode] = useState<'preset' | 'manual'>('preset')
    const [presetType, setPresetType] = useState<
        'simple' | 'DAILY' | 'WEEKLY' | 'MONTHLY'
    >('simple')
    const [hour, setHour] = useState('0')
    const [dayOfMonth, setDayOfMonth] = useState('1')
    const [dayOfWeek, setDayOfWeek] = useState('1')

    const { data: flowTasksData } = useQuery({
        queryKey: ['flow-tasks'],
        queryFn: () => flowTasksRepo.list(1, 200),
    })

    const { data: linkedTasksData } = useQuery({
        queryKey: ['linked-tasks'],
        queryFn: () => linkedTasksRepo.list(1, 100),
    })

    const form = useForm<ScheduleFormValues>({
        resolver: zodResolver(scheduleFormSchema),
        defaultValues: {
            name: '',
            description: null,
            task_type: 'FLOW_TASK',
            task_id: 0,
            cron_expression: '*/5 * * * *',
            status: 'ACTIVE',
        },
    })

    useEffect(() => {
        if (schedule) {
            form.reset({
                name: schedule.name,
                description: schedule.description,
                task_type: schedule.task_type,
                task_id: schedule.task_id,
                cron_expression: schedule.cron_expression,
                status: schedule.status,
            })
            // Determine initial cron mode/preset if possible? 
            // For now default to preset if it matches one of simple ones, else manual.
            // Or just leave default.
        }
    }, [schedule, form])

    // ─── Cron builder logic ──────────────────────────────────────────────────

    function handlePresetClick(preset: string) {
        if (['DAILY', 'WEEKLY', 'MONTHLY'].includes(preset)) {
            setPresetType(preset as any)
        } else {
            setPresetType('simple')
            form.setValue('cron_expression', preset, { shouldValidate: true, shouldDirty: true })
        }
    }

    // Update cron expression when builder inputs change
    useEffect(() => {
        if (cronMode !== 'preset') return

        if (presetType === 'DAILY') {
            form.setValue('cron_expression', `0 ${hour} * * *`, { shouldValidate: true, shouldDirty: true })
        } else if (presetType === 'WEEKLY') {
            form.setValue('cron_expression', `0 ${hour} * * ${dayOfWeek}`, { shouldValidate: true, shouldDirty: true })
        } else if (presetType === 'MONTHLY') {
            form.setValue('cron_expression', `0 ${hour} ${dayOfMonth} * *`, { shouldValidate: true, shouldDirty: true })
        }
    }, [presetType, hour, dayOfMonth, dayOfWeek, form, cronMode])

    function getCronHuman(expr: string): string {
        try {
            return cronstrue.toString(expr, { verbose: false })
        } catch {
            return 'Invalid cron expression'
        }
    }

    const flowTasks = flowTasksData?.data.items ?? []
    const linkedTasks = linkedTasksData?.data.items ?? []
    const taskType = form.watch('task_type')
    const availableTasks = taskType === 'FLOW_TASK' ? flowTasks : linkedTasks

    return (
        <Form {...form}>
            <form onSubmit={form.handleSubmit(onSubmit)} className='space-y-8'>

                {/* General Information */}
                <div className="grid gap-6">
                    <div className="flex items-center gap-2">
                        <div className="flex h-8 w-8 items-center justify-center rounded-md bg-primary/10 text-primary">
                            <Info className="h-4 w-4" />
                        </div>
                        <h3 className="text-lg font-medium">General Information</h3>
                    </div>

                    <div className="grid gap-6 md:grid-cols-2">
                        <FormField
                            control={form.control}
                            name='name'
                            render={({ field }) => (
                                <FormItem>
                                    <FormLabel>Name</FormLabel>
                                    <FormControl>
                                        <Input {...field} placeholder='e.g. daily-sync-payment' className="font-medium" />
                                    </FormControl>
                                    <FormDescription>Unique identifier for this schedule.</FormDescription>
                                    <FormMessage />
                                </FormItem>
                            )}
                        />

                        <FormField
                            control={form.control}
                            name='status'
                            render={({ field }) => (
                                <FormItem className="flex flex-row items-center justify-between rounded-lg border p-3 shadow-sm">
                                    <div className="space-y-0.5">
                                        <FormLabel>Active Status</FormLabel>
                                        <FormDescription>
                                            Enable or disable this schedule.
                                        </FormDescription>
                                    </div>
                                    <FormControl>
                                        <Switch
                                            checked={field.value === 'ACTIVE'}
                                            onCheckedChange={(checked) =>
                                                field.onChange(checked ? 'ACTIVE' : 'PAUSED')
                                            }
                                        />
                                    </FormControl>
                                </FormItem>
                            )}
                        />

                        <FormField
                            control={form.control}
                            name='description'
                            render={({ field }) => (
                                <FormItem className="md:col-span-2">
                                    <FormLabel>Description</FormLabel>
                                    <FormControl>
                                        <Textarea
                                            {...field}
                                            value={field.value ?? ''}
                                            placeholder='Optional description about what this schedule does...'
                                            className="resize-none"
                                            rows={3}
                                        />
                                    </FormControl>
                                    <FormMessage />
                                </FormItem>
                            )}
                        />
                    </div>
                </div>

                <Separator />

                {/* Task Configuration */}
                <div className="grid gap-6">
                    <div className="flex items-center gap-2">
                        <div className="flex h-8 w-8 items-center justify-center rounded-md bg-primary/10 text-primary">
                            <Sliders className="h-4 w-4" />
                        </div>
                        <h3 className="text-lg font-medium">Task Configuration</h3>
                    </div>

                    <div className='grid gap-6 md:grid-cols-2'>
                        <FormField
                            control={form.control}
                            name='task_type'
                            render={({ field }) => (
                                <FormItem>
                                    <FormLabel>Task Type</FormLabel>
                                    <Select
                                        onValueChange={(v) => {
                                            field.onChange(v)
                                            form.setValue('task_id', 0)
                                        }}
                                        value={field.value}
                                    >
                                        <FormControl>
                                            <SelectTrigger>
                                                <SelectValue />
                                            </SelectTrigger>
                                        </FormControl>
                                        <SelectContent>
                                            <SelectItem value='FLOW_TASK'>Flow Task</SelectItem>
                                            <SelectItem value='LINKED_TASK'>Linked Task</SelectItem>
                                        </SelectContent>
                                    </Select>
                                    <FormDescription>The type of task to execute.</FormDescription>
                                    <FormMessage />
                                </FormItem>
                            )}
                        />

                        <FormField
                            control={form.control}
                            name='task_id'
                            render={({ field }) => (
                                <FormItem>
                                    <FormLabel>Target Task</FormLabel>
                                    <Select
                                        onValueChange={(v) => field.onChange(Number(v))}
                                        value={field.value ? String(field.value) : ''}
                                    >
                                        <FormControl>
                                            <SelectTrigger>
                                                <SelectValue placeholder='Select a task…' />
                                            </SelectTrigger>
                                        </FormControl>
                                        <SelectContent>
                                            {availableTasks.map((t: any) => (
                                                <SelectItem key={t.id} value={String(t.id)}>
                                                    {t.name}
                                                </SelectItem>
                                            ))}
                                        </SelectContent>
                                    </Select>
                                    <FormDescription>Select the specific task to run.</FormDescription>
                                    <FormMessage />
                                </FormItem>
                            )}
                        />
                    </div>
                </div>

                <Separator />

                {/* Schedule Configuration */}
                <div className="grid gap-6">
                    <div className="flex items-center gap-2">
                        <div className="flex h-8 w-8 items-center justify-center rounded-md bg-primary/10 text-primary">
                            <Calendar className="h-4 w-4" />
                        </div>
                        <h3 className="text-lg font-medium">Schedule (Cron)</h3>
                    </div>

                    <div className="rounded-lg border bg-card text-card-foreground shadow-sm p-6 space-y-6">
                        <RadioGroup value={cronMode} onValueChange={(v: any) => setCronMode(v)} className='flex flex-wrap gap-6'>
                            <div className='flex items-start gap-2'>
                                <RadioGroupItem value='preset' id='preset' className="mt-1" />
                                <div className="grid gap-1.5 leading-none">
                                    <label htmlFor='preset' className='text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70 cursor-pointer'>
                                        Preset
                                    </label>
                                    <p className="text-sm text-muted-foreground">Choose from common schedules.</p>
                                </div>
                            </div>
                            <div className='flex items-start gap-2'>
                                <RadioGroupItem value='manual' id='manual' className="mt-1" />
                                <div className="grid gap-1.5 leading-none">
                                    <label htmlFor='manual' className='text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70 cursor-pointer'>
                                        Manual
                                    </label>
                                    <p className="text-sm text-muted-foreground">Enter a custom cron expression.</p>
                                </div>
                            </div>
                        </RadioGroup>

                        {cronMode === 'preset' && (
                            <div className='space-y-4'>
                                <div className='grid grid-cols-2 gap-3 sm:grid-cols-3 md:grid-cols-5'>
                                    {CRON_PRESETS.map((p) => {
                                        const isActive = form.watch('cron_expression') === p.value || presetType === p.value;
                                        return (
                                            <div
                                                key={p.value}
                                                onClick={() => handlePresetClick(p.value)}
                                                className={cn(
                                                    "cursor-pointer rounded-md border p-3 flex flex-col gap-1 transition-all hover:bg-muted/50 hover:border-primary/50",
                                                    isActive ? "border-primary bg-primary/5 ring-1 ring-primary" : "bg-background"
                                                )}
                                            >
                                                <span className="font-medium text-sm">{p.label}</span>
                                                <span className="text-[10px] text-muted-foreground leading-tight">{p.desc}</span>
                                            </div>
                                        )
                                    })}
                                </div>

                                {presetType === 'DAILY' && (
                                    <div className='flex items-center gap-3 animate-in fade-in slide-in-from-top-2'>
                                        <span className='text-sm font-medium'>Run daily at:</span>
                                        <Select value={hour} onValueChange={setHour}>
                                            <SelectTrigger className='w-[100px]'><SelectValue /></SelectTrigger>
                                            <SelectContent>
                                                {HOURS.map((h) => <SelectItem key={h.value} value={h.value}>{h.label}</SelectItem>)}
                                            </SelectContent>
                                        </Select>
                                    </div>
                                )}

                                {presetType === 'WEEKLY' && (
                                    <div className='flex flex-wrap items-center gap-4 animate-in fade-in slide-in-from-top-2'>
                                        <div className='flex items-center gap-2'>
                                            <span className='text-sm font-medium'>On day:</span>
                                            <Select value={dayOfWeek} onValueChange={setDayOfWeek}>
                                                <SelectTrigger className='w-[140px]'><SelectValue /></SelectTrigger>
                                                <SelectContent>{DAYS_OF_WEEK.map((d) => <SelectItem key={d.value} value={d.value}>{d.label}</SelectItem>)}</SelectContent>
                                            </Select>
                                        </div>
                                        <div className='flex items-center gap-2'>
                                            <span className='text-sm font-medium'>At time:</span>
                                            <Select value={hour} onValueChange={setHour}>
                                                <SelectTrigger className='w-[100px]'><SelectValue /></SelectTrigger>
                                                <SelectContent>{HOURS.map((h) => <SelectItem key={h.value} value={h.value}>{h.label}</SelectItem>)}</SelectContent>
                                            </Select>
                                        </div>
                                    </div>
                                )}

                                {presetType === 'MONTHLY' && (
                                    <div className='flex flex-wrap items-center gap-4 animate-in fade-in slide-in-from-top-2'>
                                        <div className='flex items-center gap-2'>
                                            <span className='text-sm font-medium'>On day:</span>
                                            <Select value={dayOfMonth} onValueChange={setDayOfMonth}>
                                                <SelectTrigger className='w-[80px]'><SelectValue /></SelectTrigger>
                                                <SelectContent>{DAYS_OF_MONTH.map((d) => <SelectItem key={d.value} value={d.value}>{d.label}</SelectItem>)}</SelectContent>
                                            </Select>
                                        </div>
                                        <div className='flex items-center gap-2'>
                                            <span className='text-sm font-medium'>At time:</span>
                                            <Select value={hour} onValueChange={setHour}>
                                                <SelectTrigger className='w-[100px]'><SelectValue /></SelectTrigger>
                                                <SelectContent>{HOURS.map((h) => <SelectItem key={h.value} value={h.value}>{h.label}</SelectItem>)}</SelectContent>
                                            </Select>
                                        </div>
                                    </div>
                                )}
                            </div>
                        )}

                        {cronMode === 'manual' && (
                            <FormField
                                control={form.control}
                                name='cron_expression'
                                render={({ field }) => (
                                    <FormItem>
                                        <FormControl>
                                            <Input {...field} placeholder='*/5 * * * *' className='font-mono' />
                                        </FormControl>
                                        <FormDescription>Standard 5-part cron expression (minute hour day month weekday)</FormDescription>
                                        <FormMessage />
                                    </FormItem>
                                )}
                            />
                        )}

                        <div className='rounded-md bg-muted/50 border px-4 py-3 flex items-center gap-2 text-sm text-muted-foreground'>
                            <Clock className='h-4 w-4 text-primary' />
                            <span>Next run: <span className="font-medium text-foreground">{getCronHuman(form.watch('cron_expression'))}</span></span>
                        </div>
                    </div>
                </div>

                <div className='flex items-center justify-end gap-3'>
                    <Button type='submit' size="lg" disabled={isSubmitting}>
                        {isSubmitting ? (
                            <><Loader2 className='mr-2 h-4 w-4 animate-spin' />Saving…</>
                        ) : (
                            <><Save className='mr-2 h-4 w-4' />Save Changes</>
                        )}
                    </Button>
                </div>
            </form>
        </Form>
    )
}
