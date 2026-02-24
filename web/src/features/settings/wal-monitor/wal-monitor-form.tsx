import { useEffect } from 'react'
import { z } from 'zod'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { configurationRepo } from '@/repo/configuration'
import { Loader2 } from 'lucide-react'
import { toast } from 'sonner'
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

const walMonitorFormSchema = z
  .object({
    warning: z
      .number()
      .min(1, 'Warning threshold must be at least 1 MB')
      .max(1000000, 'Warning threshold is too large'),
    error: z
      .number()
      .min(1, 'Error threshold must be at least 1 MB')
      .max(1000000, 'Error threshold is too large'),
  })
  .refine((data) => data.error > data.warning, {
    message: 'Error threshold must be greater than warning threshold',
    path: ['error'], // path of error
  })

type WALMonitorFormValues = z.infer<typeof walMonitorFormSchema>

export function WALMonitorForm() {
  const queryClient = useQueryClient()

  const { data: config, isLoading } = useQuery({
    queryKey: ['configuration', 'wal-thresholds'],
    queryFn: configurationRepo.getWALThresholds,
  })

  const defaultValues: WALMonitorFormValues = {
    warning: 3000,
    error: 6000,
  }

  const form = useForm<WALMonitorFormValues>({
    resolver: zodResolver(walMonitorFormSchema),
    defaultValues,
  })

  // Update form values when config data changes
  useEffect(() => {
    if (config) {
      form.reset({
        warning: config.warning,
        error: config.error,
      })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [config])

  const updateMutation = useMutation({
    mutationFn: configurationRepo.updateWALThresholds,
    onSuccess: (data) => {
      queryClient.invalidateQueries({
        queryKey: ['configuration', 'wal-thresholds'],
      })
      // Update form with the saved values
      form.reset({
        warning: data.warning,
        error: data.error,
      })
      toast.success('WAL Monitor settings updated successfully')
    },
    onError: (error: any) => {
      toast.error(error?.message || 'Failed to update WAL Monitor settings')
    },
  })

  function onSubmit(data: WALMonitorFormValues) {
    if (!config) return

    // Merge with existing config to preserve notification settings
    const payload = {
      ...config,
      ...data,
    }
    updateMutation.mutate(payload)
  }

  if (isLoading) {
    return (
      <div className='flex items-center justify-center p-8'>
        <Loader2 className='h-8 w-8 animate-spin text-muted-foreground' />
      </div>
    )
  }

  return (
    <Form {...form}>
      <form onSubmit={form.handleSubmit(onSubmit)} className='space-y-8'>
        <div className='grid grid-cols-1 gap-8 lg:grid-cols-2'>
          <div className='space-y-6'>
            <div className='grid grid-cols-1 gap-6'>
              <FormField
                control={form.control}
                name='warning'
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Warning Threshold (MB)</FormLabel>
                    <FormControl>
                      <Input
                        type='number'
                        placeholder='3000'
                        {...field}
                        onChange={(e) => field.onChange(e.target.valueAsNumber)}
                      />
                    </FormControl>
                    <FormDescription>
                      WAL size threshold for warning alerts.
                    </FormDescription>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <FormField
                control={form.control}
                name='error'
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Error Threshold (MB)</FormLabel>
                    <FormControl>
                      <Input
                        type='number'
                        placeholder='6000'
                        {...field}
                        onChange={(e) => field.onChange(e.target.valueAsNumber)}
                      />
                    </FormControl>
                    <FormDescription>
                      WAL size threshold for error alerts.
                    </FormDescription>
                    <FormMessage />
                  </FormItem>
                )}
              />
            </div>

            <div className='pt-4'>
              <Button type='submit' disabled={updateMutation.isPending}>
                {updateMutation.isPending && (
                  <Loader2 className='mr-2 h-4 w-4 animate-spin' />
                )}
                Save Changes
              </Button>
            </div>
          </div>
        </div>
      </form>
    </Form>
  )
}
