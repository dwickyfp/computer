import { useEffect } from 'react'
import { z } from 'zod'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { configurationRepo } from '@/repo/configuration'
import { Loader2, AlertCircle } from 'lucide-react'
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
import { Alert, AlertDescription } from '@/components/ui/alert'

const batchConfigFormSchema = z
  .object({
    max_batch_size: z
      .number()
      .min(1024, 'Batch size must be at least 1024')
      .max(16384, 'Batch size cannot exceed 16384'),
    max_queue_size: z
      .number()
      .min(2048, 'Queue size must be at least 2048')
      .max(65536, 'Queue size cannot exceed 65536'),
  })
  .refine((data) => data.max_queue_size >= data.max_batch_size * 2, {
    message: 'Queue size should be at least 2x batch size for optimal performance',
    path: ['max_queue_size'],
  })

type BatchConfigFormValues = z.infer<typeof batchConfigFormSchema>

export function BatchConfigurationForm() {
  const queryClient = useQueryClient()

  const { data: config, isLoading } = useQuery({
    queryKey: ['configuration', 'batch'],
    queryFn: configurationRepo.getBatchConfiguration,
  })

  const defaultValues: BatchConfigFormValues = {
    max_batch_size: 4096,
    max_queue_size: 16384,
  }

  const form = useForm<BatchConfigFormValues>({
    resolver: zodResolver(batchConfigFormSchema),
    defaultValues,
  })

  // Update form values when config data changes
  useEffect(() => {
    if (config) {
      form.reset({
        max_batch_size: config.max_batch_size,
        max_queue_size: config.max_queue_size,
      })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [config])

  const updateMutation = useMutation({
    mutationFn: configurationRepo.updateBatchConfiguration,
    onSuccess: (data) => {
      // Add 300ms delay before invalidating to allow DB transaction to commit
      setTimeout(() => {
        queryClient.invalidateQueries({
          queryKey: ['configuration', 'batch'],
        })
      }, 300)

      // Update form with the saved values
      form.reset({
        max_batch_size: data.max_batch_size,
        max_queue_size: data.max_queue_size,
      })
      toast.success('Batch configuration updated. All active pipelines will restart automatically to apply changes.')
    },
    onError: (error: any) => {
      toast.error(error?.message || 'Failed to update batch configuration')
    },
  })

  function onSubmit(data: BatchConfigFormValues) {
    updateMutation.mutate(data)
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
        <Alert>
          <AlertCircle className='h-4 w-4' />
          <AlertDescription>
            Changes to batch configuration will automatically restart all active pipelines.
            Higher values improve throughput but increase memory usage.
          </AlertDescription>
        </Alert>

        <div className='grid grid-cols-1 gap-8 lg:grid-cols-2'>
          <div className='space-y-6'>
            <div className='grid grid-cols-1 gap-6'>
              <FormField
                control={form.control}
                name='max_batch_size'
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Max Batch Size</FormLabel>
                    <FormControl>
                      <Input
                        type='number'
                        placeholder='4096'
                        {...field}
                        onChange={(e) => field.onChange(e.target.valueAsNumber)}
                      />
                    </FormControl>
                    <FormDescription>
                      Number of CDC records processed per batch (1024-16384).
                      Recommended: 4096 for balanced performance.
                    </FormDescription>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <FormField
                control={form.control}
                name='max_queue_size'
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Max Queue Size</FormLabel>
                    <FormControl>
                      <Input
                        type='number'
                        placeholder='16384'
                        {...field}
                        onChange={(e) => field.onChange(e.target.valueAsNumber)}
                      />
                    </FormControl>
                    <FormDescription>
                      Internal buffer size between Debezium and event handler (2048-65536).
                      Should be at least 2x batch size. Recommended: 16384.
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

          <div className='space-y-4'>
            <div className='rounded-lg border p-4'>
              <h3 className='font-medium mb-2'>Performance Guidelines</h3>
              <ul className='space-y-2 text-sm text-muted-foreground'>
                <li>• <strong>Batch Size 2048:</strong> Low memory, higher CPU overhead</li>
                <li>• <strong>Batch Size 4096:</strong> Balanced (recommended)</li>
                <li>• <strong>Batch Size 8192:</strong> High throughput, more memory</li>
                <li>• <strong>Queue Size:</strong> Should be 2-4x batch size</li>
                <li>• Larger values = better throughput but higher latency</li>
              </ul>
            </div>

            <div className='rounded-lg border p-4 bg-amber-50 dark:bg-amber-950/20'>
              <h3 className='font-medium mb-2 text-amber-900 dark:text-amber-100'>
                Resource Impact
              </h3>
              <ul className='space-y-1 text-sm text-amber-800 dark:text-amber-200'>
                <li>• Each pipeline uses configured values</li>
                <li>• 20 pipelines × 8192 batch = ~1.2GB memory</li>
                <li>• Monitor system resources after changes</li>
              </ul>
            </div>
          </div>
        </div>
      </form>
    </Form>
  )
}
