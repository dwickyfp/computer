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
import { Switch } from '@/components/ui/switch'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { InternalNotificationTab } from './internal-notification-tab'

const notificationsFormSchema = z.object({
  enable_webhook: z.boolean(),
  webhook_url: z.string().url('Please enter a valid URL').or(z.literal('')),
  notification_iteration: z
    .number()
    .min(1, 'Iteration must be at least 1')
    .max(100, 'Iteration cannot exceed 100'),
  enable_telegram: z.boolean(),
  telegram_bot_token: z.string(),
  telegram_chat_id: z.string(),
})

type NotificationsFormValues = z.infer<typeof notificationsFormSchema>

export function NotificationsForm() {
  const queryClient = useQueryClient()

  const { data: config, isLoading } = useQuery({
    queryKey: ['configuration', 'wal-thresholds'],
    queryFn: configurationRepo.getWALThresholds,
  })

  const form = useForm<NotificationsFormValues>({
    resolver: zodResolver(notificationsFormSchema),
    defaultValues: {
      enable_webhook: false,
      webhook_url: '',
      notification_iteration: 3,
      enable_telegram: false,
      telegram_bot_token: '',
      telegram_chat_id: '',
    },
  })

  useEffect(() => {
    if (config) {
      form.reset({
        enable_webhook: config.enable_webhook,
        webhook_url: config.webhook_url,
        notification_iteration: config.notification_iteration,
        enable_telegram: config.enable_telegram,
        telegram_bot_token: config.telegram_bot_token,
        telegram_chat_id: config.telegram_chat_id,
      })
    }
  }, [config, form])

  const updateMutation = useMutation({
    mutationFn: configurationRepo.updateWALThresholds,
    onSuccess: () => {
      setTimeout(() => {
        queryClient.invalidateQueries({
          queryKey: ['configuration', 'wal-thresholds'],
        })
      }, 300)
      toast.success('Notification settings updated successfully')
    },
    onError: (error: any) => {
      toast.error(error?.message || 'Failed to update notification settings')
    },
  })

  const toggleWebhookMutation = useMutation({
    mutationFn: configurationRepo.updateWALThresholds,
    onSuccess: () => {
      setTimeout(() => {
        queryClient.invalidateQueries({
          queryKey: ['configuration', 'wal-thresholds'],
        })
      }, 300)
      toast.success('Webhook notification status updated')
    },
    onError: (error: any) => {
      toast.error(error?.message || 'Failed to update webhook status')
      if (config) {
        form.setValue('enable_webhook', config.enable_webhook)
      }
    },
  })

  const toggleTelegramMutation = useMutation({
    mutationFn: configurationRepo.updateWALThresholds,
    onSuccess: () => {
      setTimeout(() => {
        queryClient.invalidateQueries({
          queryKey: ['configuration', 'wal-thresholds'],
        })
      }, 300)
      toast.success('Telegram notification status updated')
    },
    onError: (error: any) => {
      toast.error(error?.message || 'Failed to update Telegram status')
      if (config) {
        form.setValue('enable_telegram', config.enable_telegram)
      }
    },
  })

  function buildPayload(override: Partial<NotificationsFormValues> = {}) {
    if (!config) return null
    return { ...config, ...form.getValues(), ...override }
  }

  const handleToggleWebhook = (checked: boolean) => {
    if (!config) return
    form.setValue('enable_webhook', checked)
    const payload = buildPayload({ enable_webhook: checked })
    if (payload) toggleWebhookMutation.mutate(payload)
  }

  const handleToggleTelegram = (checked: boolean) => {
    if (!config) return
    form.setValue('enable_telegram', checked)
    const payload = buildPayload({ enable_telegram: checked })
    if (payload) toggleTelegramMutation.mutate(payload)
  }

  const isWebhookEnabled = form.watch('enable_webhook')
  const isTelegramEnabled = form.watch('enable_telegram')

  if (isLoading) {
    return (
      <div className='flex items-center justify-center p-8'>
        <Loader2 className='h-8 w-8 animate-spin text-muted-foreground' />
      </div>
    )
  }

  return (
    <Tabs defaultValue='internal' className='w-full'>
      <TabsList className='mb-6'>
        <TabsTrigger value='internal'>Internal Notification</TabsTrigger>
        <TabsTrigger value='webhook'>Webhook Notification</TabsTrigger>
        <TabsTrigger value='telegram'>Telegram Notification</TabsTrigger>
      </TabsList>

      {/* ── Tab 1: Internal Notification ── */}
      <TabsContent value='internal'>
        <InternalNotificationTab />
      </TabsContent>

      {/* ── Tab 2: Webhook Notification ── */}
      <TabsContent value='webhook'>
        <Form {...form}>
          <form
            onSubmit={form.handleSubmit(
              (data) => config && updateMutation.mutate({ ...config, ...data })
            )}
            className='max-w-xl space-y-6'
          >
            <FormField
              control={form.control}
              name='enable_webhook'
              render={({ field }) => (
                <FormItem className='flex flex-row items-center justify-between rounded-lg border p-4'>
                  <div className='space-y-0.5'>
                    <FormLabel className='text-base'>
                      Enable Webhook Notifications
                    </FormLabel>
                    <FormDescription>
                      Enable or disable sending notifications to the webhook URL
                    </FormDescription>
                  </div>
                  <FormControl>
                    <Switch
                      checked={field.value}
                      onCheckedChange={handleToggleWebhook}
                      disabled={toggleWebhookMutation.isPending}
                    />
                  </FormControl>
                </FormItem>
              )}
            />

            <FormField
              control={form.control}
              name='webhook_url'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Webhook URL</FormLabel>
                  <div className='flex gap-2'>
                    <FormControl>
                      <Input
                        type='url'
                        placeholder='https://your-webhook-endpoint.com/webhook'
                        disabled={!isWebhookEnabled}
                        {...field}
                      />
                    </FormControl>
                    <Button
                      type='button'
                      variant='secondary'
                      disabled={!isWebhookEnabled}
                      onClick={async () => {
                        const url = form.getValues('webhook_url')
                        if (!url) {
                          toast.error('Please enter a webhook URL first')
                          return
                        }
                        try {
                          await configurationRepo.testNotification(url)
                          toast.success('Test notification sent successfully')
                        } catch (e: any) {
                          toast.error(
                            e.response?.data?.detail ||
                              'Failed to trigger test notification'
                          )
                        }
                      }}
                    >
                      Test
                    </Button>
                  </div>
                  <FormDescription>
                    Webhook URL for alert notifications. Only used when webhook
                    is enabled.
                  </FormDescription>
                  <FormMessage />
                </FormItem>
              )}
            />

            <FormField
              control={form.control}
              name='notification_iteration'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Notification Iteration</FormLabel>
                  <FormControl>
                    <Input
                      type='number'
                      placeholder='3'
                      {...field}
                      onChange={(e) => field.onChange(e.target.valueAsNumber)}
                      className='max-w-32'
                    />
                  </FormControl>
                  <FormDescription>
                    Number of check iterations before sending a notification.
                  </FormDescription>
                  <FormMessage />
                </FormItem>
              )}
            />

            <div>
              <Button type='submit' disabled={updateMutation.isPending}>
                {updateMutation.isPending && (
                  <Loader2 className='mr-2 h-4 w-4 animate-spin' />
                )}
                Save Changes
              </Button>
              <p className='mt-2 text-xs text-muted-foreground'>
                Toggle updates immediately. This button saves URL and iteration
                changes.
              </p>
            </div>

            {/* Payload reference card */}
            <div className='rounded-xl border bg-card p-6 shadow-sm'>
              <div className='mb-4 flex items-center gap-2 text-muted-foreground'>
                <div className='h-2 w-2 animate-pulse rounded-full bg-green-500' />
                <h3 className='text-xs font-medium tracking-wider uppercase'>
                  Example Payload
                </h3>
              </div>
              <p className='mb-4 text-sm leading-relaxed text-muted-foreground'>
                Notifications are sent as JSON POST requests to the configured
                URL.
              </p>
              <div className='overflow-hidden rounded-lg border bg-zinc-950 p-4'>
                <pre className='custom-scrollbar overflow-auto font-mono text-[10px] text-zinc-50 sm:text-xs'>
                  {JSON.stringify(
                    {
                      key_notification: 'WAL_SIZE_WARNING',
                      title: 'WAL Size Warning',
                      message: 'WAL size exceeded 3000MB.',
                      type: 'WARNING',
                      timestamp: '2024-01-01T12:00:00+07:00',
                    },
                    null,
                    2
                  )}
                </pre>
              </div>
            </div>
          </form>
        </Form>
      </TabsContent>

      {/* ── Tab 3: Telegram Notification ── */}
      <TabsContent value='telegram'>
        <Form {...form}>
          <form
            onSubmit={form.handleSubmit(
              (data) => config && updateMutation.mutate({ ...config, ...data })
            )}
            className='max-w-xl space-y-6'
          >
            <FormField
              control={form.control}
              name='enable_telegram'
              render={({ field }) => (
                <FormItem className='flex flex-row items-center justify-between rounded-lg border p-4'>
                  <div className='space-y-0.5'>
                    <FormLabel className='text-base'>
                      Enable Telegram Notifications
                    </FormLabel>
                    <FormDescription>
                      Enable or disable sending notifications to Telegram
                    </FormDescription>
                  </div>
                  <FormControl>
                    <Switch
                      checked={field.value}
                      onCheckedChange={handleToggleTelegram}
                      disabled={toggleTelegramMutation.isPending}
                    />
                  </FormControl>
                </FormItem>
              )}
            />

            <FormField
              control={form.control}
              name='telegram_bot_token'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Telegram Bot Token</FormLabel>
                  <FormControl>
                    <Input
                      type='text'
                      placeholder='123456789:ABCdefGHIjklMNOpqrsTUVwxyz'
                      disabled={!isTelegramEnabled}
                      {...field}
                    />
                  </FormControl>
                  <FormDescription>
                    Your Telegram bot token from @BotFather.
                  </FormDescription>
                  <FormMessage />
                </FormItem>
              )}
            />

            <FormField
              control={form.control}
              name='telegram_chat_id'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Telegram Chat / Group ID</FormLabel>
                  <div className='flex gap-2'>
                    <FormControl>
                      <Input
                        type='text'
                        placeholder='-1001234567890'
                        disabled={!isTelegramEnabled}
                        {...field}
                      />
                    </FormControl>
                    <Button
                      type='button'
                      variant='secondary'
                      disabled={!isTelegramEnabled}
                      onClick={async () => {
                        const botToken = form.getValues('telegram_bot_token')
                        const chatId = form.getValues('telegram_chat_id')
                        if (!botToken || !chatId) {
                          toast.error(
                            'Please enter bot token and chat ID first'
                          )
                          return
                        }
                        try {
                          await configurationRepo.testNotification(
                            undefined,
                            botToken,
                            chatId
                          )
                          toast.success(
                            'Test notification sent to Telegram successfully'
                          )
                        } catch (e: any) {
                          toast.error(
                            e.response?.data?.detail ||
                              'Failed to send Telegram test notification'
                          )
                        }
                      }}
                    >
                      Test
                    </Button>
                  </div>
                  <FormDescription>
                    Chat or group ID where notifications will be sent. Use a
                    negative number for groups.
                  </FormDescription>
                  <FormMessage />
                </FormItem>
              )}
            />

            <div>
              <Button type='submit' disabled={updateMutation.isPending}>
                {updateMutation.isPending && (
                  <Loader2 className='mr-2 h-4 w-4 animate-spin' />
                )}
                Save Changes
              </Button>
              <p className='mt-2 text-xs text-muted-foreground'>
                Toggle updates immediately. This button saves bot token and chat
                ID changes.
              </p>
            </div>
          </form>
        </Form>
      </TabsContent>
    </Tabs>
  )
}
