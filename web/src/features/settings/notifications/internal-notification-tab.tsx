import { useState } from 'react'
import { z } from 'zod'
import { useForm, useFieldArray } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  internalNotificationRepo,
  InternalNotificationConfig,
} from '@/repo/internal-notification'
import {
  Plus,
  Trash2,
  Pencil,
  FlaskConical,
  Loader2,
  UserPlus,
} from 'lucide-react'
import { toast } from 'sonner'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
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
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

// ─────────────────────────────────────────────────────────────
// Schema
// ─────────────────────────────────────────────────────────────

const configFormSchema = z.object({
  name: z.string().min(1, 'Name is required'),
  is_enabled: z.boolean(),
  base_url: z.string().url('Enter a valid base URL'),
  requester: z.string().min(1, 'Requester is required'),
  menu_code: z.string().min(1, 'Menu code is required'),
  company_group_id: z.number().int().positive('Must be a positive number'),
  mail_from_code: z.string().min(1, 'Mail from code is required'),
  subject: z.string().min(1, 'Subject is required'),
  // Dynamic email list — internally managed, joined to mail_to on submit
  emails: z
    .array(z.object({ address: z.string().email('Enter a valid email') }))
    .min(1, 'At least one recipient is required'),
})

type ConfigFormValues = z.infer<typeof configFormSchema>

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

function mailToEmails(mailTo: string) {
  return mailTo
    .split(',')
    .map((e) => e.trim())
    .filter(Boolean)
    .map((address) => ({ address }))
}

// ─────────────────────────────────────────────────────────────
// Config Mutate Dialog
// ─────────────────────────────────────────────────────────────

interface ConfigDialogProps {
  open: boolean
  onClose: () => void
  editing: InternalNotificationConfig | null
}

function ConfigDialog({ open, onClose, editing }: ConfigDialogProps) {
  const queryClient = useQueryClient()

  const form = useForm<ConfigFormValues>({
    resolver: zodResolver(configFormSchema),
    defaultValues: editing
      ? {
          name: editing.name,
          is_enabled: editing.is_enabled,
          base_url: editing.base_url,
          requester: editing.requester,
          menu_code: editing.menu_code,
          company_group_id: editing.company_group_id,
          mail_from_code: editing.mail_from_code,
          subject: editing.subject,
          emails: mailToEmails(editing.mail_to),
        }
      : {
          name: '',
          is_enabled: true,
          base_url: '',
          requester: '',
          menu_code: '',
          company_group_id: 1,
          mail_from_code: '',
          subject: '',
          emails: [{ address: '' }],
        },
  })

  const { fields, append, remove } = useFieldArray({
    control: form.control,
    name: 'emails',
  })

  const saveMutation = useMutation({
    mutationFn: async (values: ConfigFormValues) => {
      const mail_to = values.emails.map((e) => e.address).join(',')
      const payload = {
        name: values.name,
        is_enabled: values.is_enabled,
        base_url: values.base_url,
        requester: values.requester,
        menu_code: values.menu_code,
        company_group_id: values.company_group_id,
        mail_from_code: values.mail_from_code,
        mail_to,
        subject: values.subject,
      }
      if (editing) {
        return internalNotificationRepo.update(editing.id, payload)
      }
      return internalNotificationRepo.create(payload)
    },
    onSuccess: () => {
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: ['internal-notifications'] })
      }, 300)
      toast.success(editing ? 'Configuration updated' : 'Configuration created')
      onClose()
    },
    onError: () => {
      toast.error('Failed to save configuration')
    },
  })

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className='max-h-[90vh] max-w-xl overflow-y-auto'>
        <DialogHeader>
          <DialogTitle>
            {editing
              ? 'Edit Internal Notification'
              : 'New Internal Notification'}
          </DialogTitle>
        </DialogHeader>

        <Form {...form}>
          <form
            onSubmit={form.handleSubmit((v) => saveMutation.mutate(v))}
            className='space-y-4 py-2'
          >
            {/* Name */}
            <FormField
              control={form.control}
              name='name'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Config Name</FormLabel>
                  <FormControl>
                    <Input placeholder='e.g. HR Alert Email' {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            {/* is_enabled */}
            <FormField
              control={form.control}
              name='is_enabled'
              render={({ field }) => (
                <FormItem className='flex flex-row items-center justify-between rounded-lg border p-3'>
                  <div>
                    <FormLabel>Enabled</FormLabel>
                    <FormDescription>
                      Send notifications via this config
                    </FormDescription>
                  </div>
                  <FormControl>
                    <Switch
                      checked={field.value}
                      onCheckedChange={field.onChange}
                    />
                  </FormControl>
                </FormItem>
              )}
            />

            {/* base_url */}
            <FormField
              control={form.control}
              name='base_url'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Base URL</FormLabel>
                  <FormControl>
                    <Input
                      placeholder='http://backend.example.com/NotificationSystem'
                      {...field}
                    />
                  </FormControl>
                  <FormDescription>
                    Root URL of the internal notification API (without path).
                  </FormDescription>
                  <FormMessage />
                </FormItem>
              )}
            />

            {/* requester */}
            <FormField
              control={form.control}
              name='requester'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Requester</FormLabel>
                  <FormControl>
                    <Input placeholder='e.g. CALM' {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            {/* menu_code */}
            <FormField
              control={form.control}
              name='menu_code'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Menu Code</FormLabel>
                  <FormControl>
                    <Input placeholder='e.g. CALMT01' {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            {/* company_group_id */}
            <FormField
              control={form.control}
              name='company_group_id'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Company Group ID</FormLabel>
                  <FormControl>
                    <Input
                      type='number'
                      className='max-w-32'
                      {...field}
                      onChange={(e) => field.onChange(e.target.valueAsNumber)}
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            {/* mail_from_code */}
            <FormField
              control={form.control}
              name='mail_from_code'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Mail From Code</FormLabel>
                  <FormControl>
                    <Input placeholder='e.g. MISCELLANEOUS' {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            {/* subject */}
            <FormField
              control={form.control}
              name='subject'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Email Subject</FormLabel>
                  <FormControl>
                    <Input
                      placeholder='e.g. [Rosetta] Alert Notification'
                      {...field}
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            {/* Dynamic email recipients */}
            <div className='space-y-2'>
              <div className='flex items-center justify-between'>
                <Label>Email Recipients</Label>
                <Button
                  type='button'
                  variant='outline'
                  size='sm'
                  onClick={() => append({ address: '' })}
                >
                  <UserPlus className='mr-1 h-3.5 w-3.5' />
                  Add User
                </Button>
              </div>
              {fields.map((field, index) => (
                <FormField
                  key={field.id}
                  control={form.control}
                  name={`emails.${index}.address`}
                  render={({ field: f }) => (
                    <FormItem>
                      <FormControl>
                        <div className='flex gap-2'>
                          <Input
                            type='email'
                            placeholder='user@example.com'
                            {...f}
                          />
                          {fields.length > 1 && (
                            <Button
                              type='button'
                              variant='ghost'
                              size='icon'
                              onClick={() => remove(index)}
                            >
                              <Trash2 className='h-4 w-4 text-destructive' />
                            </Button>
                          )}
                        </div>
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              ))}
              {form.formState.errors.emails?.root && (
                <p className='text-sm text-destructive'>
                  {form.formState.errors.emails.root.message}
                </p>
              )}
              <p className='text-xs text-muted-foreground'>
                Recipients are stored as comma-separated values.
              </p>
            </div>

            <DialogFooter>
              <Button type='button' variant='outline' onClick={onClose}>
                Cancel
              </Button>
              <Button type='submit' disabled={saveMutation.isPending}>
                {saveMutation.isPending && (
                  <Loader2 className='mr-2 h-4 w-4 animate-spin' />
                )}
                Save
              </Button>
            </DialogFooter>
          </form>
        </Form>
      </DialogContent>
    </Dialog>
  )
}

// ─────────────────────────────────────────────────────────────
// Main Tab Component
// ─────────────────────────────────────────────────────────────

export function InternalNotificationTab() {
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<InternalNotificationConfig | null>(
    null
  )
  const [deleteTarget, setDeleteTarget] =
    useState<InternalNotificationConfig | null>(null)

  // Global status
  const { data: globalStatus } = useQuery({
    queryKey: ['internal-notifications', 'global'],
    queryFn: internalNotificationRepo.getGlobalStatus,
  })

  // Config list
  const { data: configs = [], isLoading } = useQuery({
    queryKey: ['internal-notifications'],
    queryFn: internalNotificationRepo.getAll,
  })

  const globalToggleMutation = useMutation({
    mutationFn: (is_active: boolean) =>
      internalNotificationRepo.setGlobalToggle(is_active),
    onSuccess: () => {
      setTimeout(() => {
        queryClient.invalidateQueries({
          queryKey: ['internal-notifications', 'global'],
        })
      }, 300)
    },
    onError: () => toast.error('Failed to update global toggle'),
  })

  const perConfigToggleMutation = useMutation({
    mutationFn: ({ id, is_active }: { id: number; is_active: boolean }) =>
      internalNotificationRepo.toggleEnabled(id, is_active),
    onSuccess: () => {
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: ['internal-notifications'] })
      }, 300)
    },
    onError: () => toast.error('Failed to update config status'),
  })

  const deleteMutation = useMutation({
    mutationFn: (id: number) => internalNotificationRepo.delete(id),
    onSuccess: () => {
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: ['internal-notifications'] })
      }, 300)
      toast.success('Configuration deleted')
      setDeleteTarget(null)
    },
    onError: () => toast.error('Failed to delete configuration'),
  })

  const testMutation = useMutation({
    mutationFn: (id: number) => internalNotificationRepo.test(id),
    onSuccess: (data) => toast.success(data.message),
    onError: () =>
      toast.error('Test notification failed — check config and connectivity'),
  })

  return (
    <div className='space-y-6'>
      {/* Global toggle */}
      <div className='flex flex-row items-center justify-between rounded-lg border p-4'>
        <div className='space-y-0.5'>
          <p className='text-base font-medium'>Enable Internal Notifications</p>
          <p className='text-sm text-muted-foreground'>
            Globally enable or disable all internal notification delivery.
            Individual configs can also be toggled independently below.
          </p>
        </div>
        <Switch
          checked={globalStatus?.is_active ?? false}
          onCheckedChange={(v) => globalToggleMutation.mutate(v)}
          disabled={globalToggleMutation.isPending}
        />
      </div>

      {/* Header row */}
      <div className='flex items-center justify-between'>
        <div>
          <h3 className='text-sm font-medium'>Notification Configurations</h3>
          <p className='mt-0.5 text-xs text-muted-foreground'>
            Each configuration sends alerts to a separate internal email API
            endpoint.
          </p>
        </div>
        <Button
          size='sm'
          onClick={() => {
            setEditing(null)
            setDialogOpen(true)
          }}
        >
          <Plus className='mr-1 h-4 w-4' />
          Add Configuration
        </Button>
      </div>

      {/* Config table */}
      {isLoading ? (
        <div className='flex items-center justify-center py-8'>
          <Loader2 className='h-6 w-6 animate-spin text-muted-foreground' />
        </div>
      ) : configs.length === 0 ? (
        <div className='rounded-lg border border-dashed p-8 text-center'>
          <p className='text-sm text-muted-foreground'>
            No configurations yet. Click{' '}
            <span className='font-medium'>Add Configuration</span> to create
            one.
          </p>
        </div>
      ) : (
        <div className='overflow-hidden rounded-lg border'>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Base URL</TableHead>
                <TableHead>Recipients</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className='text-right'>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {configs.map((cfg) => (
                <TableRow key={cfg.id}>
                  <TableCell className='font-medium'>{cfg.name}</TableCell>
                  <TableCell className='max-w-48 truncate text-xs text-muted-foreground'>
                    {cfg.base_url}
                  </TableCell>
                  <TableCell>
                    <div className='flex flex-wrap gap-1'>
                      {cfg.mail_to
                        .split(',')
                        .map((e) => e.trim())
                        .filter(Boolean)
                        .map((email) => (
                          <Badge
                            key={email}
                            variant='secondary'
                            className='text-xs font-normal'
                          >
                            {email}
                          </Badge>
                        ))}
                    </div>
                  </TableCell>
                  <TableCell>
                    <Switch
                      checked={cfg.is_enabled}
                      onCheckedChange={(v) =>
                        perConfigToggleMutation.mutate({
                          id: cfg.id,
                          is_active: v,
                        })
                      }
                      disabled={perConfigToggleMutation.isPending}
                    />
                  </TableCell>
                  <TableCell className='text-right'>
                    <div className='flex items-center justify-end gap-1'>
                      <Button
                        variant='ghost'
                        size='icon'
                        title='Test'
                        onClick={() => testMutation.mutate(cfg.id)}
                        disabled={testMutation.isPending}
                      >
                        <FlaskConical className='h-4 w-4' />
                      </Button>
                      <Button
                        variant='ghost'
                        size='icon'
                        title='Edit'
                        onClick={() => {
                          setEditing(cfg)
                          setDialogOpen(true)
                        }}
                      >
                        <Pencil className='h-4 w-4' />
                      </Button>
                      <Button
                        variant='ghost'
                        size='icon'
                        title='Delete'
                        onClick={() => setDeleteTarget(cfg)}
                      >
                        <Trash2 className='h-4 w-4 text-destructive' />
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}

      {/* Add/Edit dialog */}
      {dialogOpen && (
        <ConfigDialog
          open={dialogOpen}
          onClose={() => {
            setDialogOpen(false)
            setEditing(null)
          }}
          editing={editing}
        />
      )}

      {/* Delete confirmation */}
      <AlertDialog
        open={!!deleteTarget}
        onOpenChange={(v) => !v && setDeleteTarget(null)}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Configuration</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete{' '}
              <span className='font-semibold'>{deleteTarget?.name}</span>? This
              action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className='text-destructive-foreground bg-destructive hover:bg-destructive/90'
              onClick={() =>
                deleteTarget && deleteMutation.mutate(deleteTarget.id)
              }
              disabled={deleteMutation.isPending}
            >
              {deleteMutation.isPending && (
                <Loader2 className='mr-2 h-4 w-4 animate-spin' />
              )}
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
