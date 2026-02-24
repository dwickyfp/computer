import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  destinationsRepo,
  type DestinationListResponse,
} from '@/repo/destinations'
import { Loader2 } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Sheet,
  SheetClose,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet'
import {
  type Destination,
  destinationFormSchema,
  type DestinationForm,
} from '../data/schema'

type DestinationsMutateDrawerProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  currentRow?: Destination
}

export function DestinationsMutateDrawer({
  open,
  onOpenChange,
  currentRow,
}: DestinationsMutateDrawerProps) {
  const isUpdate = !!currentRow
  const queryClient = useQueryClient()

  const form = useForm<DestinationForm>({
    resolver: zodResolver(destinationFormSchema),
    mode: 'onChange',
    defaultValues: currentRow
      ? {
          name: currentRow.name,
          type: currentRow.type || 'SNOWFLAKE',
          config: currentRow.config || {},
        }
      : {
          name: '',
          type: 'SNOWFLAKE',
          config: {},
        },
  })

  const createMutation = useMutation({
    mutationFn: destinationsRepo.create,
    onSuccess: async (newDestination) => {
      queryClient.setQueryData<DestinationListResponse>(
        ['destinations'],
        (old) => {
          if (!old) return old
          return {
            ...old,
            destinations: [newDestination, ...old.destinations],
            total: old.total + 1,
          }
        }
      )
      onOpenChange(false)
      form.reset()
      toast.success('Destination created successfully')
      // Wait for DB transaction to commit before refetching
      await new Promise((resolve) => setTimeout(resolve, 300))
      await queryClient.invalidateQueries({
        queryKey: ['destinations'],
        refetchType: 'active',
      })
    },
    onError: (error) => {
      toast.error('Failed to create destination')
      console.error(error)
    },
  })

  const updateMutation = useMutation({
    mutationFn: (data: DestinationForm) =>
      destinationsRepo.update(currentRow!.id, data),
    onSuccess: async (updatedDestination) => {
      queryClient.setQueryData<DestinationListResponse>(
        ['destinations'],
        (old) => {
          if (!old) return old
          return {
            ...old,
            destinations: old.destinations.map((d) =>
              d.id === updatedDestination.id ? updatedDestination : d
            ),
          }
        }
      )
      queryClient.setQueryData(
        ['destination', updatedDestination.id],
        updatedDestination
      )

      onOpenChange(false)
      form.reset()
      toast.success('Destination updated successfully')
      // Wait for DB transaction to commit before refetching
      await new Promise((resolve) => setTimeout(resolve, 300))
      await queryClient.invalidateQueries({
        queryKey: ['destinations'],
        refetchType: 'active',
      })
      await queryClient.invalidateQueries({
        queryKey: ['destination', currentRow?.id],
        refetchType: 'active',
      })
    },
    onError: (error) => {
      toast.error('Failed to update destination')
      console.error(error)
    },
  })

  const onSubmit = (data: DestinationForm) => {
    if (isUpdate) {
      updateMutation.mutate(data)
    } else {
      createMutation.mutate(data)
    }
  }

  const isLoading = createMutation.isPending || updateMutation.isPending

  return (
    <Sheet
      open={open}
      onOpenChange={(v) => {
        onOpenChange(v)
        form.reset()
      }}
    >
      <SheetContent className='flex w-full flex-col sm:max-w-md'>
        <SheetHeader className='text-start'>
          <SheetTitle>{isUpdate ? 'Update' : 'Add'} Destination</SheetTitle>
          <SheetDescription>
            Configure your Snowflake destination details.
          </SheetDescription>
        </SheetHeader>
        <Form {...form}>
          <form
            id='destinations-form'
            onSubmit={form.handleSubmit(onSubmit)}
            className='max-h-[calc(100vh-140px)] flex-1 space-y-4 overflow-y-auto px-4 py-4'
          >
            <FormField
              control={form.control}
              name='name'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Name</FormLabel>
                  <FormControl>
                    <Input {...field} placeholder='production-snowflake' />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name='type'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Type</FormLabel>
                  <Select
                    onValueChange={field.onChange}
                    defaultValue={field.value}
                  >
                    <FormControl>
                      <SelectTrigger>
                        <SelectValue placeholder='Select destination type' />
                      </SelectTrigger>
                    </FormControl>
                    <SelectContent>
                      <SelectItem value='SNOWFLAKE'>Snowflake</SelectItem>
                      <SelectItem value='KAFKA'>Kafka</SelectItem>
                      <SelectItem value='POSTGRES'>
                        <span>
                          Postgre<span style={{ color: '#316192' }}>SQL</span>
                        </span>
                      </SelectItem>
                    </SelectContent>
                  </Select>
                  <FormMessage />
                </FormItem>
              )}
            />
            {form.watch('type') === 'SNOWFLAKE' && (
              <>
                <div className='grid grid-cols-2 gap-4'>
                  <FormField
                    control={form.control}
                    name='config.account'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Account</FormLabel>
                        <FormControl>
                          <Input {...field} placeholder='xy12345' />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  <FormField
                    control={form.control}
                    name='config.user'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>User</FormLabel>
                        <FormControl>
                          <Input {...field} placeholder='etl_user' />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                </div>
                <div className='grid grid-cols-2 gap-4'>
                  <FormField
                    control={form.control}
                    name='config.landing_database'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>
                          Landing Database{' '}
                          <span className='text-xs text-muted-foreground'>
                            (Optional)
                          </span>
                        </FormLabel>
                        <FormControl>
                          <Input
                            {...field}
                            placeholder='LANDING (Default: Database)'
                          />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  <FormField
                    control={form.control}
                    name='config.landing_schema'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>
                          Landing Schema{' '}
                          <span className='text-xs text-muted-foreground'>
                            (Optional)
                          </span>
                        </FormLabel>
                        <FormControl>
                          <Input
                            {...field}
                            placeholder='PUBLIC (Default: Schema)'
                          />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                </div>
                <div className='grid grid-cols-2 gap-4'>
                  <FormField
                    control={form.control}
                    name='config.database'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Database</FormLabel>
                        <FormControl>
                          <Input {...field} placeholder='ANALYTICS' />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  <FormField
                    control={form.control}
                    name='config.schema'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Schema</FormLabel>
                        <FormControl>
                          <Input {...field} placeholder='PUBLIC' />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                </div>
                <FormField
                  control={form.control}
                  name='config.role'
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Role</FormLabel>
                      <FormControl>
                        <Input {...field} placeholder='ACCOUNTADMIN' />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={form.control}
                  name='config.warehouse'
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Warehouse</FormLabel>
                      <FormControl>
                        <Input {...field} placeholder='COMPUTE_WH' />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={form.control}
                  name='config.private_key'
                  render={({ field: { value, onChange, ...fieldProps } }) => (
                    <FormItem>
                      <FormLabel>Private Key</FormLabel>
                      <FormControl>
                        <div className='flex flex-col gap-2'>
                          <Input
                            {...fieldProps}
                            type='file'
                            accept='.p8,.pem,.key'
                            onChange={(event) => {
                              const file =
                                event.target.files && event.target.files[0]
                              if (file) {
                                const reader = new FileReader()
                                reader.onload = (e) => {
                                  const content = e.target?.result as string
                                  onChange(content)
                                }
                                reader.readAsText(file)
                              }
                            }}
                          />
                          {value && (
                            <div className='text-xs break-all text-muted-foreground'>
                              Key loaded ({value.length} chars)
                            </div>
                          )}
                        </div>
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={form.control}
                  name='config.private_key_passphrase'
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Private Key Passphrase</FormLabel>
                      <FormControl>
                        <Input
                          type='password'
                          {...field}
                          placeholder={
                            isUpdate
                              ? 'Leave blank to keep unchanged'
                              : 'Passphrase (if encrypted)'
                          }
                        />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              </>
            )}
            {form.watch('type') === 'POSTGRES' && (
              <>
                <div className='grid grid-cols-2 gap-4'>
                  <FormField
                    control={form.control}
                    name='config.host'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Host</FormLabel>
                        <FormControl>
                          <Input {...field} placeholder='localhost' />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  <FormField
                    control={form.control}
                    name='config.port'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Port</FormLabel>
                        <FormControl>
                          <Input
                            type='number'
                            {...field}
                            onChange={(e) =>
                              field.onChange(Number(e.target.value))
                            }
                            placeholder='5432'
                          />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                </div>
                <div className='grid grid-cols-2 gap-4'>
                  <FormField
                    control={form.control}
                    name='config.database'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Database</FormLabel>
                        <FormControl>
                          <Input {...field} placeholder='postgres' />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  <FormField
                    control={form.control}
                    name='config.user'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>User</FormLabel>
                        <FormControl>
                          <Input {...field} placeholder='postgres' />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                </div>
                <FormField
                  control={form.control}
                  name='config.password'
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Password</FormLabel>
                      <FormControl>
                        <Input
                          type='password'
                          {...field}
                          placeholder='password'
                        />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              </>
            )}
          </form>
        </Form>
        <SheetFooter className='gap-2 sm:space-x-0'>
          <Button
            type='button'
            variant='secondary'
            onClick={async () => {
              const values = form.getValues()
              const isValid = await form.trigger() // Trigger validation
              if (!isValid) return

              const promise = destinationsRepo.testConnection(values)
              toast.promise(promise, {
                loading: 'Testing connection...',
                success: (data) => {
                  if (data.error) throw new Error(data.message)
                  return 'Connection successful'
                },
                error: (err) => `Connection failed: ${err.message}`,
              })
            }}
          >
            Test Connection
          </Button>
          <SheetClose asChild>
            <Button variant='outline'>Close</Button>
          </SheetClose>
          <Button form='destinations-form' type='submit' disabled={isLoading}>
            {isLoading && <Loader2 className='mr-2 h-4 w-4 animate-spin' />}
            Save changes
          </Button>
        </SheetFooter>
      </SheetContent>
    </Sheet>
  )
}
