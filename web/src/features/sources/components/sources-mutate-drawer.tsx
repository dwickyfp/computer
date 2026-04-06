import { useState } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useMutation, useQueryClient } from '@tanstack/react-query'
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
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from '@/components/ui/accordion'
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
import { type Source, sourceFormSchema, type SourceForm } from '../data/schema'
import { sourcesRepo, type SourceCreate } from '@/repo/sources'

type SourcesMutateDrawerProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  currentRow?: Source
}

const DEFAULT_KAFKA_FORMAT = 'PLAIN_JSON'

function normalizeKafkaFormat(value?: string): string {
  void value
  return DEFAULT_KAFKA_FORMAT
}

function buildSourcePayload(data: SourceForm): SourceCreate {
  if (data.type === 'KAFKA') {
    const config: Record<string, unknown> = {
      bootstrap_servers: data.bootstrap_servers,
      topic_prefix: data.topic_prefix,
      auto_offset_reset: data.auto_offset_reset || 'earliest',
      format: normalizeKafkaFormat(data.format),
    }

    if (data.security_protocol) config.security_protocol = data.security_protocol
    if (data.sasl_mechanism) config.sasl_mechanism = data.sasl_mechanism
    if (data.sasl_username) config.sasl_username = data.sasl_username
    if (data.sasl_password) config.sasl_password = data.sasl_password
    if (data.ssl_ca_location) config.ssl_ca_location = data.ssl_ca_location
    if (data.ssl_certificate_location) {
      config.ssl_certificate_location = data.ssl_certificate_location
    }
    if (data.ssl_key_location) config.ssl_key_location = data.ssl_key_location

    return {
      name: data.name,
      type: 'KAFKA',
      config,
    }
  }

  const config: Record<string, unknown> = {
    host: data.pg_host,
    port: data.pg_port || 5432,
    database: data.pg_database,
    username: data.pg_username,
    publication_name: data.publication_name,
    replication_name: data.replication_name,
  }

  if (data.pg_password) {
    config.password = data.pg_password
  }

  return {
    name: data.name,
    type: 'POSTGRES',
    config,
  }
}

export function SourcesMutateDrawer({
  open,
  onOpenChange,
  currentRow,
}: SourcesMutateDrawerProps) {
  const isUpdate = !!currentRow
  const queryClient = useQueryClient()
  const [isTesting, setIsTesting] = useState(false)

  const form = useForm<SourceForm>({
    resolver: zodResolver(sourceFormSchema) as any,
    mode: 'onChange',
    defaultValues: currentRow
      ? {
          name: currentRow.name,
          type: currentRow.type,
          pg_host: currentRow.pg_host || '',
          pg_port: currentRow.pg_port || 5432,
          pg_database: currentRow.pg_database || '',
          pg_username: currentRow.pg_username || '',
          pg_password: '',
          publication_name: currentRow.publication_name || '',
          replication_name: currentRow.replication_name || '',
          bootstrap_servers: currentRow.bootstrap_servers || '',
          topic_prefix: currentRow.topic_prefix || '',
          auto_offset_reset: currentRow.auto_offset_reset || 'earliest',
          format: normalizeKafkaFormat(currentRow.format),
          security_protocol: '',
          sasl_mechanism: '',
          sasl_username: '',
          sasl_password: '',
          ssl_ca_location: '',
          ssl_certificate_location: '',
          ssl_key_location: '',
        }
      : {
          name: '',
          type: 'POSTGRES',
          pg_host: '',
          pg_port: 5432,
          pg_database: '',
          pg_username: '',
          pg_password: '',
          publication_name: '',
          replication_name: '',
          bootstrap_servers: '',
          topic_prefix: '',
          auto_offset_reset: 'earliest',
          format: DEFAULT_KAFKA_FORMAT,
          security_protocol: '',
          sasl_mechanism: '',
          sasl_username: '',
          sasl_password: '',
          ssl_ca_location: '',
          ssl_certificate_location: '',
          ssl_key_location: '',
        },
  })

  const sourceType = form.watch('type')

  const createMutation = useMutation({
    mutationFn: (data: SourceForm) => sourcesRepo.create(buildSourcePayload(data)),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['sources'] })
      onOpenChange(false)
      form.reset()
      toast.success('Source created successfully')
    },
    onError: (error) => {
      toast.error('Failed to create source')
      console.error(error)
    },
  })

  const updateMutation = useMutation({
    mutationFn: (data: SourceForm) =>
      sourcesRepo.update(currentRow!.id, buildSourcePayload(data)),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['sources'] })
      queryClient.invalidateQueries({ queryKey: ['source-details', currentRow?.id] })
      onOpenChange(false)
      form.reset()
      toast.success('Source updated successfully')
    },
    onError: (error) => {
      toast.error('Failed to update source')
      console.error(error)
    },
  })

  const onSubmit = (data: SourceForm) => {
    if (isUpdate) {
      updateMutation.mutate(data)
      return
    }
    createMutation.mutate(data)
  }

  const handleTestConnection = async () => {
    const isValid = await form.trigger()
    if (!isValid) {
      toast.error('Please complete the required connection fields first')
      return
    }

    const values = form.getValues()
    if (
      values.type === 'POSTGRES' &&
      isUpdate &&
      !values.pg_password &&
      currentRow?.type === 'POSTGRES'
    ) {
      toast.error('Enter the PostgreSQL password to run a live connection test')
      return
    }

    setIsTesting(true)
    try {
      const result = await sourcesRepo.testConnection(buildSourcePayload(values))
      if (result) {
        toast.success('Connection successful')
      } else {
        toast.error('Connection failed')
      }
    } catch (error) {
      toast.error('Error testing connection')
      console.error(error)
    } finally {
      setIsTesting(false)
    }
  }

  const isLoading = createMutation.isPending || updateMutation.isPending

  return (
    <Sheet
      open={open}
      onOpenChange={(value) => {
        onOpenChange(value)
        form.reset()
      }}
    >
      <SheetContent className='flex w-full flex-col sm:max-w-md'>
        <SheetHeader className='text-start'>
          <SheetTitle>{isUpdate ? 'Update' : 'Add'} Source</SheetTitle>
          <SheetDescription>
            Configure a PostgreSQL or Kafka CDC source.
          </SheetDescription>
        </SheetHeader>
        <Form {...form}>
          <form
            id='sources-form'
            onSubmit={form.handleSubmit(onSubmit)}
            className='flex-1 space-y-4 overflow-y-auto px-4 py-4'
          >
            <FormField
              control={form.control}
              name='name'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Name</FormLabel>
                  <FormControl>
                    <Input {...field} placeholder='orders-cdc' />
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
                  <Select onValueChange={field.onChange} defaultValue={field.value}>
                    <FormControl>
                      <SelectTrigger>
                        <SelectValue placeholder='Select source type' />
                      </SelectTrigger>
                    </FormControl>
                    <SelectContent>
                      <SelectItem value='POSTGRES'>PostgreSQL</SelectItem>
                      <SelectItem value='KAFKA'>Kafka</SelectItem>
                    </SelectContent>
                  </Select>
                  <FormMessage />
                </FormItem>
              )}
            />

            {sourceType === 'POSTGRES' ? (
              <>
                <div className='grid grid-cols-2 gap-4'>
                  <FormField
                    control={form.control}
                    name='pg_host'
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
                    name='pg_port'
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
                          />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                </div>
                <FormField
                  control={form.control}
                  name='pg_database'
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
                  name='pg_username'
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Username</FormLabel>
                      <FormControl>
                        <Input {...field} placeholder='postgres' />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={form.control}
                  name='pg_password'
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Password</FormLabel>
                      <FormControl>
                        <Input
                          type='password'
                          {...field}
                          placeholder={
                            isUpdate
                              ? 'Leave blank to keep unchanged'
                              : 'Password'
                          }
                        />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <div className='grid grid-cols-2 gap-4'>
                  <FormField
                    control={form.control}
                    name='publication_name'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Publication</FormLabel>
                        <FormControl>
                          <Input {...field} placeholder='dbz_publication' />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  <FormField
                    control={form.control}
                    name='replication_name'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Replication Slot</FormLabel>
                        <FormControl>
                          <Input
                            {...field}
                            placeholder='dbz_replication_slot'
                          />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                </div>

                <Accordion type='single' collapsible className='w-full'>
                  <AccordionItem
                    value='sql-commands'
                    className='rounded-md border-b-0 bg-muted px-4'
                  >
                    <AccordionTrigger className='py-3 text-xs font-medium hover:no-underline'>
                      Run these SQL commands on your source database
                    </AccordionTrigger>
                    <AccordionContent className='space-y-3 pb-4 text-xs text-muted-foreground'>
                      <div>
                        <p className='mb-1'>1. Create replication slot:</p>
                        <code className='relative block whitespace-pre-wrap break-all rounded bg-background px-[0.3rem] py-[0.2rem] font-mono font-semibold'>
                          SELECT pg_create_logical_replication_slot(
                          '{form.watch('replication_name') || 'dbz_replication_slot'}',
                          'pgoutput');
                        </code>
                      </div>
                      <div>
                        <p className='mb-1'>2. Create publication:</p>
                        <code className='relative block whitespace-pre-wrap break-all rounded bg-background px-[0.3rem] py-[0.2rem] font-mono font-semibold'>
                          CREATE PUBLICATION{' '}
                          {form.watch('publication_name') || 'dbz_publication'}{' '}
                          FOR ALL TABLES;
                        </code>
                      </div>
                    </AccordionContent>
                  </AccordionItem>
                </Accordion>
              </>
            ) : (
              <>
                <FormField
                  control={form.control}
                  name='bootstrap_servers'
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Bootstrap Servers</FormLabel>
                      <FormControl>
                        <Input {...field} placeholder='localhost:9092' />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={form.control}
                  name='topic_prefix'
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Topic Prefix</FormLabel>
                      <FormControl>
                        <Input {...field} placeholder='dbserver1.public' />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <div className='grid grid-cols-2 gap-4'>
                  <FormField
                    control={form.control}
                    name='auto_offset_reset'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Offset Reset</FormLabel>
                        <Select
                          onValueChange={field.onChange}
                          defaultValue={field.value || 'earliest'}
                        >
                          <FormControl>
                            <SelectTrigger className='w-full min-w-[200px]'>
                              <SelectValue placeholder='Select offset reset' />
                            </SelectTrigger>
                          </FormControl>
                          <SelectContent>
                            <SelectItem value='earliest'>earliest</SelectItem>
                            <SelectItem value='latest'>latest</SelectItem>
                          </SelectContent>
                        </Select>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  <FormField
                    control={form.control}
                    name='format'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Format</FormLabel>
                        <Select
                          onValueChange={field.onChange}
                          defaultValue={normalizeKafkaFormat(field.value)}
                        >
                          <FormControl>
                            <SelectTrigger className='w-full'>
                              <SelectValue placeholder='Select format' />
                            </SelectTrigger>
                          </FormControl>
                          <SelectContent>
                            <SelectItem value={DEFAULT_KAFKA_FORMAT}>
                              Plain JSON
                            </SelectItem>
                          </SelectContent>
                        </Select>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                </div>
              </>
            )}

            <div className='pt-2'>
              <Button
                type='button'
                variant='secondary'
                className='w-full'
                onClick={handleTestConnection}
                disabled={isTesting}
              >
                {isTesting && (
                  <Loader2 className='mr-2 h-4 w-4 animate-spin' />
                )}
                Test Connection
              </Button>
            </div>
          </form>
        </Form>
        <SheetFooter className='gap-2 sm:space-x-0'>
          <SheetClose asChild>
            <Button variant='outline'>Close</Button>
          </SheetClose>
          <Button form='sources-form' type='submit' disabled={isLoading}>
            {isLoading && <Loader2 className='mr-2 h-4 w-4 animate-spin' />}
            Save changes
          </Button>
        </SheetFooter>
      </SheetContent>
    </Sheet>
  )
}
