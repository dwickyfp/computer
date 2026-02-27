import { useEffect } from 'react'
import { z } from 'zod'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useMutation, useQueryClient, useQuery } from '@tanstack/react-query'
import { catalogRepo } from '@/repo/catalog'
import { chainRepo } from '@/repo/chains'
import { pipelinesRepo } from '@/repo/pipelines'
import { sourcesRepo } from '@/repo/sources'
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

const formSchema = z
  .object({
    name: z
      .string()
      .min(1, 'Name is required')
      .regex(
        /^[a-z0-9-_]+$/,
        'Name must be alphanumeric, hyphen, or underscore'
      ),
    source_type: z.enum(['POSTGRES', 'ROSETTA', 'CATALOG_TABLE']),
    source_id: z.string().optional(),
    chain_client_id: z.string().optional(),
    catalog_database_id: z.string().optional(),
    catalog_table_id: z.string().optional(),
  })
  .superRefine((data, ctx) => {
    if (data.source_type === 'POSTGRES' && !data.source_id) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'Source is required',
        path: ['source_id'],
      })
    }
    if (data.source_type === 'CATALOG_TABLE' && !data.catalog_table_id) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'Catalog table is required',
        path: ['catalog_table_id'],
      })
    }
    if (data.source_type === 'ROSETTA' && !data.catalog_table_id) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'Catalog table is required for Rosetta Chain source',
        path: ['catalog_table_id'],
      })
    }
  })

interface PipelineCreateDrawerProps {
  open: boolean
  setOpen: (open: boolean) => void
}

export function PipelineCreateDrawer({
  open,
  setOpen,
}: PipelineCreateDrawerProps) {
  const queryClient = useQueryClient()
  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      name: '',
      source_type: 'POSTGRES',
      source_id: '',
      chain_client_id: '',
      catalog_database_id: '',
      catalog_table_id: '',
    },
  })

  const sourceType = form.watch('source_type')

  // Reset dependent fields when source type changes
  useEffect(() => {
    form.setValue('source_id', '')
    form.setValue('chain_client_id', '')
    form.setValue('catalog_database_id', '')
    form.setValue('catalog_table_id', '')
  }, [sourceType, form])

  // Fetch sources, chain clients, and existing pipelines
  const { data: sources } = useQuery({
    queryKey: ['sources'],
    queryFn: sourcesRepo.getAll,
  })
  const { data: chainClients } = useQuery({
    queryKey: ['chain-clients'],
    queryFn: chainRepo.getClients,
    enabled: sourceType === 'ROSETTA',
  })
  const { data: pipelines } = useQuery({
    queryKey: ['pipelines'],
    queryFn: pipelinesRepo.getAll,
  })
  const usedSourceIds = new Set(
    pipelines?.pipelines.map((p) => p.source_id).filter(Boolean)
  )

  const { mutate, isPending } = useMutation({
    mutationFn: (values: z.infer<typeof formSchema>) => {
      const payload: any = {
        name: values.name,
        source_type: values.source_type,
      }
      if (values.source_type === 'POSTGRES') {
        payload.source_id = parseInt(values.source_id!)
      } else if (values.source_type === 'CATALOG_TABLE') {
        payload.catalog_table_id = parseInt(values.catalog_table_id!)
      } else if (values.source_type === 'ROSETTA') {
        payload.catalog_table_id = parseInt(values.catalog_table_id!)
        if (values.chain_client_id) {
          payload.chain_client_id = parseInt(values.chain_client_id)
        }
      }
      return pipelinesRepo.create(payload)
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['pipelines'] })
      setOpen(false)
      form.reset()
      toast.success('Pipeline created successfully')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.message || 'Failed to create pipeline')
    },
  })

  function onSubmit(values: z.infer<typeof formSchema>) {
    mutate(values)
  }

  useEffect(() => {
    if (!open) {
      form.reset()
    }
  }, [open, form])

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetContent>
        <div className='mx-auto w-full max-w-sm'>
          <SheetHeader>
            <SheetTitle>Create Pipeline</SheetTitle>
            <SheetDescription>
              Create a new pipeline to move data from source to destination.
            </SheetDescription>
          </SheetHeader>
          <Form {...form}>
            <form
              onSubmit={form.handleSubmit(onSubmit)}
              className='space-y-4 p-4'
            >
              <FormField
                control={form.control}
                name='name'
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Name</FormLabel>
                    <FormControl>
                      <Input placeholder='my-pipeline' {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name='source_type'
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Source Type</FormLabel>
                    <Select
                      onValueChange={field.onChange}
                      defaultValue={field.value}
                    >
                      <FormControl>
                        <SelectTrigger className='w-full'>
                          <SelectValue placeholder='Select source type' />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        <SelectItem value='POSTGRES'>
                          PostgreSQL (CDC)
                        </SelectItem>
                        <SelectItem value='ROSETTA'>Rosetta Chain</SelectItem>
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />

              {sourceType === 'POSTGRES' && (
                <FormField
                  control={form.control}
                  name='source_id'
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Source</FormLabel>
                      <Select
                        onValueChange={field.onChange}
                        defaultValue={field.value}
                      >
                        <FormControl>
                          <SelectTrigger className='w-full'>
                            <SelectValue placeholder='Select a source' />
                          </SelectTrigger>
                        </FormControl>
                        <SelectContent>
                          {sources?.sources.map((source) => (
                            <SelectItem
                              key={source.id}
                              value={source.id.toString()}
                              disabled={usedSourceIds.has(source.id)}
                            >
                              {source.name}{' '}
                              {usedSourceIds.has(source.id) && '(Already Used)'}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              )}

              {sourceType === 'ROSETTA' && (
                <>
                  <FormField
                    control={form.control}
                    name='chain_client_id'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Chain Client (optional)</FormLabel>
                        <Select
                          onValueChange={(val) => {
                            field.onChange(val)
                            form.setValue('catalog_database_id', '')
                            form.setValue('catalog_table_id', '')
                          }}
                          value={field.value}
                        >
                          <FormControl>
                            <SelectTrigger className='w-full'>
                              <SelectValue placeholder='Select a chain client' />
                            </SelectTrigger>
                          </FormControl>
                          <SelectContent>
                            {chainClients?.map((c) => (
                              <SelectItem key={c.id} value={c.id.toString()}>
                                {c.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        <FormMessage />
                      </FormItem>
                    )}
                  />

                  <FormField
                    control={form.control}
                    name='catalog_database_id'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>My Database</FormLabel>
                        <Select
                          onValueChange={(val) => {
                            field.onChange(val)
                            form.setValue('catalog_table_id', '')
                          }}
                          value={field.value}
                        >
                          <FormControl>
                            <SelectTrigger className='w-full'>
                              <SelectValue placeholder='Select a database' />
                            </SelectTrigger>
                          </FormControl>
                          <SelectContent>
                            <CatalogDatabaseSelectOptions />
                          </SelectContent>
                        </Select>
                        <FormMessage />
                      </FormItem>
                    )}
                  />

                  {form.watch('catalog_database_id') && (
                    <FormField
                      control={form.control}
                      name='catalog_table_id'
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>My Table</FormLabel>
                          <Select
                            onValueChange={field.onChange}
                            value={field.value}
                          >
                            <FormControl>
                              <SelectTrigger className='w-full'>
                                <SelectValue placeholder='Select a table' />
                              </SelectTrigger>
                            </FormControl>
                            <SelectContent>
                              <CatalogTableSelectOptions
                                dbId={parseInt(
                                  form.watch('catalog_database_id')!
                                )}
                              />
                            </SelectContent>
                          </Select>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                  )}
                </>
              )}

              {sourceType === 'CATALOG_TABLE' && (
                <>
                  <FormField
                    control={form.control}
                    name='catalog_database_id'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Catalog Database</FormLabel>
                        <Select
                          onValueChange={(val) => {
                            field.onChange(val)
                            form.setValue('catalog_table_id', '') // Reset table when DB changes
                          }}
                          defaultValue={field.value}
                        >
                          <FormControl>
                            <SelectTrigger className='w-full'>
                              <SelectValue placeholder='Select a database' />
                            </SelectTrigger>
                          </FormControl>
                          <SelectContent>
                            <CatalogDatabaseSelectOptions />
                          </SelectContent>
                        </Select>
                        <FormMessage />
                      </FormItem>
                    )}
                  />

                  {form.watch('catalog_database_id') && (
                    <FormField
                      control={form.control}
                      name='catalog_table_id'
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>Catalog Table</FormLabel>
                          <Select
                            onValueChange={field.onChange}
                            defaultValue={field.value}
                          >
                            <FormControl>
                              <SelectTrigger className='w-full'>
                                <SelectValue placeholder='Select a table' />
                              </SelectTrigger>
                            </FormControl>
                            <SelectContent>
                              <CatalogTableSelectOptions
                                dbId={parseInt(
                                  form.watch('catalog_database_id')!
                                )}
                              />
                            </SelectContent>
                          </Select>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                  )}
                </>
              )}

              <SheetFooter>
                <Button type='submit' disabled={isPending}>
                  {isPending ? 'Creating...' : 'Create'}
                </Button>
                <SheetClose asChild>
                  <Button variant='outline'>Cancel</Button>
                </SheetClose>
              </SheetFooter>
            </form>
          </Form>
        </div>
      </SheetContent>
    </Sheet>
  )
}

function CatalogDatabaseSelectOptions() {
  const { data: databases, isLoading } = useQuery({
    queryKey: ['catalog-databases'],
    queryFn: catalogRepo.getDatabases,
  })

  if (isLoading)
    return (
      <SelectItem value='loading' disabled>
        Loading...
      </SelectItem>
    )
  if (!databases?.length)
    return (
      <SelectItem value='empty' disabled>
        No databases found
      </SelectItem>
    )

  return (
    <>
      {databases.map((db: any) => (
        <SelectItem key={db.id} value={db.id.toString()}>
          {db.name}
        </SelectItem>
      ))}
    </>
  )
}

function CatalogTableSelectOptions({ dbId }: { dbId: number }) {
  const { data: tables, isLoading } = useQuery({
    queryKey: ['catalog-tables', dbId],
    queryFn: () => catalogRepo.getTables(dbId),
    enabled: !!dbId,
  })

  if (isLoading)
    return (
      <SelectItem value='loading' disabled>
        Loading...
      </SelectItem>
    )
  if (!tables?.length)
    return (
      <SelectItem value='empty' disabled>
        No tables found
      </SelectItem>
    )

  return (
    <>
      {tables.map((tbl: any) => (
        <SelectItem key={tbl.id} value={tbl.id.toString()}>
          {tbl.table_name}
        </SelectItem>
      ))}
    </>
  )
}
