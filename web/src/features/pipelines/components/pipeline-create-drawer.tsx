import { useEffect } from 'react'
import { z } from 'zod'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useMutation, useQueryClient, useQuery } from '@tanstack/react-query'
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
    source_type: z.enum(['POSTGRES', 'ROSETTA']),
    source_id: z.string().optional(),
    chain_client_id: z.string().optional(),
  })
  .superRefine((data, ctx) => {
    if (data.source_type === 'POSTGRES' && !data.source_id) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'Source is required',
        path: ['source_id'],
      })
    }
    if (data.source_type === 'ROSETTA' && !data.chain_client_id) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'Chain client is required',
        path: ['chain_client_id'],
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
    },
  })

  const sourceType = form.watch('source_type')

  // Fetch sources, chain clients, and existing pipelines
  const { data: sources } = useQuery({
    queryKey: ['sources'],
    queryFn: sourcesRepo.getAll,
  })
  const { data: pipelines } = useQuery({
    queryKey: ['pipelines'],
    queryFn: pipelinesRepo.getAll,
  })
  const { data: chainClients } = useQuery({
    queryKey: ['chain-clients'],
    queryFn: chainRepo.getClients,
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
      } else {
        payload.chain_client_id = parseInt(values.chain_client_id!)
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
                <FormField
                  control={form.control}
                  name='chain_client_id'
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Chain Client</FormLabel>
                      <Select
                        onValueChange={field.onChange}
                        defaultValue={field.value}
                      >
                        <FormControl>
                          <SelectTrigger className='w-full'>
                            <SelectValue placeholder='Select a chain client' />
                          </SelectTrigger>
                        </FormControl>
                        <SelectContent>
                          {chainClients
                            ?.filter((c) => c.is_active)
                            .map((client) => (
                              <SelectItem
                                key={client.id}
                                value={client.id.toString()}
                              >
                                {client.name} ({client.url})
                              </SelectItem>
                            ))}
                        </SelectContent>
                      </Select>
                      <FormMessage />
                    </FormItem>
                  )}
                />
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
