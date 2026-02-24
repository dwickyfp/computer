import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import {
  Sheet,
  SheetClose,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet'
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
import { useMutation, useQueryClient, useQuery } from '@tanstack/react-query'
import { pipelinesRepo } from '@/repo/pipelines'
import { sourcesRepo } from '@/repo/sources'
import { toast } from 'sonner'
import { useEffect } from 'react'

const formSchema = z.object({
  name: z.string().min(1, 'Name is required').regex(/^[a-z0-9-_]+$/, 'Name must be alphanumeric, hyphen, or underscore'),
  source_id: z.string().min(1, 'Source is required'),
})

interface PipelineCreateDrawerProps {
  open: boolean
  setOpen: (open: boolean) => void
}

export function PipelineCreateDrawer({ open, setOpen }: PipelineCreateDrawerProps) {
  const queryClient = useQueryClient()
  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      name: '',
      source_id: '',
    },
  })

  // Fetch sources, destinations, and existing pipelines for validation
  const { data: sources } = useQuery({ queryKey: ['sources'], queryFn: sourcesRepo.getAll })
  const { data: pipelines } = useQuery({ queryKey: ['pipelines'], queryFn: pipelinesRepo.getAll })

  const usedSourceIds = new Set(pipelines?.pipelines.map((p) => p.source_id))

  const { mutate, isPending } = useMutation({
    mutationFn: (values: z.infer<typeof formSchema>) =>
        pipelinesRepo.create({
            name: values.name,
            source_id: parseInt(values.source_id),
        }),
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
            <form onSubmit={form.handleSubmit(onSubmit)} className='space-y-4 p-4'>
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
                name='source_id'
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Source</FormLabel>
                    <Select onValueChange={field.onChange} defaultValue={field.value}>
                      <FormControl>
                        <SelectTrigger className="w-full">
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
                            {source.name} {usedSourceIds.has(source.id) && '(Already Used)'}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />
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
