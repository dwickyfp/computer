import { useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { chainRepo } from '@/repo/chains'
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
  Sheet,
  SheetClose,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet'
import { Switch } from '@/components/ui/switch'
import { Textarea } from '@/components/ui/textarea'
import {
  type ChainClient,
  chainClientFormSchema,
  type ChainClientForm,
} from '../data/schema'

type Props = {
  open: boolean
  onOpenChange: (open: boolean) => void
  currentRow?: ChainClient
}

export function ChainClientMutateDrawer({
  open,
  onOpenChange,
  currentRow,
}: Props) {
  const isUpdate = !!currentRow
  const queryClient = useQueryClient()

  const form = useForm<ChainClientForm>({
    resolver: zodResolver(chainClientFormSchema) as any,
    mode: 'onChange',
    defaultValues: (currentRow
      ? {
          name: currentRow.name,
          url: currentRow.url,
          port: currentRow.port ?? 8001,
          description: '',
          is_active: currentRow.is_active,
          source_chain_id: currentRow.source_chain_id ?? '',
        }
      : {
          name: '',
          url: '',
          port: 8001,
          description: '',
          is_active: true,
          source_chain_id: '',
        }) as any,
  })

  useEffect(() => {
    form.reset(
      currentRow
        ? {
            name: currentRow.name,
            url: currentRow.url,
            port: currentRow.port ?? 8001,
            description: '',
            is_active: currentRow.is_active,
            source_chain_id: currentRow.source_chain_id ?? '',
          }
        : {
            name: '',
            url: '',
            port: 8001,
            description: '',
            is_active: true,
            source_chain_id: '',
          }
    )
  }, [currentRow, form])

  const createMutation = useMutation({
    mutationFn: chainRepo.createClient,
    onSuccess: async () => {
      onOpenChange(false)
      form.reset()
      toast.success('Chain client created')
      await new Promise((r) => setTimeout(r, 300))
      await queryClient.invalidateQueries({ queryKey: ['chain-clients'] })
    },
    onError: () => toast.error('Failed to create chain client'),
  })

  const updateMutation = useMutation({
    mutationFn: (data: ChainClientForm) =>
      chainRepo.updateClient(currentRow!.id, data),
    onSuccess: async () => {
      onOpenChange(false)
      form.reset()
      toast.success('Chain client updated')
      await new Promise((r) => setTimeout(r, 300))
      await queryClient.invalidateQueries({ queryKey: ['chain-clients'] })
    },
    onError: () => toast.error('Failed to update chain client'),
  })

  const onSubmit = (data: ChainClientForm) => {
    const payload: any = { ...data }
    // Normalise: empty string → null so the backend clears the value
    if (!payload.source_chain_id || payload.source_chain_id.trim() === '') {
      payload.source_chain_id = null
    }
    delete payload.description

    if (isUpdate) {
      updateMutation.mutate(payload)
    } else {
      createMutation.mutate(payload)
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
          <SheetTitle>{isUpdate ? 'Update' : 'Add'} Chain Client</SheetTitle>
          <SheetDescription>
            Configure a remote Rosetta instance to connect to.
          </SheetDescription>
        </SheetHeader>
        <Form {...form}>
          <form
            id='chain-client-form'
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
                    <Input {...field} placeholder='production-west' />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            <FormField
              control={form.control}
              name='url'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Compute URL</FormLabel>
                  <FormControl>
                    <Input {...field} placeholder='remote-host or IP' />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            <FormField
              control={form.control}
              name='port'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Port</FormLabel>
                  <FormControl>
                    <Input {...field} type='number' placeholder='8001' />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            <FormField
              control={form.control}
              name='source_chain_id'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>
                    Source Chain ID{' '}
                    <span className='font-normal text-muted-foreground'>
                      (optional)
                    </span>
                  </FormLabel>
                  <FormControl>
                    <Input
                      {...field}
                      value={field.value ?? ''}
                      placeholder='e.g. 3  (auto-detected on first ingest if blank)'
                    />
                  </FormControl>
                  <p className='text-xs text-muted-foreground'>
                    The sender&apos;s destination ID — visible as{' '}
                    <code className='rounded bg-muted px-1 py-0.5 text-xs'>
                      X-Chain-ID
                    </code>{' '}
                    in their pipeline destination list. Leave blank to let the
                    receiver auto-detect it on first ingest.
                  </p>
                  <FormMessage />
                </FormItem>
              )}
            />

            <FormField
              control={form.control}
              name='description'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Description</FormLabel>
                  <FormControl>
                    <Textarea
                      {...field}
                      placeholder='Optional description...'
                      rows={3}
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            <FormField
              control={form.control}
              name='is_active'
              render={({ field }) => (
                <FormItem className='flex items-center justify-between rounded-lg border p-3'>
                  <div className='space-y-0.5'>
                    <FormLabel>Active</FormLabel>
                    <p className='text-xs text-muted-foreground'>
                      Enable or disable this client connection
                    </p>
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
          </form>
        </Form>
        <SheetFooter className='gap-2'>
          <SheetClose asChild>
            <Button variant='outline'>Close</Button>
          </SheetClose>
          <Button form='chain-client-form' type='submit' disabled={isLoading}>
            {isLoading && <Loader2 className='mr-2 h-4 w-4 animate-spin' />}
            {isUpdate ? 'Update' : 'Create'}
          </Button>
        </SheetFooter>
      </SheetContent>
    </Sheet>
  )
}
