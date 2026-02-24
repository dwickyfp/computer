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
          chain_key: currentRow.chain_key,
          description: currentRow.description ?? '',
          is_active: currentRow.is_active,
        }
      : {
          name: '',
          url: '',
          chain_key: '',
          description: '',
          is_active: true,
        }) as any,
  })

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
    if (isUpdate) {
      updateMutation.mutate(data)
    } else {
      createMutation.mutate(data as any)
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
                    <Input {...field} placeholder='http://remote-host:8001' />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            <FormField
              control={form.control}
              name='chain_key'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Chain Key</FormLabel>
                  <FormControl>
                    <Input
                      {...field}
                      type='password'
                      placeholder='Remote instance chain key'
                    />
                  </FormControl>
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
