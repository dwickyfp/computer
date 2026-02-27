import { useEffect, useState } from 'react'
import { z } from 'zod'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { chainRepo } from '@/repo/chains'
import { destinationsRepo } from '@/repo/destinations'
import { pipelinesRepo } from '@/repo/pipelines'
import { Check, Database, Search, Snowflake, Unplug } from 'lucide-react'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'

const formSchema = z.object({
  destination_id: z.string().min(1, 'Destination is required'),
})

interface AddDestinationModalProps {
  open: boolean
  setOpen: (open: boolean) => void
  pipelineId: number
  existingDestinationIds: Set<number>
}

export function AddDestinationModal({
  open,
  setOpen,
  pipelineId,
  existingDestinationIds,
}: AddDestinationModalProps) {
  const queryClient = useQueryClient()
  const [searchQuery, setSearchQuery] = useState('')
  const [selectedDestId, setSelectedDestId] = useState<string | null>(null)

  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      destination_id: '',
    },
  })

  // Fetch destinations
  const { data: destinations } = useQuery({
    queryKey: ['destinations'],
    queryFn: destinationsRepo.getAll,
  })

  const { mutate, isPending } = useMutation({
    mutationFn: (values: z.infer<typeof formSchema>) =>
      pipelinesRepo.addDestination(pipelineId, parseInt(values.destination_id)),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['pipeline', pipelineId] })
      setOpen(false)
      form.reset()
      setSearchQuery('')
      setSelectedDestId(null)
      toast.success('Destination added successfully')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.message || 'Failed to add destination')
    },
  })

  function onSubmit() {
    if (selectedDestId) {
      mutate({ destination_id: selectedDestId })
    }
  }

  // When the modal opens, ensure every chain client has a linked ROSETTA
  // destination so it appears in the list without manual setup.
  useEffect(() => {
    if (open) {
      chainRepo
        .syncDestinations()
        .then(({ created }) => {
          if (created > 0) {
            // New destinations were created — refresh the list
            queryClient.invalidateQueries({ queryKey: ['destinations'] })
          }
        })
        .catch(() => {
          // Non-fatal — destinations list will still show whatever exists
        })
    } else {
      form.reset()
      setSearchQuery('')
      setSelectedDestId(null)
    }
  }, [open, form, queryClient])

  // Filter out already added destinations and apply search
  const availableDestinations = destinations?.destinations
    .filter((d) => !existingDestinationIds.has(d.id))
    .filter(
      (d) =>
        d.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
        d.type.toLowerCase().includes(searchQuery.toLowerCase())
    )

  const getIconForType = (type: string) => {
    if (type.toLowerCase().includes('snowflake')) {
      return <Snowflake className='h-5 w-5 text-blue-500' />
    }
    if (type.toLowerCase().includes('rosetta')) {
      return <Unplug className='h-5 w-5 text-purple-500' />
    }
    return <Database className='h-5 w-5 text-muted-foreground' />
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogContent className='sm:max-w-[600px]'>
        <DialogHeader>
          <DialogTitle>Add Destination</DialogTitle>
          <DialogDescription>
            Select a destination to add to your pipeline flow.
          </DialogDescription>
        </DialogHeader>

        <div className='space-y-4 py-4'>
          <div className='relative'>
            <Search className='absolute top-2.5 left-2.5 h-4 w-4 text-muted-foreground' />
            <Input
              placeholder='Search destinations...'
              className='pl-9'
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </div>

          <div className='grid max-h-[400px] grid-cols-1 gap-3 overflow-y-auto pr-1 sm:grid-cols-2'>
            {availableDestinations?.length === 0 ? (
              <div className='col-span-full flex flex-col items-center justify-center py-8 text-center text-muted-foreground'>
                <p>No destinations found</p>
              </div>
            ) : (
              availableDestinations?.map((dest) => {
                const isSelected = selectedDestId === dest.id.toString()
                return (
                  <div
                    key={dest.id}
                    onClick={() => {
                      setSelectedDestId(dest.id.toString())
                      form.setValue('destination_id', dest.id.toString())
                    }}
                    className={cn(
                      'group cursor-pointer rounded-xl border p-4 transition-all duration-200 ease-in-out hover:border-[#d6e6ff] hover:bg-accent/50 hover:shadow-md active:scale-[0.98]',
                      isSelected
                        ? 'border-[#d6e6ff] bg-primary/5 shadow-sm ring-1 ring-[#d6e6ff]'
                        : 'border-border bg-card'
                    )}
                  >
                    <div className='flex items-start justify-between'>
                      <div className='flex items-center gap-3'>
                        <div
                          className={cn(
                            'flex h-10 w-10 items-center justify-center rounded-lg border shadow-sm transition-all duration-200 group-hover:shadow',
                            isSelected
                              ? 'border-primary/20 bg-primary/5'
                              : 'border-border bg-background'
                          )}
                        >
                          {getIconForType(dest.type)}
                        </div>
                        <div>
                          <h3 className='leading-none font-medium tracking-tight text-foreground'>
                            {dest.name}
                          </h3>
                          <p className='mt-1 text-xs font-semibold tracking-wider text-muted-foreground uppercase'>
                            {dest.type}
                          </p>
                        </div>
                      </div>
                      {isSelected && (
                        <div className='flex h-5 w-5 items-center justify-center rounded-full bg-primary text-primary-foreground shadow-sm'>
                          <Check className='h-3 w-3' />
                        </div>
                      )}
                    </div>
                  </div>
                )
              })
            )}
          </div>
        </div>

        <DialogFooter>
          <Button variant='outline' onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button
            onClick={form.handleSubmit(onSubmit)}
            disabled={isPending || !selectedDestId}
          >
            {isPending ? 'Adding...' : 'Add Destination'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
