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
import { destinationsRepo } from '@/repo/destinations'
import { pipelinesRepo } from '@/repo/pipelines'
import { cn } from '@/lib/utils'
import { zodResolver } from '@hookform/resolvers/zod'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Check, Database, Search, Snowflake } from 'lucide-react'
import { useEffect, useState } from 'react'
import { useForm } from 'react-hook-form'
import { toast } from 'sonner'
import { z } from 'zod'

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

  useEffect(() => {
    if (!open) {
      form.reset()
      setSearchQuery('')
      setSelectedDestId(null)
    }
  }, [open, form])

  // Filter out already added destinations and apply search
  const availableDestinations = destinations?.destinations
    .filter((d) => !existingDestinationIds.has(d.id))
    .filter((d) =>
      d.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      d.type.toLowerCase().includes(searchQuery.toLowerCase())
    )

  const getIconForType = (type: string) => {
    if (type.toLowerCase().includes('snowflake')) {
      return <Snowflake className="h-5 w-5 text-blue-500" />
    }
    return <Database className="h-5 w-5 text-muted-foreground" />
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

        <div className="space-y-4 py-4">
          <div className="relative">
            <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search destinations..."
              className="pl-9"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </div>

          <div className="grid max-h-[400px] grid-cols-1 gap-3 overflow-y-auto pr-1 sm:grid-cols-2">
            {availableDestinations?.length === 0 ? (
              <div className="col-span-full flex flex-col items-center justify-center py-8 text-center text-muted-foreground">
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
                      "cursor-pointer rounded-xl border p-4 transition-all hover:border-primary/50 hover:bg-slate-50",
                      isSelected
                        ? "border-primary ring-1 ring-primary bg-primary/5"
                        : "border-border"
                    )}
                  >
                    <div className="flex items-start justify-between">
                      <div className="flex items-center gap-3">
                        <div className={cn(
                          "flex h-10 w-10 items-center justify-center rounded-lg border bg-white shadow-sm",
                          isSelected ? "border-primary/20" : "border-slate-100"
                        )}>
                          {getIconForType(dest.type)}
                        </div>
                        <div>
                          <h3 className="font-medium leading-none tracking-tight text-foreground">
                            {dest.name}
                          </h3>
                          <p className="mt-1 text-xs text-muted-foreground uppercase tracking-wider font-semibold">
                            {dest.type}
                          </p>
                        </div>
                      </div>
                      {isSelected && (
                        <div className="flex h-5 w-5 items-center justify-center rounded-full bg-primary text-primary-foreground">
                          <Check className="h-3 w-3" />
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
          <Button variant="outline" onClick={() => setOpen(false)}>
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
