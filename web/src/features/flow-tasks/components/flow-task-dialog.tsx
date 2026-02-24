import { useEffect } from 'react'
import { useMutation } from '@tanstack/react-query'
import { useForm } from 'react-hook-form'
import { z } from 'zod'
import { zodResolver } from '@hookform/resolvers/zod'
import { Loader2 } from 'lucide-react'
import { toast } from 'sonner'

import { Button } from '@/components/ui/button'
import {
    Dialog,
    DialogContent,
    DialogHeader,
    DialogTitle,
    DialogFooter,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Label } from '@/components/ui/label'
import { flowTasksRepo, type FlowTask } from '@/repo/flow-tasks'

const formSchema = z.object({
    name: z.string().min(1, 'Name is required').max(200),
    description: z.string().optional(),
})
type FormValues = z.infer<typeof formSchema>

interface FlowTaskDialogProps {
    open: boolean
    onClose: () => void
    onSaved: () => void
    existing?: FlowTask | null
}

export function FlowTaskDialog({ open, onClose, onSaved, existing }: FlowTaskDialogProps) {
    const { register, handleSubmit, reset, formState: { errors } } =
        useForm<FormValues>({
            resolver: zodResolver(formSchema),
            defaultValues: { name: existing?.name ?? '', description: existing?.description ?? '' },
        })

    useEffect(() => {
        if (open) {
            reset({ name: existing?.name ?? '', description: existing?.description ?? '' })
        }
    }, [open, existing, reset])

    const createMutation = useMutation({
        mutationFn: (data: FormValues) => flowTasksRepo.create(data),
        onSuccess: () => {
            toast.success('Flow task created')
            onClose()
            setTimeout(() => onSaved(), 300)
        },
        onError: () => toast.error('Failed to create flow task'),
    })

    const updateMutation = useMutation({
        mutationFn: (data: FormValues) => flowTasksRepo.update(existing!.id, data),
        onSuccess: () => {
            toast.success('Flow task updated')
            onClose()
            setTimeout(() => onSaved(), 300)
        },
        onError: () => toast.error('Failed to update flow task'),
    })

    const onSubmit = (data: FormValues) => {
        if (existing) {
            updateMutation.mutate(data)
        } else {
            createMutation.mutate(data)
        }
    }

    return (
        <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
            <DialogContent>
                <DialogHeader>
                    <DialogTitle>{existing ? 'Edit Flow Task' : 'New Flow Task'}</DialogTitle>
                </DialogHeader>
                <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
                    <div className="space-y-2">
                        <Label>Name</Label>
                        <Input {...register('name')} placeholder="My ETL Flow" />
                        {errors.name && (
                            <p className="text-xs text-destructive">{errors.name.message}</p>
                        )}
                    </div>
                    <div className="space-y-2">
                        <Label>Description (optional)</Label>
                        <Textarea
                            {...register('description')}
                            placeholder="What this flow does…"
                            rows={3}
                        />
                    </div>
                    <DialogFooter>
                        <Button type="button" variant="outline" onClick={onClose}>
                            Cancel
                        </Button>
                        <Button type="submit" disabled={createMutation.isPending || updateMutation.isPending}>
                            {(createMutation.isPending || updateMutation.isPending) && <Loader2 className="h-4 w-4 animate-spin mr-2" />}
                            {existing ? 'Save Changes' : 'Create'}
                        </Button>
                    </DialogFooter>
                </form>
            </DialogContent>
        </Dialog>
    )
}
