import { useMemo, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Loader2 } from 'lucide-react'
import { toast } from 'sonner'

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
import { Label } from '@/components/ui/label'
import { getApiErrorMessage, sourcesRepo } from '@/repo/sources'

interface SourceDetailsCreateTopicDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  sourceId: number
  topicPrefix?: string
}

export function SourceDetailsCreateTopicDialog({
  open,
  onOpenChange,
  sourceId,
  topicPrefix,
}: SourceDetailsCreateTopicDialogProps) {
  const queryClient = useQueryClient()
  const [topicName, setTopicName] = useState('')

  const trimmedTopicName = topicName.trim()
  const fullTopicName = useMemo(() => {
    if (!trimmedTopicName) {
      return topicPrefix ? `${topicPrefix}.<topic-name>` : '<topic-name>'
    }
    if (topicPrefix && trimmedTopicName.startsWith(`${topicPrefix}.`)) {
      return trimmedTopicName
    }
    return topicPrefix ? `${topicPrefix}.${trimmedTopicName}` : trimmedTopicName
  }, [topicPrefix, trimmedTopicName])

  const createTopicMutation = useMutation({
    mutationFn: async () => {
      await sourcesRepo.createTopic(sourceId, trimmedTopicName)
    },
    onSuccess: () => {
      toast.success(`Topic ${fullTopicName} created successfully`)
      queryClient.invalidateQueries({ queryKey: ['source-details', sourceId] })
      queryClient.invalidateQueries({
        queryKey: ['source-available-tables', sourceId],
      })
      queryClient.invalidateQueries({
        queryKey: ['source-kafka-topics-summary', sourceId],
      })
      queryClient.invalidateQueries({
        queryKey: ['source-kafka-topic-preview', sourceId],
      })
      setTopicName('')
      onOpenChange(false)
    },
    onError: (err) => {
      toast.error(getApiErrorMessage(err, 'Failed to create topic'))
    },
  })

  const handleOpenChange = (nextOpen: boolean) => {
    if (!nextOpen && !createTopicMutation.isPending) {
      setTopicName('')
    }
    onOpenChange(nextOpen)
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Create Topic</DialogTitle>
          <DialogDescription>
            Create a Kafka topic under this source namespace.
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 py-2">
          <div className="grid gap-2">
            <Label htmlFor="topic-name">Topic Name</Label>
            <Input
              id="topic-name"
              value={topicName}
              onChange={(event) => setTopicName(event.target.value)}
              placeholder="orders_cdc"
              autoFocus
            />
          </div>
          <div className="rounded-md border border-border/60 bg-muted/30 px-3 py-2 text-sm text-muted-foreground">
            <div>
              Final topic:
              {' '}
              <span className="font-mono text-foreground">{fullTopicName}</span>
            </div>
            <div>Defaults: retention 12 hours, partitions 1, replica factor 1.</div>
          </div>
        </div>
        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => handleOpenChange(false)}
            disabled={createTopicMutation.isPending}
          >
            Cancel
          </Button>
          <Button
            onClick={() => createTopicMutation.mutate()}
            disabled={!trimmedTopicName || createTopicMutation.isPending}
          >
            {createTopicMutation.isPending && (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            )}
            Create Topic
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
