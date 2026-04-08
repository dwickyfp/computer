import { Badge } from '@/components/ui/badge'
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet'
import { ScrollArea } from '@/components/ui/scroll-area'
import { type DLQMessage } from '@/repo/dlq-manager'

type Props = {
  message: DLQMessage | null
  onOpenChange: (open: boolean) => void
  open: boolean
}

function formatJsonBlock(value: unknown) {
  if (value == null) {
    return 'null'
  }
  return JSON.stringify(value, null, 2)
}

function formatTimestamp(value: string | null | undefined) {
  if (!value) return 'Unknown'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return date.toLocaleString()
}

export function DLQMessagePreviewSheet({
  message,
  onOpenChange,
  open,
}: Props) {
  return (
    <Sheet
      open={open}
      onOpenChange={(nextOpen) => {
        onOpenChange(nextOpen)
      }}
    >
      <SheetContent className='w-full sm:max-w-2xl'>
        <SheetHeader>
          <div className='flex items-center gap-2'>
            <SheetTitle>DLQ Message Preview</SheetTitle>
            {message?.operation && <Badge variant='secondary'>{message.operation}</Badge>}
          </div>
          <SheetDescription>
            {message
              ? `Inspecting ${message.message_id} without consuming the DLQ stream.`
              : 'Select a row to inspect its DLQ payload.'}
          </SheetDescription>
        </SheetHeader>

        {message ? (
          <ScrollArea className='h-[calc(100vh-8rem)] px-4 pb-6'>
            <div className='space-y-4'>
              <div className='grid gap-3 rounded-lg border border-border/50 bg-muted/20 p-4 text-sm sm:grid-cols-2'>
                <PreviewMeta label='Message ID' value={message.message_id} mono />
                <PreviewMeta
                  label='Failed At'
                  value={formatTimestamp(message.first_failed_at)}
                />
                <PreviewMeta
                  label='Event Timestamp'
                  value={formatTimestamp(message.event_timestamp)}
                />
                <PreviewMeta
                  label='Retry Count'
                  value={String(message.retry_count)}
                />
                <PreviewMeta label='Source Table' value={message.table_name} mono />
                <PreviewMeta
                  label='Target Table'
                  value={message.table_name_target ?? 'Unknown'}
                  mono
                />
              </div>

              <PreviewSection title='Key'>{formatJsonBlock(message.key)}</PreviewSection>
              <PreviewSection title='Value'>
                {formatJsonBlock(message.value)}
              </PreviewSection>
              <PreviewSection title='Schema'>
                {formatJsonBlock(message.schema)}
              </PreviewSection>
              <PreviewSection title='Table Sync Config'>
                {formatJsonBlock(message.table_sync_config)}
              </PreviewSection>
            </div>
          </ScrollArea>
        ) : null}
      </SheetContent>
    </Sheet>
  )
}

function PreviewMeta({
  label,
  mono = false,
  value,
}: {
  label: string
  mono?: boolean
  value: string
}) {
  return (
    <div className='space-y-1'>
      <p className='text-xs font-medium tracking-wide text-muted-foreground uppercase'>
        {label}
      </p>
      <p className={mono ? 'font-mono text-sm break-all' : 'text-sm break-words'}>
        {value}
      </p>
    </div>
  )
}

function PreviewSection({
  children,
  title,
}: {
  children: string
  title: string
}) {
  return (
    <section className='space-y-2 rounded-lg border border-border/50 p-4'>
      <h3 className='text-sm font-semibold'>{title}</h3>
      <pre className='overflow-x-auto rounded-lg bg-muted p-4 font-mono text-xs leading-relaxed whitespace-pre-wrap break-words'>
        {children}
      </pre>
    </section>
  )
}
