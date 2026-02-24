import { Plus, RefreshCw } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useDestinations } from './destinations-provider'
import { cn } from '@/lib/utils'
import { formatDistanceToNow } from 'date-fns'

interface DestinationsPrimaryButtonsProps {
    onRefreshAll?: () => void
    isRefreshing?: boolean
    lastRefreshedAt?: Date | null
}

export function DestinationsPrimaryButtons({ onRefreshAll, isRefreshing, lastRefreshedAt }: DestinationsPrimaryButtonsProps) {
    const { setOpen } = useDestinations()
    return (
        <div className='flex items-center gap-3'>
            {lastRefreshedAt && (
                <span className='text-xs text-muted-foreground'>
                    Last refresh{' '}
                    {formatDistanceToNow(lastRefreshedAt, { addSuffix: true })}
                </span>
            )}
            <Button
                variant='outline'
                className='space-x-1'
                onClick={onRefreshAll}
                disabled={isRefreshing}
            >
                <RefreshCw
                    size={16}
                    className={cn(isRefreshing && 'animate-spin')}
                />
                <span>Refresh Table List</span>
            </Button>
            <Button className='space-x-1' onClick={() => setOpen('create')}>
                <span>Add Destination</span> <Plus size={18} />
            </Button>
        </div>
    )
}
