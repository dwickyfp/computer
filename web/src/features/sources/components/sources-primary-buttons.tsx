import { Plus } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useSources } from './sources-provider'

export function SourcesPrimaryButtons() {
    const { setOpen } = useSources()
    return (
        <div className='flex gap-2'>
            <Button className='space-x-1' onClick={() => setOpen('create')}>
                <span>Add Source</span> <Plus size={18} />
            </Button>
        </div>
    )
}
