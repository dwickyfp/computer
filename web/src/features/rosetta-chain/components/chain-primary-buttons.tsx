import { Plus } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useChain } from './chain-provider'

export function ChainPrimaryButtons() {
  const { setOpen } = useChain()

  return (
    <Button onClick={() => setOpen('create')}>
      Add Client <Plus className='ml-2 h-4 w-4' />
    </Button>
  )
}
