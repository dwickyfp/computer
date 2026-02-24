import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { chainRepo } from '@/repo/chains'
import { Copy, KeyRound, RefreshCw, Eye, EyeOff } from 'lucide-react'
import { toast } from 'sonner'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { ConfirmDialog } from '@/components/confirm-dialog'

export function ChainKeyCard() {
  const queryClient = useQueryClient()
  const [revealedKey, setRevealedKey] = useState<string | null>(null)
  const [isRevealing, setIsRevealing] = useState(false)
  const [confirmGenerate, setConfirmGenerate] = useState(false)
  const [newRawKey, setNewRawKey] = useState<string | null>(null)
  const [showNewKey, setShowNewKey] = useState(false)

  const { data: chainKey, isLoading } = useQuery({
    queryKey: ['chain-key'],
    queryFn: chainRepo.getKey,
    retry: false,
  })

  const generateMutation = useMutation({
    mutationFn: () => chainRepo.generateKey({}),
    onSuccess: async (result) => {
      setConfirmGenerate(false)
      setRevealedKey(null)
      setNewRawKey(result.chain_key)
      setShowNewKey(false)
      await new Promise((r) => setTimeout(r, 300))
      await queryClient.invalidateQueries({ queryKey: ['chain-key'] })
    },
    onError: () => toast.error('Failed to generate chain key'),
  })

  const toggleMutation = useMutation({
    mutationFn: (active: boolean) => chainRepo.toggleActive(active),
    onSuccess: async () => {
      toast.success('Chain status updated')
      await new Promise((r) => setTimeout(r, 300))
      await queryClient.invalidateQueries({ queryKey: ['chain-key'] })
    },
    onError: () => toast.error('Failed to toggle chain status'),
  })

  const handleCopyNewKey = async () => {
    if (!newRawKey) return
    try {
      await navigator.clipboard.writeText(newRawKey)
      toast.success('Chain key copied to clipboard')
    } catch {
      toast.error('Failed to copy key')
    }
  }

  const handleReveal = async () => {
    if (revealedKey) {
      setRevealedKey(null)
      return
    }
    setIsRevealing(true)
    try {
      const result = await chainRepo.revealKey()
      setRevealedKey(result.chain_key ?? null)
    } catch {
      toast.error('Failed to reveal key')
    } finally {
      setIsRevealing(false)
    }
  }

  const handleCopyRevealed = async () => {
    if (!revealedKey) return
    try {
      await navigator.clipboard.writeText(revealedKey)
      toast.success('Chain key copied to clipboard')
    } catch {
      toast.error('Failed to copy key')
    }
  }

  const hasKey = chainKey && chainKey.chain_key_masked

  return (
    <>
      <Card>
        <CardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
          <div className='space-y-1'>
            <CardTitle className='flex items-center gap-2 text-base font-semibold'>
              <KeyRound className='h-4 w-4' />
              Chain Key
            </CardTitle>
            <CardDescription>
              This key allows remote Rosetta instances to stream data into this
              instance.
            </CardDescription>
          </div>
          {hasKey && (
            <Badge variant={chainKey.is_active ? 'default' : 'secondary'}>
              {chainKey.is_active ? 'Accepting Connections' : 'Disabled'}
            </Badge>
          )}
        </CardHeader>
        <CardContent className='space-y-4'>
          {isLoading ? (
            <div className='h-10 animate-pulse rounded bg-muted' />
          ) : hasKey ? (
            <>
              <div className='flex items-center gap-2'>
                <code className='flex-1 rounded bg-muted px-3 py-2 font-mono text-sm break-all'>
                  {revealedKey ? revealedKey : '••••••••••••••••••••••••••••'}
                </code>
                {revealedKey && (
                  <Button
                    variant='ghost'
                    size='icon'
                    onClick={handleCopyRevealed}
                    title='Copy key'
                  >
                    <Copy className='h-4 w-4' />
                  </Button>
                )}
                <Button
                  variant='ghost'
                  size='icon'
                  onClick={handleReveal}
                  disabled={isRevealing}
                  title={revealedKey ? 'Hide key' : 'Reveal full key'}
                >
                  {revealedKey ? (
                    <EyeOff className='h-4 w-4' />
                  ) : (
                    <Eye className='h-4 w-4' />
                  )}
                </Button>
              </div>

              <div className='flex items-center justify-between'>
                <div className='flex items-center gap-2'>
                  <Switch
                    id='chain-active'
                    checked={chainKey.is_active}
                    onCheckedChange={(checked) =>
                      toggleMutation.mutate(checked)
                    }
                    disabled={toggleMutation.isPending}
                  />
                  <Label htmlFor='chain-active'>
                    Accept incoming connections
                  </Label>
                </div>
                <Button
                  variant='outline'
                  size='sm'
                  onClick={() => setConfirmGenerate(true)}
                >
                  <RefreshCw className='mr-2 h-3 w-3' />
                  Regenerate
                </Button>
              </div>
            </>
          ) : (
            <div className='flex flex-col items-center gap-3 py-4'>
              <p className='text-sm text-muted-foreground'>
                No chain key configured. Generate one to allow remote Rosetta
                instances to connect.
              </p>
              <Button
                onClick={() => generateMutation.mutate()}
                disabled={generateMutation.isPending}
              >
                <KeyRound className='mr-2 h-4 w-4' />
                Generate Chain Key
              </Button>
            </div>
          )}
        </CardContent>
      </Card>

      <ConfirmDialog
        open={confirmGenerate}
        onOpenChange={setConfirmGenerate}
        title='Regenerate Chain Key?'
        desc='This will invalidate the current key. All connected remote instances will need to update their configuration with the new key.'
        destructive
        handleConfirm={() => generateMutation.mutate()}
        isLoading={generateMutation.isPending}
        confirmText='Regenerate'
      />

      {/* One-time raw key display dialog */}
      <Dialog
        open={!!newRawKey}
        onOpenChange={(open) => {
          if (!open) setNewRawKey(null)
        }}
      >
        <DialogContent className='sm:max-w-lg'>
          <DialogHeader>
            <DialogTitle className='flex items-center gap-2'>
              <KeyRound className='h-4 w-4' />
              Chain Key Generated
            </DialogTitle>
            <DialogDescription>
              Copy this key now — it will not be shown again. You will need it
              to configure remote Rosetta instances that stream data into this
              instance.
            </DialogDescription>
          </DialogHeader>
          <div className='flex items-center gap-2 rounded-md border bg-muted p-3'>
            <code className='flex-1 font-mono text-sm break-all'>
              {showNewKey
                ? newRawKey
                : '••••••••••••••••••••••••••••••••••••••••'}
            </code>
            <Button
              variant='ghost'
              size='icon'
              onClick={() => setShowNewKey(!showNewKey)}
              title={showNewKey ? 'Hide' : 'Reveal key'}
            >
              {showNewKey ? (
                <EyeOff className='h-4 w-4' />
              ) : (
                <Eye className='h-4 w-4' />
              )}
            </Button>
            <Button
              variant='ghost'
              size='icon'
              onClick={handleCopyNewKey}
              title='Copy to clipboard'
            >
              <Copy className='h-4 w-4' />
            </Button>
          </div>
          <DialogFooter>
            <Button onClick={() => setNewRawKey(null)}>Done</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}
