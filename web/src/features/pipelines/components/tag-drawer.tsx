import { useState, useEffect, useRef } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { tagsRepo } from '@/repo/tags'
import { X, Hash, Loader2, Plus } from 'lucide-react'
import { toast } from 'sonner'
import { getApiErrorMessage } from '@/lib/handle-server-error'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { TagBadge } from './tag-badge'

interface TagDrawerProps {
  tableSyncId: number
  tableName: string
  open: boolean
  onClose: () => void
}

export function TagDrawer({
  tableSyncId,
  tableName,
  open,
  onClose,
}: TagDrawerProps) {
  const [tagInput, setTagInput] = useState('')
  const [showSuggestions, setShowSuggestions] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)
  const queryClient = useQueryClient()

  // Query existing tags for this table sync
  const { data: tableSyncTagsData, isLoading: loadingTags } = useQuery({
    queryKey: ['table-sync-tags', tableSyncId],
    queryFn: () => tagsRepo.getTableSyncTags(tableSyncId),
    enabled: open,
  })

  // Query suggestions based on input
  const { data: suggestionsData, isLoading: loadingSuggestions } = useQuery({
    queryKey: ['tag-suggestions', tagInput],
    queryFn: () => tagsRepo.search(tagInput, 10),
    enabled: showSuggestions && tagInput.length > 0,
  })

  // Add tag mutation
  const addTagMutation = useMutation({
    mutationFn: (tag: string) =>
      tagsRepo.addTagToTableSync(tableSyncId, { tag }),
    onSuccess: () => {
      toast.success('Tag added successfully')
      setTagInput('')
      setShowSuggestions(false)
      // Invalidate after 300ms to allow DB transaction to commit
      setTimeout(() => {
        queryClient.invalidateQueries({
          queryKey: ['table-sync-tags', tableSyncId],
        })
      }, 300)
    },
    onError: (error) => {
      toast.error(getApiErrorMessage(error, 'Failed to add tag'))
    },
  })

  // Remove tag mutation
  const removeTagMutation = useMutation({
    mutationFn: (tagId: number) =>
      tagsRepo.removeTagFromTableSync(tableSyncId, tagId),
    onSuccess: () => {
      toast.success('Tag removed successfully')
      // Invalidate after 300ms to allow DB transaction to commit
      setTimeout(() => {
        queryClient.invalidateQueries({
          queryKey: ['table-sync-tags', tableSyncId],
        })
      }, 300)
    },
    onError: (error) => {
      toast.error(getApiErrorMessage(error, 'Failed to remove tag'))
    },
  })

  const currentTags = tableSyncTagsData?.tags || []
  const suggestions = suggestionsData?.suggestions || []

  // Filter out already added tags from suggestions
  const filteredSuggestions = suggestions.filter(
    (suggestion) => !currentTags.some((tag) => tag.id === suggestion.id)
  )

  const handleAddTag = (tag: string) => {
    const trimmedTag = tag.trim()
    if (!trimmedTag) {
      toast.error('Tag cannot be empty')
      return
    }

    // Check if tag already exists in current tags (case-insensitive check)
    if (
      currentTags.some((t) => t.tag.toLowerCase() === trimmedTag.toLowerCase())
    ) {
      toast.error('Tag already added')
      return
    }

    addTagMutation.mutate(trimmedTag)
  }

  const handleRemoveTag = (tagId: number) => {
    removeTagMutation.mutate(tagId)
  }

  const handleInputChange = (value: string) => {
    setTagInput(value)
    setShowSuggestions(value.length > 0)
  }

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      if (filteredSuggestions.length > 0) {
        // Use first suggestion
        handleAddTag(filteredSuggestions[0].tag)
      } else if (tagInput.trim()) {
        // Create new tag
        handleAddTag(tagInput)
      }
    } else if (e.key === 'Escape') {
      setShowSuggestions(false)
    }
  }

  // Focus input when opened
  useEffect(() => {
    if (open) {
      setTimeout(() => {
        inputRef.current?.focus()
      }, 100)
    }
  }, [open])

  // Click outside to close suggestions
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as HTMLElement
      if (!target.closest('.tag-input-container')) {
        setShowSuggestions(false)
      }
    }

    document.addEventListener('click', handleClickOutside)
    return () => document.removeEventListener('click', handleClickOutside)
  }, [])

  if (!open) return null

  return (
    <div
      className='fixed top-2 bottom-2 left-[520px] flex w-[600px] animate-in flex-col rounded-2xl border bg-background shadow-2xl duration-300 slide-in-from-left-4'
      style={{ zIndex: 100 }}
      onClick={(e) => e.stopPropagation()}
      onMouseDown={(e) => e.stopPropagation()}
    >
      {/* Header */}
      <div className='flex items-center justify-between border-b bg-muted/30 px-6 py-4'>
        <div>
          <div className='flex items-center gap-2'>
            <Hash className='h-5 w-5 text-primary' />
            <h2 className='text-lg font-semibold'>Manage Tags</h2>
          </div>
          <p className='mt-1 text-sm text-muted-foreground'>
            Add tags to organize and group{' '}
            <span className='font-medium text-foreground'>{tableName}</span>
          </p>
        </div>
        <Button
          variant='ghost'
          size='icon'
          onClick={onClose}
          className='h-8 w-8'
        >
          <X className='h-4 w-4' />
        </Button>
      </div>

      {/* Content */}
      <div className='flex-1 overflow-y-auto p-6'>
        <div className='space-y-6'>
          {/* Add Tag Input */}
          <div className='tag-input-container relative'>
            <label className='mb-2 block text-sm font-medium'>Add Tag</label>
            <div className='relative'>
              <Input
                ref={inputRef}
                placeholder='Type tag name...'
                value={tagInput}
                onChange={(e) => handleInputChange(e.target.value)}
                onKeyDown={handleKeyDown}
                onFocus={() => tagInput.length > 0 && setShowSuggestions(true)}
                className='pr-10'
                disabled={addTagMutation.isPending}
              />
              {addTagMutation.isPending ? (
                <Loader2 className='absolute top-1/2 right-3 h-4 w-4 -translate-y-1/2 animate-spin text-muted-foreground' />
              ) : (
                <Button
                  size='icon'
                  variant='ghost'
                  className='absolute top-1/2 right-1 h-7 w-7 -translate-y-1/2'
                  onClick={() => handleAddTag(tagInput)}
                  disabled={!tagInput.trim()}
                >
                  <Plus className='h-4 w-4' />
                </Button>
              )}
            </div>

            {/* Suggestions Dropdown */}
            {showSuggestions && (
              <div className='absolute top-full right-0 left-0 z-50 mt-1 max-h-48 overflow-y-auto rounded-md border bg-popover shadow-lg'>
                {loadingSuggestions ? (
                  <div className='p-3 text-center'>
                    <Loader2 className='mx-auto h-4 w-4 animate-spin text-muted-foreground' />
                  </div>
                ) : filteredSuggestions.length > 0 ? (
                  <div className='py-1'>
                    {filteredSuggestions.map((suggestion) => (
                      <button
                        key={suggestion.id}
                        onClick={() => handleAddTag(suggestion.tag)}
                        className='flex w-full items-center gap-2 px-3 py-2 text-left text-sm transition-colors hover:bg-muted'
                      >
                        <Hash className='h-3 w-3 text-muted-foreground' />
                        {suggestion.tag}
                      </button>
                    ))}
                  </div>
                ) : tagInput.trim() ? (
                  <div className='p-3 text-sm text-muted-foreground'>
                    <div className='flex items-center gap-2'>
                      <Plus className='h-3 w-3' />
                      <span>
                        Press Enter to create "
                        <span className='font-medium text-foreground'>
                          {tagInput}
                        </span>
                        "
                      </span>
                    </div>
                  </div>
                ) : null}
              </div>
            )}
          </div>

          {/* Current Tags */}
          <div>
            <label className='mb-3 block text-sm font-medium'>
              Current Tags ({currentTags.length})
            </label>
            {loadingTags ? (
              <div className='flex items-center justify-center py-8'>
                <Loader2 className='h-6 w-6 animate-spin text-muted-foreground' />
              </div>
            ) : currentTags.length > 0 ? (
              <div className='flex flex-wrap gap-2'>
                {currentTags.map((tag) => (
                  <TagBadge
                    key={tag.id}
                    tag={tag.tag}
                    onRemove={() => handleRemoveTag(tag.id)}
                  />
                ))}
              </div>
            ) : (
              <div className='rounded-lg border-2 border-dashed py-8 text-center text-sm text-muted-foreground'>
                <Hash className='mx-auto mb-2 h-8 w-8 opacity-50' />
                <p>No tags added yet</p>
                <p className='mt-1 text-xs'>
                  Start typing to add your first tag
                </p>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
