import { useEffect, useState } from 'react'
import { cn } from '@/lib/utils'
import { Separator } from '@/components/ui/separator'
import { SidebarTrigger } from '@/components/ui/sidebar'

type HeaderProps = React.HTMLAttributes<HTMLElement> & {
  fixed?: boolean
  ref?: React.Ref<HTMLElement>
  scrolled?: boolean
  trackScroll?: boolean
}

export function Header({
  className,
  fixed,
  children,
  scrolled,
  trackScroll = true,
  ...props
}: HeaderProps) {
  const [offset, setOffset] = useState(0)
  const shouldTrackScroll = fixed && trackScroll && scrolled === undefined

  useEffect(() => {
    if (!shouldTrackScroll) {
      return
    }

    const onScroll = () => {
      setOffset(document.body.scrollTop || document.documentElement.scrollTop)
    }

    // Add scroll listener to the body
    document.addEventListener('scroll', onScroll, { passive: true })
    onScroll()

    // Clean up the event listener on unmount
    return () => document.removeEventListener('scroll', onScroll)
  }, [shouldTrackScroll])

  const isScrolled = scrolled ?? offset > 10

  return (
    <header
      className={cn(
        'z-50 h-16',
        fixed && 'header-fixed peer/header sticky top-0 w-[inherit]',
        isScrolled && fixed ? 'shadow' : 'shadow-none',
        className
      )}
      {...props}
    >
      <div
        className={cn(
          'relative flex h-full items-center gap-3 p-4 sm:gap-4',
          fixed && 'after:absolute after:inset-0 after:-z-10',
          isScrolled &&
            fixed &&
            'after:bg-background/20 after:backdrop-blur-lg'
        )}
      >
        <SidebarTrigger variant='outline' className='max-md:scale-125' />
        <Separator orientation='vertical' className='h-6' />
        {children}
      </div>
    </header>
  )
}
