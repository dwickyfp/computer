import { useState } from 'react'
import { formatDistanceToNow } from 'date-fns'
import {
  Bell,
  Inbox,
  Trash2,
  AlertCircle,
  AlertTriangle,
  Info,
  CheckCheck,
  MoreVertical,
} from 'lucide-react'
import { useNotifications } from '@/hooks/use-notifications'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  CustomTabs,
  CustomTabsContent,
  CustomTabsList,
  CustomTabsTrigger,
} from '@/components/ui/custom-tabs'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from '@/components/ui/hover-card'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { ScrollArea } from '@/components/ui/scroll-area'
import { SidebarMenuButton, useSidebar } from '@/components/ui/sidebar'

export function NotificationPopover() {
  const { isMobile } = useSidebar()
  const {
    notifications,
    unreadCount,
    markAllAsRead,
    deleteNotification,
    markAsRead,
    deleteAllNotifications,
  } = useNotifications()
  const [isOpen, setIsOpen] = useState(false)

  const [tempReadIds, setTempReadIds] = useState<number[]>([])

  const handleOpenChange = (open: boolean) => {
    setIsOpen(open)
    if (!open) {
      if (unreadCount > 0) {
        markAllAsRead()
      }
      // Clear temp read IDs when closing
      setTempReadIds([])
    }
  }

  // Filter unread notifications:
  // Show if it is NOT read OR if it is in tempReadIds (meaning we just marked it as read)
  const unreadNotifications = notifications.filter(
    (n) => !n.is_read || tempReadIds.includes(n.id)
  )

  const getIcon = (type: string) => {
    switch (type) {
      case 'ERROR':
        return <AlertCircle className='h-4 w-4 text-red-500' />
      case 'WARNING':
        return <AlertTriangle className='h-4 w-4 text-yellow-500' />
      default:
        return <Info className='h-4 w-4 text-blue-500' />
    }
  }

  const NotificationList = ({
    items,
    emptyMessage,
  }: {
    items: typeof notifications
    emptyMessage: string
  }) => {
    if (items.length === 0) {
      return (
        <div className='flex h-full flex-col items-center justify-center gap-3 p-8 text-center text-muted-foreground select-none'>
          <div className='rounded-full bg-muted/30 p-4'>
            <Inbox className='h-10 w-10 stroke-1 opacity-50' />
          </div>
          <div className='space-y-1'>
            <p className='text-lg font-semibold text-foreground'>
              {emptyMessage}
            </p>
            {items === unreadNotifications && (
              <p className='text-sm'>All caught up!</p>
            )}
          </div>
        </div>
      )
    }

    return (
      <ScrollArea className='h-full'>
        <div className='flex flex-col'>
          {items.map((notification) => (
            <div
              key={notification.id}
              className={`group relative flex items-start gap-3 border-b p-4 transition-colors hover:bg-muted/50 ${
                !notification.is_read ? 'bg-muted/30' : ''
              }`}
            >
              <div className='mt-1 shrink-0'>{getIcon(notification.type)}</div>
              <div className='min-w-0 flex-1 space-y-1 overflow-hidden'>
                <div className='flex items-start justify-between gap-2'>
                  <p
                    className={`min-w-0 flex-1 text-sm leading-tight font-medium break-words ${!notification.is_read ? 'text-foreground' : 'text-muted-foreground'}`}
                    style={{
                      wordBreak: 'break-word',
                      overflowWrap: 'anywhere',
                    }}
                  >
                    {notification.title}
                  </p>
                  <span className='shrink-0 text-xs whitespace-nowrap text-muted-foreground tabular-nums'>
                    {formatDistanceToNow(new Date(notification.created_at), {
                      addSuffix: true,
                    })}
                  </span>
                </div>

                <HoverCard openDelay={200}>
                  <HoverCardTrigger asChild>
                    <p
                      className={`text-sm ${!notification.is_read ? 'text-foreground/90' : 'text-muted-foreground'} line-clamp-2 cursor-default break-words`}
                      style={{
                        wordBreak: 'break-word',
                        overflowWrap: 'anywhere',
                      }}
                    >
                      {notification.message}
                    </p>
                  </HoverCardTrigger>
                  <HoverCardContent
                    side='right'
                    align='start'
                    className='z-50 w-80'
                  >
                    <div className='space-y-2 text-sm'>
                      <p
                        className='font-semibold break-words'
                        style={{
                          wordBreak: 'break-word',
                          overflowWrap: 'anywhere',
                        }}
                      >
                        {notification.title}
                      </p>
                      <p
                        className='break-words text-muted-foreground'
                        style={{
                          wordBreak: 'break-word',
                          overflowWrap: 'anywhere',
                        }}
                      >
                        {notification.message}
                      </p>
                    </div>
                  </HoverCardContent>
                </HoverCard>
              </div>

              {/* Actions Group - Visible on Hover */}
              <div className='absolute top-2 right-2 flex items-center gap-1 rounded-md border bg-background/80 opacity-0 shadow-sm backdrop-blur-sm transition-opacity group-hover:opacity-100'>
                {!notification.is_read && (
                  <Button
                    variant='ghost'
                    size='icon'
                    className='h-6 w-6 text-muted-foreground hover:text-primary'
                    onClick={(e) => {
                      e.stopPropagation()
                      markAsRead(notification.id)
                      setTempReadIds((prev) => [...prev, notification.id])
                    }}
                    title='Mark as read'
                  >
                    <CheckCheck className='h-3 w-3' />
                  </Button>
                )}
                <Button
                  variant='ghost'
                  size='icon'
                  className='h-6 w-6 text-muted-foreground hover:text-destructive'
                  onClick={(e) => {
                    e.stopPropagation()
                    deleteNotification(notification.id)
                  }}
                  title='Delete'
                >
                  <Trash2 className='h-3 w-3' />
                </Button>
              </div>

              {/* Unread Indicator Dot */}
              {!notification.is_read && (
                <div className='absolute top-0 bottom-0 left-0 w-0.75 bg-primary' />
              )}
            </div>
          ))}
        </div>
      </ScrollArea>
    )
  }

  return (
    <Popover open={isOpen} onOpenChange={handleOpenChange}>
      <PopoverTrigger asChild>
        <SidebarMenuButton
          size='lg'
          tooltip='Notifications'
          className='relative flex aspect-square h-8 w-8 shrink-0 items-center justify-center overflow-visible rounded-lg p-0 data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground'
        >
          <Bell
            className={`h-4 w-4 ${unreadCount > 0 ? 'animate-bell-ring' : ''}`}
            style={unreadCount > 0 ? { color: '#1b7df7' } : undefined}
          />
          {unreadCount > 0 && (
            <span className='pointer-events-none absolute -top-1.5 -right-1.5 z-10 flex h-4 w-4 items-center justify-center rounded-full bg-red-500 text-[10px] font-bold text-white shadow-sm ring-2 ring-background'>
              {unreadCount > 9 ? '9+' : unreadCount}
            </span>
          )}
        </SidebarMenuButton>
      </PopoverTrigger>
      <PopoverContent
        className='flex h-150 w-105 flex-col overflow-hidden border-border p-0 shadow-xl'
        side={isMobile ? 'bottom' : 'top'}
        align={isMobile ? 'end' : 'start'}
        sideOffset={12}
      >
        <div className='flex items-center justify-between bg-background px-6 pt-5 pb-2'>
          <h4 className='text-lg font-semibold'>Notifications</h4>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button
                variant='ghost'
                size='icon'
                className='h-8 w-8 text-muted-foreground hover:text-foreground'
              >
                <MoreVertical className='h-5 w-5' />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align='end'>
              <DropdownMenuItem onClick={() => markAllAsRead()}>
                <CheckCheck className='mr-2 h-4 w-4' />
                <span>Mark all as read</span>
              </DropdownMenuItem>
              <DropdownMenuItem
                onClick={() => deleteAllNotifications()}
                className='text-destructive focus:text-destructive'
              >
                <Trash2 className='mr-2 h-4 w-4' />
                <span>Clear all notifications</span>
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>

        <CustomTabs
          defaultValue='unread'
          className='flex w-full flex-1 flex-col overflow-hidden bg-background'
        >
          <div className='border-b border-border/40 px-6'>
            <CustomTabsList className='h-auto w-full justify-start gap-6 border-0 bg-transparent p-0'>
              <CustomTabsTrigger
                value='unread'
                className='rounded-none border-b-2 border-transparent px-0 py-3 transition-none data-[state=active]:border-primary data-[state=active]:bg-transparent data-[state=active]:shadow-none'
              >
                Unread
                {unreadCount > 0 && (
                  <Badge
                    variant='secondary'
                    className='ml-2 h-5 border-0 bg-red-500 px-1.5 text-[10px] text-white hover:bg-red-600'
                  >
                    {unreadCount}
                  </Badge>
                )}
              </CustomTabsTrigger>
              <CustomTabsTrigger
                value='all'
                className='rounded-none border-b-2 border-transparent px-0 py-3 transition-none data-[state=active]:border-primary data-[state=active]:bg-transparent data-[state=active]:shadow-none'
              >
                All
              </CustomTabsTrigger>
            </CustomTabsList>
          </div>

          <div className='relative flex-1 overflow-hidden bg-background'>
            <CustomTabsContent
              value='unread'
              className='absolute inset-0 mt-0 h-full ring-0 outline-none focus-visible:ring-0'
            >
              <NotificationList
                items={unreadNotifications}
                emptyMessage='No new notifications'
              />
            </CustomTabsContent>
            <CustomTabsContent
              value='all'
              className='absolute inset-0 mt-0 h-full ring-0 outline-none focus-visible:ring-0'
            >
              <NotificationList
                items={notifications}
                emptyMessage='No notifications'
              />
            </CustomTabsContent>
          </div>
        </CustomTabs>
      </PopoverContent>
    </Popover>
  )
}
