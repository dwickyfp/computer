import * as React from 'react'
import * as TabsPrimitive from '@radix-ui/react-tabs'
import { cn } from '@/lib/utils'

const CustomTabs = TabsPrimitive.Root

const CustomTabsList = React.forwardRef<
    React.ElementRef<typeof TabsPrimitive.List>,
    React.ComponentPropsWithoutRef<typeof TabsPrimitive.List>
>(({ className, children, ...props }, ref) => {
    const [indicatorStyle, setIndicatorStyle] = React.useState({ left: 0, width: 0 })
    const listRef = React.useRef<HTMLDivElement>(null)

    const updateIndicator = React.useCallback(() => {
        const list = listRef.current
        if (!list) return

        const activeTab = list.querySelector('[data-state="active"]') as HTMLElement
        if (activeTab) {
            setIndicatorStyle({
                left: activeTab.offsetLeft,
                width: activeTab.offsetWidth,
            })
        }
    }, [])

    React.useEffect(() => {
        updateIndicator()
        window.addEventListener('resize', updateIndicator)

        // Observe attribute changes on children to detect active state change
        const observer = new MutationObserver(updateIndicator)
        if (listRef.current) {
            observer.observe(listRef.current, {
                attributes: true,
                childList: true,
                subtree: true,
                attributeFilter: ['data-state']
            })
        }

        return () => {
            window.removeEventListener('resize', updateIndicator)
            observer.disconnect()
        }
    }, [updateIndicator])

    // Initial update after a short delay to ensure rendering
    React.useEffect(() => {
        const timer = setTimeout(updateIndicator, 50)
        return () => clearTimeout(timer)
    }, [updateIndicator])

    return (
        <TabsPrimitive.List
            ref={(node) => {
                // Handle both refs
                if (typeof ref === 'function') ref(node)
                else if (ref) ref.current = node
                // Local ref
                // @ts-ignore
                listRef.current = node
            }}
            className={cn(
                'relative inline-flex h-9 items-center justify-start rounded-none border-b bg-transparent p-0 text-muted-foreground w-full',
                className
            )}
            {...props}
        >
            {children}
            <div
                className="absolute bottom-0 h-0.5 bg-[#5999f8] transition-all duration-300 ease-in-out"
                style={{
                    left: `${indicatorStyle.left}px`,
                    width: `${indicatorStyle.width}px`,
                }}
            />
        </TabsPrimitive.List>
    )
})
CustomTabsList.displayName = TabsPrimitive.List.displayName

const CustomTabsTrigger = React.forwardRef<
    React.ElementRef<typeof TabsPrimitive.Trigger>,
    React.ComponentPropsWithoutRef<typeof TabsPrimitive.Trigger>
>(({ className, ...props }, ref) => (
    <TabsPrimitive.Trigger
        ref={ref}
        className={cn(
            'inline-flex items-center justify-center whitespace-nowrap py-1 px-4 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50',
            'border-b-2 border-transparent bg-transparent font-medium text-muted-foreground shadow-none transition-none data-[state=active]:text-[#5999f8] data-[state=active]:shadow-none',
            'relative h-9 rounded-none pb-3 pt-2', // Adjust height and padding for proper underline placement
            className
        )}
        {...props}
    />
))
CustomTabsTrigger.displayName = TabsPrimitive.Trigger.displayName

const CustomTabsContent = React.forwardRef<
    React.ElementRef<typeof TabsPrimitive.Content>,
    React.ComponentPropsWithoutRef<typeof TabsPrimitive.Content>
>(({ className, ...props }, ref) => (
    <TabsPrimitive.Content
        ref={ref}
        className={cn(
            'mt-2 ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2',
            'data-[state=active]:animate-fade-in-slide-up',
            className
        )}
        {...props}
    />
))
CustomTabsContent.displayName = TabsPrimitive.Content.displayName

export { CustomTabs, CustomTabsList, CustomTabsTrigger, CustomTabsContent }
