import { PipelinesSidebar } from "@/features/pipelines/components/pipelines-sidebar"
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/ui/resizable"
import { useState, useRef, useEffect } from "react"
import { type PanelImperativeHandle } from "react-resizable-panels"
import { ChevronRight } from "lucide-react"
import { cn } from "@/lib/utils"

interface PipelinesLayoutProps {
    children: React.ReactNode
}

export function PipelinesLayout({ children }: PipelinesLayoutProps) {
    const [isCollapsed, setIsCollapsed] = useState(false)
    const [isAnimating, setIsAnimating] = useState(false)
    const [containerWidth, setContainerWidth] = useState(0)
    const containerRef = useRef<HTMLDivElement>(null)
    const panelRef = useRef<PanelImperativeHandle>(null)

    useEffect(() => {
        if (!containerRef.current) return

        const observer = new ResizeObserver((entries) => {
            const entry = entries[0]
            if (entry) {
                setContainerWidth(entry.contentRect.width)
            }
        })

        observer.observe(containerRef.current)
        return () => observer.disconnect()
    }, [])

    const minSizePercentage = containerWidth > 0 ? (100 / containerWidth) * 100 : 0
    const maxSizePercentage = 360 // Default to 30% if width unknown

    const handleExpand = () => {
        setIsAnimating(true)
        panelRef.current?.resize(maxSizePercentage)
        // Disable animation after it completes
        setTimeout(() => setIsAnimating(false), 310)
    }

    return (
        <div ref={containerRef} className="relative h-full w-full">
            {isCollapsed && (
                <button
                    onClick={handleExpand}
                    className="absolute left-0 top-1/2 -translate-y-1/2 z-50 flex h-16 w-4 items-center justify-center rounded-r-md border border-l-0 bg-sidebar border-sidebar-border hover:bg-sidebar-accent transition-all cursor-pointer group"
                    title="Expand sidebar"
                >
                    <ChevronRight className="h-4 w-4 text-muted-foreground group-hover:text-foreground transition-colors" />
                </button>
            )}
            <ResizablePanelGroup
                className={cn(
                    "h-full w-full rounded-lg border-t border-border",
                    isAnimating && "[&_[data-panel]]:transition-[flex-grow,width] [&_[data-panel]]:duration-300 [&_[data-panel]]:ease-in-out"
                )}
                orientation="horizontal"
            >
                <ResizablePanel
                    panelRef={panelRef}
                    defaultSize={maxSizePercentage}
                    minSize={minSizePercentage}
                    maxSize={maxSizePercentage}
                    collapsible
                    onResize={(size) => {
                        setIsCollapsed(size.asPercentage === 0)
                    }}
                >
                    <PipelinesSidebar />
                </ResizablePanel>

                <ResizableHandle withHandle />

                <ResizablePanel defaultSize={80}>
                    <div className="h-full w-full overflow-y-auto">
                        {children}
                    </div>
                </ResizablePanel>
            </ResizablePanelGroup>
        </div>
    )
}
