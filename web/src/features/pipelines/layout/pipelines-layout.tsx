import { PipelinesSidebar } from "@/features/pipelines/components/pipelines-sidebar"
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/ui/resizable"
import { useEffect, useRef, useState } from "react"
import { type PanelImperativeHandle } from "react-resizable-panels"
import { ChevronRight } from "lucide-react"
import { cn } from "@/lib/utils"

const DEFAULT_SIDEBAR_WIDTH = 360
const MIN_SIDEBAR_WIDTH = 20
const AUTO_COLLAPSE_THRESHOLD_PERCENTAGE = 7

interface PipelinesLayoutProps {
    children: React.ReactNode
}

export function PipelinesLayout({ children }: PipelinesLayoutProps) {
    const [isCollapsed, setIsCollapsed] = useState(false)
    const [isAnimating, setIsAnimating] = useState(false)
    const [isResizeInteractionLocked, setIsResizeInteractionLocked] = useState(false)
    const panelRef = useRef<PanelImperativeHandle>(null)
    const handleRef = useRef<HTMLDivElement>(null)
    const activePointerIdRef = useRef<number | null>(null)
    const activePointerTypeRef = useRef<string>("mouse")
    const animationFrameRef = useRef<number | null>(null)
    const animationTimeoutRef = useRef<number | null>(null)

    useEffect(() => {
        if (!isResizeInteractionLocked) {
            return
        }

        const unlockResizeInteraction = (event: PointerEvent) => {
            if (!event.isTrusted) {
                return
            }

            activePointerIdRef.current = null
            setIsResizeInteractionLocked(false)
        }

        window.addEventListener("pointerup", unlockResizeInteraction, true)
        window.addEventListener("pointercancel", unlockResizeInteraction, true)

        return () => {
            window.removeEventListener("pointerup", unlockResizeInteraction, true)
            window.removeEventListener("pointercancel", unlockResizeInteraction, true)
        }
    }, [isResizeInteractionLocked])

    useEffect(() => {
        return () => {
            if (animationFrameRef.current !== null) {
                window.cancelAnimationFrame(animationFrameRef.current)
            }

            if (animationTimeoutRef.current !== null) {
                window.clearTimeout(animationTimeoutRef.current)
            }
        }
    }, [])

    const animatePanelTransition = (callback: () => void) => {
        if (animationFrameRef.current !== null) {
            window.cancelAnimationFrame(animationFrameRef.current)
        }

        if (animationTimeoutRef.current !== null) {
            window.clearTimeout(animationTimeoutRef.current)
        }

        setIsAnimating(true)

        animationFrameRef.current = window.requestAnimationFrame(() => {
            callback()
            animationFrameRef.current = null
            animationTimeoutRef.current = window.setTimeout(() => {
                setIsAnimating(false)
                animationTimeoutRef.current = null
            }, 310)
        })
    }

    const cancelResizeInteraction = () => {
        const activePointerId = activePointerIdRef.current

        if (activePointerId !== null && handleRef.current?.hasPointerCapture?.(activePointerId)) {
            handleRef.current.releasePointerCapture(activePointerId)
        }

        if (typeof document !== "undefined" && typeof PointerEvent !== "undefined") {
            document.dispatchEvent(new PointerEvent("pointerup", {
                bubbles: true,
                cancelable: true,
                button: 0,
                buttons: 0,
                pointerId: activePointerId ?? 1,
                pointerType: activePointerTypeRef.current,
            }))
        }
    }

    const handleExpand = () => {
        setIsCollapsed(false)
        animatePanelTransition(() => {
            panelRef.current?.resize(DEFAULT_SIDEBAR_WIDTH)
        })
    }

    return (
        <div className="relative h-full w-full">
            {isCollapsed && (
                <button
                    onClick={handleExpand}
                    className={cn(
                        "absolute left-0 top-1/2 z-50 flex h-16 w-4 -translate-y-1/2 items-center justify-center rounded-r-md border border-l-0 border-sidebar-border bg-sidebar transition-all group",
                        isResizeInteractionLocked
                            ? "pointer-events-none opacity-0"
                            : "cursor-pointer hover:bg-sidebar-accent"
                    )}
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
                    defaultSize={DEFAULT_SIDEBAR_WIDTH}
                    minSize={MIN_SIDEBAR_WIDTH}
                    maxSize={DEFAULT_SIDEBAR_WIDTH}
                    collapsible
                    onResize={(size, _panelId, previousSize) => {
                        const isCollapsedNow = size.asPercentage === 0
                        const isShrinking = previousSize
                            ? size.inPixels < previousSize.inPixels
                            : false

                        if (
                            previousSize &&
                            isShrinking &&
                            size.asPercentage <= AUTO_COLLAPSE_THRESHOLD_PERCENTAGE &&
                            !panelRef.current?.isCollapsed()
                        ) {
                            setIsResizeInteractionLocked(true)
                            cancelResizeInteraction()
                            animatePanelTransition(() => {
                                panelRef.current?.collapse()
                                setIsCollapsed(true)
                            })
                            return
                        }

                        setIsCollapsed(isCollapsedNow)
                    }}
                >
                    <PipelinesSidebar />
                </ResizablePanel>

                <ResizableHandle
                    withHandle
                    disabled={isResizeInteractionLocked}
                    elementRef={handleRef}
                    onPointerDownCapture={(event) => {
                        activePointerIdRef.current = event.pointerId
                        activePointerTypeRef.current = event.pointerType
                    }}
                />

                <ResizablePanel defaultSize={80}>
                    <div className="h-full w-full overflow-y-auto">
                        {children}
                    </div>
                </ResizablePanel>
            </ResizablePanelGroup>
        </div>
    )
}
