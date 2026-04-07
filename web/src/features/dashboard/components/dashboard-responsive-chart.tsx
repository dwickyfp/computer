import {
  memo,
  useEffect,
  useRef,
  useState,
  type Dispatch,
  type ReactNode,
  type SetStateAction,
} from 'react'

interface DashboardResponsiveChartProps {
  children: (size: { height: number; width: number }) => ReactNode
  className?: string
  freeze?: boolean
}

function readContainerSize(element: HTMLDivElement) {
  return {
    width: Math.max(element.clientWidth, 1),
    height: Math.max(element.clientHeight, 1),
  }
}

function applyMeasuredSize(
  setSize: Dispatch<SetStateAction<{ height: number; width: number }>>,
  nextSize: { height: number; width: number }
) {
  setSize((currentSize) => {
    if (
      currentSize.width === nextSize.width &&
      currentSize.height === nextSize.height
    ) {
      return currentSize
    }

    return nextSize
  })
}

export const DashboardResponsiveChart = memo(function DashboardResponsiveChart({
  children,
  className,
  freeze = false,
}: DashboardResponsiveChartProps) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const frameRef = useRef<number | null>(null)
  const pendingSizeRef = useRef<{ height: number; width: number } | null>(null)
  const freezeRef = useRef(freeze)
  const [size, setSize] = useState({ width: 0, height: 0 })

  useEffect(() => {
    freezeRef.current = freeze

    if (freeze) {
      return
    }

    const nextSize =
      pendingSizeRef.current ??
      (containerRef.current ? readContainerSize(containerRef.current) : null)

    pendingSizeRef.current = null

    if (nextSize) {
      applyMeasuredSize(setSize, nextSize)
    }
  }, [freeze])

  useEffect(() => {
    const element = containerRef.current

    if (!element) {
      return
    }

    const scheduleMeasure = () => {
      const nextSize = readContainerSize(element)

      if (freezeRef.current) {
        pendingSizeRef.current = nextSize
        return
      }

      if (frameRef.current !== null) {
        cancelAnimationFrame(frameRef.current)
      }

      frameRef.current = requestAnimationFrame(() => {
        applyMeasuredSize(setSize, nextSize)
        frameRef.current = null
      })
    }

    scheduleMeasure()

    if (typeof ResizeObserver === 'undefined') {
      return () => {
        if (frameRef.current !== null) {
          cancelAnimationFrame(frameRef.current)
        }
      }
    }

    const observer = new ResizeObserver(() => {
      scheduleMeasure()
    })

    observer.observe(element)

    return () => {
      observer.disconnect()

      if (frameRef.current !== null) {
        cancelAnimationFrame(frameRef.current)
      }
    }
  }, [])

  return (
    <div ref={containerRef} className={className}>
      {size.width > 0 && size.height > 0 ? children(size) : null}
    </div>
  )
})
