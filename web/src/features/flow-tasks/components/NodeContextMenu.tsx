/**
 * NodeContextMenu — right-click popup on a canvas node.
 * Shows: Edit Node, Preview Data, Delete.
 */

import { useEffect, useRef } from 'react'
import { Settings2, Eye, Trash2 } from 'lucide-react'

interface NodeContextMenuProps {
    x: number
    y: number
    nodeId: string
    nodeLabel: string
    onEdit: () => void
    onPreview: () => void
    onDelete: () => void
    onClose: () => void
}

export function NodeContextMenu({
    x,
    y,
    onEdit,
    onPreview,
    onDelete,
    onClose,
}: NodeContextMenuProps) {
    const menuRef = useRef<HTMLDivElement>(null)

    // Close on outside click or Escape
    useEffect(() => {
        const handleClick = (e: MouseEvent) => {
            if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
                onClose()
            }
        }
        const handleKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onClose()
        }
        document.addEventListener('mousedown', handleClick)
        document.addEventListener('keydown', handleKey)
        return () => {
            document.removeEventListener('mousedown', handleClick)
            document.removeEventListener('keydown', handleKey)
        }
    }, [onClose])

    const items = [
        {
            icon: <Settings2 className="h-3.5 w-3.5" />,
            label: 'Edit Node',
            onClick: () => { onEdit(); onClose() },
            className: 'text-foreground hover:bg-accent',
        },
        {
            icon: <Eye className="h-3.5 w-3.5 text-violet-500" />,
            label: 'Preview Data',
            onClick: () => { onPreview(); onClose() },
            className: 'text-foreground hover:bg-accent',
        },
        {
            icon: <Trash2 className="h-3.5 w-3.5 text-destructive" />,
            label: 'Delete',
            onClick: () => { onDelete(); onClose() },
            className: 'text-destructive hover:bg-destructive/10',
        },
    ]

    return (
        <div
            ref={menuRef}
            style={{ top: y, left: x }}
            className="fixed z-50 min-w-[160px] rounded-md border border-border bg-popover shadow-md py-1 animate-in fade-in-0 zoom-in-95 duration-100"
        >
            {items.map((item, i) => (
                <button
                    key={i}
                    onClick={item.onClick}
                    className={`flex w-full items-center gap-2.5 px-3 py-1.5 text-xs font-medium transition-colors ${item.className}`}
                >
                    {item.icon}
                    {item.label}
                </button>
            ))}
        </div>
    )
}
