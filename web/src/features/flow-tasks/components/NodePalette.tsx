/**
 * NodePalette — left sidebar accordion listing all draggable node types.
 * Users drag a node type onto the canvas to add it.
 */

import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
} from '@/components/ui/accordion'
import {
    Database,
    Sparkles,
    BarChart2,
    GitMerge,
    Rows,
    Table2,
    PlusCircle,
    HardDriveDownload,
    StickyNote,
    Code2,
} from 'lucide-react'
import type { FlowNodeType } from '@/repo/flow-tasks'

interface NodeDef {
    type: FlowNodeType
    label: string
    description: string
    icon: React.ReactNode
    accentClass: string
}

const INPUT_NODES: NodeDef[] = [
    {
        type: 'input',
        label: 'Input',
        description: 'Read from a source or destination table',
        icon: <Database className="h-3.5 w-3.5" />,
        accentClass: 'border-l-emerald-500',
    },
]

const TRANSFORM_NODES: NodeDef[] = [
    {
        type: 'clean',
        label: 'Clean',
        description: 'Drop nulls, deduplicate, filter, rename columns',
        icon: <Sparkles className="h-3.5 w-3.5" />,
        accentClass: 'border-l-sky-500',
    },
    {
        type: 'aggregate',
        label: 'Aggregate',
        description: 'Group by and aggregate columns',
        icon: <BarChart2 className="h-3.5 w-3.5" />,
        accentClass: 'border-l-violet-500',
    },
    {
        type: 'join',
        label: 'Join',
        description: 'Join two datasets on key columns',
        icon: <GitMerge className="h-3.5 w-3.5" />,
        accentClass: 'border-l-orange-500',
    },
    {
        type: 'union',
        label: 'Union',
        description: 'Stack multiple datasets vertically',
        icon: <Rows className="h-3.5 w-3.5" />,
        accentClass: 'border-l-teal-500',
    },
    {
        type: 'pivot',
        label: 'Pivot / Unpivot',
        description: 'Reshape rows to columns or vice versa',
        icon: <Table2 className="h-3.5 w-3.5" />,
        accentClass: 'border-l-pink-500',
    },
    {
        type: 'new_rows',
        label: 'New Rows',
        description: 'Inject static rows into the flow',
        icon: <PlusCircle className="h-3.5 w-3.5" />,
        accentClass: 'border-l-amber-500',
    },
    {
        type: 'sql',
        label: 'SQL',
        description: 'Write a custom SQL expression',
        icon: <Code2 className="h-3.5 w-3.5" />,
        accentClass: 'border-l-indigo-500',
    },
]

const OUTPUT_NODES: NodeDef[] = [
    {
        type: 'output',
        label: 'Output',
        description: 'Write to a destination table',
        icon: <HardDriveDownload className="h-3.5 w-3.5" />,
        accentClass: 'border-l-rose-500',
    },
]

const UTILITY_NODES: NodeDef[] = [
    {
        type: 'note',
        label: 'Note',
        description: 'Add a free-text annotation to the canvas',
        icon: <StickyNote className="h-3.5 w-3.5" />,
        accentClass: 'border-l-amber-400',
    },
]

function DraggableNode({ node }: { node: NodeDef }) {
    const onDragStart = (event: React.DragEvent) => {
        event.dataTransfer.setData('application/reactflow-node-type', node.type)
        event.dataTransfer.setData('application/reactflow-node-label', node.label)
        event.dataTransfer.effectAllowed = 'move'
    }

    return (
        <div
            draggable
            onDragStart={onDragStart}
            className={`flex items-center gap-2 p-2 rounded-md border-l-4 ${node.accentClass} bg-muted/50 hover:bg-muted cursor-grab active:cursor-grabbing transition-colors mb-1.5`}
        >
            <span className="text-muted-foreground">{node.icon}</span>
            <div className="min-w-0">
                <p className="text-xs font-medium leading-none">{node.label}</p>
                <p className="text-[10px] text-muted-foreground mt-0.5 truncate">{node.description}</p>
            </div>
        </div>
    )
}

export function NodePalette() {
    return (
        <div className="flex flex-col h-full overflow-y-auto">
            <div className="px-3 py-2 border-b border-border/50">
                <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                    Node Palette
                </p>
                <p className="text-[10px] text-muted-foreground mt-0.5">Drag nodes onto the canvas</p>
            </div>

            <Accordion
                type="multiple"
                defaultValue={['input', 'transform', 'output', 'utilities']}
                className="px-2 py-2"
            >
                <AccordionItem value="input" className="border-none">
                    <AccordionTrigger className="py-1.5 text-xs font-semibold hover:no-underline">
                        Input
                    </AccordionTrigger>
                    <AccordionContent className="pb-2 pt-0">
                        {INPUT_NODES.map((n) => (
                            <DraggableNode key={n.type} node={n} />
                        ))}
                    </AccordionContent>
                </AccordionItem>

                <AccordionItem value="transform" className="border-none">
                    <AccordionTrigger className="py-1.5 text-xs font-semibold hover:no-underline">
                        Transform
                    </AccordionTrigger>
                    <AccordionContent className="pb-2 pt-0">
                        {TRANSFORM_NODES.map((n) => (
                            <DraggableNode key={n.type} node={n} />
                        ))}
                    </AccordionContent>
                </AccordionItem>

                <AccordionItem value="output" className="border-none">
                    <AccordionTrigger className="py-1.5 text-xs font-semibold hover:no-underline">
                        Output
                    </AccordionTrigger>
                    <AccordionContent className="pb-2 pt-0">
                        {OUTPUT_NODES.map((n) => (
                            <DraggableNode key={n.type} node={n} />
                        ))}
                    </AccordionContent>
                </AccordionItem>

                <AccordionItem value="utilities" className="border-none">
                    <AccordionTrigger className="py-1.5 text-xs font-semibold hover:no-underline">
                        Utilities
                    </AccordionTrigger>
                    <AccordionContent className="pb-2 pt-0">
                        {UTILITY_NODES.map((n) => (
                            <DraggableNode key={n.type} node={n} />
                        ))}
                    </AccordionContent>
                </AccordionItem>
            </Accordion>
        </div>
    )
}
