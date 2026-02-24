import { Button } from '@/components/ui/button'

import { pipelinesRepo } from '@/repo/pipelines'
import { useQuery } from '@tanstack/react-query'
import { ReactFlow, Background, Controls, type Node, type Edge, Position } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { Loader2, Plus, Snowflake } from 'lucide-react'
import { useState, useMemo } from 'react'
import { useParams } from '@tanstack/react-router'
import { AddDestinationModal } from '../components/add-destination-modal'

export default function PipelineFlowPage() {
  const { pipelineId } = useParams({ from: '/_authenticated/pipelines/$pipelineId' })
  const [openAddDest, setOpenAddDest] = useState(false)

  const { data: pipeline, isLoading } = useQuery({
    queryKey: ['pipelines', parseInt(pipelineId)],
    queryFn: () => pipelinesRepo.get(parseInt(pipelineId)),
  })

  // Determine existing destination IDs
  const existingDestinationIds = useMemo(() => {
    const ids = new Set<number>()
    pipeline?.destinations?.forEach((d) => ids.add(d.destination.id))
    return ids
  }, [pipeline])

  const { nodes, edges } = useMemo(() => {
    if (!pipeline) return { nodes: [], edges: [] }

    const nodes: Node[] = []
    const edges: Edge[] = []

    // Source Node
    if (pipeline.source) {
      nodes.push({
        id: 'source',
        type: 'input',
        position: { x: 100, y: 100 },
        data: { label: `${pipeline.source.name} (Source)` },
        style: { background: '#f0fdf4', borderColor: '#16a34a', width: 200 },
        sourcePosition: Position.Right,
      })
    }

    // Destination Nodes
    pipeline.destinations?.forEach((d, index) => {
      const destId = `dest-${d.destination.id}`
      const isSnowflake = d.destination.type.toLowerCase().includes('snowflake')

      const label = isSnowflake ? (
        <div className="flex items-center justify-center gap-2">
          <Snowflake className="h-4 w-4 text-blue-500" />
          <span>{d.destination.name}</span>
        </div>
      ) : (
        `${d.destination.name} (${d.destination.type})`
      )

      nodes.push({
        id: destId,
        type: 'output',
        position: { x: 500, y: 100 + index * 150 }, // Simple vertical stack
        data: { label },
        style: { background: '#eff6ff', borderColor: '#2563eb', width: 200 },
        targetPosition: Position.Left,
      })

      edges.push({
        id: `e-source-${d.destination.id}`,
        source: 'source',
        target: destId,
        animated: true,
        style: { stroke: '#2563eb' },
      })
    })

    return { nodes, edges }
  }, [pipeline])

  if (isLoading) {
    return (
      <div className='flex h-full items-center justify-center'>
        <Loader2 className='h-8 w-8 animate-spin' />
      </div>
    )
  }

  if (!pipeline) return <div>Pipeline not found</div>

  return (
    <div className='flex h-full flex-col'>
      <div className='flex items-center justify-between border-b px-6 py-4'>
        <div>
          <h1 className='text-2xl font-bold tracking-tight'>{pipeline.name}</h1>
          <p className='text-sm text-muted-foreground'>Visual flow editor</p>
        </div>
        <Button onClick={() => setOpenAddDest(true)}>
          <Plus className='mr-2 h-4 w-4' />
          Add Destination
        </Button>
      </div>

      <div className='flex-1 bg-slate-50'>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          fitView
          attributionPosition="bottom-right"
        >
          <Background />
          <Controls />
        </ReactFlow>
      </div>

      <AddDestinationModal
        open={openAddDest}
        setOpen={setOpenAddDest}
        pipelineId={pipeline.id}
        existingDestinationIds={existingDestinationIds}
      />
    </div>
  )
}
