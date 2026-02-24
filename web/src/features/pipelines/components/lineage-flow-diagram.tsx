import { useEffect, useMemo, useState } from 'react'
import {
  ReactFlow,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  MarkerType,
  Position,
  Handle,
  type Node,
  type Edge,
  BackgroundVariant,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import {
  Database,
  Table,
  ChevronDown,
  ChevronRight,
  Layers,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { useTheme } from '@/context/theme-provider'

interface LineageData {
  source_tables: { table: string; type: string; schema?: string }[]
  source_columns: string[]
  output_columns: string[]
  column_lineage: Record<string, { sources: string[]; transform: string }>
  referenced_tables: string[]
  ctes?: { name: string; source_tables: string[]; columns: string[] }[]
}

interface LineageFlowDiagramProps {
  lineage: LineageData
  destinationName?: string
  sourceName?: string
}

// Parse table name to extract schema and table parts
function parseTableName(fullName: string): {
  schema: string
  table: string
  database?: string
} {
  const parts = fullName.split('.')
  if (parts.length >= 3) {
    return {
      database: parts[0],
      schema: parts[1],
      table: parts.slice(2).join('.'),
    }
  } else if (parts.length === 2) {
    return { schema: parts[0], table: parts[1] }
  }
  return { schema: 'public', table: fullName }
}

// Table Card Node - Main visualization for source/destination tables
function TableCardNode({
  data,
}: {
  data: {
    tableName: string
    schema: string
    database?: string
    type: 'source' | 'cte' | 'destination'
    columns: string[]
    isExpanded?: boolean
  }
}) {
  const [expanded, setExpanded] = useState(data.isExpanded ?? false)

  const typeConfig = {
    source: {
      border: 'border-blue-400 dark:border-blue-600',
      bg: 'bg-blue-50 dark:bg-blue-950/50',
      headerBg: 'bg-blue-100 dark:bg-blue-900/50',
      iconColor: 'text-blue-500',
      badge: 'bg-blue-500',
      badgeText: 'Table',
    },
    cte: {
      border: 'border-violet-400 dark:border-violet-600',
      bg: 'bg-violet-50 dark:bg-violet-950/50',
      headerBg: 'bg-violet-100 dark:bg-violet-900/50',
      iconColor: 'text-violet-500',
      badge: 'bg-violet-500',
      badgeText: 'CTE',
    },
    destination: {
      border: 'border-green-400 dark:border-green-600',
      bg: 'bg-green-50 dark:bg-green-950/50',
      headerBg: 'bg-green-100 dark:bg-green-900/50',
      iconColor: 'text-green-500',
      badge: 'bg-green-500',
      badgeText: 'Output',
    },
  }

  const config = typeConfig[data.type]

  return (
    <div
      className={cn(
        'min-w-[220px] rounded-lg border-2 shadow-md',
        config.border,
        config.bg
      )}
    >
      {/* Handles */}
      {data.type !== 'source' && (
        <Handle
          type='target'
          position={Position.Left}
          className='!h-4 !w-4 !rounded-full !border-2 !border-white !bg-blue-500 dark:!border-gray-800'
        />
      )}
      {data.type !== 'destination' && (
        <Handle
          type='source'
          position={Position.Right}
          className='!h-4 !w-4 !rounded-full !border-2 !border-white !bg-green-500 dark:!border-gray-800'
        />
      )}

      {/* Header */}
      <div className={cn('rounded-t-md px-3 py-2', config.headerBg)}>
        <div className='flex items-center justify-between gap-2'>
          <div className='flex items-center gap-2'>
            {data.type === 'cte' ? (
              <Layers className={cn('h-4 w-4', config.iconColor)} />
            ) : (
              <Table className={cn('h-4 w-4', config.iconColor)} />
            )}
            <span className='font-semibold text-foreground'>
              {data.tableName}
            </span>
          </div>
          <span
            className={cn(
              'rounded px-1.5 py-0.5 text-[10px] font-medium text-white',
              config.badge
            )}
          >
            {config.badgeText}
          </span>
        </div>
      </div>

      {/* Schema Info */}
      <div className='border-b border-border/50 px-3 py-1.5'>
        <div className='flex items-center gap-1 text-xs text-muted-foreground'>
          <Database className='h-3 w-3' />
          <span>
            {data.database ? `${data.database}.${data.schema}` : data.schema}
          </span>
        </div>
      </div>

      {/* Columns Section */}
      {data.columns.length > 0 && (
        <div className='px-3 py-2'>
          <button
            onClick={() => setExpanded(!expanded)}
            className='flex w-full items-center gap-1 text-xs font-medium text-muted-foreground hover:text-foreground'
          >
            {expanded ? (
              <ChevronDown className='h-3 w-3' />
            ) : (
              <ChevronRight className='h-3 w-3' />
            )}
            <span>{data.columns.length} columns</span>
          </button>
          {expanded && (
            <div className='mt-2 space-y-1'>
              {data.columns.map((col, idx) => (
                <div
                  key={idx}
                  className='flex items-center gap-2 rounded bg-background/50 px-2 py-1 font-mono text-xs'
                >
                  <span className='h-1.5 w-1.5 rounded-full bg-current opacity-40' />
                  {col}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

const nodeTypes = {
  tableCard: TableCardNode,
}

export function LineageFlowDiagram({
  lineage,
  destinationName,
  sourceName,
}: LineageFlowDiagramProps) {
  const { theme } = useTheme()
  const isDark = theme === 'dark'

  const { initialNodes, initialEdges } = useMemo(() => {
    const nodes: Node[] = []
    const edges: Edge[] = []

    // Layout constants
    const nodeSpacingY = 150
    const layerSpacingX = 320
    const startY = 50

    // Colors for edges
    const edgeColor = isDark ? '#60a5fa' : '#3b82f6'
    const edgeColorViolet = isDark ? '#a78bfa' : '#8b5cf6'
    const edgeColorGreen = isDark ? '#4ade80' : '#22c55e'

    // Helper to normalize table names for comparison (extract base name)
    const normalizeTableName = (name: string): string => {
      const parts = name.split('.')
      return parts[parts.length - 1].toLowerCase()
    }

    // Helper to find matching table in allTables by base name
    const findMatchingTable = (
      searchName: string,
      tableMap: Map<string, unknown>
    ): string | null => {
      const searchBase = normalizeTableName(searchName)
      for (const [key] of tableMap) {
        if (normalizeTableName(key) === searchBase) {
          return key
        }
      }
      return null
    }

    // Analyze lineage data to build proper hierarchy
    // 1. Collect all physical tables from source_tables
    const allTables = new Map<
      string,
      { columns: string[]; type: string; isCte: boolean }
    >()

    lineage.source_tables.forEach((st) => {
      if (!allTables.has(st.table)) {
        allTables.set(st.table, { columns: [], type: st.type, isCte: false })
      }
    })

    // 2. Get CTEs from lineage.ctes or detect from referenced_tables
    const ctePattern = /^[A-Z][a-zA-Z]*(?:Data|View|Temp|CTE|Info|List)?$/
    const detectedCTEs = lineage.referenced_tables.filter(
      (t) =>
        ctePattern.test(t) &&
        !lineage.source_tables.some((st) => st.table === t)
    )

    const cteList =
      lineage.ctes ||
      detectedCTEs.map((name) => ({
        name,
        source_tables: [] as string[],
        columns: [] as string[],
      }))
    cteList.forEach((cte) => {
      const cteName = typeof cte === 'string' ? cte : cte.name
      allTables.set(cteName, {
        columns: typeof cte === 'object' ? cte.columns : [],
        type: 'cte',
        isCte: true,
      })
    })

    // 3. Extract tables from column_lineage sources
    Object.values(lineage.column_lineage).forEach((colInfo) => {
      colInfo.sources.forEach((source) => {
        const parts = source.split('.')
        if (parts.length >= 2) {
          const tableName = parts.slice(0, -1).join('.')
          const colName = parts[parts.length - 1]
          if (!allTables.has(tableName)) {
            // Skip short aliases (like 'e', 's', 'a') and CTE names
            if (tableName.length <= 2 || ctePattern.test(tableName)) {
              return
            }
            allTables.set(tableName, {
              columns: [],
              type: 'table',
              isCte: false,
            })
          }
          const existing = allTables.get(tableName)
          if (existing && !existing.columns.includes(colName)) {
            existing.columns.push(colName)
          }
        }
      })
    })

    // 4. Build CTE → Source table mapping using explicit source_tables from parser
    // Also use name matching to handle schema prefix differences
    const cteSourceTableIds = new Set<string>() // IDs of tables that build CTEs
    const cteToSourceMapping = new Map<string, string[]>() // CTE name → list of source table IDs in allTables

    cteList.forEach((cte) => {
      const cteName = typeof cte === 'string' ? cte : cte.name
      const sourceTables =
        typeof cte === 'object' && cte.source_tables ? cte.source_tables : []

      const mappedSources: string[] = []

      sourceTables.forEach((srcTable) => {
        // Try exact match first, then base name match
        let matchedKey = allTables.has(srcTable)
          ? srcTable
          : findMatchingTable(srcTable, allTables)

        if (!matchedKey) {
          // Add the table if it doesn't exist
          allTables.set(srcTable, { columns: [], type: 'table', isCte: false })
          matchedKey = srcTable
        }

        if (matchedKey) {
          cteSourceTableIds.add(matchedKey)
          mappedSources.push(matchedKey)
        }
      })

      cteToSourceMapping.set(cteName, mappedSources)
    })

    // 5. Categorize tables into layers:
    // - cteSources: Tables that build CTEs (from CTE.source_tables)
    // - mainTables: Tables used in main FROM/JOIN (not CTE sources)
    // - cteNames: The CTEs themselves
    const cteSources: string[] = []
    const mainTables: string[] = []
    const cteNames: string[] = cteList.map((c) =>
      typeof c === 'string' ? c : c.name
    )

    Array.from(allTables.entries()).forEach(([tableName, info]) => {
      if (info.isCte) {
        return // CTEs are handled separately
      }

      if (cteSourceTableIds.has(tableName)) {
        cteSources.push(tableName)
      } else {
        mainTables.push(tableName)
      }
    })

    // 6. Calculate layer X positions
    const hasCteSources = cteSources.length > 0
    const hasCtes = cteNames.length > 0
    const hasMainTables = mainTables.length > 0

    let currentX = 50
    const layerX = {
      cteSources: currentX,
      ctes: 0,
      mainTables: 0,
      output: 0,
    }

    if (hasCteSources) {
      layerX.ctes = currentX + layerSpacingX
      currentX = layerX.ctes
    }
    if (hasCtes) {
      layerX.mainTables = currentX + layerSpacingX
      currentX = layerX.mainTables
    } else {
      layerX.mainTables = currentX
    }
    if (hasMainTables || hasCtes) {
      layerX.output = currentX + layerSpacingX
    } else {
      layerX.output = currentX + layerSpacingX
    }

    // 7. Create nodes for each layer
    // Layer 0: CTE Source Tables
    cteSources.forEach((tableName, idx) => {
      const parsed = parseTableName(tableName)
      nodes.push({
        id: `source-${tableName}`,
        type: 'tableCard',
        position: { x: layerX.cteSources, y: startY + idx * nodeSpacingY },
        data: {
          tableName: parsed.table,
          schema: parsed.database
            ? `${parsed.database}.${parsed.schema}`
            : parsed.schema,
          database: sourceName,
          type: 'source',
          columns: allTables.get(tableName)?.columns || [],
        },
      })
    })

    // Layer 1: CTEs
    cteNames.forEach((cteName, idx) => {
      const cteInfo = allTables.get(cteName)
      nodes.push({
        id: `cte-${cteName}`,
        type: 'tableCard',
        position: {
          x: hasCteSources ? layerX.ctes : layerX.cteSources,
          y: startY + idx * nodeSpacingY,
        },
        data: {
          tableName: cteName,
          schema: 'CTE',
          type: 'cte',
          columns: cteInfo?.columns || [],
        },
      })

      // Edges from source tables to this CTE using cteToSourceMapping
      const sourcesForThisCte = cteToSourceMapping.get(cteName) || []
      sourcesForThisCte.forEach((srcTable) => {
        edges.push({
          id: `edge-${srcTable}-${cteName}`,
          source: `source-${srcTable}`,
          target: `cte-${cteName}`,
          type: 'smoothstep',
          markerEnd: { type: MarkerType.ArrowClosed, color: edgeColor },
          style: { stroke: edgeColor, strokeWidth: 2 },
          animated: false,
        })
      })
    })

    // Layer 2: Main Tables (FROM clause tables)
    mainTables.forEach((tableName, idx) => {
      const parsed = parseTableName(tableName)
      nodes.push({
        id: `main-${tableName}`,
        type: 'tableCard',
        position: {
          x: layerX.mainTables,
          y: startY + (cteSources.length + idx) * nodeSpacingY,
        },
        data: {
          tableName: parsed.table,
          schema: parsed.database
            ? `${parsed.database}.${parsed.schema}`
            : parsed.schema,
          database: sourceName,
          type: 'source',
          columns: allTables.get(tableName)?.columns || [],
        },
      })
    })

    // Layer 3: Output/Destination
    const outputY =
      startY +
      ((Math.max(cteSources.length, cteNames.length, mainTables.length, 1) -
        1) *
        nodeSpacingY) /
        2
    nodes.push({
      id: 'destination-table',
      type: 'tableCard',
      position: { x: layerX.output, y: outputY },
      data: {
        tableName: destinationName || 'Output',
        schema: destinationName ? 'Destination' : 'Query Result',
        type: 'destination',
        columns: lineage.output_columns,
      },
    })

    // Create edges to output
    // CTEs → Output
    cteNames.forEach((cteName) => {
      edges.push({
        id: `edge-${cteName}-output`,
        source: `cte-${cteName}`,
        target: 'destination-table',
        type: 'smoothstep',
        markerEnd: { type: MarkerType.ArrowClosed, color: edgeColorViolet },
        style: { stroke: edgeColorViolet, strokeWidth: 2 },
        animated: false,
      })
    })

    // Main tables → Output
    mainTables.forEach((tableName) => {
      edges.push({
        id: `edge-${tableName}-output`,
        source: `main-${tableName}`,
        target: 'destination-table',
        type: 'smoothstep',
        markerEnd: { type: MarkerType.ArrowClosed, color: edgeColorGreen },
        style: { stroke: edgeColorGreen, strokeWidth: 2 },
        animated: false,
      })
    })

    // If no CTEs and no main tables, connect sources directly to output
    if (cteNames.length === 0 && mainTables.length === 0) {
      cteSources.forEach((tableName) => {
        edges.push({
          id: `edge-${tableName}-output`,
          source: `source-${tableName}`,
          target: 'destination-table',
          type: 'smoothstep',
          markerEnd: { type: MarkerType.ArrowClosed, color: edgeColorGreen },
          style: { stroke: edgeColorGreen, strokeWidth: 2 },
          animated: false,
        })
      })
    }

    return { initialNodes: nodes, initialEdges: edges }
  }, [lineage, isDark, destinationName, sourceName])

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges)

  // Update nodes and edges when lineage data changes
  useEffect(() => {
    setNodes(initialNodes)
    setEdges(initialEdges)
  }, [initialNodes, initialEdges, setNodes, setEdges])

  if (
    lineage.output_columns.length === 0 &&
    lineage.source_columns.length === 0 &&
    lineage.source_tables.length === 0
  ) {
    return (
      <div className='flex h-full items-center justify-center text-muted-foreground'>
        <div className='text-center'>
          <Database className='mx-auto mb-4 h-12 w-12 opacity-50' />
          <p>No lineage data available</p>
          <p className='text-sm'>Generate lineage to visualize data flow</p>
        </div>
      </div>
    )
  }

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      nodeTypes={nodeTypes}
      fitView
      fitViewOptions={{ padding: 0.3 }}
      attributionPosition='bottom-left'
      className='bg-background'
      minZoom={0.3}
      maxZoom={2}
      defaultViewport={{ x: 0, y: 0, zoom: 1 }}
    >
      <Controls className='!border !border-border !bg-background !shadow-sm [&_button]:!border-border [&_button]:!bg-background [&_button]:hover:!bg-accent [&_button_svg]:!fill-foreground' />
      <Background
        variant={BackgroundVariant.Dots}
        gap={20}
        size={1}
        color={isDark ? '#475569' : '#cbd5e1'}
        className='bg-background'
      />
    </ReactFlow>
  )
}
