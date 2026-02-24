import {
  createContext,
  useContext,
  useState,
  useCallback,
  type ReactNode,
} from 'react'

interface TableSelection {
  destId: number | null
  syncId: number | null
}

interface PipelineSelectionContextType {
  selection: TableSelection
  selectTable: (destId: number, syncId: number) => void
  clearSelection: () => void
}

const PipelineSelectionContext = createContext<
  PipelineSelectionContextType | undefined
>(undefined)

export function PipelineSelectionProvider({
  children,
}: {
  children: ReactNode
}) {
  const [selection, setSelection] = useState<TableSelection>({
    destId: null,
    syncId: null,
  })

  const selectTable = useCallback((destId: number, syncId: number) => {
    setSelection({ destId, syncId })
  }, [])

  const clearSelection = useCallback(() => {
    setSelection({ destId: null, syncId: null })
  }, [])

  return (
    <PipelineSelectionContext.Provider
      value={{ selection, selectTable, clearSelection }}
    >
      {children}
    </PipelineSelectionContext.Provider>
  )
}

export function usePipelineSelection() {
  const context = useContext(PipelineSelectionContext)
  if (!context) {
    throw new Error(
      'usePipelineSelection must be used within PipelineSelectionProvider'
    )
  }
  return context
}
