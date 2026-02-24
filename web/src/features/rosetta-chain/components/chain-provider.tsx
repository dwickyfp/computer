import React, { useState } from 'react'
import useDialogState from '@/hooks/use-dialog-state'
import { type ChainClient } from '../data/schema'

type ChainDialogType = 'create' | 'update' | 'delete' | 'test'

interface ChainContextType {
  open: ChainDialogType | null
  setOpen: (str: ChainDialogType | null) => void
  currentRow: ChainClient | null
  setCurrentRow: React.Dispatch<React.SetStateAction<ChainClient | null>>
}

const ChainContext = React.createContext<ChainContextType | null>(null)

export function ChainProvider({ children }: { children: React.ReactNode }) {
  const [open, setOpen] = useDialogState<ChainDialogType>(null)
  const [currentRow, setCurrentRow] = useState<ChainClient | null>(null)

  return (
    <ChainContext.Provider value={{ open, setOpen, currentRow, setCurrentRow }}>
      {children}
    </ChainContext.Provider>
  )
}

export const useChain = () => {
  const context = React.useContext(ChainContext)
  if (!context) {
    throw new Error('useChain must be used within <ChainProvider>')
  }
  return context
}
