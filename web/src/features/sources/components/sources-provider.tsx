import React, { useState } from 'react'
import useDialogState from '@/hooks/use-dialog-state'
import { type Source } from '../data/schema'

type SourcesDialogType = 'create' | 'update' | 'delete'

interface SourcesContextType {
    open: SourcesDialogType | null
    setOpen: (str: SourcesDialogType | null) => void
    currentRow: Source | null
    setCurrentRow: React.Dispatch<React.SetStateAction<Source | null>>
}

const SourcesContext = React.createContext<SourcesContextType | null>(null)

export function SourcesProvider({ children }: { children: React.ReactNode }) {
    const [open, setOpen] = useDialogState<SourcesDialogType>(null)
    const [currentRow, setCurrentRow] = useState<Source | null>(null)

    return (
        <SourcesContext.Provider value={{ open, setOpen, currentRow, setCurrentRow }}>
            {children}
        </SourcesContext.Provider>
    )
}

export const useSources = () => {
    const context = React.useContext(SourcesContext)

    if (!context) {
        throw new Error('useSources has to be used within <SourcesContext>')
    }

    return context
}
