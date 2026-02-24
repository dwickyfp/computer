import React, { useState } from 'react'
import useDialogState from '@/hooks/use-dialog-state'
import { type Destination } from '../data/schema'

type DestinationsDialogType = 'create' | 'update' | 'delete' | 'table-list'

interface DestinationsContextType {
    open: DestinationsDialogType | null
    setOpen: (str: DestinationsDialogType | null) => void
    currentRow: Destination | null
    setCurrentRow: React.Dispatch<React.SetStateAction<Destination | null>>
}

const DestinationsContext = React.createContext<DestinationsContextType | null>(null)

export function DestinationsProvider({ children }: { children: React.ReactNode }) {
    const [open, setOpen] = useDialogState<DestinationsDialogType>(null)
    const [currentRow, setCurrentRow] = useState<Destination | null>(null)

    return (
        <DestinationsContext.Provider value={{ open, setOpen, currentRow, setCurrentRow }}>
            {children}
        </DestinationsContext.Provider>
    )
}

export const useDestinations = () => {
    const context = React.useContext(DestinationsContext)

    if (!context) {
        throw new Error('useDestinations has to be used within <DestinationsContext>')
    }

    return context
}
