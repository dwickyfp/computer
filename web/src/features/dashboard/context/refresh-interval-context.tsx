import { createContext, useContext, useState, type ReactNode } from 'react'

type RefreshIntervalContextType = {
  refreshInterval: number
  setRefreshInterval: (interval: number) => void
}

const RefreshIntervalContext = createContext<RefreshIntervalContextType | null>(null)

export function useRefreshInterval() {
  const context = useContext(RefreshIntervalContext)
  if (!context) {
    throw new Error('useRefreshInterval must be used within RefreshIntervalProvider')
  }
  return context
}

export function RefreshIntervalProvider({ children }: { children: ReactNode }) {
  const [refreshInterval, setRefreshInterval] = useState<number>(5000)

  return (
    <RefreshIntervalContext.Provider value={{ refreshInterval, setRefreshInterval }}>
      {children}
    </RefreshIntervalContext.Provider>
  )
}
