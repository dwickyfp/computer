import { useQuery } from '@tanstack/react-query'
import { chainRepo } from '@/repo/chains'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { ChainClientTable } from './components/chain-client-table'
import { ChainDialogs } from './components/chain-dialogs'
import { ChainKeyCard } from './components/chain-key-card'
import { ChainPrimaryButtons } from './components/chain-primary-buttons'
import { ChainProvider } from './components/chain-provider'

export function RosettaChain() {
  const { data: clients } = useQuery({
    queryKey: ['chain-clients'],
    queryFn: chainRepo.getClients,
    refetchInterval: 10000,
  })

  return (
    <ChainProvider>
      <Header fixed>
        <Search />
        <div className='ms-auto flex items-center space-x-4'>
          <ThemeSwitch />
        </div>
      </Header>

      <Main className='flex flex-1 flex-col gap-4 sm:gap-6'>
        {/* Chain Key management card */}
        <ChainKeyCard />

        {/* Chain Clients section */}
        <div className='flex flex-wrap items-end justify-between gap-2'>
          <div>
            <h2 className='text-2xl font-bold tracking-tight'>Chain Clients</h2>
            <p className='text-muted-foreground'>
              Remote Rosetta instances that this instance can stream data to.
            </p>
          </div>
          <ChainPrimaryButtons />
        </div>

        <ChainClientTable data={clients ?? []} />
      </Main>

      <ChainDialogs />
    </ChainProvider>
  )
}
