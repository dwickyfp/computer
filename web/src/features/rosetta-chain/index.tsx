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

import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { DataExplorer } from './components/data-explorer'

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
        <div className='flex items-center justify-between'>
          <div>
            <h1 className='text-3xl font-bold tracking-tight'>Rosetta Chain</h1>
            <p className='text-muted-foreground'>
              Manage chain clients, sync streams, and discover cataloged data.
            </p>
          </div>
        </div>

        <Tabs defaultValue='clients' className='flex-1 flex flex-col'>
          <TabsList className='w-fit mb-4'>
            <TabsTrigger value='clients'>Chain Clients</TabsTrigger>
            <TabsTrigger value='explorer'>Data Explorer</TabsTrigger>
          </TabsList>

          <TabsContent value='clients' className='mt-0 flex-1 space-y-4'>
            {/* Chain Key management card */}
            <ChainKeyCard />

            {/* Chain Clients section */}
            <div className='flex flex-wrap items-end justify-between gap-2 mt-6'>
              <div>
                <h2 className='text-2xl font-bold tracking-tight'>Active Clients</h2>
                <p className='text-muted-foreground'>
                  Remote Rosetta instances that this instance can stream data to.
                </p>
              </div>
              <ChainPrimaryButtons />
            </div>

            <ChainClientTable data={clients ?? []} />
          </TabsContent>

          <TabsContent value='explorer' className='h-full mt-0'>
            <DataExplorer />
          </TabsContent>
        </Tabs>
      </Main>

      <ChainDialogs />
    </ChainProvider>
  )
}
