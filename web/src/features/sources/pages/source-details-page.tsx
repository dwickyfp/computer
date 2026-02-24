import { useState, useEffect } from 'react'
import { useParams, useNavigate, Link } from '@tanstack/react-router'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { sourcesRepo } from '@/repo/sources'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { SourceDetailsMetrics } from '../components/source-details-metrics'
import { SourceReplicationTable } from '../components/source-replication-table'
import { SourceDetailsCreatePublicationDialog } from '../components/source-details-create-publication-dialog'
import { SourceDetailsListTable } from '../components/source-details-list-table'
import { SourceDetailsPresets } from '../components/source-details-presets'
import { Skeleton } from '@/components/ui/skeleton'
import { CustomTabs, CustomTabsContent, CustomTabsList, CustomTabsTrigger } from '@/components/ui/custom-tabs'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { RefreshCcw, Activity, Database, Sliders } from 'lucide-react'
import { toast } from 'sonner'
import {
    AlertDialog,
    AlertDialogAction,
    AlertDialogCancel,
    AlertDialogContent,
    AlertDialogDescription,
    AlertDialogFooter,
    AlertDialogHeader,
    AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import {
    Breadcrumb,
    BreadcrumbItem,
    BreadcrumbLink,
    BreadcrumbList,
    BreadcrumbPage,
    BreadcrumbSeparator,
} from '@/components/ui/breadcrumb'

export default function SourceDetailsPage() {
    // Use TansStack Router useParams
    const { sourceId } = useParams({ from: '/_authenticated/sources/$sourceId/details' })
    const id = parseInt(sourceId)
    const navigate = useNavigate()
    const queryClient = useQueryClient()
    const [isRefreshing, setIsRefreshing] = useState(false)
    const [isPublicationLoading, setIsPublicationLoading] = useState(false)
    const [isReplicationLoading, setIsReplicationLoading] = useState(false)
    const [createPubDialogOpen, setCreatePubDialogOpen] = useState(false)
    const [dropPublicationDialogOpen, setDropPublicationDialogOpen] = useState(false)
    const [dropReplicationDialogOpen, setDropReplicationDialogOpen] = useState(false)

    const { data, isLoading, error, isError } = useQuery({
        queryKey: ['source-details', id],
        queryFn: () => sourcesRepo.getDetails(id!),
        enabled: !!id,
        retry: false, // Don't retry if it fails (e.g. 404)
    })

    useEffect(() => {
        if (isError) {
            toast.error("Source not found or access denied")
            navigate({ to: '/sources' })
        }
    }, [isError, navigate])

    const handleRefresh = async () => {
        setIsRefreshing(true)
        try {
            await sourcesRepo.refreshSource(id)
            queryClient.invalidateQueries({ queryKey: ['source-details', id] })
            queryClient.invalidateQueries({ queryKey: ['source-available-tables', id] })
            toast.success("Source refreshed successfully")
        } catch (err) {
            console.error(err)
            toast.error("Failed to refresh source")
        } finally {
            setIsRefreshing(false)
        }
    }

    const handlePublicationAction = () => {
        if (data?.source.is_publication_enabled) {
            // Open Drop Publication Dialog
            setDropPublicationDialogOpen(true)
        } else {
            // Create Publication -> Open Dialog
            setCreatePubDialogOpen(true)
        }
    }

    const handleDropPublication = async () => {
        setIsPublicationLoading(true)
        setDropPublicationDialogOpen(false)
        try {
            await sourcesRepo.dropPublication(id)
            queryClient.invalidateQueries({ queryKey: ['source-details', id] })
            toast.success("Publication dropped successfully")
        } catch (err) {
            console.error(err)
            toast.error("Failed to drop publication")
        } finally {
            setIsPublicationLoading(false)
        }
    }

    const handleReplicationAction = async () => {
        if (data?.source.is_replication_enabled) {
            // Open Drop Replication Dialog
            setDropReplicationDialogOpen(true)
        } else {
            // Create Replication
            await handleCreateReplication()
        }
    }

    const handleCreateReplication = async () => {
        setIsReplicationLoading(true)
        try {
            await sourcesRepo.createReplication(id)
            toast.success("Replication slot created successfully")
            queryClient.invalidateQueries({ queryKey: ['source-details', id] })
        } catch (err) {
            console.error(err)
            toast.error("Failed to create replication slot")
        } finally {
            setIsReplicationLoading(false)
        }
    }

    const handleDropReplication = async () => {
        setIsReplicationLoading(true)
        setDropReplicationDialogOpen(false)
        try {
            await sourcesRepo.dropReplication(id)
            toast.success("Replication slot dropped successfully")
            queryClient.invalidateQueries({ queryKey: ['source-details', id] })
        } catch (err) {
            console.error(err)
            toast.error("Failed to drop replication slot")
        } finally {
            setIsReplicationLoading(false)
        }
    }

    if (!id) return <div>Invalid Source ID</div>


    return (
        <>
            <Header fixed>
                <Search />
                <div className='ms-auto flex items-center space-x-4'>
                    <ThemeSwitch />
                    
                </div>
            </Header>

            <Main className='flex flex-1 flex-col gap-4 sm:gap-6'>
                <div className="flex flex-col gap-2">
                    <Breadcrumb>
                        <BreadcrumbList>
                            <BreadcrumbItem>
                                <BreadcrumbLink asChild>
                                    <Link to="/sources">Sources</Link>
                                </BreadcrumbLink>
                            </BreadcrumbItem>
                            <BreadcrumbSeparator />
                            <BreadcrumbItem>
                                <BreadcrumbPage>{data?.source.name || 'Loading...'}</BreadcrumbPage>
                            </BreadcrumbItem>
                        </BreadcrumbList>
                    </Breadcrumb>

                    <div className='flex items-start justify-between mt-2'>
                        <div className="space-y-1">
                            <h2 className='text-3xl font-bold tracking-tight'>
                                {isLoading ? <Skeleton className="h-8 w-48" /> : data?.source.name}
                            </h2>
                            <p className="text-muted-foreground text-sm">
                                Manage your source configuration, replication, and monitored tables.
                            </p>
                        </div>
                        {/* Action Buttons */}
                        <div className="flex items-center gap-2">
                            <Button
                                variant="outline"
                                size="sm"
                                onClick={handleRefresh}
                                disabled={isRefreshing || isLoading}
                            >
                                <RefreshCcw className={cn("h-4 w-4 mr-2", isRefreshing && "animate-spin")} />
                                Refresh
                            </Button>
                        </div>
                    </div>
                </div>

                {isLoading ? (
                    <div className="space-y-4">
                        <Skeleton className="h-32 w-full" />
                        <Skeleton className="h-64 w-full" />
                    </div>
                ) : error ? (
                    <div className="text-red-500">Error loading source details</div>
                ) : (
                    <>
                        {/* Overview Section */}
                        <div className="grid gap-4">
                            <SourceDetailsMetrics
                                data={data?.wal_monitor || null}
                                source={data!.source}
                                onPublicationAction={handlePublicationAction}
                                onReplicationAction={handleReplicationAction}
                                isPublicationLoading={isPublicationLoading}
                                isReplicationLoading={isReplicationLoading}
                            />
                        </div>

                        <div className="grid gap-6">
                            {/* Tables Section */}
                            <CustomTabs defaultValue="monitored" className="w-full">
                                <CustomTabsList className="w-full justify-start border-b mb-4">
                                    <CustomTabsTrigger value="monitored">
                                        <Activity className="h-4 w-4 mr-2" />
                                        Monitored Tables
                                    </CustomTabsTrigger>
                                    <CustomTabsTrigger value="available">
                                        <Database className="h-4 w-4 mr-2" />
                                        Available Tables
                                    </CustomTabsTrigger>
                                    <CustomTabsTrigger value="presets">
                                        <Sliders className="h-4 w-4 mr-2" />
                                        Presets
                                    </CustomTabsTrigger>
                                </CustomTabsList>

                                <CustomTabsContent value="monitored" className="space-y-4 mt-0">
                                    <SourceReplicationTable
                                        sourceId={id}
                                        tables={data?.tables || []}
                                    />
                                </CustomTabsContent>
                                <CustomTabsContent value="available" className="mt-0">
                                    <SourceDetailsListTable
                                        sourceId={id}
                                        isPublicationEnabled={data?.source.is_publication_enabled || false}
                                        publishedTableNames={data?.tables.map(t => t.table_name) || []}
                                    />
                                </CustomTabsContent>
                                <CustomTabsContent value="presets" className="mt-0">
                                    <SourceDetailsPresets />
                                </CustomTabsContent>
                            </CustomTabs>
                        </div>

                        <SourceDetailsCreatePublicationDialog
                            open={createPubDialogOpen}
                            onOpenChange={setCreatePubDialogOpen}
                            sourceId={id}
                        />

                        <AlertDialog open={dropPublicationDialogOpen} onOpenChange={setDropPublicationDialogOpen}>
                            <AlertDialogContent>
                                <AlertDialogHeader>
                                    <AlertDialogTitle>Are you absolutely sure?</AlertDialogTitle>
                                    <AlertDialogDescription>
                                        This action will drop the publication and stop Change Data Capture (CDC).
                                        This action cannot be undone.
                                    </AlertDialogDescription>
                                </AlertDialogHeader>
                                <AlertDialogFooter>
                                    <AlertDialogCancel>Cancel</AlertDialogCancel>
                                    <AlertDialogAction onClick={handleDropPublication} className="bg-destructive text-white hover:bg-destructive/90">
                                        Drop Publication
                                    </AlertDialogAction>
                                </AlertDialogFooter>
                            </AlertDialogContent>
                        </AlertDialog>

                        <AlertDialog open={dropReplicationDialogOpen} onOpenChange={setDropReplicationDialogOpen}>
                            <AlertDialogContent>
                                <AlertDialogHeader>
                                    <AlertDialogTitle>Are you absolutely sure?</AlertDialogTitle>
                                    <AlertDialogDescription>
                                        This action will drop the replication slot.
                                        This action cannot be undone.
                                    </AlertDialogDescription>
                                </AlertDialogHeader>
                                <AlertDialogFooter>
                                    <AlertDialogCancel>Cancel</AlertDialogCancel>
                                    <AlertDialogAction onClick={handleDropReplication} className="bg-destructive text-white hover:bg-destructive/90">
                                        Drop Replication
                                    </AlertDialogAction>
                                </AlertDialogFooter>
                            </AlertDialogContent>
                        </AlertDialog>
                    </>
                )}
            </Main>
        </>
    )
}
