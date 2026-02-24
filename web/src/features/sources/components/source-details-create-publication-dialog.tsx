import { useState } from 'react'
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Loader2, Search } from 'lucide-react'
import { toast } from 'sonner'
import { sourcesRepo } from '@/repo/sources'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Checkbox } from '@/components/ui/checkbox'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from '@/components/ui/table'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'

interface SourceDetailsCreatePublicationDialogProps {
    open: boolean
    onOpenChange: (open: boolean) => void
    sourceId: number
}

export function SourceDetailsCreatePublicationDialog({
    open,
    onOpenChange,
    sourceId
}: SourceDetailsCreatePublicationDialogProps) {
    const [selectedTables, setSelectedTables] = useState<string[]>([])
    const [searchQuery, setSearchQuery] = useState('')
    const queryClient = useQueryClient()
    const [activeTab, setActiveTab] = useState("tables")

    const { data: tables, isLoading: isLoadingTables } = useQuery({
        queryKey: ['source-available-tables', sourceId],
        queryFn: () => sourcesRepo.getAvailableTables(sourceId),
        enabled: open, // Only fetch when dialog is open
    })

    const { data: presets, isLoading: isLoadingPresets } = useQuery({
        queryKey: ['source-presets', sourceId],
        queryFn: () => sourcesRepo.getPresets(sourceId),
        enabled: open,
    })

    const createPublicationMutation = useMutation({
        mutationFn: async (tables: string[]) => {
            if (tables.length === 0) throw new Error("No tables selected")
            await sourcesRepo.createPublication(sourceId, tables)
        },
        onSuccess: () => {
            toast.success("Publication created successfully")
            queryClient.invalidateQueries({ queryKey: ['source-details', sourceId] })
            onOpenChange(false)
            setSelectedTables([])
        },
        onError: (err) => {
            toast.error("Failed to create publication")
            console.error(err)
        }
    })

    const filteredTables = tables?.filter(t => t.toLowerCase().includes(searchQuery.toLowerCase())) || []

    const handleSelectAll = () => {
        if (selectedTables.length === filteredTables.length) {
            setSelectedTables([])
        } else {
            setSelectedTables(filteredTables)
        }
    }

    const handleSelect = (name: string) => {
        if (selectedTables.includes(name)) {
            setSelectedTables(selectedTables.filter(t => t !== name))
        } else {
            setSelectedTables([...selectedTables, name])
        }
    }

    const loadPreset = (presetTables: string[]) => {
        // Merge or Replace? "Auto load" implies setting the selection.
        // Let's replace for clarity, or maybe merge if user wants?
        // Standard behavior typically replaces current selection or adds to it.
        // Let's Replace to match "Load Preset" mental model.
        setSelectedTables(presetTables)
        setActiveTab("tables")
        toast.info(`Loaded ${presetTables.length} tables from preset`)
    }

    return (
        <Dialog open={open} onOpenChange={onOpenChange}>
            <DialogContent className="sm:max-w-[700px] h-[80vh] flex flex-col p-0">
                <DialogHeader className="p-6 pb-2">
                    <DialogTitle>Create Publication</DialogTitle>
                    <DialogDescription>
                        Select tables to include in the publication. You can select manually or load a preset.
                    </DialogDescription>
                </DialogHeader>

                <div className="flex-1 overflow-hidden px-6">
                    <Tabs value={activeTab} onValueChange={setActiveTab} className="h-full flex flex-col">
                        <TabsList className="grid w-full grid-cols-2">
                            <TabsTrigger value="tables">List Tables ({selectedTables.length})</TabsTrigger>
                            <TabsTrigger value="presets">Presets</TabsTrigger>
                        </TabsList>

                        <TabsContent value="tables" className="flex-1 flex flex-col overflow-hidden mt-4 border border-border/50 rounded-md">
                            <div className="p-2 border-b bg-muted/20 flex items-center gap-2">
                                <Search className="h-4 w-4 text-muted-foreground ml-2" />
                                <Input
                                    placeholder="Search tables..."
                                    value={searchQuery}
                                    onChange={e => setSearchQuery(e.target.value)}
                                    className="border-none bg-transparent focus-visible:ring-0"
                                />
                            </div>
                            <div className="flex-1 overflow-auto">
                                {isLoadingTables ? (
                                    <div className="flex justify-center p-8"><Loader2 className="h-8 w-8 animate-spin" /></div>
                                ) : (
                                    <Table>
                                        <TableHeader className="sticky top-0 bg-background z-10 shadow-sm">
                                            <TableRow>
                                                <TableHead className="w-[50px]">
                                                    <Checkbox
                                                        checked={filteredTables.length > 0 && selectedTables.length === filteredTables.length}
                                                        onCheckedChange={handleSelectAll}
                                                    />
                                                </TableHead>
                                                <TableHead>Table Name</TableHead>
                                            </TableRow>
                                        </TableHeader>
                                        <TableBody>
                                            {filteredTables.length === 0 ? (
                                                <TableRow>
                                                    <TableCell colSpan={2} className="text-center h-24">No tables found.</TableCell>
                                                </TableRow>
                                            ) : (
                                                filteredTables.map(table => (
                                                    <TableRow key={table} className="cursor-pointer" onClick={() => handleSelect(table)}>
                                                        <TableCell onClick={(e) => e.stopPropagation()}>
                                                            <Checkbox
                                                                checked={selectedTables.includes(table)}
                                                                onCheckedChange={() => handleSelect(table)}
                                                            />
                                                        </TableCell>
                                                        <TableCell>{table}</TableCell>
                                                    </TableRow>
                                                ))
                                            )}
                                        </TableBody>
                                    </Table>
                                )}
                            </div>
                        </TabsContent>

                        <TabsContent value="presets" className="flex-1 overflow-auto mt-4">
                            {isLoadingPresets ? (
                                <div className="flex justify-center p-8"><Loader2 className="h-8 w-8 animate-spin" /></div>
                            ) : presets && presets.length > 0 ? (
                                <div className="grid grid-cols-1 gap-4 pb-4">
                                    {presets.map(preset => (
                                        <Card key={preset.id} className="cursor-pointer hover:border-primary transition-colors" onClick={() => loadPreset(preset.table_names)}>
                                            <CardHeader className="flex flex-row items-start justify-between space-y-0 pb-2">
                                                <div>
                                                    <CardTitle className="text-base font-medium">{preset.name}</CardTitle>
                                                    <CardDescription className="text-xs mt-1">
                                                        {new Date(preset.created_at).toLocaleDateString()}
                                                    </CardDescription>
                                                </div>
                                                <Badge variant="secondary">{preset.table_names.length} tables</Badge>
                                            </CardHeader>
                                            <CardContent>
                                                <p className="text-xs text-muted-foreground truncate">
                                                    {preset.table_names.join(", ")}
                                                </p>
                                                <Button size="sm" variant="outline" className="mt-4 w-full" onClick={(e) => { e.stopPropagation(); loadPreset(preset.table_names); }}>
                                                    Load Preset
                                                </Button>
                                            </CardContent>
                                        </Card>
                                    ))}
                                </div>
                            ) : (
                                <div className="text-center p-8 text-muted-foreground border border-dashed rounded-md">
                                    No presets found. Create presets in the "List Table" tab.
                                </div>
                            )}
                        </TabsContent>
                    </Tabs>
                </div>

                <DialogFooter className="p-6 pt-2">
                    <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
                    <Button
                        onClick={() => createPublicationMutation.mutate(selectedTables)}
                        disabled={createPublicationMutation.isPending || selectedTables.length === 0}
                    >
                        {createPublicationMutation.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                        Create Publication ({selectedTables.length})
                    </Button>
                </DialogFooter>
            </DialogContent>
        </Dialog>
    )
}
