import { useState, useMemo } from 'react'
import {
    Key,
    AlertTriangle,
    Search,
    CheckCircle2,
    Database,
    TableProperties,
    Hash,
    Type,
    Clock,
    Calendar,
    ToggleLeft,
    Braces,
    List,
    Globe,
    HelpCircle,
    Lock
} from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'
import { type SchemaColumn, type TableSchemaDiff } from '@/repo/sources'
import {
    Sheet,
    SheetContent,
    SheetDescription,
    SheetHeader,
    SheetTitle,
} from '@/components/ui/sheet'
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from '@/components/ui/table'
import { Input } from '@/components/ui/input'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'


interface SourceDetailsSchemaDrawerProps {
    open: boolean
    onOpenChange: (open: boolean) => void
    tableName: string
    schema: SchemaColumn[]
    diff?: TableSchemaDiff
    isLoading?: boolean
    version?: number
}

function TypeBadge({ type, changed }: { type: string, changed?: boolean }) {
    if (!type) return <span className="flex items-center gap-2 text-sm text-muted-foreground"><HelpCircle className="h-3.5 w-3.5" /> Unknown</span>

    const lowerType = type.toLowerCase()

    let Icon = HelpCircle

    if (lowerType.includes('int') || lowerType.includes('serial') || lowerType.includes('numeric') || lowerType.includes('float') || lowerType.includes('double') || lowerType.includes('decimal') || lowerType.includes('real')) {
        Icon = Hash
    } else if (lowerType.includes('char') || lowerType.includes('text') || lowerType.includes('string')) {
        Icon = Type
    } else if (lowerType.includes('time') || lowerType.includes('timestamp')) {
        Icon = Clock
    } else if (lowerType.includes('date')) {
        Icon = Calendar
    } else if (lowerType.includes('bool')) {
        Icon = ToggleLeft
    } else if (lowerType.includes('json') || lowerType.includes('variant')) {
        Icon = Braces
    } else if (lowerType.includes('array')) {
        Icon = List
    } else if (lowerType.includes('geo')) {
        Icon = Globe
    }

    return (
        <span className={cn(
            "flex items-center gap-2 text-sm",
            changed ? "text-orange-600 font-medium" : "text-muted-foreground/80"
        )}>
            <Icon className="h-3.5 w-3.5" />
            {type}
            {changed && <span className="text-[10px] bg-orange-100 text-orange-700 px-1.5 py-0.5 rounded-full ml-1">Changed</span>}
        </span>
    )
}

export function SourceDetailsSchemaDrawer({
    open,
    onOpenChange,
    tableName,
    schema,
    diff,
    isLoading = false,
    version
}: SourceDetailsSchemaDrawerProps) {
    const [searchQuery, setSearchQuery] = useState('')

    const filteredSchema = useMemo(() => {
        if (!searchQuery) return schema
        return schema.filter(col =>
            col.column_name.toLowerCase().includes(searchQuery.toLowerCase()) ||
            (col.real_data_type || col.data_type || '').toLowerCase().includes(searchQuery.toLowerCase())
        )
    }, [schema, searchQuery])

    const droppedColumns = diff?.dropped_columns || []
    const hasDroppedColumns = droppedColumns.length > 0

    return (
        <Sheet open={open} onOpenChange={onOpenChange}>
            <SheetContent className='sm:max-w-[900px] w-full p-0 flex flex-col gap-0'>
                <SheetHeader className="px-6 py-6 border-b bg-muted/10">
                    <div className="flex items-start justify-between">
                        <div>
                            <div className="flex items-center gap-2 text-muted-foreground mb-2">
                                <Database className="h-4 w-4" />
                                <span className="text-xs uppercase font-semibold tracking-wider">Table Schema</span>
                            </div>
                            <SheetTitle className="text-2xl font-bold flex items-center gap-3">
                                {tableName}
                                {version && (
                                    <Badge variant="outline" className="font-mono font-normal">v{version}</Badge>
                                )}
                            </SheetTitle>
                            <SheetDescription className="mt-1">
                                View column definitions, types, and constraints.
                            </SheetDescription>
                        </div>
                        {diff && (
                            <div className="flex flex-col items-end gap-2">
                                {diff.new_columns && diff.new_columns.length > 0 && (
                                    <Badge className="bg-green-500 hover:bg-green-600">{diff.new_columns.length} New Columns</Badge>
                                )}
                                {hasDroppedColumns && (
                                    <Badge variant="destructive">{droppedColumns.length} Dropped Columns</Badge>
                                )}
                            </div>
                        )}
                    </div>
                </SheetHeader>

                <div className="flex-1 flex flex-col overflow-hidden">
                    <Tabs defaultValue="active" className="flex-1 flex flex-col">
                        <div className="px-6 py-4 flex items-center justify-between gap-4 border-b">
                            <TabsList>
                                <TabsTrigger value="active" className="flex items-center gap-2">
                                    <TableProperties className="h-4 w-4" />
                                    Active Schema
                                    <Badge variant="secondary" className="ml-1 h-5 px-1.5 min-w-[20px]">{schema?.length || 0}</Badge>
                                </TabsTrigger>
                                {hasDroppedColumns && (
                                    <TabsTrigger value="dropped" className="flex items-center gap-2">
                                        <AlertTriangle className="h-4 w-4" />
                                        Dropped Columns
                                        <Badge variant="secondary" className="ml-1 h-5 px-1.5 min-w-[20px]">{droppedColumns.length}</Badge>
                                    </TabsTrigger>
                                )}
                            </TabsList>
                            <div className="relative w-[300px]">
                                <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
                                <Input
                                    placeholder="Search columns..."
                                    className="pl-9 h-9"
                                    value={searchQuery}
                                    onChange={(e) => setSearchQuery(e.target.value)}
                                />
                            </div>
                        </div>

                        <TabsContent value="active" className="flex-1 flex flex-col overflow-hidden m-0 p-0 border-0 bg-muted/5">
                            {isLoading ? (
                                <div className="flex h-full items-center justify-center">
                                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
                                </div>
                            ) : (
                                <div className="flex-1 flex flex-col min-h-0 p-6">
                                    <div className="rounded-md border border-border/50 bg-background overflow-auto shadow-sm max-h-[600px]">
                                        <Table>
                                            <TableHeader className="sticky top-0 z-10 shadow-sm backdrop-blur-sm">
                                                <TableRow>
                                                    <TableHead className="w-[30%] pl-6">Column Name</TableHead>
                                                    <TableHead className="w-[20%]">Type</TableHead>
                                                    <TableHead className="w-[15%] text-center">Nullable</TableHead>
                                                    <TableHead className="w-[15%] text-center">Default</TableHead>
                                                    <TableHead className="w-[20%] pr-6">Value</TableHead>
                                                </TableRow>
                                            </TableHeader>
                                            <TableBody>
                                                {filteredSchema && filteredSchema.length > 0 ? (
                                                    filteredSchema.map((col, index) => (
                                                        <TableRow key={index}>
                                                            <TableCell className='font-medium pl-6'>
                                                                <div className="flex items-center gap-2">
                                                                    {col.is_primary_key && (
                                                                        <div title="Primary Key" className="flex items-center justify-center h-6 w-6 rounded-md bg-yellow-100 text-yellow-700">
                                                                            <Key className="h-3.5 w-3.5" />
                                                                        </div>
                                                                    )}
                                                                    <span className={col.is_primary_key ? "font-bold" : ""}>{col.column_name}</span>
                                                                    {diff?.new_columns?.includes(col.column_name) && (
                                                                        <Badge variant="secondary" className="bg-green-100 text-green-800 border-green-200 h-5 px-1.5 ml-auto">New</Badge>
                                                                    )}
                                                                </div>
                                                            </TableCell>
                                                            <TableCell>
                                                                <div className="flex items-center gap-2">
                                                                    <TypeBadge type={col.real_data_type || col.data_type || 'Unknown'} changed={!!diff?.type_changes?.[col.column_name]} />
                                                                </div>
                                                            </TableCell>
                                                            <TableCell className="text-center">
                                                                {col.is_nullable === 'YES' ? (
                                                                    <span className="text-sm text-muted-foreground/50">Nullable</span>
                                                                ) : (
                                                                    <div className="flex items-center justify-center gap-1.5 text-sm text-foreground/80 font-medium">
                                                                        <Lock className="h-3.5 w-3.5 text-muted-foreground/70" />
                                                                        <span>Required</span>
                                                                    </div>
                                                                )}
                                                            </TableCell>
                                                            <TableCell className="text-center">
                                                                {col.has_default ? (
                                                                    <CheckCircle2 className="h-4 w-4 text-emerald-500 mx-auto" />
                                                                ) : (
                                                                    <span className="text-muted-foreground/30">-</span>
                                                                )}
                                                            </TableCell>
                                                            <TableCell className="pr-6">
                                                                {col.default_value ? (
                                                                    <code className="relative rounded bg-muted px-[0.3rem] py-[0.2rem] font-mono text-xs text-muted-foreground">
                                                                        {col.default_value}
                                                                    </code>
                                                                ) : (
                                                                    <span className="text-muted-foreground/30 text-sm">-</span>
                                                                )}
                                                            </TableCell>
                                                        </TableRow>
                                                    ))
                                                ) : (
                                                    <TableRow>
                                                        <TableCell colSpan={5} className='text-center h-32 text-muted-foreground'>
                                                            {searchQuery ? (
                                                                <div className="flex flex-col items-center gap-2">
                                                                    <Search className="h-8 w-8 opacity-20" />
                                                                    <p>No columns found matching "{searchQuery}"</p>
                                                                </div>
                                                            ) : (
                                                                "No schema information available"
                                                            )}
                                                        </TableCell>
                                                    </TableRow>
                                                )}
                                            </TableBody>
                                        </Table>
                                    </div>
                                </div>
                            )}
                        </TabsContent>

                        {hasDroppedColumns && (
                            <TabsContent value="dropped" className="flex-1 overflow-hidden m-0 p-0 border-0">
                                <div className="h-full overflow-y-auto">
                                    <div className="p-6 bg-red-50/10 min-h-full">
                                        <div className="flex items-center gap-3 text-destructive mb-4 p-4 bg-red-50 rounded-lg border border-red-100">
                                            <AlertTriangle className="h-5 w-5" />
                                            <div>
                                                <h3 className="font-semibold">Dropped Columns</h3>
                                                <p className="text-sm opacity-90">These columns have been removed from the source table but may still be retained in Snowflake history.</p>
                                            </div>
                                        </div>

                                        <div className='rounded-md border border-red-200 bg-white overflow-hidden'>
                                            <Table>
                                                <TableHeader className="bg-red-50/50">
                                                    <TableRow>
                                                        <TableHead className="text-red-900 w-[30%] pl-6">Column Name</TableHead>
                                                        <TableHead className="text-red-900 w-[20%]">Data Type</TableHead>
                                                        <TableHead className="text-center text-red-900 w-[15%]">Nullable</TableHead>
                                                        <TableHead className="text-center text-red-900 w-[15%]">Default</TableHead>
                                                        <TableHead className="text-red-900 w-[20%] pr-6">Value</TableHead>
                                                    </TableRow>
                                                </TableHeader>
                                                <TableBody>
                                                    {droppedColumns.map((col, index) => (
                                                        <TableRow key={index} className="bg-red-50/10 hover:bg-red-50/20">
                                                            <TableCell className='font-medium text-red-900 pl-6'>
                                                                <span className="line-through decoration-red-900/40 opacity-70">{col.column_name}</span>
                                                            </TableCell>
                                                            <TableCell>
                                                                <Badge variant="outline" className="border-red-200 text-red-700 bg-red-50">{col.real_data_type}</Badge>
                                                            </TableCell>
                                                            <TableCell className="text-center">
                                                                {col.is_nullable === 'YES' ? (
                                                                    <span className="text-xs font-medium text-red-800/70">Yes</span>
                                                                ) : (
                                                                    <span className="text-xs font-medium text-red-800/70">No</span>
                                                                )}
                                                            </TableCell>
                                                            <TableCell className="text-center">
                                                                {col.has_default ? (
                                                                    <CheckCircle2 className="h-4 w-4 text-red-400 mx-auto" />
                                                                ) : (
                                                                    <span className="text-red-300">-</span>
                                                                )}
                                                            </TableCell>
                                                            <TableCell className="pr-6">
                                                                <span className="text-xs text-red-900/60 font-mono">
                                                                    {col.default_value || '-'}
                                                                </span>
                                                            </TableCell>
                                                        </TableRow>
                                                    ))}
                                                </TableBody>
                                            </Table>
                                        </div>
                                    </div>
                                </div>
                            </TabsContent>
                        )}
                    </Tabs>
                </div>
            </SheetContent>
        </Sheet>
    )
}
