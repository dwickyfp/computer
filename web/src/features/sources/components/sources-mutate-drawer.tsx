import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import {
    Form,
    FormControl,
    FormField,
    FormItem,
    FormLabel,
    FormMessage,
} from '@/components/ui/form'
import { Input } from '@/components/ui/input'
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from '@/components/ui/accordion'
import {
    Sheet,
    SheetClose,
    SheetContent,
    SheetDescription,
    SheetFooter,
    SheetHeader,
    SheetTitle,
} from '@/components/ui/sheet'
import { type Source, sourceFormSchema, type SourceForm } from '../data/schema'
import { sourcesRepo } from '@/repo/sources'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Loader2 } from 'lucide-react'

type SourcesMutateDrawerProps = {
    open: boolean
    onOpenChange: (open: boolean) => void
    currentRow?: Source
}

export function SourcesMutateDrawer({
    open,
    onOpenChange,
    currentRow,
}: SourcesMutateDrawerProps) {
    const isUpdate = !!currentRow
    const queryClient = useQueryClient()
    const [isTesting, setIsTesting] = useState(false)

    const form = useForm<SourceForm>({
        resolver: zodResolver(sourceFormSchema) as any,
        mode: 'onChange',
        defaultValues: (currentRow
            ? {
                name: currentRow.name,
                pg_host: currentRow.pg_host,
                pg_port: currentRow.pg_port,
                pg_database: currentRow.pg_database,
                pg_username: currentRow.pg_username,
                publication_name: currentRow.publication_name,
                replication_name: currentRow.replication_name,
                pg_password: '',
            }
            : {
                name: '',
                pg_host: '',
                pg_port: 5432,
                pg_database: '',
                pg_username: '',
                pg_password: '',
                publication_name: '',
                replication_name: '',
            }) as any,
    })

    const createMutation = useMutation({
        mutationFn: sourcesRepo.create,
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['sources'] })
            onOpenChange(false)
            form.reset()
            toast.success('Source created successfully')
        },
        onError: (error) => {
            toast.error('Failed to create source')
            console.error(error)
        },
    })

    const updateMutation = useMutation({
        mutationFn: (data: SourceForm) =>
            sourcesRepo.update(currentRow!.id, data),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['sources'] })
            onOpenChange(false)
            form.reset()
            toast.success('Source updated successfully')
        },
        onError: (error) => {
            toast.error('Failed to update source')
            console.error(error)
        },
    })

    const onSubmit = (data: SourceForm) => {
        if (isUpdate) {
            updateMutation.mutate(data)
        } else {
            createMutation.mutate(data as any) // Type assertion due to password optionality mismatch potential
        }
    }

    const handleTestConnection = async () => {
        const data = form.getValues()
        // Validate required fields for test
        const isValid = await form.trigger(['pg_host', 'pg_port', 'pg_database', 'pg_username', 'pg_password'])

        if (!isValid) {
            toast.error('Please fill in connection details to test')
            return
        }

        setIsTesting(true)
        try {
            const result = await sourcesRepo.testConnection(data as any)
            if (result) {
                toast.success('Connection successful')
            } else {
                toast.error('Connection failed')
            }
        } catch (err) {
            toast.error('Error testing connection')
        } finally {
            setIsTesting(false)
        }
    }

    const isLoading = createMutation.isPending || updateMutation.isPending

    return (
        <Sheet
            open={open}
            onOpenChange={(v) => {
                onOpenChange(v)
                form.reset()
            }}
        >
            <SheetContent className='flex flex-col sm:max-w-md w-full'>
                <SheetHeader className='text-start'>
                    <SheetTitle>{isUpdate ? 'Update' : 'Add'} Source</SheetTitle>
                    <SheetDescription>
                        Configure your PostgreSQL source connection details.
                    </SheetDescription>
                </SheetHeader>
                <Form {...form}>
                    <form
                        id='sources-form'
                        onSubmit={form.handleSubmit(onSubmit)}
                        className='flex-1 space-y-4 overflow-y-auto px-4 py-4'
                    >
                        <FormField
                            control={form.control}
                            name='name'
                            render={({ field }) => (
                                <FormItem>
                                    <FormLabel>Name</FormLabel>
                                    <FormControl>
                                        <Input {...field} placeholder='production_db' />
                                    </FormControl>
                                    <FormMessage />
                                </FormItem>
                            )}
                        />
                        <div className='grid grid-cols-2 gap-4'>
                            <FormField
                                control={form.control}
                                name='pg_host'
                                render={({ field }) => (
                                    <FormItem>
                                        <FormLabel>Host</FormLabel>
                                        <FormControl>
                                            <Input {...field} placeholder='localhost' />
                                        </FormControl>
                                        <FormMessage />
                                    </FormItem>
                                )}
                            />
                            <FormField
                                control={form.control}
                                name='pg_port'
                                render={({ field }) => (
                                    <FormItem>
                                        <FormLabel>Port</FormLabel>
                                        <FormControl>
                                            <Input type='number' {...field} onChange={e => field.onChange(Number(e.target.value))} />
                                        </FormControl>
                                        <FormMessage />
                                    </FormItem>
                                )}
                            />
                        </div>
                        <FormField
                            control={form.control}
                            name='pg_database'
                            render={({ field }) => (
                                <FormItem>
                                    <FormLabel>Database</FormLabel>
                                    <FormControl>
                                        <Input {...field} placeholder='postgres' />
                                    </FormControl>
                                    <FormMessage />
                                </FormItem>
                            )}
                        />
                        <FormField
                            control={form.control}
                            name='pg_username'
                            render={({ field }) => (
                                <FormItem>
                                    <FormLabel>Username</FormLabel>
                                    <FormControl>
                                        <Input {...field} placeholder='postgres' />
                                    </FormControl>
                                    <FormMessage />
                                </FormItem>
                            )}
                        />
                        <FormField
                            control={form.control}
                            name='pg_password'
                            render={({ field }) => (
                                <FormItem>
                                    <FormLabel>Password</FormLabel>
                                    <FormControl>
                                        <Input type='password' {...field} placeholder={isUpdate ? 'Leave blank to keep unchanged' : ''} />
                                    </FormControl>
                                    <FormMessage />
                                </FormItem>
                            )}
                        />
                        <div className='grid grid-cols-2 gap-4'>
                            <FormField
                                control={form.control}
                                name='publication_name'
                                render={({ field }) => (
                                    <FormItem>
                                        <FormLabel>Publication</FormLabel>
                                        <FormControl>
                                            <Input {...field} placeholder='dbz_publication' />
                                        </FormControl>
                                        <FormMessage />
                                    </FormItem>
                                )}
                            />
                            <FormField
                                control={form.control}
                                name='replication_name'
                                render={({ field }) => (
                                    <FormItem>
                                        <FormLabel>Slot Name</FormLabel>
                                        <FormControl>
                                            <Input 
                                                {...field} 
                                                placeholder='dbz_replication_slot'
                                                onChange={e => {
                                                   // sanitize? Or just let zod handle it?
                                                   // Let's allow typing and validat on submit, or basic restrict
                                                   field.onChange(e.target.value);
                                                }}
                                            />
                                        </FormControl>
                                        <FormMessage />
                                    </FormItem>
                                )}
                            />
                        </div>

                        {/* Dynamic Helper Info */}

                        <Accordion type="single" collapsible className="w-full">
                            <AccordionItem value="sql-commands" className="border-b-0 rounded-md bg-muted px-4">
                                <AccordionTrigger className="py-3 hover:no-underline text-xs font-medium text-foreground">
                                    Run these SQL commands on your source database
                                </AccordionTrigger>
                                <AccordionContent className="pb-4 text-xs text-muted-foreground space-y-3">
                                    <div>
                                        <p className='mb-1'>1. Create Replication Slot:</p>
                                        <code className='relative block whitespace-pre-wrap break-all rounded bg-background px-[0.3rem] py-[0.2rem] font-mono font-semibold'>
                                            SELECT pg_create_logical_replication_slot('{form.watch('replication_name') || 'dbz_replication_slot'}', 'pgoutput');
                                        </code>
                                    </div>

                                    <div>
                                        <p className='mb-1'>2. Create Publication:</p>
                                        <code className='relative block whitespace-pre-wrap break-all rounded bg-background px-[0.3rem] py-[0.2rem] font-mono font-semibold'>
                                            CREATE PUBLICATION {form.watch('publication_name') || 'dbz_publication'} FOR ALL TABLES;
                                        </code>
                                    </div>
                                </AccordionContent>
                            </AccordionItem>
                        </Accordion>

                        <div className='pt-2'>
                            <Button
                                type='button'
                                variant='secondary'
                                className='w-full'
                                onClick={handleTestConnection}
                                disabled={isTesting}
                            >
                                {isTesting && <Loader2 className='mr-2 h-4 w-4 animate-spin' />}
                                Test Connection
                            </Button>
                        </div>

                    </form>
                </Form>
                <SheetFooter className='gap-2 sm:space-x-0'>
                    <SheetClose asChild>
                        <Button variant='outline'>Close</Button>
                    </SheetClose>
                    <Button form='sources-form' type='submit' disabled={isLoading}>
                        {isLoading && <Loader2 className='mr-2 h-4 w-4 animate-spin' />}
                        Save changes
                    </Button>
                </SheetFooter>
            </SheetContent>
        </Sheet >
    )
}
