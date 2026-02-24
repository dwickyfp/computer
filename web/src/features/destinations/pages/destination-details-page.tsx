import { useParams, Link, useNavigate } from '@tanstack/react-router'
import { useEffect } from 'react'
import { useCreditUsage, useRefreshCredits } from '@/repo/credits'
import { Main } from '@/components/layout/main'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Bar, BarChart, ResponsiveContainer, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts'
import { Button } from '@/components/ui/button'
import {
    Loader2,
    RefreshCw,
    Snowflake,
    TrendingUp,
    TrendingDown,
    DollarSign,
    CreditCard,
    Database,
    CalendarCheck
} from 'lucide-react'
import { toast } from 'sonner'
import { format, subDays, isSameDay } from 'date-fns'
import { Header } from '@/components/layout/header'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'

import { useQuery } from '@tanstack/react-query'
import { destinationsRepo } from '@/repo/destinations'
import {
    Breadcrumb,
    BreadcrumbItem,
    BreadcrumbLink,
    BreadcrumbList,
    BreadcrumbPage,
    BreadcrumbSeparator,
} from '@/components/ui/breadcrumb'

interface MetricCardProps {
    title: string
    value: number
    previousValue?: number
    icon: React.ElementType
    formatter?: (val: number) => string
    subFormatter?: (val: number) => string
    trendLabel?: string
}

function MetricCard({ title, value, previousValue, icon: Icon, formatter, subFormatter, trendLabel }: MetricCardProps) {
    const diff = previousValue ? value - previousValue : 0
    const percentChange = previousValue ? ((value - previousValue) / previousValue) * 100 : 0
    const isPositive = diff > 0
    const isNeutral = diff === 0

    // For credits, lower usage might be "better", but usually "up" is red/green depending on context. 
    // Here we'll treat "more usage" as "up" (neutral color) or potentially warning if desired.
    // For now, let's just show direction.

    return (
        <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">{title}</CardTitle>
                <Icon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
                <div className="text-2xl font-bold">{formatter ? formatter(value) : value}</div>
                <div className="flex items-center text-xs text-muted-foreground mt-1">
                    {previousValue !== undefined && (
                        <>
                            {isNeutral ? (
                                <span className="flex items-center text-muted-foreground mr-1">No change</span>
                            ) : isPositive ? (
                                <span className="flex items-center text-rose-500 mr-1">
                                    <TrendingUp className="mr-1 h-3 w-3" />
                                    {Math.abs(percentChange).toFixed(1)}%
                                </span>
                            ) : (
                                <span className="flex items-center text-emerald-500 mr-1">
                                    <TrendingDown className="mr-1 h-3 w-3" />
                                    {Math.abs(percentChange).toFixed(1)}%
                                </span>
                            )}
                            {trendLabel && <span className="text-muted-foreground">{trendLabel}</span>}
                        </>
                    )}
                </div>
                {subFormatter && (
                    <p className='mt-2 text-xs text-muted-foreground'>
                        {subFormatter(value)}
                    </p>
                )}
            </CardContent>
        </Card>
    )
}

export function DestinationDetailsPage() {
    const { destinationId } = useParams({ from: '/_authenticated/destinations/$destinationId' })
    const { data, isLoading, refetch } = useCreditUsage(destinationId)
    const { mutate: refreshCredits, isPending: isRefreshing } = useRefreshCredits()

    const { data: destination, isError, isLoading: isDestinationLoading } = useQuery({
        queryKey: ['destination', destinationId],
        queryFn: () => destinationsRepo.get(Number(destinationId)),
        retry: false, // Don't retry if it's a 404
    })

    const navigate = useNavigate()

    useEffect(() => {
        if (!isDestinationLoading && (isError || !destination || (destination && destination.type !== 'SNOWFLAKE'))) {
            navigate({ to: '/destinations' })
        }
    }, [destination, isError, isDestinationLoading, navigate])

    const handleRefresh = () => {
        refreshCredits(destinationId, {
            onSuccess: () => {
                toast.success('Credit usage data refreshed')
                refetch()
            },
            onError: (error) => {
                toast.error('Failed to refresh data: ' + error.message)
            }
        })
    }

    if (isLoading) {
        return (
            <Main>
                <div className="flex h-full items-center justify-center">
                    <Loader2 className="h-8 w-8 animate-spin" />
                </div>
            </Main>
        )
    }

    const summary = data?.summary
    const chartData = Array.from({ length: 7 }, (_, i) => {
        const date = subDays(new Date(), 6 - i)
        // Adjust date to match local timezone effectively or just simple filtering
        const dayData = data?.daily_usage?.find(d => isSameDay(new Date(d.date), date))
        return {
            date: format(date, 'MMM dd'),
            credits: dayData?.credits ?? 0,
            cost: (dayData?.credits ?? 0) * 3.7
        }
    })

    const formatCredits = (val: number) => val.toFixed(4)
    const formatCost = (val: number) => `$${(val * 3.7).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`

    return (
        <>
            <Header fixed>
                <Search />
                <div className='ms-auto flex items-center space-x-4'>
                    <ThemeSwitch />
                    
                </div>
            </Header>

            <Main className='flex flex-1 flex-col gap-4 sm:gap-6'>
                {/* Breadcrumbs */}
                <Breadcrumb>
                    <BreadcrumbList>
                        <BreadcrumbItem>
                            <BreadcrumbLink asChild>
                                <Link to="/destinations">Destinations</Link>
                            </BreadcrumbLink>
                        </BreadcrumbItem>
                        <BreadcrumbSeparator />
                        <BreadcrumbItem>
                            <BreadcrumbPage className="font-semibold">{destination?.name || 'Details'}</BreadcrumbPage>
                        </BreadcrumbItem>
                    </BreadcrumbList>
                </Breadcrumb>

                {/* Page Header */}
                <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4 border-b pb-6">
                    <div className="flex items-center gap-4">
                        <div className="p-3 bg-blue-500/10 rounded-lg">
                            <Snowflake className="h-8 w-8 text-blue-500" />
                        </div>
                        <div>
                            <h2 className="text-3xl font-bold tracking-tight">{destination?.name}</h2>
                            <p className="text-muted-foreground flex items-center gap-2">
                                <Database className="h-4 w-4" />
                                Snowflake Destination
                                <span className="text-gray-300">•</span>
                                Credit Usage & Cost Monitoring
                            </p>
                        </div>
                    </div>
                    <div className="flex items-center gap-2">

                        <Button onClick={handleRefresh} disabled={isRefreshing} variant="outline">
                            {isRefreshing ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <RefreshCw className="mr-2 h-4 w-4" />}
                            Refresh
                        </Button>
                    </div>
                </div>

                {/* Metrics Grid */}
                <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                    <MetricCard
                        title="Credits (This Week)"
                        value={summary?.current_week ?? 0}
                        previousValue={summary?.previous_week}
                        icon={CreditCard}
                        formatter={formatCredits}
                        subFormatter={formatCost}
                        trendLabel="vs last week"
                    />
                    <MetricCard
                        title="Estimated Cost (This Week)"
                        value={(summary?.current_week ?? 0) * 3.7}
                        previousValue={(summary?.previous_week ?? 0) * 3.7}
                        icon={DollarSign}
                        formatter={(val) => `$${val.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
                        trendLabel="vs last week"
                    />
                    <MetricCard
                        title="Credits (This Month)"
                        value={summary?.current_month ?? 0}
                        previousValue={summary?.previous_month}
                        icon={CalendarCheck}
                        formatter={formatCredits}
                        subFormatter={formatCost}
                        trendLabel="vs last month"
                    />
                    <MetricCard
                        title="Estimated Cost (This Month)"
                        value={(summary?.current_month ?? 0) * 3.7}
                        previousValue={(summary?.previous_month ?? 0) * 3.7}
                        icon={DollarSign}
                        formatter={(val) => `$${val.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
                        trendLabel="vs last month"
                    />
                </div>

                {/* Charts Area */}
                <div className="grid gap-4 md:grid-cols-1">
                    <Card>
                        <CardHeader>
                            <CardTitle>Daily Usage Trends</CardTitle>
                            <CardDescription>
                                Credit consumption over the last 7 days.
                            </CardDescription>
                        </CardHeader>
                        <CardContent className="pl-0">
                            <div className="h-[400px] w-full mt-4">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                                        <CartesianGrid strokeDasharray="3 3" vertical={false} className="stroke-muted/30" />
                                        <XAxis
                                            dataKey="date"
                                            stroke="#888888"
                                            fontSize={12}
                                            tickLine={false}
                                            axisLine={false}
                                            tickMargin={10}
                                        />
                                        <YAxis
                                            stroke="#888888"
                                            fontSize={12}
                                            tickLine={false}
                                            axisLine={false}
                                            tickFormatter={(value) => `${value}`}
                                        />
                                        <Tooltip
                                            cursor={{ fill: 'var(--muted)', opacity: 0.1 }}
                                            content={({ active, payload, label }) => {
                                                if (active && payload && payload.length) {
                                                    return (
                                                        <div className="rounded-lg border bg-background p-2 shadow-sm">
                                                            <div className="grid grid-cols-2 gap-2">
                                                                <div className="flex flex-col">
                                                                    <span className="text-[0.70rem] uppercase text-muted-foreground">
                                                                        Date
                                                                    </span>
                                                                    <span className="font-bold text-muted-foreground">
                                                                        {label}
                                                                    </span>
                                                                </div>
                                                                <div className="flex flex-col">
                                                                    <span className="text-[0.70rem] uppercase text-muted-foreground">
                                                                        Credits
                                                                    </span>
                                                                    <span className="font-bold">
                                                                        {Number(payload[0].value).toFixed(6)}
                                                                    </span>
                                                                </div>
                                                                <div className="flex flex-col col-span-2">
                                                                    <span className="text-[0.70rem] uppercase text-muted-foreground">
                                                                        Est. Cost
                                                                    </span>
                                                                    <span className="font-bold text-emerald-600">
                                                                        ${(Number(payload[0].payload.cost)).toFixed(4)}
                                                                    </span>
                                                                </div>
                                                            </div>
                                                        </div>
                                                    );
                                                }
                                                return null;
                                            }}
                                        />
                                        <Bar
                                            dataKey="credits"
                                            fill="#3b82f6"
                                            radius={[4, 4, 0, 0]}
                                            maxBarSize={50}
                                        />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </CardContent>
                    </Card>
                </div>
            </Main>
        </>
    )
}
