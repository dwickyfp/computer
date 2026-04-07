export const DASHBOARD_QUERY_CACHE_TIME = 5 * 60 * 1000

export function getDashboardPollingQueryOptions(refreshInterval: number) {
  return {
    refetchInterval: refreshInterval,
    staleTime: refreshInterval,
    gcTime: DASHBOARD_QUERY_CACHE_TIME,
    refetchOnWindowFocus: false,
  }
}
