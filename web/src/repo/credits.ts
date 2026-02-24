import { useMutation, useQuery } from '@tanstack/react-query'
import { api } from './client'

export interface WeeklyMonthlyUsage {
    current_week: number
    current_month: number
    previous_week: number
    previous_month: number
}

export interface DailyUsage {
    date: string
    credits: number
}

export interface CreditUsageResponse {
    summary: WeeklyMonthlyUsage
    daily_usage: DailyUsage[]
}

export const creditsRepo = {
    getUsage: async (destinationId: number): Promise<CreditUsageResponse> => {
        const response = await api.get<CreditUsageResponse>(`/destinations/${destinationId}/credits`)
        return response.data
    },

    refresh: async (destinationId: number): Promise<{ message: string }> => {
        const response = await api.post<{ message: string }>(`/destinations/${destinationId}/credits/refresh`)
        return response.data
    },
}

export const useCreditUsage = (destinationId: number) => {
    return useQuery({
        queryKey: ['credits', destinationId],
        queryFn: () => creditsRepo.getUsage(destinationId),
    })
}

export const useRefreshCredits = () => {
    return useMutation({
        mutationFn: (destinationId: number) => creditsRepo.refresh(destinationId),
    })
}
