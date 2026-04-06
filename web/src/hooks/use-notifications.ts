import { useEffect, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { notificationRepo, type NotificationLog } from '../repo/notifications'

const NOTIFICATIONS_KEY = ['notifications'] as const

export function useNotifications() {
  const queryClient = useQueryClient()

  // ── Data fetching ────────────────────────────────────────────────────────
  const {
    data: notifications = [],
    isLoading,
    error,
  } = useQuery({
    queryKey: NOTIFICATIONS_KEY,
    queryFn: () => notificationRepo.getAll({ limit: 50 }),
    refetchInterval: 30_000,
    refetchIntervalInBackground: false,
    staleTime: 5_000,
  })

  const unreadCount = notifications.filter((n) => !n.is_read).length

  // ── Document title badge ─────────────────────────────────────────────────
  useEffect(() => {
    const applyTitle = () => {
      const base = document.title.replace(/^\(\d+\) /, '')
      document.title = unreadCount > 0 ? `(${unreadCount}) ${base}` : base
    }

    applyTitle()

    const titleEl = document.querySelector('title')
    if (!titleEl) return

    const observer = new MutationObserver(() => {
      if (unreadCount > 0 && !document.title.startsWith(`(${unreadCount})`)) {
        applyTitle()
      }
    })

    observer.observe(titleEl, {
      childList: true,
      characterData: true,
      subtree: true,
    })

    return () => observer.disconnect()
  }, [unreadCount])

  // ── markAsRead ────────────────────────────────────────────────────────────
  const markAsReadMutation = useMutation({
    mutationFn: notificationRepo.markAsRead,
    onMutate: async (id) => {
      await queryClient.cancelQueries({ queryKey: NOTIFICATIONS_KEY })
      const prev = queryClient.getQueryData<NotificationLog[]>(NOTIFICATIONS_KEY)
      queryClient.setQueryData<NotificationLog[]>(NOTIFICATIONS_KEY, (old = []) =>
        old.map((n) => (n.id === id ? { ...n, is_read: true } : n))
      )
      return { prev }
    },
    onError: (_err, _id, ctx) => {
      if (ctx?.prev) queryClient.setQueryData(NOTIFICATIONS_KEY, ctx.prev)
    },
    onSettled: () => queryClient.invalidateQueries({ queryKey: NOTIFICATIONS_KEY }),
  })

  // ── markAllAsRead ─────────────────────────────────────────────────────────
  const markAllAsReadMutation = useMutation({
    mutationFn: notificationRepo.markAllAsRead,
    onMutate: async () => {
      await queryClient.cancelQueries({ queryKey: NOTIFICATIONS_KEY })
      const prev = queryClient.getQueryData<NotificationLog[]>(NOTIFICATIONS_KEY)
      queryClient.setQueryData<NotificationLog[]>(NOTIFICATIONS_KEY, (old = []) =>
        old.map((n) => ({ ...n, is_read: true }))
      )
      return { prev }
    },
    onError: (_err, _vars, ctx) => {
      if (ctx?.prev) queryClient.setQueryData(NOTIFICATIONS_KEY, ctx.prev)
    },
    onSettled: () => queryClient.invalidateQueries({ queryKey: NOTIFICATIONS_KEY }),
  })

  // ── deleteNotification ────────────────────────────────────────────────────
  const deleteMutation = useMutation({
    mutationFn: notificationRepo.delete,
    onMutate: async (id) => {
      await queryClient.cancelQueries({ queryKey: NOTIFICATIONS_KEY })
      const prev = queryClient.getQueryData<NotificationLog[]>(NOTIFICATIONS_KEY)
      queryClient.setQueryData<NotificationLog[]>(NOTIFICATIONS_KEY, (old = []) =>
        old.filter((n) => n.id !== id)
      )
      return { prev }
    },
    onError: (_err, _id, ctx) => {
      if (ctx?.prev) queryClient.setQueryData(NOTIFICATIONS_KEY, ctx.prev)
    },
    onSettled: () => queryClient.invalidateQueries({ queryKey: NOTIFICATIONS_KEY }),
  })

  // ── deleteAllNotifications ────────────────────────────────────────────────
  const deleteAllMutation = useMutation({
    mutationFn: notificationRepo.deleteAll,
    onMutate: async () => {
      await queryClient.cancelQueries({ queryKey: NOTIFICATIONS_KEY })
      const prev = queryClient.getQueryData<NotificationLog[]>(NOTIFICATIONS_KEY)
      queryClient.setQueryData<NotificationLog[]>(NOTIFICATIONS_KEY, [])
      return { prev }
    },
    onError: (_err, _vars, ctx) => {
      if (ctx?.prev) queryClient.setQueryData(NOTIFICATIONS_KEY, ctx.prev)
    },
    onSettled: () => queryClient.invalidateQueries({ queryKey: NOTIFICATIONS_KEY }),
  })

  // ── Manual refetch (backward-compat) ─────────────────────────────────────
  const fetchNotifications = useCallback(
    () => queryClient.invalidateQueries({ queryKey: NOTIFICATIONS_KEY }),
    [queryClient]
  )

  return {
    notifications,
    unreadCount,
    isLoading,
    error: error as Error | null,
    fetchNotifications,
    markAsRead: (id: number) => markAsReadMutation.mutate(id),
    markAllAsRead: () => markAllAsReadMutation.mutate(),
    deleteNotification: (id: number) => deleteMutation.mutate(id),
    deleteAllNotifications: () => deleteAllMutation.mutate(),
  }
}
