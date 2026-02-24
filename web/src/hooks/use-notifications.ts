import { useState, useEffect, useCallback } from 'react'
import { notificationRepo, type NotificationLog } from '../repo/notifications'

export function useNotifications() {
    const [notifications, setNotifications] = useState<NotificationLog[]>([])
    const [unreadCount, setUnreadCount] = useState(0)
    const [isLoading, setIsLoading] = useState(false)
    const [error, setError] = useState<Error | null>(null)

    const fetchNotifications = useCallback(async () => {
        setIsLoading(true)
        try {
            // Fetch all active notifications (is_deleted=False is default in backend)
            // We want both read and unread to show in the list, but we need unread count.
            const data = await notificationRepo.getAll({ limit: 50 })
            setNotifications(data)

            // Calculate unread count from the fetched data
            // This assumes the limit is high enough to capture most unread ones, 
            // or we might need a separate endpoint for count if pagination is used.
            // For now, filtering the fetched list is a reasonable started.
            const unread = data.filter(n => !n.is_read).length
            setUnreadCount(unread)
            setError(null)
        } catch (err: any) {
            setError(err)
            console.error('Failed to fetch notifications:', err)
        } finally {
            setIsLoading(false)
        }
    }, [])

    const markAsRead = async (id: number) => {
        try {
            await notificationRepo.markAsRead(id)
            // Optimistic update
            setNotifications(prev =>
                prev.map(n => n.id === id ? { ...n, is_read: true } : n)
            )
            setUnreadCount(prev => Math.max(0, prev - 1))
        } catch (err) {
            console.error('Failed to mark notification as read:', err)
            // Revert or re-fetch would be ideal here
            fetchNotifications()
        }
    }

    const markAllAsRead = async () => {
        try {
            await notificationRepo.markAllAsRead()
            // Optimistic update
            setNotifications(prev => prev.map(n => ({ ...n, is_read: true })))
            setUnreadCount(0)
        } catch (err) {
            console.error('Failed to mark all as read:', err)
            fetchNotifications()
        }
    }

    const deleteNotification = async (id: number) => {
        try {
            await notificationRepo.delete(id)
            // Optimistic update
            setNotifications(prev => {
                const target = prev.find(n => n.id === id)
                const newNotifications = prev.filter(n => n.id !== id)

                // If the deleted one was unread, decrement count
                if (target && !target.is_read) {
                    setUnreadCount(prevCount => Math.max(0, prevCount - 1))
                }
                return newNotifications
            })
        } catch (err) {
            console.error('Failed to delete notification:', err)
            fetchNotifications()
        }
    }

    const deleteAllNotifications = async () => {
        try {
            await notificationRepo.deleteAll()
            // Optimistic update
            setNotifications([])
            setUnreadCount(0)
        } catch (err) {
            console.error('Failed to delete all notifications:', err)
            fetchNotifications()
        }
    }

    useEffect(() => {
        fetchNotifications()

        // Polling every 30 seconds
        const interval = setInterval(fetchNotifications, 30000)
        return () => clearInterval(interval)
    }, [fetchNotifications])

    return {
        notifications,
        unreadCount,
        isLoading,
        error,
        fetchNotifications,
        markAsRead,
        markAllAsRead,
        deleteNotification,
        deleteAllNotifications
    }
}
