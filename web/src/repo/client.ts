import axios from 'axios'

const getBaseUrl = (): string => {
    if (import.meta.env.VITE_API_URL) {
        return import.meta.env.VITE_API_URL
    }
    if (import.meta.env.DEV) {
        return 'http://localhost:8000/api/v1'
    }
    const origin = typeof window !== 'undefined' ? window.location.origin : ''
    return `${origin}/api`
}

const baseURL = getBaseUrl()

export const api = axios.create({
    baseURL,
    headers: {
        'Content-Type': 'application/json',
    },
})

api.interceptors.response.use(
    (response) => response,
    (error) => {
        return Promise.reject(error)
    }
)
