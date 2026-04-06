type QueryParamValue = string | number | boolean | Date | null | undefined

type QueryParams = object

type QueryParamInput = QueryParamValue | QueryParamValue[]

export interface ApiRequestOptions<TParams extends QueryParams = QueryParams> {
  headers?: HeadersInit
  params?: TParams
  signal?: AbortSignal
}

export interface ApiResponse<T> {
  data: T
  headers: Headers
  status: number
}

interface ApiErrorOptions<T = unknown> {
  data?: T
  headers?: Headers
  message: string
  status: number
}

const JSON_CONTENT_TYPE = 'application/json'

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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function formatQueryParamValue(
  value: Exclude<QueryParamValue, null | undefined>
) {
  if (value instanceof Date) {
    return value.toISOString()
  }
  return String(value)
}

function isSerializableQueryParamValue(
  value: unknown
): value is Exclude<QueryParamValue, null | undefined> {
  return (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean' ||
    value instanceof Date
  )
}

function isQueryParamInput(value: unknown): value is QueryParamInput {
  if (value == null) {
    return true
  }

  if (Array.isArray(value)) {
    return value.every(
      (item) => item == null || isSerializableQueryParamValue(item)
    )
  }

  return isSerializableQueryParamValue(value)
}

function appendSearchParams(
  searchParams: URLSearchParams,
  params?: QueryParams
) {
  if (!params) return

  for (const [key, value] of Object.entries(
    params as Record<string, unknown>
  )) {
    if (value == null || !isQueryParamInput(value)) continue

    if (Array.isArray(value)) {
      for (const item of value) {
        if (item == null || !isSerializableQueryParamValue(item)) continue
        searchParams.append(key, formatQueryParamValue(item))
      }
      continue
    }

    if (!isSerializableQueryParamValue(value)) continue
    searchParams.set(key, formatQueryParamValue(value))
  }
}

async function parseResponseBody(response: Response): Promise<unknown> {
  if (response.status === 204 || response.status === 205) {
    return undefined
  }

  const contentType = response.headers.get('content-type') ?? ''
  if (
    contentType.includes(JSON_CONTENT_TYPE) ||
    contentType.includes('+json')
  ) {
    return response.json()
  }

  const text = await response.text()
  if (!text) {
    return undefined
  }

  return text
}

export function getApiErrorMessage(
  error: unknown,
  fallback = 'Something went wrong!'
): string {
  if (error instanceof ApiError) {
    return getApiErrorMessage(error.data, error.message || fallback)
  }

  if (typeof error === 'string' && error.trim()) {
    return error
  }

  if (isRecord(error)) {
    for (const key of ['detail', 'title', 'message', 'error']) {
      const value = error[key]
      if (typeof value === 'string' && value.trim()) {
        return value
      }
    }
  }

  if (error instanceof Error && error.message.trim()) {
    return error.message
  }

  return fallback
}

export class ApiError<T = unknown> extends Error {
  data?: T
  headers?: Headers
  status: number

  constructor({ data, headers, message, status }: ApiErrorOptions<T>) {
    super(message)
    this.name = 'ApiError'
    this.data = data
    this.headers = headers
    this.status = status
  }

  get response() {
    return {
      data: this.data,
      headers: this.headers,
      status: this.status,
    }
  }
}

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
  options: ApiRequestOptions = {}
): Promise<ApiResponse<T>> {
  const baseUrl = getBaseUrl()
  const normalizedBaseUrl = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`
  const normalizedPath = path.startsWith('/') ? path.slice(1) : path
  const url = new URL(normalizedPath, normalizedBaseUrl)

  appendSearchParams(url.searchParams, options.params)

  const headers = new Headers(options.headers)
  const hasJsonBody =
    body !== undefined && body !== null && !(body instanceof FormData)

  if (hasJsonBody && !headers.has('Content-Type')) {
    headers.set('Content-Type', JSON_CONTENT_TYPE)
  }
  if (!headers.has('Accept')) {
    headers.set('Accept', JSON_CONTENT_TYPE)
  }

  try {
    const response = await fetch(url.toString(), {
      body: hasJsonBody
        ? JSON.stringify(body)
        : body instanceof FormData
          ? body
          : undefined,
      credentials: 'same-origin',
      headers,
      method,
      signal: options.signal,
    })

    const data = await parseResponseBody(response)

    if (!response.ok) {
      throw new ApiError({
        data,
        headers: response.headers,
        message: getApiErrorMessage(
          data,
          response.statusText || 'Request failed'
        ),
        status: response.status,
      })
    }

    return {
      data: data as T,
      headers: response.headers,
      status: response.status,
    }
  } catch (error) {
    if (error instanceof ApiError) {
      throw error
    }

    if (error instanceof DOMException && error.name === 'AbortError') {
      throw error
    }

    throw new ApiError({
      message: getApiErrorMessage(error, 'Network request failed'),
      status: 0,
    })
  }
}

export const api = {
  delete<T, TParams extends QueryParams = QueryParams>(
    path: string,
    options?: ApiRequestOptions<TParams>
  ) {
    return request<T>('DELETE', path, undefined, options)
  },

  get<T, TParams extends QueryParams = QueryParams>(
    path: string,
    options?: ApiRequestOptions<TParams>
  ) {
    return request<T>('GET', path, undefined, options)
  },

  patch<T, TParams extends QueryParams = QueryParams>(
    path: string,
    body?: unknown,
    options?: ApiRequestOptions<TParams>
  ) {
    return request<T>('PATCH', path, body, options)
  },

  post<T, TParams extends QueryParams = QueryParams>(
    path: string,
    body?: unknown,
    options?: ApiRequestOptions<TParams>
  ) {
    return request<T>('POST', path, body, options)
  },

  put<T, TParams extends QueryParams = QueryParams>(
    path: string,
    body?: unknown,
    options?: ApiRequestOptions<TParams>
  ) {
    return request<T>('PUT', path, body, options)
  },
}
