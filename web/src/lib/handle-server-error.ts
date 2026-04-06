import { ApiError, getApiErrorMessage } from '@/repo/client'
import { toast } from 'sonner'

export { ApiError, getApiErrorMessage }
export function handleServerError(error: unknown) {
  // eslint-disable-next-line no-console
  console.log(error)

  let errMsg = 'Something went wrong!'

  if (
    error &&
    typeof error === 'object' &&
    'status' in error &&
    Number(error.status) === 204
  ) {
    errMsg = 'Content not found.'
  }

  if (error instanceof ApiError) {
    errMsg = getApiErrorMessage(error, errMsg)
  } else if (error instanceof Error) {
    errMsg = error.message || errMsg
  }

  toast.error(errMsg)
}
