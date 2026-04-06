import { useEffect } from 'react'
import { Moon, Sun } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useTheme } from '@/context/theme-provider'

export function ThemeSwitch() {
  const { setTheme, resolvedTheme } = useTheme()

  /* Update theme-color meta tag
   * when theme is updated */
  useEffect(() => {
    const themeColor = resolvedTheme === 'dark' ? '#020817' : '#fff'
    const metaThemeColor = document.querySelector("meta[name='theme-color']")
    if (metaThemeColor) metaThemeColor.setAttribute('content', themeColor)
  }, [resolvedTheme])

  const isDark = resolvedTheme === 'dark'

  const toggleTheme = () => {
    // Toggle between light and dark (ignore system for simple switch)
    if (isDark) {
      setTheme('light')
    } else {
      setTheme('dark')
    }
  }

  return (
    <button
      type='button'
      onClick={toggleTheme}
      className={cn(
        'relative inline-flex h-6 w-11 items-center overflow-hidden rounded-full p-0.5 transition-all duration-300 ease-out',
        'focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-background',
        isDark
          ? 'bg-gradient-to-r from-slate-700 via-indigo-600 to-violet-600 shadow-[inset_0_1px_0_rgba(255,255,255,0.08)] focus:ring-violet-400'
          : 'bg-gradient-to-r from-amber-300 via-orange-400 to-amber-500 shadow-[inset_0_1px_0_rgba(255,255,255,0.35)] focus:ring-orange-400'
      )}
      aria-label={`Switch to ${isDark ? 'light' : 'dark'} mode`}
      aria-pressed={isDark}
    >
      <span className='pointer-events-none absolute inset-y-0 left-1 flex items-center'>
        <Sun
          className={cn(
            'h-3 w-3 transition-all duration-300',
            isDark
              ? '-rotate-90 scale-75 opacity-0'
              : 'rotate-0 scale-100 text-white/90 opacity-100'
          )}
        />
      </span>

      <span className='pointer-events-none absolute inset-y-0 right-1 flex items-center'>
        <Moon
          className={cn(
            'h-3 w-3 transition-all duration-300',
            isDark
              ? 'rotate-0 scale-100 text-white/90 opacity-100'
              : 'rotate-90 scale-75 opacity-0'
          )}
        />
      </span>

      <span
        className={cn(
          'relative z-10 inline-flex h-5 w-5 items-center justify-center rounded-full ring-1 shadow-lg transition-all duration-300 ease-out',
          isDark
            ? 'translate-x-5 bg-slate-950 text-sky-100 ring-white/10 shadow-black/50'
            : 'translate-x-0 bg-white/95 text-amber-500 ring-black/5 shadow-orange-950/20'
        )}
      >
        {isDark ? <Moon className='h-3 w-3' /> : <Sun className='h-3 w-3' />}
      </span>
    </button>
  )
}
