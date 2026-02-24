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
      onClick={toggleTheme}
      className={cn(
        'relative inline-flex h-5 w-10 items-center rounded-full transition-all duration-500 ease-in-out',
        'focus:outline-none focus:ring-2 focus:ring-offset-1 focus:ring-offset-background',
        isDark
          ? 'bg-gradient-to-r from-indigo-600 via-purple-600 to-indigo-700 focus:ring-purple-500'
          : 'bg-gradient-to-r from-amber-400 via-orange-400 to-amber-500 focus:ring-orange-400'
      )}
      aria-label='Toggle theme'
    >
      {/* Sliding Circle */}
      <span
        className={cn(
          'inline-flex h-4 w-4 transform items-center justify-center rounded-full bg-white shadow-lg transition-all duration-500 ease-in-out',
          isDark ? 'translate-x-[22px]' : 'translate-x-0.5'
        )}
      >
        {/* Sun Icon - visible in light mode */}
        <Sun
          className={cn(
            'h-2.5 w-2.5 text-amber-500 transition-all duration-500',
            isDark
              ? 'scale-0 rotate-90 opacity-0'
              : 'scale-100 rotate-0 opacity-100'
          )}
        />
        {/* Moon Icon - visible in dark mode */}
        <Moon
          className={cn(
            'absolute h-2.5 w-2.5 text-indigo-600 transition-all duration-500',
            isDark
              ? 'scale-100 rotate-0 opacity-100'
              : 'scale-0 -rotate-90 opacity-0'
          )}
        />
      </span>

      {/* Background Icons */}
      <span className='absolute inset-0 flex items-center justify-between px-1'>
        <Sun
          className={cn(
            'h-2 w-2 text-white transition-all duration-500',
            isDark ? 'scale-0 opacity-0' : 'scale-100 opacity-100'
          )}
        />
        <Moon
          className={cn(
            'h-2 w-2 text-white transition-all duration-500',
            isDark ? 'scale-100 opacity-100' : 'scale-0 opacity-0'
          )}
        />
      </span>
    </button>
  )
}
