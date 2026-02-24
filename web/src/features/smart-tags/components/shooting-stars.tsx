import { useEffect, useRef } from 'react'

interface Star {
  id: number
  x: number
  y: number
  angle: number // radians — direction of travel
  speed: number // px per frame
  length: number // tail length
  opacity: number
  life: number // remaining frames
  maxLife: number
  size: number
}

/**
 * Shooting-stars canvas overlay for dark-mode ReactFlow backgrounds.
 * Stars spawn at random positions, travel in a random direction with a
 * glowing tail, and fade out after a short life span.
 */
export function ShootingStars() {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const starsRef = useRef<Star[]>([])
  const nextIdRef = useRef(0)
  const animRef = useRef<number>(0)

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return

    const ctx = canvas.getContext('2d')
    if (!ctx) return

    // Resize canvas to fill parent
    const resize = () => {
      const parent = canvas.parentElement
      if (parent) {
        canvas.width = parent.clientWidth
        canvas.height = parent.clientHeight
      }
    }
    resize()
    const resizeObserver = new ResizeObserver(resize)
    if (canvas.parentElement) resizeObserver.observe(canvas.parentElement)

    // Spawn a new star
    const spawnStar = () => {
      const w = canvas.width
      const h = canvas.height
      if (w === 0 || h === 0) return

      // Random start point anywhere on the canvas
      const x = Math.random() * w
      const y = Math.random() * h

      // Random angle — biased toward lower-right for a natural "falling star" feel
      // Range: roughly 20° to 70° from horizontal (0.35 to 1.22 rad)
      const angle = Math.PI * 0.1 + Math.random() * Math.PI * 0.35

      const speed = 3 + Math.random() * 5
      const length = 40 + Math.random() * 80
      const maxLife = 40 + Math.floor(Math.random() * 60)
      const size = 1 + Math.random() * 1.5

      starsRef.current.push({
        id: nextIdRef.current++,
        x,
        y,
        angle,
        speed,
        length,
        opacity: 0,
        life: maxLife,
        maxLife,
        size,
      })
    }

    // Animation loop
    let spawnAccum = 0
    const draw = () => {
      ctx.clearRect(0, 0, canvas.width, canvas.height)

      // Spawn new stars probabilistically
      spawnAccum += 0.03 // ~1 star every ~33 frames on average
      while (spawnAccum >= 1) {
        spawnStar()
        spawnAccum -= 1
      }
      // Small chance of burst (2-3 stars at once)
      if (Math.random() < 0.005) {
        const burst = 2 + Math.floor(Math.random() * 2)
        for (let b = 0; b < burst; b++) spawnStar()
      }

      const alive: Star[] = []

      for (const star of starsRef.current) {
        // Move
        star.x += Math.cos(star.angle) * star.speed
        star.y += Math.sin(star.angle) * star.speed
        star.life--

        // Fade in / out
        const lifeRatio = star.life / star.maxLife
        if (lifeRatio > 0.8) {
          // Fade in during first 20%
          star.opacity = (1 - lifeRatio) / 0.2
        } else if (lifeRatio < 0.3) {
          // Fade out during last 30%
          star.opacity = lifeRatio / 0.3
        } else {
          star.opacity = 1
        }

        if (star.life <= 0) continue

        // Draw tail (gradient line)
        const tailX = star.x - Math.cos(star.angle) * star.length
        const tailY = star.y - Math.sin(star.angle) * star.length

        const gradient = ctx.createLinearGradient(tailX, tailY, star.x, star.y)
        gradient.addColorStop(0, `rgba(129, 140, 248, 0)`) // indigo-400 transparent
        gradient.addColorStop(0.6, `rgba(129, 140, 248, ${star.opacity * 0.3})`)
        gradient.addColorStop(1, `rgba(199, 210, 254, ${star.opacity * 0.8})`) // indigo-200

        ctx.beginPath()
        ctx.moveTo(tailX, tailY)
        ctx.lineTo(star.x, star.y)
        ctx.strokeStyle = gradient
        ctx.lineWidth = star.size
        ctx.lineCap = 'round'
        ctx.stroke()

        // Draw head glow
        const headGlow = ctx.createRadialGradient(
          star.x, star.y, 0,
          star.x, star.y, star.size * 4
        )
        headGlow.addColorStop(0, `rgba(224, 231, 255, ${star.opacity * 0.9})`) // indigo-100
        headGlow.addColorStop(0.5, `rgba(129, 140, 248, ${star.opacity * 0.4})`) // indigo-400
        headGlow.addColorStop(1, `rgba(129, 140, 248, 0)`)

        ctx.beginPath()
        ctx.arc(star.x, star.y, star.size * 4, 0, Math.PI * 2)
        ctx.fillStyle = headGlow
        ctx.fill()

        // Bright core
        ctx.beginPath()
        ctx.arc(star.x, star.y, star.size * 0.8, 0, Math.PI * 2)
        ctx.fillStyle = `rgba(255, 255, 255, ${star.opacity * 0.9})`
        ctx.fill()

        alive.push(star)
      }

      starsRef.current = alive
      animRef.current = requestAnimationFrame(draw)
    }

    animRef.current = requestAnimationFrame(draw)

    return () => {
      cancelAnimationFrame(animRef.current)
      resizeObserver.disconnect()
    }
  }, [])

  return (
    <canvas
      ref={canvasRef}
      className="pointer-events-none absolute inset-0 z-[1]"
      style={{ mixBlendMode: 'screen' }}
    />
  )
}
