import { cn } from "@/lib/utils"

interface GlassCardProps extends React.HTMLAttributes<HTMLDivElement> {
  children: React.ReactNode
  className?: string
  noPadding?: boolean
}

export function GlassCard({ children, className, noPadding, ...props }: GlassCardProps) {
  return (
    <div
      className={cn(
        "relative overflow-hidden rounded-xl border-2 border-border/60 bg-white/5 backdrop-blur-md transition-all duration-300",
        "dark:bg-black/20 dark:border-white/10",
        "hover:bg-white/10 dark:hover:bg-black/30 hover:border-border",
        className
      )}
      {...props}
    >
      <div className={cn("relative z-10", !noPadding && "p-6")}>
        {children}
      </div>
      
      {/* Subtle gradient overlay */}
      <div className="absolute inset-0 z-0 bg-gradient-to-br from-white/5 to-transparent dark:from-white/5 dark:to-transparent pointer-events-none" />
    </div>
  )
}

interface GlassCardHeaderProps extends React.HTMLAttributes<HTMLDivElement> {}

export function GlassCardHeader({ className, children, ...props }: GlassCardHeaderProps) {
  return (
    <div
      className={cn("flex flex-col space-y-1.5 p-6 pb-2", className)}
      {...props}
    >
      {children}
    </div>
  )
}

interface GlassCardTitleProps extends React.HTMLAttributes<HTMLHeadingElement> {}

export function GlassCardTitle({ className, children, ...props }: GlassCardTitleProps) {
  return (
    <h3
      className={cn("font-semibold leading-none tracking-tight", className)}
      {...props}
    >
      {children}
    </h3>
  )
}

interface GlassCardDescriptionProps extends React.HTMLAttributes<HTMLParagraphElement> {}

export function GlassCardDescription({ className, children, ...props }: GlassCardDescriptionProps) {
  return (
    <p
      className={cn("text-sm text-muted-foreground", className)}
      {...props}
    >
      {children}
    </p>
  )
}

interface GlassCardContentProps extends React.HTMLAttributes<HTMLDivElement> {}

export function GlassCardContent({ className, children, ...props }: GlassCardContentProps) {
  return (
    <div className={cn("p-6 pt-0", className)} {...props}>
      {children}
    </div>
  )
}
