import * as React from 'react'
import { Check, Copy } from 'lucide-react'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import {
    Tooltip,
    TooltipContent,
    TooltipProvider,
    TooltipTrigger,
} from '@/components/ui/tooltip'

interface CopyButtonProps extends React.ComponentProps<typeof Button> {
    value: string
    src?: string
}

export function CopyButton({
    value,
    className,
    src,
    variant = 'ghost',
    ...props
}: CopyButtonProps) {
    const [hasCopied, setHasCopied] = React.useState(false)

    React.useEffect(() => {
        setTimeout(() => {
            setHasCopied(false)
        }, 2000)
    }, [hasCopied])

    return (
        <TooltipProvider>
            <Tooltip>
                <TooltipTrigger asChild>
                    <Button
                        size='icon'
                        variant={variant}
                        className={cn(
                            'relative z-10 h-6 w-6 text-zinc-50h hover:bg-zinc-700 hover:text-zinc-50 [&_svg]:h-3 [&_svg]:w-3',
                            className
                        )}
                        onClick={() => {
                            if (typeof window === 'undefined') return
                            setHasCopied(true)
                            void window.navigator.clipboard.writeText(value)
                        }}
                        {...props}
                    >
                        <span className='sr-only'>Copy</span>
                        {hasCopied ? <Check /> : <Copy />}
                    </Button>
                </TooltipTrigger>
                <TooltipContent className='bg-black text-white'>
                    <p>Copy to clipboard</p>
                </TooltipContent>
            </Tooltip>
        </TooltipProvider>
    )
}
