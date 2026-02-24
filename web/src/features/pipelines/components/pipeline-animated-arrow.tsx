export function PipelineAnimatedArrow() {
  return (
    <div className='relative flex items-center justify-center w-24 h-6'>
      {/* Base track line */}
      <div className='absolute inset-x-0 h-[1px] bg-border/40' />

      {/* Animated flow gradient */}
      <div className='absolute inset-x-0 h-[1px] overflow-hidden'>
        <div className='absolute inset-0 w-full h-full bg-gradient-to-r from-transparent via-primary to-transparent -translate-x-full animate-[flowLink_1.5s_cubic-bezier(0.4,0,0.2,1)_infinite]' />
      </div>

      {/* Arrow head */}
      <div className='absolute right-0 flex items-center justify-center text-primary/80'>
        <svg
          width="12"
          height="12"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="3"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="ml-0.5"
        >
          <path d="M5 12h14" />
          <path d="m12 5 7 7-7 7" />
        </svg>
      </div>

      {/* Animation Styles */}
      <style dangerouslySetInnerHTML={{
        __html: `
        @keyframes flowLink {
          0% {
            transform: translateX(-100%);
            opacity: 0;
          }
          50% {
            opacity: 1;
          }
          100% {
            transform: translateX(100%);
            opacity: 0;
          }
        }
      `}} />
    </div>
  )
}
