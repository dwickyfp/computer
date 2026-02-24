import {
  LayoutDashboard,
  Settings,
  Snowflake,
  Workflow,
  Command,
  Database,
  Activity,
  BellRing,
  Server,
  Hash,
  GitBranch,
  Link2,
  Calendar,
} from 'lucide-react'
import { type SidebarData } from '../types'

export const sidebarData: SidebarData = {
  user: {
    name: 'ETL Developer',
    email: 'rosetta@rosetta.com',
    avatar: '',
  },
  teams: [
    {
      name: 'Rosetta',
      logo: Command,
      plan: 'CDC Platform',
    },
  ],
  navGroups: [
    {
      title: 'General',
      items: [
        {
          title: 'Dashboard',
          url: '/',
          icon: LayoutDashboard,
        },
      ],
    },
    {
      title: 'Connections',
      items: [
        {
          title: 'Sources',
          icon: Database,
          url: '/sources',
        },
        {
          title: 'Destinations',
          icon: Snowflake,
          url: '/destinations',
        },
      ],
    },
    {
      title: 'Integrations',
      items: [
        {
          title: 'Pipelines',
          icon: Workflow,
          url: '/pipelines',
        },
      ],
    },
    {
      title: 'Transformation',
      items: [
        {
          title: 'Flow Task',
          icon: GitBranch,
          url: '/flow-tasks',
        },
        {
          title: 'Linked Task',
          icon: Link2,
          url: '/linked-tasks',
        },
        {
          title: 'Schedule',
          icon: Calendar,
          url: '/schedules',
        },
      ],
    },
    {
      title: 'Other',
      items: [
        {
          title: 'Smart Tags',
          icon: Hash,
          url: '/smart-tags',
        },
        {
          title: 'Settings',
          icon: Settings,
          items: [
            {
              title: 'WAL Monitor',
              url: '/settings/wal-monitor',
              icon: Activity,
            },
            {
              title: 'Batch Configuration',
              url: '/settings/batch-configuration',
              icon: Server,
            },
            {
              title: 'Notification Setting',
              url: '/settings/notifications',
              icon: BellRing,
            },
          ],
        },
      ],
    },
  ],
}
