import { create } from 'zustand'
import {
    addEdge,
    applyNodeChanges,
    applyEdgeChanges,
    type Connection,
    type NodeChange,
    type EdgeChange,
} from '@xyflow/react'
import type { FlowNode, FlowEdge, FlowNodeData, NodePreviewResult } from '@/repo/flow-tasks'

interface PreviewState {
    isOpen: boolean
    isLoading: boolean
    nodeId: string | null
    nodeLabel: string | null
    nodeType: string | null
    /** Unique token per preview request — used to discard stale Celery results. */
    previewSessionId: string | null
    celeryTaskId: string | null
    result: NodePreviewResult | null
    error: string | null
}

interface FlowTaskStore {
    // Graph state
    nodes: FlowNode[]
    edges: FlowEdge[]
    selectedNodeId: string | null
    isDirty: boolean

    // Preview state
    preview: PreviewState

    // Drawer state
    activeDrawerTab: 'config' | 'table' | 'chart' | 'profiling'
    setActiveDrawerTab: (tab: 'config' | 'table' | 'chart' | 'profiling') => void

    // Actions — graph
    setNodes: (nodes: FlowNode[]) => void
    setEdges: (edges: FlowEdge[]) => void
    onNodesChange: (changes: NodeChange[]) => void
    onEdgesChange: (changes: EdgeChange[]) => void
    onConnect: (connection: Connection) => void
    addNode: (node: FlowNode) => void
    updateNodeData: (nodeId: string, data: Partial<FlowNodeData>) => void
    selectNode: (nodeId: string | null) => void
    removeNode: (nodeId: string) => void
    markClean: () => void

    // Actions — preview
    openPreview: (nodeId: string, nodeLabel: string, nodeType: string, celeryTaskId: string) => string
    setPreviewLoading: (loading: boolean) => void
    setPreviewResult: (result: NodePreviewResult) => void
    setPreviewError: (error: string) => void
    closePreview: () => void
    // Slot set by FlowCanvas so NodeConfigPanel can trigger preview
    requestPreview: ((nodeId: string, nodeLabel: string) => void) | null
    setRequestPreview: (fn: ((nodeId: string, nodeLabel: string) => void) | null) => void

    // Config Panel state
    configPanelWidth: number
    setConfigPanelWidth: (width: number) => void
}

export const useFlowTaskStore = create<FlowTaskStore>((set) => ({
    // Initial state
    nodes: [],
    edges: [],
    selectedNodeId: null,
    isDirty: false,
    configPanelWidth: 288, // Default width

    preview: {
        isOpen: false,
        isLoading: false,
        nodeId: null,
        nodeLabel: null,
        nodeType: null,
        previewSessionId: null,
        celeryTaskId: null,
        result: null,
        error: null,
    },

    activeDrawerTab: 'config',
    setActiveDrawerTab: (tab) => set({ activeDrawerTab: tab }),

    // ─── Graph actions ──────────────────────────────────────────────────────────

    setNodes: (nodes) => set({ nodes: nodes ?? [] }),
    setEdges: (edges) => set({ edges: edges ?? [] }),

    onNodesChange: (changes) =>
        set((state) => ({
            nodes: applyNodeChanges(changes, state.nodes ?? []) as FlowNode[],
            isDirty: true,
        })),

    onEdgesChange: (changes) =>
        set((state) => ({
            edges: applyEdgeChanges(changes, state.edges ?? []) as FlowEdge[],
            isDirty: true,
        })),

    onConnect: (connection) =>
        set((state) => ({
            edges: addEdge(
                {
                    ...connection,
                    animated: true,
                    style: { stroke: '#6366f1', strokeWidth: 2 },
                    markerEnd: { type: 'arrowclosed', color: '#6366f1' },
                },
                state.edges ?? []
            ) as FlowEdge[],
            isDirty: true,
        })),

    addNode: (node) =>
        set((state) => ({
            nodes: [...(state.nodes ?? []), node],
            isDirty: true,
        })),

    updateNodeData: (nodeId, data) =>
        set((state) => ({
            nodes: (state.nodes ?? []).map((n) =>
                n.id === nodeId ? { ...n, data: { ...n.data, ...data } } : n
            ),
            isDirty: true,
        })),

    selectNode: (nodeId) => set((state) => ({
        selectedNodeId: nodeId,
        activeDrawerTab: (state.selectedNodeId != null || state.preview.isOpen) ? state.activeDrawerTab : 'config'
    })),

    removeNode: (nodeId) =>
        set((state) => ({
            nodes: state.nodes.filter((n) => n.id !== nodeId),
            edges: state.edges.filter(
                (e) => e.source !== nodeId && e.target !== nodeId
            ),
            selectedNodeId:
                state.selectedNodeId === nodeId ? null : state.selectedNodeId,
            isDirty: true,
        })),

    markClean: () => set({ isDirty: false }),

    requestPreview: null,
    setRequestPreview: (fn) => set({ requestPreview: fn }),

    // ─── Config Panel actions ──────────────────────────────────────────────────

    setConfigPanelWidth: (width) => set({ configPanelWidth: width }),

    // ─── Preview actions ────────────────────────────────────────────────────────

    openPreview: (nodeId, nodeLabel, nodeType, celeryTaskId) => {
        const previewSessionId = crypto.randomUUID()
        set({
            preview: {
                isOpen: true,
                isLoading: true,
                nodeId,
                nodeLabel,
                nodeType,
                previewSessionId,
                celeryTaskId,
                result: null,
                error: null,
            },
            activeDrawerTab: 'table',
        })
        return previewSessionId
    },

    setPreviewLoading: (loading) =>
        set((state) => ({
            preview: { ...state.preview, isLoading: loading },
        })),

    setPreviewResult: (result) =>
        set((state) => ({
            preview: { ...state.preview, isLoading: false, result, error: null },
        })),

    setPreviewError: (error) =>
        set((state) => ({
            preview: { ...state.preview, isLoading: false, error, result: null },
        })),

    closePreview: () =>
        set({
            preview: {
                isOpen: false,
                isLoading: false,
                nodeId: null,
                nodeLabel: null,
                nodeType: null,
                previewSessionId: null,
                celeryTaskId: null,
                result: null,
                error: null,
            },
        }),
}))
