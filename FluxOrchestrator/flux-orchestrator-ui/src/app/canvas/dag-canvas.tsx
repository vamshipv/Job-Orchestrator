import React, { useMemo } from 'react';
import { 
  ReactFlow, 
  Background, 
  Controls, 
  Node, 
  Edge, 
  Position, 
  MarkerType 
} from '@xyflow/react';
import { PipelineStepDefinition, StepExecutionStatus } from '../models/pipeline.model';

interface Props {
  steps: PipelineStepDefinition[];
  statuses: StepExecutionStatus[];
  onNodeClick?: (stepId: string) => void;
}

export const ReactDagCanvas: React.FC<Props> = ({ steps, statuses, onNodeClick }) => {
  // Map step execution states: stepId -> state ('Pending' | 'Running' | 'Succeeded' | 'Failed')
  const statusMap = useMemo(() => {
    const map = new Map<string, string>();
    statuses.forEach(s => map.set(s.stepId, s.state));
    return map;
  }, [statuses]);

  // Convert PipelineStepDefinition[] into React Flow Nodes with positions and status colors
  const nodes: Node[] = useMemo(() => {
    // Simple layered positioning algorithm
    const xOffsets: Record<string, number> = {};
    const ySpacing = 90;
    const xSpacing = 240;

    // Track layers based on dependency depth
    const getDepth = (stepId: string, visited = new Set<string>()): number => {
      if (visited.has(stepId)) return 0;
      visited.add(stepId);
      const step = steps.find(s => s.stepId === stepId);
      if (!step || !step.dependsOn || step.dependsOn.length === 0) return 0;
      return 1 + Math.max(...step.dependsOn.map(dep => getDepth(dep, visited)));
    };

    const depthMap: Record<string, number> = {};
    steps.forEach(s => {
      depthMap[s.stepId] = getDepth(s.stepId);
    });

    const layerCounters: Record<number, number> = {};

    return steps.map(step => {
      const depth = depthMap[step.stepId] || 0;
      const indexInLayer = layerCounters[depth] || 0;
      layerCounters[depth] = indexInLayer + 1;

      const state = statusMap.get(step.stepId) || 'Pending';

      // Colors matching our clean Material UI theme
      let borderColor = '#e2e8f0';
      let bgColor = '#ffffff';
      let textColor = '#0f172a';
      let statusBadgeBg = '#f1f5f9';
      let statusBadgeColor = '#64748b';

      if (state === 'Running') {
        borderColor = '#3b82f6';
        bgColor = '#eff6ff';
        textColor = '#1d4ed8';
        statusBadgeBg = '#dbeafe';
        statusBadgeColor = '#1e40af';
      } else if (state === 'Succeeded') {
        borderColor = '#10b981';
        bgColor = '#f0fdf4';
        textColor = '#065f46';
        statusBadgeBg = '#dcfce7';
        statusBadgeColor = '#166534';
      } else if (state === 'Failed') {
        borderColor = '#ef4444';
        bgColor = '#fef2f2';
        textColor = '#991b1b';
        statusBadgeBg = '#fee2e2';
        statusBadgeColor = '#991b1b';
      }

      return {
        id: step.stepId,
        position: { x: depth * xSpacing + 40, y: indexInLayer * ySpacing + 40 },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
        data: { 
          label: (
            <div style={{ padding: '8px 12px', minWidth: '150px' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '4px' }}>
                <span style={{ fontSize: '10px', fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase' }}>
                  {step.estimatedSeconds}s
                </span>
                <span style={{ 
                  fontSize: '9px', 
                  fontWeight: 600, 
                  padding: '2px 6px', 
                  borderRadius: '4px', 
                  background: statusBadgeBg, 
                  color: statusBadgeColor 
                }}>
                  {state}
                </span>
              </div>
              <div style={{ fontSize: '12px', fontWeight: 600, color: textColor, fontFamily: 'monospace' }}>
                {step.stepId}
              </div>
            </div>
          )
        },
        style: {
          background: bgColor,
          border: `1.5px solid ${borderColor}`,
          borderRadius: '8px',
          boxShadow: state === 'Running' ? '0 0 0 2px rgba(59, 130, 246, 0.25)' : '0 1px 3px rgba(0,0,0,0.05)',
          transition: 'all 0.3s ease',
        }
      };
    });
  }, [steps, statusMap]);

  // Convert step dependencies into React Flow Edges (connecting arrows)
  const edges: Edge[] = useMemo(() => {
    const list: Edge[] = [];
    steps.forEach(step => {
      step.dependsOn?.forEach(parentStepId => {
        const isParentSucceeded = statusMap.get(parentStepId) === 'Succeeded';
        list.push({
          id: `${parentStepId}->${step.stepId}`,
          source: parentStepId,
          target: step.stepId,
          animated: statusMap.get(step.stepId) === 'Running',
          style: { 
            stroke: isParentSucceeded ? '#10b981' : '#cbd5e1', 
            strokeWidth: 2 
          },
          markerEnd: {
            type: MarkerType.ArrowClosed,
            color: isParentSucceeded ? '#10b981' : '#cbd5e1',
          }
        });
      });
    });
    return list;
  }, [steps, statusMap]);

  return (
    <div style={{ width: '100%', height: '360px', background: '#f8fafc', borderRadius: '8px', border: '1px solid #e2e8f0' }}>
      <ReactFlow 
        nodes={nodes} 
        edges={edges} 
        fitView
        onNodeClick={(_, node) => onNodeClick?.(node.id)}
      >
        <Background color="#cbd5e1" gap={16} size={1} />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
};