import React, { useState, useEffect, useRef } from 'react';
import { 
  Folder, Plus, X, Play, Square, History, 
  DollarSign, Activity, GitBranch, Terminal, MessageSquare, 
  Users, CheckCircle2, AlertCircle, 
  ChevronRight, RefreshCw, Layers, Copy, Check
} from 'lucide-react';

// --- Types ---
interface Project {
  id: string;
  name: string;
  path: string;
  created_at: string;
  last_opened_at: string;
  runner?: {
    running: boolean;
    pid: number | null;
    goal: string;
    exit_code: number | null;
  };
}

interface DirEntry {
  name: string;
  path: string;
}

interface BrowseResponse {
  current_path: string;
  parent_path: string | null;
  directories: DirEntry[];
}

interface TaskNode {
  id: string;
  description: string;
  dependencies: string[];
  status: string;
  verify_commands?: string[];
}

interface TaskRun {
  task_id: string;
  status: string;
  description: string;
  result: string;
  cost_usd: number;
  verified: boolean;
  wave: number;
  retry_count: number;
  model: string | null;
}

interface Agent {
  task_id: string;
  wave: number;
  status: string;
  detail: string;
  ruflo_agent_id: string | null;
  started_at: string;
  updated_at: string;
}

interface SwarmAgent {
  id: string;
  name?: string;
  status: string;
  type?: string;
}

interface SwarmSnapshot {
  total_agents?: number;
  agents?: SwarmAgent[];
}

interface EventLog {
  event_id: number;
  ts: string;
  level: 'info' | 'warn' | 'error';
  source: string;
  message: string;
  wave: number;
}

interface Thought {
  thought_id: number;
  task_id: string;
  wave: number;
  kind: 'thinking' | 'tool_use' | 'text';
  content: string;
  timestamp: string;
}

interface Confidence {
  score: number;
  rationale: string;
}

interface StatePayload {
  run: {
    run_id: number;
    goal: string;
    status: string;
    working_dir: string;
    started_at: string;
    finished_at: string | null;
    total_cost_usd: number;
  } | null;
  tasks: TaskRun[];
  agents: Agent[];
  events: EventLog[];
  dag: TaskNode[];
  reasoning: any[];
  thoughts: Thought[];
  swarm?: SwarmSnapshot;
  runner?: Project['runner'];
  project?: Project;
  task_confidence?: Record<string, Confidence>;
}

// --- App Component ---
export default function App() {
  // Navigation & Project selection
  const [projects, setProjects] = useState<Project[]>([]);
  const [currentProject, setCurrentProject] = useState<Project | null>(null);
  const [activeTab, setActiveTab] = useState<'live' | 'history'>('live');
  const [inspectingRunId, setInspectingRunId] = useState<number | null>(null);
  
  // Dialog Modals
  const [openModalOpen, setOpenModalOpen] = useState(false);
  const [scaffoldModalOpen, setScaffoldModalOpen] = useState(false);
  
  // Folder Browser state
  const [browserData, setBrowserData] = useState<BrowseResponse | null>(null);
  const [browserError, setBrowserError] = useState('');
  const [openName, setOpenName] = useState('');
  
  // Scaffold state
  const [scaffoldName, setScaffoldName] = useState('');
  const [scaffoldPath, setScaffoldPath] = useState('');
  const [scaffoldError, setScaffoldError] = useState('');
  
  // Run status state
  const [goal, setGoal] = useState('');
  const [payload, setPayload] = useState<StatePayload | null>(null);
  const [eventsList, setEventsList] = useState<EventLog[]>([]);
  const [runHistory, setRunHistory] = useState<any[]>([]);
  
  // Sub-tabs
  const [consoleTab, setConsoleTab] = useState<'thoughts' | 'logs' | 'swarm'>('thoughts');
  const [selectedThoughtTask, setSelectedThoughtTask] = useState<string | null>(null);
  const [logFilter, setLogFilter] = useState<'all' | 'info' | 'warn' | 'error'>('all');
  
  // DAG Positioning State
  const [nodePositions, setNodePositions] = useState<Record<string, { x: number; y: number; w: number; h: number }>>({});
  const [dagDepths, setDagDepths] = useState<Record<string, number>>({});
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);
  
  // References for scrolling/measurements
  const dagContainerRef = useRef<HTMLDivElement>(null);
  const logEndRef = useRef<HTMLDivElement>(null);
  const thoughtsEndRef = useRef<HTMLDivElement>(null);
  
  // WS reference
  const wsRef = useRef<WebSocket | null>(null);

  // Copy to clipboard state
  const [copiedTaskId, setCopiedTaskId] = useState<string | null>(null);
  
  // Load Projects on start
  useEffect(() => {
    fetchProjects();
    const interval = setInterval(fetchProjects, 5000);
    return () => clearInterval(interval);
  }, []);
  
  // WebSocket subscription on current project change
  useEffect(() => {
    if (!currentProject || activeTab !== 'live' || inspectingRunId) {
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      return;
    }
    
    setEventsList([]);
    setPayload(null);
    setSelectedThoughtTask(null);
    
    const wsHost = window.location.host.includes('5173') ? 'localhost:8787' : window.location.host;
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${wsHost}/api/projects/${currentProject.id}/ws`;
    
    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;
    
    ws.onmessage = (event) => {
      try {
        const data: StatePayload = JSON.parse(event.data);
        setPayload(data);
        
        // Append unique events
        if (data.events && data.events.length > 0) {
          setEventsList(prev => {
            const existingIds = new Set(prev.map(e => e.event_id));
            const newEvents = data.events.filter(e => !existingIds.has(e.event_id));
            return [...prev, ...newEvents].sort((a, b) => a.event_id - b.event_id);
          });
        }
      } catch (err) {
        console.error('WS parsing error:', err);
      }
    };
    
    ws.onclose = () => {
      console.log('WS connection closed.');
    };
    
    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [currentProject, activeTab, inspectingRunId]);
  
  // Auto Scroll Logs & Thoughts
  useEffect(() => {
    if (logEndRef.current) {
      logEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [eventsList]);
  
  useEffect(() => {
    if (thoughtsEndRef.current) {
      thoughtsEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [payload?.thoughts, selectedThoughtTask]);
  
  // Measure DAG coordinates after rendering
  useEffect(() => {
    if (!payload?.dag || payload.dag.length === 0 || !dagContainerRef.current) return;
    
    const positions: Record<string, { x: number; y: number; w: number; h: number }> = {};
    const containerRect = dagContainerRef.current.getBoundingClientRect();
    
    payload.dag.forEach(node => {
      const el = document.getElementById(`node-${node.id}`);
      if (el) {
        const rect = el.getBoundingClientRect();
        positions[node.id] = {
          x: rect.left - containerRect.left + rect.width / 2,
          y: rect.top - containerRect.top + rect.height / 2,
          w: rect.width,
          h: rect.height
        };
      }
    });
    
    setNodePositions(positions);
  }, [payload?.dag, payload?.tasks, payload?.runner]);
  
  // Topological Sort / Wave calculations in frontend
  useEffect(() => {
    if (!payload?.dag) return;
    
    const depths: Record<string, number> = {};
    const visited: Record<string, boolean> = {};
    
    const getDepth = (id: string): number => {
      if (id in depths) return depths[id];
      if (visited[id]) return 1; // avoid cycles
      visited[id] = true;
      
      const node = payload.dag.find(n => n.id === id);
      if (!node || !node.dependencies || node.dependencies.length === 0) {
        depths[id] = 1;
        return 1;
      }
      
      let maxDepDepth = 0;
      for (const depId of node.dependencies) {
        maxDepDepth = Math.max(maxDepDepth, getDepth(depId));
      }
      depths[id] = maxDepDepth + 1;
      return depths[id];
    };
    
    payload.dag.forEach(n => getDepth(n.id));
    setDagDepths(depths);
  }, [payload?.dag]);
  
  // Load History
  useEffect(() => {
    if (currentProject && activeTab === 'history') {
      fetchHistory();
    }
  }, [currentProject, activeTab]);
  
  const fetchProjects = async () => {
    try {
      const res = await fetch('/api/hub/projects');
      const data = await res.json();
      setProjects(data.projects || []);
      
      // Update selected project instance if it was already selected
      if (currentProject) {
        const updated = data.projects.find((p: Project) => p.id === currentProject.id);
        if (updated) setCurrentProject(updated);
      }
    } catch (e) {
      console.error('Error fetching projects:', e);
    }
  };
  
  const fetchHistory = async () => {
    if (!currentProject) return;
    try {
      const res = await fetch(`/api/projects/${currentProject.id}/runs`);
      const data = await res.json();
      setRunHistory(data.runs || []);
    } catch (e) {
      console.error('Error fetching run history:', e);
    }
  };
  
  const inspectRun = async (runId: number) => {
    if (!currentProject) return;
    setInspectingRunId(runId);
    setPayload(null);
    setEventsList([]);
    try {
      const res = await fetch(`/api/projects/${currentProject.id}/runs/${runId}`);
      const data = await res.json();
      setPayload(data);
      if (data.events) setEventsList(data.events);
      setActiveTab('live');
    } catch (e) {
      console.error('Error fetching run details:', e);
    }
  };
  
  const closeInspect = () => {
    setInspectingRunId(null);
    setPayload(null);
    setEventsList([]);
  };
  
  const browseFolder = async (path?: string) => {
    setBrowserError('');
    try {
      const url = path ? `/api/hub/browse?path=${encodeURIComponent(path)}` : '/api/hub/browse';
      const res = await fetch(url);
      const data = await res.json();
      setBrowserData(data);
    } catch (e) {
      setBrowserError('Failed to fetch directories.');
    }
  };
  
  const selectProject = (project: Project) => {
    setCurrentProject(project);
    setInspectingRunId(null);
    setActiveTab('live');
  };
  
  const handleOpenConfirm = async () => {
    if (!browserData?.current_path) return;
    try {
      const res = await fetch('/api/hub/projects', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: browserData.current_path, name: openName || null })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || data.error || 'Failed to open project.');
      setOpenModalOpen(false);
      setOpenName('');
      setBrowserData(null);
      await fetchProjects();
      selectProject(data);
    } catch (e: any) {
      setBrowserError(e.message);
    }
  };
  
  const handleScaffoldConfirm = async () => {
    if (!scaffoldName || !scaffoldPath) {
      setScaffoldError('Name and path are both required.');
      return;
    }
    try {
      const res = await fetch('/api/hub/scaffold', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: scaffoldName, path: scaffoldPath })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || data.error || 'Failed to scaffold project.');
      setScaffoldModalOpen(false);
      setScaffoldName('');
      setScaffoldPath('');
      await fetchProjects();
      selectProject(data);
    } catch (e: any) {
      setScaffoldError(e.message);
    }
  };
  
  const handleForgetProject = async (id: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (!confirm('Forget this project workspace?')) return;
    try {
      await fetch(`/api/hub/projects/${id}`, { method: 'DELETE' });
      if (currentProject?.id === id) {
        setCurrentProject(null);
      }
      fetchProjects();
    } catch (e) {
      console.error(e);
    }
  };
  
  const startRun = async () => {
    if (!currentProject || !goal.trim()) return;
    try {
      const res = await fetch(`/api/projects/${currentProject.id}/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ goal: goal.trim() })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || data.error);
      setInspectingRunId(null);
      fetchProjects();
    } catch (e: any) {
      alert(`Error starting run: ${e.message}`);
    }
  };
  
  const stopRun = async () => {
    if (!currentProject || !confirm('Stop the active run?')) return;
    try {
      await fetch(`/api/projects/${currentProject.id}/stop`, { method: 'POST' });
      fetchProjects();
    } catch (e) {
      console.error(e);
    }
  };
  
  // --- UI Helpers ---
  const getStatusColor = (status: string) => {
    const s = status.toLowerCase();
    if (['done', 'complete'].includes(s)) return 'text-emerald-400 border-emerald-500/30 bg-emerald-500/10';
    if (['running', 'executing', 'verifying', 'spawning'].includes(s)) return 'text-cyan-400 border-cyan-500/30 bg-cyan-500/10 shadow-[0_0_8px_rgba(34,211,238,0.15)] animate-pulse';
    if (['failed', 'errored'].includes(s)) return 'text-rose-400 border-rose-500/30 bg-rose-500/10';
    if (['retrying', 'stalled'].includes(s)) return 'text-amber-400 border-amber-500/30 bg-amber-500/10';
    if (['ready'].includes(s)) return 'text-indigo-400 border-indigo-500/30 bg-indigo-500/10';
    return 'text-slate-400 border-slate-500/20 bg-slate-500/5';
  };
  
  const getModelColor = (model: string) => {
    const m = model.toLowerCase();
    if (m.includes('haiku')) return 'text-teal-400 bg-teal-500/10 border-teal-500/20';
    if (m.includes('sonnet')) return 'text-purple-400 bg-purple-500/10 border-purple-500/20';
    if (m.includes('opus')) return 'text-amber-400 bg-amber-500/10 border-amber-500/20';
    return 'text-slate-400 bg-slate-500/10 border-slate-500/20';
  };
  
  // Map Task Statuses into actual run task objects
  const taskMap = React.useMemo(() => {
    const map: Record<string, TaskRun> = {};
    if (payload?.tasks) {
      payload.tasks.forEach(t => { map[t.task_id] = t; });
    }
    return map;
  }, [payload?.tasks]);

  const copyTaskLogsToClipboard = (taskId: string) => {
    const node = payload?.dag.find(n => n.id === taskId);
    const runInfo = taskMap[taskId];
    const thoughts = payload?.thoughts.filter(t => t.task_id === taskId) || [];
    const reasoning = payload?.reasoning.filter(r => r.task_id === taskId) || [];
    
    if (!node) return;
    
    let text = `=== Task Logs for ${taskId} ===\n`;
    text += `Description: ${node.description}\n`;
    text += `Status: ${runInfo?.status || node.status}\n`;
    if (runInfo) {
      text += `Model: ${runInfo.model || 'N/A'}\n`;
      text += `Cost: $${runInfo.cost_usd.toFixed(4)}\n`;
      text += `Retries: ${runInfo.retry_count}\n`;
      text += `Verified: ${runInfo.verified ? 'Yes' : 'No'}\n`;
    }
    if (node.verify_commands && node.verify_commands.length > 0) {
      text += `Verification Commands:\n  ${node.verify_commands.join('\n  ')}\n`;
    }
    if (reasoning.length > 0) {
      text += `\nReasoning / Failures:\n`;
      reasoning.forEach(r => {
        text += `  - [Attempt ${r.attempt_number}] ${r.failure_type}: ${r.diagnostic_message}\n`;
      });
    }
    if (thoughts.length > 0) {
      text += `\nThoughts & Actions:\n`;
      const sortedThoughts = [...thoughts].sort((a, b) => a.thought_id - b.thought_id);
      sortedThoughts.forEach(t => {
        const time = t.timestamp ? new Date(t.timestamp).toLocaleTimeString() : '';
        text += `[${time}] [${t.kind.toUpperCase()}] ${t.content}\n`;
      });
    }
    
    navigator.clipboard.writeText(text)
      .then(() => {
        setCopiedTaskId(taskId);
        setTimeout(() => setCopiedTaskId(null), 1500);
      })
      .catch(err => {
        console.error('Failed to copy: ', err);
      });
  };
  
  // Sort DAG into columns
  const columns = React.useMemo(() => {
    if (!payload?.dag || Object.keys(dagDepths).length === 0) return [];
    
    const cols: Record<number, TaskNode[]> = {};
    payload.dag.forEach(node => {
      const depth = dagDepths[node.id] || 1;
      if (!cols[depth]) cols[depth] = [];
      cols[depth].push(node);
    });
    
    return Object.entries(cols)
      .sort(([a], [b]) => parseInt(a) - parseInt(b))
      .map(([, nodes]) => nodes);
  }, [payload?.dag, dagDepths]);
  
  return (
    <div className="flex h-screen bg-[#0b0d19] text-[#e2e8f0] overflow-hidden select-none">
      
      {/* ================================= SIDEBAR ================================= */}
      <div className="w-80 border-r border-white/5 bg-slate-950/80 backdrop-blur-md flex flex-col z-20">
        
        {/* Brand */}
        <div className="h-16 px-6 border-b border-white/5 flex items-center gap-3">
          <Layers className="w-6 h-6 text-blue-400 drop-shadow-[0_0_6px_rgba(59,130,246,0.5)]" />
          <span className="font-extrabold tracking-widest text-lg bg-gradient-to-r from-blue-400 via-indigo-400 to-cyan-400 bg-clip-text text-transparent">
            MERIDIAN
          </span>
        </div>
        
        {/* Workspace selector */}
        <div className="p-4 flex-1 flex flex-col overflow-hidden">
          <div className="flex items-center justify-between text-xs font-semibold uppercase text-slate-500 tracking-wider mb-3 px-1">
            <span>Workspaces</span>
            <span className="text-[10px] bg-slate-800 text-slate-400 px-2 py-0.5 rounded-full font-normal">
              {projects.length}
            </span>
          </div>
          
          <div className="flex-1 overflow-y-auto space-y-1.5 pr-1">
            {projects.length === 0 ? (
              <div className="text-center text-xs text-slate-600 py-8 px-4 border border-dashed border-white/5 rounded-lg">
                No active projects registered. Use the controls below to select one.
              </div>
            ) : (
              projects.map(proj => {
                const isSelected = currentProject?.id === proj.id;
                const isRunning = proj.runner?.running;
                
                return (
                  <div
                    key={proj.id}
                    onClick={() => selectProject(proj)}
                    className={`group relative flex flex-col p-3 rounded-lg border text-left cursor-pointer transition-all duration-200 hover:-translate-y-[1px] ${
                      isSelected 
                        ? 'bg-slate-900/60 border-blue-500/40 shadow-md shadow-blue-900/5' 
                        : 'bg-slate-900/20 border-white/5 hover:bg-slate-900/40 hover:border-white/10'
                    }`}
                  >
                    <div className="flex items-center justify-between mb-1">
                      <div className="font-semibold text-sm truncate flex items-center gap-2">
                        {isRunning && (
                          <span className="relative flex h-2 w-2">
                            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-cyan-400 opacity-75"></span>
                            <span className="relative inline-flex rounded-full h-2 w-2 bg-cyan-500"></span>
                          </span>
                        )}
                        <span className={isSelected ? 'text-blue-300' : 'text-slate-300'}>
                          {proj.name}
                        </span>
                      </div>
                      <button 
                        onClick={(e) => handleForgetProject(proj.id, e)}
                        className="opacity-0 group-hover:opacity-100 text-slate-600 hover:text-rose-400 text-xs p-1 transition-all rounded"
                      >
                        <X className="w-3.5 h-3.5" />
                      </button>
                    </div>
                    <div className="text-[10px] text-slate-500 font-mono truncate">
                      {proj.path}
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </div>
        
        {/* Sidebar Actions */}
        <div className="p-4 border-t border-white/5 bg-slate-950/60 flex flex-col gap-2">
          <button
            onClick={() => {
              setBrowserData(null);
              setBrowserError('');
              setOpenModalOpen(true);
              browseFolder();
            }}
            className="w-full flex items-center justify-center gap-2 bg-slate-900 hover:bg-slate-800 text-slate-300 border border-white/5 rounded-lg py-2.5 text-xs font-semibold transition-all duration-150 active:scale-[0.98]"
          >
            <Folder className="w-4 h-4 text-blue-400" />
            Open local folder
          </button>
          
          <button
            onClick={() => {
              setScaffoldError('');
              setScaffoldModalOpen(true);
            }}
            className="w-full flex items-center justify-center gap-2 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 text-white rounded-lg py-2.5 text-xs font-bold shadow-lg shadow-indigo-900/10 transition-all duration-150 active:scale-[0.98]"
          >
            <Plus className="w-4 h-4" />
            New empty workspace
          </button>
        </div>
        
      </div>
      
      {/* ================================= MAIN CANVAS ================================= */}
      <div className="flex-1 flex flex-col overflow-hidden relative">
        
        {currentProject ? (
          <>
            {/* Header / Topbar */}
            <div className="h-16 border-b border-white/5 px-6 flex items-center justify-between bg-slate-950/40 backdrop-blur-md z-10 shrink-0">
              
              <div className="flex items-center gap-3">
                <span className="text-xs font-mono text-slate-500 bg-slate-900/80 px-2.5 py-1 rounded-md border border-white/5">
                  {currentProject.name}
                </span>
                <ChevronRight className="w-3.5 h-3.5 text-slate-600" />
                
                {/* Active/Inspect Run Title */}
                <div className="flex items-center gap-2">
                  {inspectingRunId ? (
                    <div className="flex items-center gap-2 text-amber-400 bg-amber-500/10 border border-amber-500/20 px-3 py-1 rounded-md text-xs font-bold">
                      <History className="w-3.5 h-3.5 animate-spin-reverse" />
                      Inspecting Historical Run #{inspectingRunId}
                    </div>
                  ) : (
                    <div className={`px-3 py-1 rounded-md text-xs font-bold border flex items-center gap-1.5 ${
                      currentProject.runner?.running 
                        ? 'text-cyan-400 bg-cyan-500/10 border-cyan-500/20' 
                        : payload?.run?.status === 'complete'
                        ? 'text-emerald-400 bg-emerald-500/10 border-emerald-500/20'
                        : 'text-slate-400 bg-slate-500/10 border-slate-500/20'
                    }`}>
                      <Activity className={`w-3.5 h-3.5 ${currentProject.runner?.running ? 'animate-pulse' : ''}`} />
                      {currentProject.runner?.running 
                        ? `Running: Wave ${payload?.tasks.reduce((max, t) => Math.max(max, t.wave || 0), 1) || 1}` 
                        : (payload?.run?.status || 'Idle')}
                    </div>
                  )}
                </div>
              </div>
              
              {/* Tab selector */}
              <div className="flex items-center gap-2 bg-slate-900/80 border border-white/5 p-1 rounded-lg">
                <button
                  onClick={() => { setActiveTab('live'); }}
                  className={`px-4 py-1.5 rounded-md text-xs font-semibold transition-all ${
                    activeTab === 'live'
                      ? 'bg-blue-600 text-white shadow-sm'
                      : 'text-slate-400 hover:text-slate-200'
                  }`}
                >
                  Live Run
                </button>
                <button
                  onClick={() => { setActiveTab('history'); }}
                  className={`px-4 py-1.5 rounded-md text-xs font-semibold transition-all ${
                    activeTab === 'history'
                      ? 'bg-blue-600 text-white shadow-sm'
                      : 'text-slate-400 hover:text-slate-200'
                  }`}
                >
                  History
                </button>
              </div>
            </div>
            
            {/* View container */}
            <div className="flex-1 overflow-hidden flex flex-col">
              {activeTab === 'live' ? (
                <div className="flex-1 overflow-hidden flex flex-col p-6 space-y-6">
                  
                  {/* Goal Prompt Input */}
                  <div className="bg-slate-900/30 border border-white/5 backdrop-blur-md rounded-xl p-4 shrink-0 flex items-center gap-3">
                    <div className="flex-1 relative">
                      <input
                        value={goal}
                        disabled={inspectingRunId !== null || currentProject.runner?.running}
                        onChange={(e) => setGoal(e.target.value)}
                        placeholder={
                          inspectingRunId 
                            ? `Inspecting Run #${inspectingRunId} goal: "${payload?.run?.goal}"` 
                            : 'Enter orchestration goal, e.g. "build a calculator module with tests and a README"'
                        }
                        className="w-full bg-slate-950/80 border border-white/5 rounded-lg py-2.5 px-4 text-sm text-[#f8fafc] placeholder-slate-500 focus:outline-none focus:border-blue-500/50 transition-all font-medium disabled:opacity-50"
                      />
                    </div>
                    
                    {inspectingRunId ? (
                      <button
                        onClick={closeInspect}
                        className="flex items-center gap-1.5 border border-slate-600 text-slate-300 hover:border-slate-500 px-4 py-2.5 rounded-lg text-xs font-bold transition-all active:scale-[0.98]"
                      >
                        Exit Inspect Mode
                      </button>
                    ) : currentProject.runner?.running ? (
                      <button
                        onClick={stopRun}
                        className="flex items-center gap-1.5 bg-rose-600 hover:bg-rose-500 text-white px-5 py-2.5 rounded-lg text-xs font-bold shadow-lg shadow-rose-950/20 transition-all active:scale-[0.98]"
                      >
                        <Square className="w-4 h-4 fill-white" />
                        Stop Run
                      </button>
                    ) : (
                      <button
                        onClick={startRun}
                        disabled={!goal.trim()}
                        className="flex items-center gap-1.5 bg-blue-600 hover:bg-blue-500 disabled:opacity-40 text-white px-5 py-2.5 rounded-lg text-xs font-bold shadow-lg shadow-blue-950/20 transition-all active:scale-[0.98]"
                      >
                        <Play className="w-4 h-4 fill-white" />
                        Start Run
                      </button>
                    )}
                  </div>
                  
                  {/* Tiles Stats */}
                  <div className="grid grid-cols-4 gap-4 shrink-0">
                    <div className="bg-slate-900/40 border border-white/5 backdrop-blur-md rounded-xl p-4 flex flex-col justify-between h-20">
                      <span className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider flex items-center gap-1">
                        <GitBranch className="w-3.5 h-3.5 text-blue-400" /> Active Wave
                      </span>
                      <span className="text-2xl font-black text-blue-300 font-mono leading-none">
                        {payload?.tasks ? (Math.max(...payload.tasks.map(t => t.wave || 0), 0) || 1) : '–'}
                      </span>
                    </div>
                    
                    <div className="bg-slate-900/40 border border-white/5 backdrop-blur-md rounded-xl p-4 flex flex-col justify-between h-20">
                      <span className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider flex items-center gap-1">
                        <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" /> Tasks Progress
                      </span>
                      <span className="text-2xl font-black text-emerald-300 font-mono leading-none flex items-baseline gap-1">
                        {payload?.tasks ? (
                          <>
                            {payload.tasks.filter(t => t.status === 'done').length}
                            <span className="text-xs font-bold text-slate-600">/ {payload.dag.length}</span>
                          </>
                        ) : '–'}
                      </span>
                    </div>
                    
                    <div className="bg-slate-900/40 border border-white/5 backdrop-blur-md rounded-xl p-4 flex flex-col justify-between h-20">
                      <span className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider flex items-center gap-1">
                        <Users className="w-3.5 h-3.5 text-cyan-400" /> Swarm Swelled
                      </span>
                      <span className="text-2xl font-black text-cyan-300 font-mono leading-none">
                        {payload?.swarm?.total_agents || 0}
                      </span>
                    </div>
                    
                    <div className="bg-slate-900/40 border border-white/5 backdrop-blur-md rounded-xl p-4 flex flex-col justify-between h-20">
                      <span className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider flex items-center gap-1">
                        <DollarSign className="w-3.5 h-3.5 text-amber-400" /> Swarm cost
                      </span>
                      <span className="text-2xl font-black text-amber-300 font-mono leading-none">
                        ${payload?.tasks ? payload.tasks.reduce((sum, t) => sum + (t.cost_usd || 0), 0).toFixed(4) : '0.0000'}
                      </span>
                    </div>
                  </div>
                  
                  {/* Visual Workspace Split */}
                  <div className="flex-1 overflow-hidden grid grid-cols-12 gap-6 min-h-0">
                    
                    {/* SVG DAG Visualizer (8 cols) */}
                    <div className="col-span-8 bg-slate-900/20 border border-white/5 rounded-xl flex flex-col overflow-hidden relative">
                      <div className="px-4 py-3 border-b border-white/5 flex items-center justify-between shrink-0 bg-slate-950/20">
                        <span className="text-xs font-bold text-slate-400 uppercase tracking-wider">Task Graph visualizer</span>
                        <div className="text-[10px] text-slate-500 flex items-center gap-2">
                          <span className="inline-block w-2.5 h-2.5 rounded bg-emerald-500/20 border border-emerald-500/40"></span> Done
                          <span className="inline-block w-2.5 h-2.5 rounded bg-cyan-500/20 border border-cyan-500/40"></span> Active
                          <span className="inline-block w-2.5 h-2.5 rounded bg-indigo-500/20 border border-indigo-500/40"></span> Ready
                        </div>
                      </div>
                      
                      {/* Graph Panel */}
                      <div 
                        ref={dagContainerRef}
                        className="flex-1 overflow-auto p-8 relative min-h-0"
                      >
                        {columns.length === 0 ? (
                          <div className="flex items-center justify-center h-full w-full text-slate-500 text-xs">
                            No visual task graph. Enter a goal above to scaffold tasks.
                          </div>
                        ) : (
                          <div className="relative flex gap-16 items-center justify-start min-w-max min-h-max py-4 px-4 mx-auto">
                            {/* SVG Connection Layer */}
                            <svg className="absolute inset-0 w-full h-full pointer-events-none z-0">
                              {payload?.dag && payload.dag.map(node => {
                                return node.dependencies.map(depId => {
                                  const from = nodePositions[depId];
                                  const to = nodePositions[node.id];
                                  if (!from || !to) return null;
                                  
                                  const isHovered = hoveredNode === node.id || hoveredNode === depId;
                                  const isDepDone = taskMap[depId]?.status === 'done';
                                  const isActive = taskMap[node.id]?.status === 'running' || taskMap[depId]?.status === 'running';
                                  
                                  // Curved bezier line path
                                  const pathData = `M ${from.x + from.w/2} ${from.y} C ${from.x + from.w/2 + 60} ${from.y}, ${to.x - to.w/2 - 60} ${to.y}, ${to.x - to.w/2} ${to.y}`;
                                  
                                  return (
                                    <g key={`${depId}-${node.id}`}>
                                      {/* Shadow glow line on hover */}
                                      {isHovered && (
                                        <path 
                                          d={pathData} 
                                          fill="none" 
                                          stroke={isDepDone ? '#10b981' : '#3b82f6'} 
                                          strokeWidth="5" 
                                          strokeLinecap="round"
                                          className="opacity-20 blur-sm"
                                        />
                                      )}
                                      {/* Solid core line */}
                                      <path 
                                        d={pathData} 
                                        fill="none" 
                                        stroke={
                                          isDepDone ? '#10b981' : 
                                          isActive ? '#06b6d4' : 
                                          'rgba(148, 163, 184, 0.15)'
                                        } 
                                        strokeWidth={isHovered ? '2.5' : '1.5'} 
                                        strokeLinecap="round"
                                        className="transition-all duration-150"
                                      />
                                    </g>
                                  );
                                });
                              })}
                            </svg>
                            
                            {/* Horizontal Waves */}
                            {columns.map((nodes, colIndex) => (
                              <div key={colIndex} className="flex flex-col gap-6 items-center z-10 mx-6">
                                <div className="text-[10px] font-semibold text-slate-600 bg-slate-950/60 border border-white/5 px-2.5 py-0.5 rounded-full uppercase tracking-wider mb-2">
                                  Wave {colIndex + 1}
                                </div>
                                
                                <div className="flex flex-col gap-4">
                                  {nodes.map(node => {
                                    const tInfo = taskMap[node.id] || node;
                                    const hasReasoning = (tInfo.status === 'failed' || tInfo.status === 'ready') && 
                                      payload?.reasoning.find(r => r.task_id === node.id);
                                    const cRating = payload?.task_confidence?.[node.id];
                                    
                                    return (
                                      <div
                                        key={node.id}
                                        id={`node-${node.id}`}
                                        onMouseEnter={() => setHoveredNode(node.id)}
                                        onMouseLeave={() => setHoveredNode(null)}
                                        onClick={() => {
                                          // Auto select task thoughts on click
                                          if (payload?.thoughts.some(th => th.task_id === node.id)) {
                                            setSelectedThoughtTask(node.id);
                                            setConsoleTab('thoughts');
                                          }
                                        }}
                                        className={`w-72 bg-slate-950/90 border rounded-xl p-4 transition-all duration-200 cursor-pointer shadow-lg hover:shadow-xl hover:scale-[1.02] flex flex-col gap-2 ${
                                          hoveredNode === node.id 
                                            ? 'border-blue-500 shadow-blue-900/10' 
                                            : 'border-white/5'
                                        }`}
                                      >
                                        {/* Node Header */}
                                        <div className="flex items-center justify-between">
                                          <div className="flex items-center gap-1.5">
                                            <span className="text-xs font-bold text-slate-400 font-mono uppercase">
                                              {node.id}
                                            </span>
                                            <button
                                              onClick={(e) => {
                                                e.stopPropagation();
                                                copyTaskLogsToClipboard(node.id);
                                              }}
                                              className="text-slate-600 hover:text-blue-400 p-0.5 rounded transition-colors"
                                              title="Copy task logs"
                                            >
                                              {copiedTaskId === node.id ? (
                                                <Check className="w-3.5 h-3.5 text-emerald-400" />
                                              ) : (
                                                <Copy className="w-3.5 h-3.5" />
                                              )}
                                            </button>
                                          </div>
                                          <div className="flex gap-1.5 items-center">
                                            {tInfo.model && (
                                              <span className={`text-[9px] font-semibold border px-1.5 py-0.2 rounded-md ${getModelColor(tInfo.model)}`}>
                                                {tInfo.model.replace(/^claude-3-/, '')}
                                              </span>
                                            )}
                                            <span className={`text-[9px] font-bold border px-2 py-0.5 rounded-full uppercase ${getStatusColor(tInfo.status)}`}>
                                              {tInfo.status}
                                            </span>
                                          </div>
                                        </div>
                                        
                                        {/* Node Description */}
                                        <div className="text-xs font-medium text-slate-300 leading-normal line-clamp-2">
                                          {node.description}
                                        </div>
                                        
                                        {/* Node Footer */}
                                        <div className="flex items-center justify-between pt-1 border-t border-white/5 mt-1 text-[10px] text-slate-500">
                                          <div className="flex gap-2">
                                            {tInfo.retry_count ? (
                                              <span className="text-amber-500 font-bold bg-amber-500/10 px-1 rounded">
                                                {tInfo.retry_count} retries
                                              </span>
                                            ) : null}
                                            {tInfo.cost_usd ? (
                                              <span className="font-mono text-slate-400">
                                                ${tInfo.cost_usd.toFixed(3)}
                                              </span>
                                            ) : null}
                                          </div>
                                          
                                          {cRating && cRating.score > 0 && (
                                            <span 
                                              title={cRating.rationale}
                                              className={`font-semibold rounded-md px-1.5 py-0.2 ${
                                                cRating.score >= 0.8 
                                                  ? 'text-emerald-400 bg-emerald-500/10 border border-emerald-500/20' 
                                                  : cRating.score >= 0.5 
                                                  ? 'text-amber-400 bg-amber-500/10 border border-amber-500/20' 
                                                  : 'text-rose-400 bg-rose-500/10 border border-rose-500/20'
                                              }`}
                                            >
                                              Conf: {(cRating.score * 100).toFixed(0)}%
                                            </span>
                                          )}
                                        </div>
                                        
                                        {/* Reasoning Snippet if Failed */}
                                        {hasReasoning && (
                                          <div className="text-[10px] text-rose-300 border-l border-rose-500 bg-rose-500/10 p-2 rounded mt-1 overflow-hidden truncate">
                                            {payload?.reasoning?.find(r => r.task_id === node.id)?.diagnostic_message}
                                          </div>
                                        )}
                                      </div>
                                    );
                                  })}
                                </div>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    </div>
                    
                    {/* Active Agents (4 cols) */}
                    <div className="col-span-4 bg-slate-900/20 border border-white/5 rounded-xl flex flex-col overflow-hidden min-h-0">
                      <div className="px-4 py-3 border-b border-white/5 shrink-0 bg-slate-950/20 flex items-center justify-between">
                        <span className="text-xs font-bold text-slate-400 uppercase tracking-wider">Active Agents Swarm</span>
                        <Activity className="w-3.5 h-3.5 text-cyan-400" />
                      </div>
                      
                      <div className="flex-1 overflow-y-auto p-4 space-y-3">
                        {payload?.agents && payload.agents.length > 0 ? (
                          payload.agents.slice().reverse().map((agent, i) => {
                            const isRunning = ['spawning', 'running', 'verifying', 'executed'].includes(agent.status);
                            return (
                              <div 
                                key={i}
                                className={`p-3 rounded-lg border flex flex-col gap-1.5 transition-all ${
                                  isRunning 
                                    ? 'bg-slate-900/60 border-cyan-500/20 shadow-md shadow-cyan-950/5' 
                                    : 'bg-slate-950/40 border-white/5'
                                }`}
                              >
                                <div className="flex items-center justify-between">
                                  <span className="text-xs font-bold text-blue-300 font-mono">
                                    w{agent.wave}·{agent.task_id}
                                  </span>
                                  <span className={`text-[9px] font-bold border px-1.5 py-0.5 rounded-full uppercase ${getStatusColor(agent.status)}`}>
                                    {agent.status}
                                  </span>
                                </div>
                                <div className="text-xs text-slate-400 truncate font-mono">
                                  {agent.detail || 'Ready.'}
                                </div>
                              </div>
                            );
                          })
                        ) : (
                          <div className="text-center text-xs text-slate-600 py-12">
                            No agents active. Launch a goal to populate swarm.
                          </div>
                        )}
                      </div>
                    </div>
                    
                  </div>
                  
                  {/* Console Panel (Console Tabs + Code Display) */}
                  <div className="h-72 bg-slate-900/30 border border-white/5 rounded-xl flex flex-col overflow-hidden shrink-0">
                    
                    {/* Console Header/Tabs */}
                    <div className="px-4 border-b border-white/5 flex items-center justify-between bg-slate-950/40 shrink-0">
                      <div className="flex items-center gap-1.5">
                        <button
                          onClick={() => setConsoleTab('thoughts')}
                          className={`px-4 py-3 text-xs font-bold uppercase tracking-wider border-b-2 transition-all flex items-center gap-1.5 ${
                            consoleTab === 'thoughts'
                              ? 'border-blue-500 text-blue-400'
                              : 'border-transparent text-slate-500 hover:text-slate-300'
                          }`}
                        >
                          <MessageSquare className="w-3.5 h-3.5" />
                          Thoughts Stream
                        </button>
                        
                        <button
                          onClick={() => setConsoleTab('logs')}
                          className={`px-4 py-3 text-xs font-bold uppercase tracking-wider border-b-2 transition-all flex items-center gap-1.5 ${
                            consoleTab === 'logs'
                              ? 'border-blue-500 text-blue-400'
                              : 'border-transparent text-slate-500 hover:text-slate-300'
                          }`}
                        >
                          <Terminal className="w-3.5 h-3.5" />
                          System Logs
                        </button>
                        
                        <button
                          onClick={() => setConsoleTab('swarm')}
                          className={`px-4 py-3 text-xs font-bold uppercase tracking-wider border-b-2 transition-all flex items-center gap-1.5 ${
                            consoleTab === 'swarm'
                              ? 'border-blue-500 text-blue-400'
                              : 'border-transparent text-slate-500 hover:text-slate-300'
                          }`}
                        >
                          <Users className="w-3.5 h-3.5" />
                          Swarm Ledger
                        </button>
                      </div>
                      
                      {/* Sub-controls based on tab */}
                      {consoleTab === 'logs' && (
                        <div className="flex items-center gap-1">
                          {(['all', 'info', 'warn', 'error'] as const).map(f => (
                            <button
                              key={f}
                              onClick={() => setLogFilter(f)}
                              className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase ${
                                logFilter === f
                                  ? 'bg-blue-600 text-white'
                                  : 'bg-slate-900 text-slate-500 hover:text-slate-300'
                              }`}
                            >
                              {f}
                            </button>
                          ))}
                        </div>
                      )}
                    </div>
                    
                    {/* Console Tab Content */}
                    <div className="flex-1 min-h-0 bg-slate-950/60 p-4 font-mono overflow-auto text-xs leading-relaxed text-slate-300">
                      
                      {consoleTab === 'thoughts' && (() => {
                        // Gather tasks that have thoughts
                        const taskIdsWithThoughts = Array.from(
                          new Set((payload?.thoughts || []).map(t => t.task_id))
                        );
                        
                        if (taskIdsWithThoughts.length === 0) {
                          return (
                            <div className="text-center text-slate-600 py-8">
                              No agent reasoning logged yet. Stream starts automatically when agents start thinking.
                            </div>
                          );
                        }
                        
                        const activeTask = selectedThoughtTask || taskIdsWithThoughts[0];
                        const thoughts = (payload?.thoughts || []).filter(t => t.task_id === activeTask);
                        
                        return (
                          <div className="flex flex-col h-full overflow-hidden">
                            {/* Task Tabs */}
                            <div className="flex items-center justify-between mb-3 border-b border-white/5 pb-2 shrink-0">
                              <div className="flex gap-2 flex-wrap">
                                {taskIdsWithThoughts.map(tid => (
                                  <button
                                    key={tid}
                                    onClick={() => setSelectedThoughtTask(tid)}
                                    className={`px-2.5 py-1 rounded text-[10px] font-bold uppercase transition-all ${
                                      activeTask === tid
                                        ? 'bg-blue-900/60 text-blue-300 border border-blue-500/30'
                                        : 'bg-slate-900 text-slate-500 hover:text-slate-300 border border-white/5'
                                    }`}
                                  >
                                    {tid}
                                  </button>
                                ))}
                              </div>
                              
                              <button
                                onClick={() => copyTaskLogsToClipboard(activeTask)}
                                className="flex items-center gap-1.5 px-3 py-1 rounded-md text-[10px] font-bold uppercase bg-slate-900 text-slate-400 hover:text-blue-300 border border-white/5 transition-all"
                              >
                                {copiedTaskId === activeTask ? (
                                  <>
                                    <Check className="w-3.5 h-3.5 text-emerald-400" />
                                    Copied
                                  </>
                                ) : (
                                  <>
                                    <Copy className="w-3.5 h-3.5" />
                                    Copy {activeTask} Logs
                                  </>
                                )}
                              </button>
                            </div>
                            
                            {/* Thoughts Display */}
                            <div className="flex-1 overflow-y-auto space-y-2.5 pr-2">
                              {thoughts.slice().reverse().map((th, i) => {
                                let badgeColor = 'bg-slate-800 text-slate-400';
                                if (th.kind === 'thinking') badgeColor = 'bg-purple-900/30 text-purple-400 border border-purple-500/20';
                                if (th.kind === 'tool_use') badgeColor = 'bg-blue-900/30 text-blue-400 border border-blue-500/20';
                                if (th.kind === 'text') badgeColor = 'bg-emerald-900/30 text-emerald-400 border border-emerald-500/20';
                                
                                return (
                                  <div key={i} className="flex gap-3 items-start border-b border-white/5 pb-2">
                                    <span className={`text-[9px] uppercase tracking-wider font-bold rounded-md px-1.5 py-0.5 shrink-0 select-none ${badgeColor}`}>
                                      {th.kind === 'tool_use' ? 'tool' : th.kind === 'text' ? 'says' : 'thought'}
                                    </span>
                                    <span className="text-slate-300 whitespace-pre-wrap font-sans break-words flex-1">
                                      {th.content}
                                    </span>
                                  </div>
                                );
                              })}
                              <div ref={thoughtsEndRef} />
                            </div>
                          </div>
                        );
                      })()}
                      
                      {consoleTab === 'logs' && (() => {
                        const filteredLogs = eventsList.filter(log => {
                          if (logFilter === 'all') return true;
                          return log.level === logFilter;
                        });
                        
                        return (
                          <div className="space-y-1 pr-2">
                            {filteredLogs.map((log, i) => {
                              let levelColor = 'text-slate-400';
                              if (log.level === 'warn') levelColor = 'text-amber-400';
                              if (log.level === 'error') levelColor = 'text-rose-400';
                              
                              return (
                                <div key={i} className="flex gap-4 items-start py-0.5 border-b border-white/5">
                                  <span className="text-slate-600 select-none text-[10px] shrink-0 font-mono">
                                    {log.ts.split('T')[1]?.slice(0, 8)}
                                  </span>
                                  <span className={`font-bold select-none text-[9px] uppercase shrink-0 px-1 rounded-sm bg-slate-900 border border-white/5 ${levelColor}`}>
                                    {log.level}
                                  </span>
                                  <span className={`${levelColor} whitespace-pre-wrap flex-1`}>
                                    {log.message}
                                  </span>
                                </div>
                              );
                            })}
                            <div ref={logEndRef} />
                          </div>
                        );
                      })()}
                      
                      {consoleTab === 'swarm' && (() => {
                        const agents = payload?.swarm?.agents || [];
                        if (agents.length === 0) {
                          return (
                            <div className="text-center text-slate-600 py-8">
                              No coordination agents registered in swarm vector DB.
                            </div>
                          );
                        }
                        
                        return (
                          <div className="grid grid-cols-3 gap-4">
                            {agents.map((ag, i) => (
                              <div key={i} className="bg-slate-900/60 p-3 rounded-lg border border-white/5 flex flex-col gap-1">
                                <div className="flex items-center justify-between">
                                  <span className="text-xs font-bold text-slate-300 truncate">
                                    {ag.name || ag.id}
                                  </span>
                                  <span className="text-[9px] font-bold bg-slate-800 text-slate-400 px-1.5 py-0.2 rounded border border-white/5">
                                    {ag.status}
                                  </span>
                                </div>
                                <span className="text-[10px] text-slate-500">
                                  Type: {ag.type || 'coordination'}
                                </span>
                              </div>
                            ))}
                          </div>
                        );
                      })()}
                      
                    </div>
                  </div>
                  
                </div>
              ) : (
                /* Run History Tab view */
                <div className="flex-1 overflow-y-auto p-6 max-w-4xl mx-auto w-full space-y-4">
                  <div className="flex items-center justify-between pb-2 border-b border-white/5 mb-4">
                    <span className="font-bold text-sm text-slate-400 uppercase tracking-wider">Past Runs History</span>
                    <button 
                      onClick={fetchHistory}
                      className="text-xs text-blue-400 hover:text-blue-300 flex items-center gap-1.5"
                    >
                      <RefreshCw className="w-3.5 h-3.5" /> Refresh
                    </button>
                  </div>
                  
                  {runHistory.length === 0 ? (
                    <div className="text-center py-12 text-slate-600 border border-dashed border-white/5 rounded-xl">
                      No past runs recorded for this project workspace.
                    </div>
                  ) : (
                    <div className="space-y-2">
                      {runHistory.map(run => (
                        <div
                          key={run.run_id}
                          onClick={() => inspectRun(run.run_id)}
                          className="bg-slate-900/40 hover:bg-slate-900/70 border border-white/5 hover:border-blue-500/20 rounded-xl p-4 cursor-pointer transition-all flex items-center justify-between gap-6"
                        >
                          <div className="flex items-center gap-4 min-w-0">
                            <span className="text-xs font-mono font-bold text-slate-600 bg-slate-950 px-2 py-1 rounded">
                              #{run.run_id}
                            </span>
                            <div className="min-w-0">
                              <div className="font-semibold text-sm truncate text-slate-300">
                                {run.goal}
                              </div>
                              <div className="text-[10px] text-slate-500 mt-0.5">
                                Started: {new Date(run.started_at).toLocaleString()}
                              </div>
                            </div>
                          </div>
                          
                          <div className="flex items-center gap-3 shrink-0">
                            <span className={`text-[10px] font-bold border px-2.5 py-0.5 rounded-full uppercase ${getStatusColor(run.status === 'complete' ? 'done' : run.status)}`}>
                              {run.status === 'complete' ? 'done' : run.status}
                            </span>
                            <span className="text-xs font-bold text-amber-300 font-mono bg-amber-500/10 border border-amber-500/20 rounded-lg px-2.5 py-1">
                              ${(run.total_cost_usd || 0).toFixed(3)}
                            </span>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          </>
        ) : (
          /* Splash Screen when no project workspace is loaded */
          <div className="flex-1 flex flex-col items-center justify-center p-8 bg-slate-950/20">
            <div className="max-w-md w-full text-center space-y-6">
              <div className="inline-flex p-4 rounded-2xl bg-blue-500/10 border border-blue-500/20 shadow-xl shadow-blue-900/5 animate-pulse">
                <Layers className="w-12 h-12 text-blue-400" />
              </div>
              <div className="space-y-2">
                <h2 className="text-2xl font-black tracking-tight text-white">
                  Meridian Orchestration Dashboard
                </h2>
                <p className="text-sm text-slate-400 leading-relaxed">
                  Meridian decomposes goals into task dependency graphs, executes them in parallel waves, and performs independent terminal verification.
                </p>
              </div>
              
              <div className="border border-white/5 rounded-2xl p-6 bg-slate-950/40 space-y-3">
                <span className="text-xs font-bold uppercase tracking-wider text-slate-500">
                  Select a workspace to begin
                </span>
                
                {projects.length > 0 ? (
                  <div className="grid grid-cols-1 gap-2 max-h-48 overflow-y-auto pt-2">
                    {projects.slice(0, 3).map(p => (
                      <button
                        key={p.id}
                        onClick={() => selectProject(p)}
                        className="w-full text-left p-3 rounded-lg border border-white/5 bg-slate-900/40 hover:bg-slate-900 hover:border-blue-500/20 text-xs font-semibold truncate transition-all duration-150"
                      >
                        {p.name}
                        <span className="block font-mono text-[9px] text-slate-500 mt-1">{p.path}</span>
                      </button>
                    ))}
                  </div>
                ) : (
                  <p className="text-xs text-slate-600 py-4">
                    No workspaces open yet. Click the buttons below or in the sidebar to open or scaffold a project.
                  </p>
                )}
              </div>
            </div>
          </div>
        )}
        
      </div>

      {/* ================================= DIALOG MODAL: OPEN FOLDER ================================= */}
      {openModalOpen && (
        <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="bg-slate-900 border border-white/5 w-[500px] max-w-full rounded-2xl p-6 shadow-2xl flex flex-col max-h-[85vh]">
            
            <div className="flex items-center justify-between border-b border-white/5 pb-4 mb-4">
              <h3 className="font-extrabold text-base text-white flex items-center gap-2">
                <Folder className="w-5 h-5 text-blue-400" />
                Select Project Workspace Folder
              </h3>
              <button 
                onClick={() => setOpenModalOpen(false)}
                className="text-slate-400 hover:text-white p-1 rounded transition-colors"
              >
                <X className="w-5 h-5" />
              </button>
            </div>
            
            {browserError && (
              <div className="mb-4 text-xs text-rose-400 border border-rose-500/20 bg-rose-500/10 p-3 rounded-lg flex items-center gap-2">
                <AlertCircle className="w-4 h-4 shrink-0" />
                {browserError}
              </div>
            )}
            
            {/* Visual File Browser */}
            <div className="flex-1 overflow-y-auto border border-white/5 rounded-xl bg-slate-950 p-2 min-h-[250px] space-y-0.5">
              {browserData ? (
                <>
                  {/* Parent path button */}
                  {browserData.parent_path && (
                    <button
                      onClick={() => browseFolder(browserData.parent_path!)}
                      className="w-full text-left px-3 py-2 text-xs font-semibold text-slate-400 hover:text-blue-300 hover:bg-slate-900 rounded-lg flex items-center gap-2 border-b border-white/5"
                    >
                      <span className="text-lg">..</span> Go up to parent directory
                    </button>
                  )}
                  
                  {browserData.directories.length === 0 ? (
                    <div className="text-center text-xs text-slate-600 py-12">
                      No subdirectories inside this folder.
                    </div>
                  ) : (
                    browserData.directories.map((dir, i) => (
                      <button
                        key={i}
                        onClick={() => browseFolder(dir.path)}
                        className="w-full text-left px-3 py-2 rounded-lg text-xs font-medium hover:bg-slate-900 flex items-center justify-between text-slate-300 hover:text-white group transition-colors"
                      >
                        <span className="flex items-center gap-2">
                          <Folder className="w-4 h-4 text-blue-500 group-hover:text-blue-400" />
                          {dir.name}
                        </span>
                        <ChevronRight className="w-3.5 h-3.5 text-slate-700 group-hover:text-slate-400 transition-colors" />
                      </button>
                    ))
                  )}
                </>
              ) : (
                <div className="flex items-center justify-center h-full py-12">
                  <RefreshCw className="w-6 h-6 text-blue-500 animate-spin" />
                </div>
              )}
            </div>
            
            {/* Path displays & confirms */}
            <div className="mt-4 space-y-4">
              <div>
                <span className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">
                  Current Selected Path
                </span>
                <div className="w-full bg-slate-950 border border-white/5 rounded-lg py-2 px-3 text-xs font-mono truncate text-slate-300">
                  {browserData?.current_path || 'Retrieving...'}
                </div>
              </div>
              
              <div>
                <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">
                  Workspace Display Name (Optional)
                </label>
                <input
                  value={openName}
                  onChange={(e) => setOpenName(e.target.value)}
                  placeholder="e.g. My Website backend"
                  className="w-full bg-slate-950 border border-white/5 rounded-lg py-2 px-3 text-xs text-white focus:outline-none focus:border-blue-500/50"
                />
              </div>
              
              <div className="flex justify-end gap-2 pt-2 border-t border-white/5">
                <button
                  onClick={() => setOpenModalOpen(false)}
                  className="px-4 py-2 text-xs font-bold text-slate-400 hover:text-white rounded-lg transition-all"
                >
                  Cancel
                </button>
                <button
                  onClick={handleOpenConfirm}
                  disabled={!browserData?.current_path}
                  className="bg-blue-600 hover:bg-blue-500 disabled:opacity-40 text-white px-5 py-2 rounded-lg text-xs font-bold transition-all shadow-md shadow-blue-900/10 active:scale-[0.98]"
                >
                  Select Folder
                </button>
              </div>
            </div>
            
          </div>
        </div>
      )}

      {/* ================================= DIALOG MODAL: SCAFFOLD NEW ================================= */}
      {scaffoldModalOpen && (
        <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="bg-slate-900 border border-white/5 w-[450px] max-w-full rounded-2xl p-6 shadow-2xl flex flex-col">
            
            <div className="flex items-center justify-between border-b border-white/5 pb-4 mb-4">
              <h3 className="font-extrabold text-base text-white flex items-center gap-2">
                <Plus className="w-5 h-5 text-blue-400" />
                Scaffold New Empty Workspace
              </h3>
              <button 
                onClick={() => setScaffoldModalOpen(false)}
                className="text-slate-400 hover:text-white p-1 rounded transition-colors"
              >
                <X className="w-5 h-5" />
              </button>
            </div>
            
            {scaffoldError && (
              <div className="mb-4 text-xs text-rose-400 border border-rose-500/20 bg-rose-500/10 p-3 rounded-lg flex items-center gap-2">
                <AlertCircle className="w-4 h-4 shrink-0" />
                {scaffoldError}
              </div>
            )}
            
            <div className="space-y-4">
              <div>
                <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">
                  Project Display Name
                </label>
                <input
                  value={scaffoldName}
                  onChange={(e) => {
                    setScaffoldName(e.target.value);
                    // Autofill path in a temp / sandbox location if empty
                    if (!scaffoldPath) {
                      setScaffoldPath(`/home/haris/Documents/dev_playground/batteries/python/Meridian/${e.target.value.toLowerCase().replace(/\s+/g, '_')}`);
                    }
                  }}
                  placeholder="e.g. My Fresh Project"
                  className="w-full bg-slate-950 border border-white/5 rounded-lg py-2 px-3 text-xs text-white focus:outline-none focus:border-blue-500/50"
                />
              </div>
              
              <div>
                <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">
                  Absolute Path (Will be created)
                </label>
                <input
                  value={scaffoldPath}
                  onChange={(e) => setScaffoldPath(e.target.value)}
                  placeholder="e.g. /home/haris/Documents/dev_playground/my_project"
                  className="w-full bg-slate-950 border border-white/5 rounded-lg py-2 px-3 text-xs text-white focus:outline-none focus:border-blue-500/50"
                />
              </div>
              
              <div className="flex justify-end gap-2 pt-2 border-t border-white/5">
                <button
                  onClick={() => setScaffoldModalOpen(false)}
                  className="px-4 py-2 text-xs font-bold text-slate-400 hover:text-white rounded-lg transition-all"
                >
                  Cancel
                </button>
                <button
                  onClick={handleScaffoldConfirm}
                  className="bg-blue-600 hover:bg-blue-500 text-white px-5 py-2 rounded-lg text-xs font-bold transition-all shadow-md shadow-blue-900/10 active:scale-[0.98]"
                >
                  Create & Open
                </button>
              </div>
            </div>
            
          </div>
        </div>
      )}
      
    </div>
  );
}
