import React, { useState, useEffect, useRef, useCallback } from 'react';
import { base44 } from '@/api/base44Client';
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { 
    Play, Pause, Download, RefreshCw, Loader2, CheckCircle2, 
    XCircle, AlertCircle, Clock, Server, FileText, Cpu, DollarSign, Square
} from 'lucide-react';
import { toast } from 'sonner';

const STATUS_CONFIG = {
    queued:  { label: 'Queued',     color: 'text-amber-600',  bg: 'bg-amber-50 border-amber-200',  dot: 'bg-amber-400' },
    running: { label: 'Processing', color: 'text-blue-600',   bg: 'bg-blue-50 border-blue-200',    dot: 'bg-blue-500 animate-pulse' },
    done:    { label: 'Completed',  color: 'text-green-700',  bg: 'bg-green-50 border-green-200',  dot: 'bg-green-500' },
    error:   { label: 'Error',      color: 'text-red-600',    bg: 'bg-red-50 border-red-200',      dot: 'bg-red-500' },
    paused:  { label: 'Paused',     color: 'text-slate-600',  bg: 'bg-slate-50 border-slate-200',  dot: 'bg-slate-400' },
};

export default function JobProgress({ jobId, onComplete }) {
    const [job, setJob] = useState(null);
    const [statusCounts, setStatusCounts] = useState(null);
    const [loading, setLoading] = useState(true);
    const [resuming, setResuming] = useState(false);
    const [generating, setGenerating] = useState(false);
    const [pausing, setPausing] = useState(false);
    const [stopping, setStopping] = useState(false);
    const pollRef = useRef(null);
    const jobIdRef = useRef(jobId);

    useEffect(() => { jobIdRef.current = jobId; }, [jobId]);

    useEffect(() => {
        if (pollRef.current) clearInterval(pollRef.current);
        setJob(null);
        setStatusCounts(null);
        setLoading(true);
        if (jobId) loadJobStatus();
        return () => { if (pollRef.current) clearInterval(pollRef.current); };
    }, [jobId]);

    const loadJobStatus = useCallback(async () => {
        try {
            const response = await base44.functions.invoke('jobProcessor', {
                action: 'getStatus',
                job_id: jobIdRef.current
            });
            const jobData = response.data.job;
            const counts = response.data.statusCounts;
            setJob(jobData);
            setStatusCounts(counts);
            if (jobData.status === 'done') onComplete?.(jobData);
            return { jobData, counts };
        } catch (error) {
            console.error('Failed to load job status:', error);
            return null;
        } finally {
            setLoading(false);
        }
    }, [onComplete]);

    useEffect(() => {
        if (pollRef.current) clearInterval(pollRef.current);
        const isActive = job?.status === 'queued' || job?.status === 'running';
        if (isActive) pollRef.current = setInterval(loadJobStatus, 5000);
        return () => { if (pollRef.current) clearInterval(pollRef.current); };
    }, [job?.status, loadJobStatus]);

    const handleResume = async () => {
        setResuming(true);
        try {
            await base44.functions.invoke('jobProcessor', { action: 'resume', job_id: jobId });
            await loadJobStatus();
            toast.success('Task resumed — server will continue processing shortly');
        } catch {
            toast.error('Failed to resume processing');
        } finally {
            setResuming(false);
        }
    };

    const handleStop = async () => {
        if (!confirm('Abort this task? All pending and in-progress rows will be cancelled and the job will be marked as aborted.')) return;
        setStopping(true);
        try {
            await base44.functions.invoke('jobProcessor', { action: 'stop', job_id: jobId });
            toast.success('Task aborted');
            await loadJobStatus();
        } catch {
            toast.error('Failed to stop task');
        } finally {
            setStopping(false);
        }
    };

    const handlePause = async () => {
        setPausing(true);
        try {
            await base44.functions.invoke('jobProcessor', { action: 'pause', job_id: jobId });
            toast.success('Task paused');
            await loadJobStatus();
        } catch {
            toast.error('Failed to pause task');
        } finally {
            setPausing(false);
        }
    };

    const downloadOutput = async () => {
        setGenerating(true);
        try {
            const response = await base44.functions.invoke('generateOutput', { job_id: jobId });
            if (response.data.success) {
                const byteCharacters = atob(response.data.data);
                const byteArray = new Uint8Array(byteCharacters.length);
                for (let i = 0; i < byteCharacters.length; i++) {
                    byteArray[i] = byteCharacters.charCodeAt(i);
                }
                const blob = new Blob([byteArray], { type: response.data.mimeType });
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = response.data.filename;
                document.body.appendChild(a);
                a.click();
                window.URL.revokeObjectURL(url);
                a.remove();
                toast.success('Download started');
            }
        } catch {
            toast.error('Failed to generate output file');
        } finally {
            setGenerating(false);
        }
    };

    if (loading) {
        return (
            <div className="flex items-center justify-center h-32">
                <Loader2 className="w-5 h-5 animate-spin text-slate-400" />
            </div>
        );
    }

    if (!job) return null;

    const cfg = STATUS_CONFIG[job.status] || STATUS_CONFIG.queued;
    const isActive = job.status === 'queued' || job.status === 'running';
    const actualProcessed = statusCounts ? (statusCounts.done + statusCounts.error) : job.processed_rows;
    const progress = job.total_rows > 0 ? Math.round((actualProcessed / job.total_rows) * 100) : 0;
    const canResume = (job.status === 'error' || job.status === 'paused') && statusCounts?.pending > 0;
    const canDownload = job.status === 'done' || actualProcessed > 0;

    return (
        <div className="space-y-4">
            {/* Status header */}
            <div className={`flex items-center justify-between p-3 rounded-lg border ${cfg.bg}`}>
                <div className="flex items-center gap-2">
                    <span className={`w-2 h-2 rounded-full ${cfg.dot}`} />
                    <span className={`text-sm font-semibold ${cfg.color}`}>{cfg.label}</span>
                </div>
                <button onClick={loadJobStatus} className="text-slate-400 hover:text-slate-600 transition-colors">
                    <RefreshCw className="w-3.5 h-3.5" />
                </button>
            </div>

            {/* Progress bar */}
            <div className="space-y-1.5">
                <div className="flex justify-between items-baseline">
                    <span className="text-sm text-slate-500">Rows processed</span>
                    <span className="text-sm font-semibold text-slate-800">
                        {actualProcessed} <span className="font-normal text-slate-400">/ {job.total_rows}</span>
                        <span className="ml-2 text-xs text-slate-400">{progress}%</span>
                    </span>
                </div>
                <Progress value={progress} className="h-1.5" />
            </div>

            {/* Row counts */}
            {statusCounts && (
                <div className="grid grid-cols-4 gap-2">
                    <div className="text-center p-2 rounded-lg bg-slate-50">
                        <p className="text-base font-bold text-slate-500">{statusCounts.pending}</p>
                        <p className="text-xs text-slate-400 mt-0.5">Pending</p>
                    </div>
                    <div className="text-center p-2 rounded-lg bg-blue-50">
                        <p className="text-base font-bold text-blue-600">{statusCounts.processing}</p>
                        <p className="text-xs text-slate-400 mt-0.5">Running</p>
                    </div>
                    <div className="text-center p-2 rounded-lg bg-green-50">
                        <p className="text-base font-bold text-green-600">{statusCounts.done}</p>
                        <p className="text-xs text-slate-400 mt-0.5">Done</p>
                    </div>
                    <div className="text-center p-2 rounded-lg bg-red-50">
                        <p className="text-base font-bold text-red-500">{statusCounts.error}</p>
                        <p className="text-xs text-slate-400 mt-0.5">Errors</p>
                    </div>
                </div>
            )}

            {/* Job details */}
            <div className="space-y-1.5 text-sm">
                <div className="flex items-center gap-2 text-slate-600">
                    <Server className="w-3.5 h-3.5 text-slate-400 shrink-0" />
                    <span className="truncate">{job.connection_name} — {job.model_name}</span>
                </div>
                <div className="flex items-center gap-2 text-slate-600">
                    <FileText className="w-3.5 h-3.5 text-slate-400 shrink-0" />
                    <span className="truncate">{job.input_file_name}</span>
                </div>
                {(job.total_input_tokens > 0 || job.estimated_cost_usd > 0) && (
                    <div className="flex items-center gap-2 text-slate-600">
                        <Cpu className="w-3.5 h-3.5 text-slate-400 shrink-0" />
                        <span>
                            {(job.total_input_tokens || 0).toLocaleString()} in / {(job.total_output_tokens || 0).toLocaleString()} out tokens
                        </span>
                        {job.estimated_cost_usd > 0 && (
                            <span className="ml-auto font-medium text-green-700">
                                ~${job.estimated_cost_usd < 0.01 ? '<0.01' : job.estimated_cost_usd.toFixed(4)}
                            </span>
                        )}
                    </div>
                )}
            </div>

            {/* Error message */}
            {job.error_message && job.status === 'error' && (
                <div className="flex items-start gap-2 p-3 bg-red-50 rounded-lg border border-red-100">
                    <AlertCircle className="w-4 h-4 text-red-500 mt-0.5 shrink-0" />
                    <p className="text-xs text-red-700 leading-relaxed">{job.error_message}</p>
                </div>
            )}

            {/* Server notice */}
            {isActive && (
                <div className="flex items-center gap-2 text-xs text-slate-500">
                    <Loader2 className="w-3.5 h-3.5 animate-spin shrink-0" />
                    <span>Processing on server — safe to close this tab</span>
                </div>
            )}

            {/* Actions */}
            <div className="flex gap-2 pt-1">
                {isActive && (
                    <Button variant="outline" size="sm" onClick={handlePause} disabled={pausing} className="gap-1.5">
                        {pausing ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Pause className="w-3.5 h-3.5" />}
                        Pause
                    </Button>
                )}
                {isActive && (
                    <Button variant="outline" size="sm" onClick={handleStop} disabled={stopping} className="gap-1.5 text-red-600 border-red-200 hover:bg-red-50">
                        {stopping ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Square className="w-3.5 h-3.5" />}
                        Stop
                    </Button>
                )}
                {canResume && (
                    <Button size="sm" onClick={handleResume} disabled={resuming} className="gap-1.5 bg-slate-900 hover:bg-slate-800 flex-1">
                        {resuming ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Play className="w-3.5 h-3.5" />}
                        Resume
                    </Button>
                )}
                {canDownload && (
                    <Button
                        size="sm"
                        onClick={downloadOutput}
                        disabled={generating}
                        variant={job.status === 'done' ? 'default' : 'outline'}
                        className="gap-1.5 flex-1"
                    >
                        {generating ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Download className="w-3.5 h-3.5" />}
                        {generating ? 'Generating…' : 'Download Output'}
                    </Button>
                )}
            </div>
        </div>
    );
}