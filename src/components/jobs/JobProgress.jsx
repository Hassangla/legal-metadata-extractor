import React, { useState, useEffect, useRef, useCallback } from 'react';
import { base44 } from '@/api/base44Client';
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import {
    Play, Pause, Download, RefreshCw, Loader2, CheckCircle2,
    XCircle, AlertCircle, Clock, Server, FileText, Cpu, DollarSign, Square, Search
} from 'lucide-react';
import { toast } from 'sonner';

const STATUS_CONFIG = {
    queued:  { label: 'Queued — Waiting to Start', color: 'text-amber-600',  bg: 'bg-amber-50 border-amber-200',  dot: 'bg-amber-400 animate-pulse' },
    running: { label: 'Processing', color: 'text-blue-600',   bg: 'bg-blue-50 border-blue-200',    dot: 'bg-blue-500 animate-pulse' },
    done:    { label: 'Completed',  color: 'text-green-700',  bg: 'bg-green-50 border-green-200',  dot: 'bg-green-500' },
    error:   { label: 'Error',      color: 'text-red-600',    bg: 'bg-red-50 border-red-200',      dot: 'bg-red-500' },
    stopped: { label: 'Stopped',    color: 'text-orange-600', bg: 'bg-orange-50 border-orange-200', dot: 'bg-orange-500' },
    paused:  { label: 'Paused',     color: 'text-slate-600',  bg: 'bg-slate-50 border-slate-200',  dot: 'bg-slate-400' },
    stopped: { label: 'Stopped',    color: 'text-orange-600', bg: 'bg-orange-50 border-orange-200', dot: 'bg-orange-500' },
};

function formatDuration(ms) {
    if (ms < 0 || !Number.isFinite(ms)) return '';
    const s = Math.floor(ms / 1000);
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    const rs = s % 60;
    if (m < 60) return `${m}m ${rs}s`;
    const h = Math.floor(m / 60);
    const rm = m % 60;
    return `${h}h ${rm}m`;
}

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
        return () => {
            if (pollRef.current) clearInterval(pollRef.current);
        };
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

    // Poll status every 5s while the job is active (processing runs server-side)
    useEffect(() => {
        if (pollRef.current) clearInterval(pollRef.current);
        const isActive = job?.status === 'queued' || job?.status === 'running';
        if (isActive) {
            pollRef.current = setInterval(loadJobStatus, 5000);
        }
        return () => {
            if (pollRef.current) clearInterval(pollRef.current);
        };
    }, [job?.status, loadJobStatus]);

    const handleResume = async () => {
        setResuming(true);
        try {
            await base44.functions.invoke('jobProcessor', { action: 'resume', job_id: jobId });
            await loadJobStatus();
            toast.success('Task resumed — processing will continue on the server in the background');
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
            const { filename, data, totalRows, error } = response.data;
            if (error) {
                toast.error(error);
                return;
            }
            if (!data) {
                toast.error('No data returned from server');
                return;
            }
            const byteCharacters = atob(data);
            const byteArray = new Uint8Array(byteCharacters.length);
            for (let i = 0; i < byteCharacters.length; i++) {
                byteArray[i] = byteCharacters.charCodeAt(i);
            }
            const blob = new Blob([byteArray], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename || 'output.xlsx';
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            a.remove();
            toast.success(`Downloaded ${totalRows || 0} rows`);
        } catch (err) {
            console.error('Download output error:', err);
            toast.error(err?.response?.data?.error || err?.message || 'Failed to generate output file');
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
    const canResume = (job.status === 'error' || job.status === 'paused' || job.status === 'stopped') && statusCounts?.pending > 0;
    const canDownload = job.status === 'done' || job.status === 'stopped' || actualProcessed > 0;

    // Elapsed time since job creation
    const elapsedMs = job.created_date ? Date.now() - new Date(job.created_date).getTime() : 0;
    const elapsedStr = elapsedMs > 0 ? formatDuration(elapsedMs) : '';

    // ETA based on processing rate
    let etaStr = '';
    if (job.status === 'running' && actualProcessed > 0 && statusCounts?.pending > 0) {
        const msPerRow = elapsedMs / actualProcessed;
        const remainingMs = msPerRow * statusCounts.pending;
        etaStr = formatDuration(remainingMs);
    }

    return (
        <div className="space-y-4">
            {/* Status header */}
            <div className={`flex items-center justify-between p-3 rounded-lg border ${cfg.bg}`}>
                <div className="flex items-center gap-2">
                    <span className={`w-2 h-2 rounded-full ${cfg.dot}`} />
                    <span className={`text-sm font-semibold ${cfg.color}`}>{cfg.label}</span>
                    {elapsedStr && (
                        <span className="text-xs text-slate-400 ml-1">({elapsedStr})</span>
                    )}
                </div>
                <button onClick={loadJobStatus} className="text-slate-400 hover:text-slate-600 transition-colors">
                    <RefreshCw className="w-3.5 h-3.5" />
                </button>
            </div>

            {/* Queued — extra info about what's happening */}
            {job.status === 'queued' && (
                <div className="flex items-start gap-2 p-3 bg-amber-50 rounded-lg border border-amber-100">
                    <Clock className="w-4 h-4 text-amber-500 mt-0.5 shrink-0" />
                    <div className="text-xs text-amber-800 leading-relaxed">
                        <p className="font-medium mb-1">Task is queued and will start shortly</p>
                        <p>Processing runs entirely on the server. You can safely close this page, browser, or device — your task will continue running and complete on its own.</p>
                    </div>
                </div>
            )}

            {/* Progress bar */}
            <div className="space-y-1.5">
                <div className="flex justify-between items-baseline">
                    <span className="text-sm text-slate-500">Rows processed</span>
                    <div className="flex items-baseline gap-2">
                        <span className="text-sm font-semibold text-slate-800">
                            {actualProcessed} <span className="font-normal text-slate-400">/ {job.total_rows}</span>
                            <span className="ml-2 text-xs text-slate-400">{progress}%</span>
                        </span>
                        {etaStr && (
                            <span className="text-xs text-blue-500 font-medium">~{etaStr} left</span>
                        )}
                    </div>
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
                {job.web_search_choice && job.web_search_choice !== 'none' && (
                    <div className="flex items-center gap-2 text-slate-600">
                        <Search className="w-3.5 h-3.5 text-slate-400 shrink-0" />
                        <span>Web search: {job.web_search_choice.replace(/_/g, ' ')}</span>
                    </div>
                )}
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

            {/* Error/stopped message */}
            {job.error_message && (job.status === 'error' || job.status === 'stopped') && (
                <div className="flex items-start gap-2 p-3 bg-red-50 rounded-lg border border-red-100">
                    <AlertCircle className="w-4 h-4 text-red-500 mt-0.5 shrink-0" />
                    <p className="text-xs text-red-700 leading-relaxed">{job.error_message}</p>
                </div>
            )}

            {job.error_message && job.status === 'stopped' && (
                <div className="flex items-start gap-2 p-3 bg-orange-50 rounded-lg border border-orange-100">
                    <Square className="w-4 h-4 text-orange-500 mt-0.5 shrink-0" />
                    <p className="text-xs text-orange-700 leading-relaxed">{job.error_message}</p>
                </div>
            )}

            {/* Server notice */}
            {job.status === 'running' && (
                <div className="flex items-center gap-2 text-xs text-slate-500">
                    <Loader2 className="w-3.5 h-3.5 animate-spin shrink-0" />
                    <span>Processing on server — safe to close this page or device</span>
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
                        {generating ? 'Generating...' : 'Download Output'}
                    </Button>
                )}
            </div>
        </div>
    );
}