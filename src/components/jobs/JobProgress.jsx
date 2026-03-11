import React, { useState, useEffect, useRef, useCallback } from 'react';
import { base44 } from '@/api/base44Client';
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import { 
    Play, Pause, Download, RefreshCw, Loader2, CheckCircle, 
    XCircle, AlertCircle, Clock, ServerCog
} from 'lucide-react';
import { toast } from 'sonner';

export default function JobProgress({ jobId, onComplete }) {
    const [job, setJob] = useState(null);
    const [statusCounts, setStatusCounts] = useState(null);
    const [loading, setLoading] = useState(true);
    const [resuming, setResuming] = useState(false);
    const [generating, setGenerating] = useState(false);
    const [pausing, setPausing] = useState(false);
    const pollRef = useRef(null);
    const jobIdRef = useRef(jobId);

    useEffect(() => {
        jobIdRef.current = jobId;
    }, [jobId]);

    useEffect(() => {
        if (pollRef.current) clearInterval(pollRef.current);
        setJob(null);
        setStatusCounts(null);
        setLoading(true);
        if (jobId) {
            loadJobStatus();
        }
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
            
            if (jobData.status === 'done') {
                onComplete?.(jobData);
            }

            return { jobData, counts };
        } catch (error) {
            console.error('Failed to load job status:', error);
            return null;
        } finally {
            setLoading(false);
        }
    }, [onComplete]);

    // Poll for status updates while the server processes in the background
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

    // Resume: marks job as queued so the server automation picks it up
    const handleResume = async () => {
        setResuming(true);
        try {
            await base44.functions.invoke('jobProcessor', {
                action: 'resume',
                job_id: jobId
            });
            await loadJobStatus();
            toast.success('Task resumed — server will continue processing shortly');
        } catch (error) {
            console.error('Failed to resume:', error);
            toast.error('Failed to resume processing');
        } finally {
            setResuming(false);
        }
    };

    // Pause: non-destructive — pending rows stay pending, job can be resumed
    const handlePause = async () => {
        setPausing(true);
        try {
            await base44.functions.invoke('jobProcessor', {
                action: 'pause',
                job_id: jobId
            });
            toast.success('Task paused');
            await loadJobStatus();
        } catch (error) {
            toast.error('Failed to pause task');
        } finally {
            setPausing(false);
        }
    };

    const downloadOutput = async () => {
        setGenerating(true);
        try {
            const response = await base44.functions.invoke('generateOutput', {
                job_id: jobId
            });

            if (response.data.success) {
                const byteCharacters = atob(response.data.data);
                const byteNumbers = new Array(byteCharacters.length);
                for (let i = 0; i < byteCharacters.length; i++) {
                    byteNumbers[i] = byteCharacters.charCodeAt(i);
                }
                const byteArray = new Uint8Array(byteNumbers);
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
        } catch (error) {
            toast.error('Failed to generate output file');
        } finally {
            setGenerating(false);
        }
    };

    if (loading) {
        return (
            <div className="flex items-center justify-center h-32">
                <Loader2 className="w-6 h-6 animate-spin text-slate-400" />
            </div>
        );
    }

    if (!job) return null;

    const actualProcessed = statusCounts ? (statusCounts.done + statusCounts.error) : job.processed_rows;
    const progress = job.total_rows > 0 
        ? Math.round((actualProcessed / job.total_rows) * 100) 
        : 0;

    const statusConfigMap = {
        queued: { icon: Clock, color: 'text-amber-500', bg: 'bg-amber-100', label: 'Queued' },
        running: { icon: ServerCog, color: 'text-blue-500', bg: 'bg-blue-100', label: 'Server Processing' },
        done: { icon: CheckCircle, color: 'text-green-500', bg: 'bg-green-100', label: 'Completed' },
        error: { icon: XCircle, color: 'text-red-500', bg: 'bg-red-100', label: 'Error' },
        paused: { icon: Pause, color: 'text-slate-500', bg: 'bg-slate-100', label: 'Paused' }
    };

    const status = statusConfigMap[job.status] || statusConfigMap.queued;
    const StatusIcon = status.icon;
    const isActive = job.status === 'queued' || job.status === 'running';

    return (
        <Card>
            <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                    <CardTitle className="text-lg">Job Progress</CardTitle>
                    <Badge className={`${status.bg} ${status.color}`}>
                        <StatusIcon className={`w-3 h-3 mr-1 ${isActive ? 'animate-spin' : ''}`} />
                        {status.label}
                    </Badge>
                </div>
            </CardHeader>
            <CardContent className="space-y-4">
                {/* Progress bar */}
                <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                        <span className="text-slate-500">Progress</span>
                        <span className="font-medium">
                            {statusCounts ? (statusCounts.done + statusCounts.error) : job.processed_rows} / {job.total_rows} rows
                        </span>
                    </div>
                    <Progress value={progress} className="h-2" />
                </div>

                {/* Status counts */}
                {statusCounts && (
                    <div className="grid grid-cols-4 gap-2 text-center">
                        <div className="p-2 bg-slate-50 rounded-lg">
                            <p className="text-lg font-semibold text-slate-400">{statusCounts.pending}</p>
                            <p className="text-xs text-slate-500">Pending</p>
                        </div>
                        <div className="p-2 bg-blue-50 rounded-lg">
                            <p className="text-lg font-semibold text-blue-600">{statusCounts.processing}</p>
                            <p className="text-xs text-slate-500">Processing</p>
                        </div>
                        <div className="p-2 bg-green-50 rounded-lg">
                            <p className="text-lg font-semibold text-green-600">{statusCounts.done}</p>
                            <p className="text-xs text-slate-500">Done</p>
                        </div>
                        <div className="p-2 bg-red-50 rounded-lg">
                            <p className="text-lg font-semibold text-red-600">{statusCounts.error}</p>
                            <p className="text-xs text-slate-500">Errors</p>
                        </div>
                    </div>
                )}

                {/* Job details */}
                <div className="text-sm text-slate-500 space-y-1">
                    <p><span className="font-medium">Connection:</span> {job.connection_name}</p>
                    <p><span className="font-medium">Model:</span> {job.model_name}</p>
                    <p><span className="font-medium">Input:</span> {job.input_file_name}</p>
                </div>

                {/* Error message */}
                {job.error_message && (
                    <div className="p-3 bg-red-50 rounded-lg">
                        <div className="flex items-start gap-2">
                            <AlertCircle className="w-4 h-4 text-red-500 mt-0.5" />
                            <p className="text-sm text-red-700">{job.error_message}</p>
                        </div>
                    </div>
                )}

                {/* Token usage & cost */}
                {(job.total_input_tokens > 0 || job.estimated_cost_usd > 0) && (
                    <div className="p-3 bg-slate-50 rounded-lg text-sm space-y-1">
                        <div className="flex justify-between">
                            <span className="text-slate-500">Tokens:</span>
                            <span className="font-medium">
                                {(job.total_input_tokens || 0).toLocaleString()} in / {(job.total_output_tokens || 0).toLocaleString()} out
                            </span>
                        </div>
                        {job.estimated_cost_usd > 0 && (
                            <div className="flex justify-between">
                                <span className="text-slate-500">Estimated cost:</span>
                                <span className="font-medium text-green-700">
                                    ${job.estimated_cost_usd < 0.01 ? '<0.01' : job.estimated_cost_usd.toFixed(4)}
                                </span>
                            </div>
                        )}
                    </div>
                )}

                {/* Server-processing notice */}
                {isActive && (
                    <div className="flex items-center gap-2 p-3 bg-blue-50 rounded-lg text-sm text-blue-700">
                        <ServerCog className="w-4 h-4 animate-pulse shrink-0" />
                        <span>Processing on the server — you can close this tab safely.</span>
                    </div>
                )}

                {/* Actions */}
                <div className="flex gap-2 pt-2">
                    {isActive && (
                        <Button
                            variant="outline"
                            onClick={handlePause}
                            disabled={pausing}
                            className="gap-2"
                        >
                            {pausing ? (
                                <Loader2 className="w-4 h-4 animate-spin" />
                            ) : (
                                <Pause className="w-4 h-4" />
                            )}
                            Pause
                        </Button>
                    )}

                    {(job.status === 'error' || job.status === 'paused') && statusCounts && statusCounts.pending > 0 && (
                        <Button
                            onClick={handleResume}
                            disabled={resuming}
                            className="flex-1 gap-2 bg-slate-900 hover:bg-slate-800"
                        >
                            {resuming ? (
                                <Loader2 className="w-4 h-4 animate-spin" />
                            ) : (
                                <Play className="w-4 h-4" />
                            )}
                            Resume Processing
                        </Button>
                    )}

                    {(job.status === 'done' || job.processed_rows > 0) && (
                        <Button
                            onClick={downloadOutput}
                            disabled={generating}
                            variant={job.status === 'done' ? 'default' : 'outline'}
                            className="flex-1 gap-2"
                        >
                            {generating ? (
                                <Loader2 className="w-4 h-4 animate-spin" />
                            ) : (
                                <Download className="w-4 h-4" />
                            )}
                            Download Output
                        </Button>
                    )}

                    <Button
                        variant="ghost"
                        size="icon"
                        onClick={loadJobStatus}
                    >
                        <RefreshCw className="w-4 h-4" />
                    </Button>
                </div>
            </CardContent>
        </Card>
    );
}