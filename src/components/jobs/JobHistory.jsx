import React, { useState, useEffect } from 'react';
import { base44 } from '@/api/base44Client';
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Download, Eye, RefreshCw, Loader2, Clock, CheckCircle, XCircle, Play } from 'lucide-react';
import { format } from 'date-fns';
import { toast } from 'sonner';

export default function JobHistory({ onSelectJob, selectedJobId }) {
    const [jobs, setJobs] = useState([]);
    const [loading, setLoading] = useState(true);
    const [generating, setGenerating] = useState(null);

    useEffect(() => {
        loadJobs();
    }, []);

    const loadJobs = async () => {
        try {
            const response = await base44.functions.invoke('jobProcessor', { action: 'list' });
            setJobs(response.data.jobs || []);
        } catch (error) {
            toast.error('Failed to load job history');
        } finally {
            setLoading(false);
        }
    };

    const downloadOutput = async (jobId) => {
        setGenerating(jobId);
        try {
            const response = await base44.functions.invoke('generateOutput', { job_id: jobId });

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
            }
        } catch (error) {
            toast.error('Failed to download');
        } finally {
            setGenerating(null);
        }
    };

    const statusConfig = {
        queued: { icon: Clock, color: 'bg-amber-100 text-amber-800' },
        running: { icon: Play, color: 'bg-blue-100 text-blue-800' },
        done: { icon: CheckCircle, color: 'bg-green-100 text-green-800' },
        error: { icon: XCircle, color: 'bg-red-100 text-red-800' }
    };

    if (loading) {
        return (
            <div className="flex items-center justify-center h-32">
                <Loader2 className="w-6 h-6 animate-spin text-slate-400" />
            </div>
        );
    }

    return (
        <Card>
            <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                    <CardTitle className="text-lg">Run History</CardTitle>
                    <Button variant="ghost" size="sm" onClick={loadJobs}>
                        <RefreshCw className="w-4 h-4" />
                    </Button>
                </div>
            </CardHeader>
            <CardContent>
                {jobs.length === 0 ? (
                    <p className="text-center text-slate-500 py-8">No jobs yet</p>
                ) : (
                    <div className="space-y-2">
                        {jobs.slice(0, 10).map((job) => {
                            const status = statusConfig[job.status] || statusConfig.queued;
                            const StatusIcon = status.icon;
                            
                            return (
                                <div
                                    key={job.id}
                                    className={`p-3 rounded-lg border transition-all cursor-pointer ${
                                        selectedJobId === job.id 
                                            ? 'border-slate-300 bg-slate-50' 
                                            : 'border-transparent hover:bg-slate-50'
                                    }`}
                                    onClick={() => onSelectJob(job.id)}
                                >
                                    <div className="flex items-center justify-between">
                                        <div className="flex items-center gap-3">
                                            <Badge className={status.color}>
                                                <StatusIcon className="w-3 h-3 mr-1" />
                                                {job.status}
                                            </Badge>
                                            <div>
                                                <p className="text-sm font-medium text-slate-900">
                                                    {job.input_file_name || 'Untitled'}
                                                </p>
                                                <p className="text-xs text-slate-500">
                                                    {format(new Date(job.created_date), 'MMM d, yyyy HH:mm')}
                                                </p>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-2">
                                            <span className="text-sm text-slate-500">
                                                {job.processed_rows}/{job.total_rows}
                                            </span>
                                            {(job.status === 'done' || job.processed_rows > 0) && (
                                                <Button
                                                    variant="ghost"
                                                    size="icon"
                                                    onClick={(e) => {
                                                        e.stopPropagation();
                                                        downloadOutput(job.id);
                                                    }}
                                                    disabled={generating === job.id}
                                                >
                                                    {generating === job.id ? (
                                                        <Loader2 className="w-4 h-4 animate-spin" />
                                                    ) : (
                                                        <Download className="w-4 h-4" />
                                                    )}
                                                </Button>
                                            )}
                                        </div>
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                )}
            </CardContent>
        </Card>
    );
}