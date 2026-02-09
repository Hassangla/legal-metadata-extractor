import React, { useState, useEffect } from 'react';
import { base44 } from '@/api/base44Client';
import { Link, useSearchParams } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { 
    ArrowLeft, Download, Eye, RefreshCw, Loader2, Clock, 
    CheckCircle, XCircle, Play, Search, FileSpreadsheet, RotateCcw
} from 'lucide-react';
import { format } from 'date-fns';
import { toast } from 'sonner';

import JobProgress from '@/components/jobs/JobProgress';

export default function History() {
    const [searchParams] = useSearchParams();
    const initialJobId = searchParams.get('job');
    
    const [jobs, setJobs] = useState([]);
    const [loading, setLoading] = useState(true);
    const [selectedJobId, setSelectedJobId] = useState(initialJobId);
    const [searchTerm, setSearchTerm] = useState('');
    const [generating, setGenerating] = useState(null);
    const [rerunning, setRerunning] = useState(null);

    useEffect(() => {
        loadJobs();
    }, []);

    const loadJobs = async () => {
        try {
            const response = await base44.functions.invoke('jobProcessor', { action: 'list' });
            setJobs(response.data.jobs || []);
        } catch (error) {
            toast.error('Failed to load jobs');
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
                
                toast.success('Download started');
            }
        } catch (error) {
            toast.error('Failed to download');
        } finally {
            setGenerating(null);
        }
    };

    // Fix 11: Rerun functionality
    const handleRerun = async (jobId, useLatestSpec) => {
        setRerunning(jobId);
        try {
            const response = await base44.functions.invoke('jobProcessor', {
                action: 'rerun',
                job_id: jobId,
                use_latest_spec: useLatestSpec
            });

            const newJob = response.data.job;
            setSelectedJobId(newJob.id);
            toast.success('Rerun started');
            await loadJobs();
        } catch (error) {
            toast.error('Failed to rerun job');
        } finally {
            setRerunning(null);
        }
    };

    const statusConfig = {
        queued: { icon: Clock, color: 'text-amber-500', bg: 'bg-amber-100', label: 'Queued' },
        running: { icon: Play, color: 'text-blue-500', bg: 'bg-blue-100', label: 'Running' },
        done: { icon: CheckCircle, color: 'text-green-500', bg: 'bg-green-100', label: 'Completed' },
        error: { icon: XCircle, color: 'text-red-500', bg: 'bg-red-100', label: 'Error' }
    };

    const filteredJobs = jobs.filter(job =>
        job.task_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        job.input_file_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        job.model_id?.toLowerCase().includes(searchTerm.toLowerCase())
    );

    const selectedJob = jobs.find(j => j.id === selectedJobId);

    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
            <div className="max-w-6xl mx-auto px-6 py-12">
                <Link to={createPageUrl('Dashboard')} className="inline-flex items-center text-slate-500 hover:text-slate-700 mb-8">
                    <ArrowLeft className="w-4 h-4 mr-2" />
                    Back to Dashboard
                </Link>

                <div className="mb-8 flex items-center justify-between">
                    <div>
                        <h1 className="text-3xl font-light text-slate-900 mb-2">Job History</h1>
                        <p className="text-slate-500">View and manage extraction jobs</p>
                    </div>
                    <Button variant="ghost" onClick={loadJobs}>
                        <RefreshCw className="w-4 h-4 mr-2" />
                        Refresh
                    </Button>
                </div>

                <div className="grid lg:grid-cols-2 gap-6">
                    {/* Job List */}
                    <div className="space-y-4">
                        <div className="relative">
                            <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
                            <Input
                                value={searchTerm}
                                onChange={(e) => setSearchTerm(e.target.value)}
                                placeholder="Search jobs..."
                                className="pl-10"
                            />
                        </div>

                        {loading ? (
                            <div className="flex items-center justify-center py-12">
                                <Loader2 className="w-6 h-6 animate-spin text-slate-400" />
                            </div>
                        ) : filteredJobs.length === 0 ? (
                            <Card className="border-dashed">
                                <CardContent className="flex flex-col items-center justify-center py-12">
                                    <FileSpreadsheet className="w-12 h-12 text-slate-300 mb-4" />
                                    <p className="text-slate-500">No jobs found</p>
                                    <Link to={createPageUrl('NewRun')}>
                                        <Button className="mt-4" variant="outline">
                                            Start New Run
                                        </Button>
                                    </Link>
                                </CardContent>
                            </Card>
                        ) : (
                            <div className="space-y-2">
                                {filteredJobs.map((job) => {
                                    const status = statusConfig[job.status] || statusConfig.queued;
                                    const StatusIcon = status.icon;
                                    const isSelected = selectedJobId === job.id;
                                    
                                    return (
                                        <Card
                                            key={job.id}
                                            className={`cursor-pointer transition-all ${
                                                isSelected 
                                                    ? 'ring-2 ring-slate-900 bg-slate-50' 
                                                    : 'hover:bg-slate-50'
                                            }`}
                                            onClick={() => setSelectedJobId(job.id)}
                                        >
                                            <CardContent className="p-4">
                                                <div className="flex items-center justify-between">
                                                    <div className="flex items-center gap-3">
                                                        <div className={`w-10 h-10 rounded-lg ${status.bg} flex items-center justify-center`}>
                                                            <StatusIcon className={`w-5 h-5 ${status.color}`} />
                                                        </div>
                                                        <div>
                                                            <p className="font-medium text-slate-900">
                                                                {job.task_name || job.input_file_name || 'Untitled'}
                                                            </p>
                                                            {job.task_name && (
                                                                <p className="text-xs text-slate-400">{job.input_file_name}</p>
                                                            )}
                                                            <p className="text-sm text-slate-500">
                                                                {format(new Date(job.created_date), 'MMM d, yyyy HH:mm')}
                                                            </p>
                                                        </div>
                                                    </div>
                                                    <div className="flex items-center gap-2">
                                                        <div className="text-right mr-2">
                                                            <p className="text-sm font-medium">
                                                                {job.processed_rows}/{job.total_rows}
                                                            </p>
                                                            <Badge className={`${status.bg} ${status.color}`}>
                                                                {status.label}
                                                            </Badge>
                                                            {job.status === 'done' && job.estimated_cost_usd > 0 && (
                                                                <p className="text-xs text-green-600 mt-1">
                                                                    ${job.estimated_cost_usd < 0.01 ? '<0.01' : job.estimated_cost_usd.toFixed(4)}
                                                                </p>
                                                            )}
                                                        </div>
                                                        {(job.status === 'done' || job.processed_rows > 0) && (
                                                            <Button
                                                                variant="ghost"
                                                                size="icon"
                                                                onClick={(e) => {
                                                                    e.stopPropagation();
                                                                    downloadOutput(job.id);
                                                                }}
                                                                disabled={generating === job.id}
                                                                title="Download output"
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
                                            </CardContent>
                                        </Card>
                                    );
                                })}
                            </div>
                        )}
                    </div>

                    {/* Job Details */}
                    <div className="space-y-4">
                        {selectedJobId ? (
                            <>
                                <JobProgress 
                                    jobId={selectedJobId}
                                    onComplete={() => loadJobs()}
                                />

                                {/* Fix 11: Rerun buttons */}
                                {selectedJob && selectedJob.status === 'done' && (
                                    <Card>
                                        <CardContent className="p-4">
                                            <p className="text-sm font-medium text-slate-700 mb-3">Rerun this job</p>
                                            <div className="flex gap-2">
                                                <Button
                                                    variant="outline"
                                                    size="sm"
                                                    className="flex-1 gap-2"
                                                    onClick={() => handleRerun(selectedJobId, false)}
                                                    disabled={rerunning === selectedJobId}
                                                >
                                                    {rerunning === selectedJobId ? (
                                                        <Loader2 className="w-3 h-3 animate-spin" />
                                                    ) : (
                                                        <RotateCcw className="w-3 h-3" />
                                                    )}
                                                    Same Spec
                                                </Button>
                                                <Button
                                                    variant="outline"
                                                    size="sm"
                                                    className="flex-1 gap-2"
                                                    onClick={() => handleRerun(selectedJobId, true)}
                                                    disabled={rerunning === selectedJobId}
                                                >
                                                    {rerunning === selectedJobId ? (
                                                        <Loader2 className="w-3 h-3 animate-spin" />
                                                    ) : (
                                                        <RotateCcw className="w-3 h-3" />
                                                    )}
                                                    Latest Spec
                                                </Button>
                                            </div>
                                        </CardContent>
                                    </Card>
                                )}
                            </>
                        ) : (
                            <Card className="border-dashed">
                                <CardContent className="flex flex-col items-center justify-center py-16">
                                    <Eye className="w-12 h-12 text-slate-300 mb-4" />
                                    <p className="text-slate-500">Select a job to view details</p>
                                </CardContent>
                            </Card>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}