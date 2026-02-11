import React, { useState, useEffect } from 'react';
import { base44 } from '@/api/base44Client';
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { 
    FileText, PlayCircle, Settings, History, ArrowRight, 
    CheckCircle, Clock, AlertCircle, TrendingUp
} from 'lucide-react';
import { formatDCTime } from '@/components/utils/formatDCTime';

export default function Dashboard() {
    const [stats, setStats] = useState({
        totalJobs: 0,
        completedJobs: 0,
        recentJobs: []
    });
    const [hasSpec, setHasSpec] = useState(false);

    useEffect(() => {
        loadData();
    }, []);

    const loadData = async () => {
        // Load spec and jobs independently so one failure doesn't block the other
        try {
            const specResponse = await base44.functions.invoke('specManager', { action: 'getActive' });
            setHasSpec(!!specResponse.data.spec);
        } catch (error) {
            console.error('Failed to load spec:', error);
        }

        try {
            const jobsResponse = await base44.functions.invoke('jobProcessor', { action: 'list' });
            const jobs = jobsResponse.data.jobs || [];
            setStats({
                totalJobs: jobs.length,
                completedJobs: jobs.filter(j => j.status === 'done').length,
                recentJobs: jobs.slice(0, 5)
            });
        } catch (error) {
            console.error('Failed to load jobs:', error);
        }
    };

    const statusConfig = {
        queued: { icon: Clock, color: 'text-amber-500', bg: 'bg-amber-100' },
        running: { icon: TrendingUp, color: 'text-blue-500', bg: 'bg-blue-100' },
        done: { icon: CheckCircle, color: 'text-green-500', bg: 'bg-green-100' },
        error: { icon: AlertCircle, color: 'text-red-500', bg: 'bg-red-100' }
    };

    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
            <div className="max-w-6xl mx-auto px-6 py-12">
                {/* Header */}
                <div className="mb-12">
                    <h1 className="text-4xl font-light text-slate-900 mb-2">
                        Legal Metadata Extractor
                    </h1>
                    <p className="text-lg text-slate-500">
                        Extract and standardize legal instrument metadata from Excel files using AI
                    </p>
                </div>

                {/* Spec Warning */}
                {!hasSpec && (
                    <Card className="mb-8 border-amber-200 bg-amber-50">
                        <CardContent className="flex items-center gap-4 py-4">
                            <div className="w-10 h-10 rounded-full bg-amber-100 flex items-center justify-center">
                                <AlertCircle className="w-5 h-5 text-amber-600" />
                            </div>
                            <div className="flex-1">
                                <p className="font-medium text-amber-900">Specification Required</p>
                                <p className="text-sm text-amber-700">
                                    Please configure the extraction specification before running jobs.
                                </p>
                            </div>
                            <Link to={createPageUrl('SpecEditor')}>
                                <Button variant="outline" className="border-amber-300 text-amber-700 hover:bg-amber-100">
                                    Configure Spec
                                </Button>
                            </Link>
                        </CardContent>
                    </Card>
                )}

                {/* Quick Actions */}
                <div className="grid md:grid-cols-3 gap-6 mb-12">
                    <Link to={createPageUrl('NewRun')} className="group">
                        <Card className="h-full hover:shadow-lg transition-all hover:border-slate-300">
                            <CardContent className="p-6">
                                <div className="w-12 h-12 rounded-xl bg-slate-900 flex items-center justify-center mb-4 group-hover:scale-105 transition-transform">
                                    <PlayCircle className="w-6 h-6 text-white" />
                                </div>
                                <h3 className="text-xl font-semibold text-slate-900 mb-2">New Run</h3>
                                <p className="text-slate-500 text-sm mb-4">
                                    Upload an Excel file and start a new extraction job
                                </p>
                                <div className="flex items-center text-slate-400 group-hover:text-slate-600 transition-colors">
                                    <span className="text-sm">Get started</span>
                                    <ArrowRight className="w-4 h-4 ml-2 group-hover:translate-x-1 transition-transform" />
                                </div>
                            </CardContent>
                        </Card>
                    </Link>

                    <Link to={createPageUrl('SpecEditor')} className="group">
                        <Card className="h-full hover:shadow-lg transition-all hover:border-slate-300">
                            <CardContent className="p-6">
                                <div className="w-12 h-12 rounded-xl bg-slate-100 flex items-center justify-center mb-4 group-hover:scale-105 transition-transform">
                                    <FileText className="w-6 h-6 text-slate-600" />
                                </div>
                                <h3 className="text-xl font-semibold text-slate-900 mb-2">Specification</h3>
                                <p className="text-slate-500 text-sm mb-4">
                                    View and edit the extraction rules and output format
                                </p>
                                <div className="flex items-center text-slate-400 group-hover:text-slate-600 transition-colors">
                                    <span className="text-sm">Edit spec</span>
                                    <ArrowRight className="w-4 h-4 ml-2 group-hover:translate-x-1 transition-transform" />
                                </div>
                            </CardContent>
                        </Card>
                    </Link>

                    <Link to={createPageUrl('Settings')} className="group">
                        <Card className="h-full hover:shadow-lg transition-all hover:border-slate-300">
                            <CardContent className="p-6">
                                <div className="w-12 h-12 rounded-xl bg-slate-100 flex items-center justify-center mb-4 group-hover:scale-105 transition-transform">
                                    <Settings className="w-6 h-6 text-slate-600" />
                                </div>
                                <h3 className="text-xl font-semibold text-slate-900 mb-2">Settings</h3>
                                <p className="text-slate-500 text-sm mb-4">
                                    Manage API connections and economy codes
                                </p>
                                <div className="flex items-center text-slate-400 group-hover:text-slate-600 transition-colors">
                                    <span className="text-sm">Configure</span>
                                    <ArrowRight className="w-4 h-4 ml-2 group-hover:translate-x-1 transition-transform" />
                                </div>
                            </CardContent>
                        </Card>
                    </Link>
                </div>

                {/* Stats */}
                <div className="grid md:grid-cols-2 gap-6 mb-12">
                    <Card>
                        <CardContent className="p-6">
                            <div className="flex items-center justify-between">
                                <div>
                                    <p className="text-sm text-slate-500 mb-1">Total Jobs</p>
                                    <p className="text-4xl font-light text-slate-900">{stats.totalJobs}</p>
                                </div>
                                <div className="w-16 h-16 rounded-full bg-slate-100 flex items-center justify-center">
                                    <History className="w-8 h-8 text-slate-400" />
                                </div>
                            </div>
                        </CardContent>
                    </Card>

                    <Card>
                        <CardContent className="p-6">
                            <div className="flex items-center justify-between">
                                <div>
                                    <p className="text-sm text-slate-500 mb-1">Completed</p>
                                    <p className="text-4xl font-light text-green-600">{stats.completedJobs}</p>
                                </div>
                                <div className="w-16 h-16 rounded-full bg-green-100 flex items-center justify-center">
                                    <CheckCircle className="w-8 h-8 text-green-500" />
                                </div>
                            </div>
                        </CardContent>
                    </Card>
                </div>

                {/* Recent Jobs */}
                <Card>
                    <CardHeader>
                        <div className="flex items-center justify-between">
                            <CardTitle className="text-xl font-semibold">Recent Jobs</CardTitle>
                            <Link to={createPageUrl('History')}>
                                <Button variant="ghost" size="sm">
                                    View All
                                    <ArrowRight className="w-4 h-4 ml-2" />
                                </Button>
                            </Link>
                        </div>
                    </CardHeader>
                    <CardContent>
                        {stats.recentJobs.length === 0 ? (
                            <div className="text-center py-12">
                                <div className="w-16 h-16 rounded-full bg-slate-100 flex items-center justify-center mx-auto mb-4">
                                    <History className="w-8 h-8 text-slate-300" />
                                </div>
                                <p className="text-slate-500">No jobs yet</p>
                                <Link to={createPageUrl('NewRun')}>
                                    <Button className="mt-4" variant="outline">
                                        Start your first extraction
                                    </Button>
                                </Link>
                            </div>
                        ) : (
                            <div className="space-y-3">
                                {stats.recentJobs.map((job) => {
                                    const status = statusConfig[job.status] || statusConfig.queued;
                                    const StatusIcon = status.icon;
                                    
                                    return (
                                        <Link 
                                            key={job.id} 
                                            to={createPageUrl(`History?job=${job.id}`)}
                                            className="flex items-center justify-between p-4 rounded-lg hover:bg-slate-50 transition-colors"
                                        >
                                            <div className="flex items-center gap-4">
                                                <div className={`w-10 h-10 rounded-lg ${status.bg} flex items-center justify-center`}>
                                                    <StatusIcon className={`w-5 h-5 ${status.color}`} />
                                                </div>
                                                <div>
                                                    <p className="font-medium text-slate-900">
                                                        {job.input_file_name || 'Untitled Job'}
                                                    </p>
                                                    <p className="text-sm text-slate-500">
                                                        {formatDCTime(job.created_date)}
                                                    </p>
                                                </div>
                                            </div>
                                            <div className="flex items-center gap-4">
                                                <div className="text-right">
                                                    <p className="text-sm font-medium text-slate-900">
                                                        {job.processed_rows}/{job.total_rows}
                                                    </p>
                                                    <p className="text-xs text-slate-500">rows</p>
                                                </div>
                                                <Badge className={`${status.bg} ${status.color}`}>
                                                    {job.status}
                                                </Badge>
                                            </div>
                                        </Link>
                                    );
                                })}
                            </div>
                        )}
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}