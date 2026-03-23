import React, { useState, useEffect } from 'react';
import { base44 } from '@/api/base44Client';
import { Link, useNavigate } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { 
    ArrowLeft, ArrowRight, Play, AlertCircle, CheckCircle, 
    Loader2, FileSpreadsheet, Server, Cpu, Search
} from 'lucide-react';
import { toast } from 'sonner';

import ConnectionManager from '@/components/connections/ConnectionManager';
import ModelSelector from '@/components/models/ModelSelector';
import FileUploader from '@/components/jobs/FileUploader';
import JobProgress from '@/components/jobs/JobProgress';

export default function NewRun() {
    const [user, setUser] = useState(null);
    const [step, setStep] = useState(1);
    const [selectedConnection, setSelectedConnection] = useState(null);
    const [selectedModel, setSelectedModel] = useState(null);
    const [selectedWebSearch, setSelectedWebSearch] = useState('none');
    const [parsedFile, setParsedFile] = useState(null);
    const [specVersion, setSpecVersion] = useState(null);
    const [createdJobId, setCreatedJobId] = useState(null);
    const [creating, setCreating] = useState(false);
    const [hasSpec, setHasSpec] = useState(true);
    const [taskName, setTaskName] = useState('');

    useEffect(() => {
        base44.auth.me().then(u => setUser(u)).catch(() => {});
        checkSpec();
    }, []);

    const canManageConnections = user?.role === 'admin';

    const checkSpec = async () => {
        try {
            const response = await base44.functions.invoke('specManager', { action: 'getLatestVersionId' });
            if (response.data.version_id) {
                setSpecVersion({
                    id: response.data.version_id,
                    number: response.data.version_number
                });
                setHasSpec(true);
            } else {
                setHasSpec(false);
            }
        } catch (error) {
            setHasSpec(false);
        }
    };

    const canProceedToStep2 = selectedConnection && selectedModel;
    const canProceedToStep3 = canProceedToStep2 && parsedFile;
    const canSubmit = canProceedToStep3 && hasSpec;

    const handleSubmit = async () => {
        if (!canSubmit) return;

        setCreating(true);
        try {
            const response = await base44.functions.invoke('jobProcessor', {
                action: 'create',
                connection_id: selectedConnection,
                model_id: selectedModel,
                web_search_choice: selectedWebSearch,
                input_file_url: parsedFile.file_url,
                input_file_name: parsedFile.file_name,
                total_rows: parsedFile.total_rows,
                input_rows: parsedFile.rows,
                task_name: taskName
            });

            const jobId = response.data.job.id;
            setCreatedJobId(jobId);
            setStep(4);
            toast.success('Job created — processing will run on the server in the background');
        } catch (error) {
            toast.error('Failed to create job');
        } finally {
            setCreating(false);
        }
    };

    const steps = [
        { number: 1, title: 'Upload File', icon: FileSpreadsheet },
        { number: 2, title: 'Configure API', icon: Server },
        { number: 3, title: 'Review & Run', icon: Play }
    ];

    if (!hasSpec) {
        return (
            <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 flex items-center justify-center">
                <Card className="max-w-md w-full mx-4">
                    <CardContent className="p-8 text-center">
                        <div className="w-16 h-16 rounded-full bg-amber-100 flex items-center justify-center mx-auto mb-4">
                            <AlertCircle className="w-8 h-8 text-amber-600" />
                        </div>
                        <h2 className="text-xl font-semibold text-slate-900 mb-2">
                            Specification Required
                        </h2>
                        <p className="text-slate-500 mb-6">
                            The extraction specification must be configured before you can run jobs.
                        </p>
                        <div className="flex gap-3 justify-center">
                            <Link to={createPageUrl('Dashboard')}>
                                <Button variant="outline">
                                    <ArrowLeft className="w-4 h-4 mr-2" />
                                    Back
                                </Button>
                            </Link>
                            <Link to={createPageUrl('SpecEditor')}>
                                <Button className="bg-slate-900 hover:bg-slate-800">
                                    Configure Spec
                                </Button>
                            </Link>
                        </div>
                    </CardContent>
                </Card>
            </div>
        );
    }

    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
            <div className="max-w-4xl mx-auto px-6 py-12">
                {/* Header */}
                <div className="mb-8">
                    <Link to={createPageUrl('Dashboard')} className="inline-flex items-center text-slate-500 hover:text-slate-700 mb-4">
                        <ArrowLeft className="w-4 h-4 mr-2" />
                        Back to Dashboard
                    </Link>
                    <h1 className="text-3xl font-light text-slate-900">New Extraction Run</h1>
                </div>

                {/* Progress Steps */}
                {step < 4 && (
                    <div className="flex items-center justify-center mb-12">
                        {steps.map((s, index) => (
                            <React.Fragment key={s.number}>
                                <div 
                                    className={`flex items-center gap-2 ${
                                        step >= s.number ? 'text-slate-900' : 'text-slate-400'
                                    }`}
                                >
                                    <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                                        step >= s.number 
                                            ? 'bg-slate-900 text-white' 
                                            : 'bg-slate-200 text-slate-400'
                                    }`}>
                                        {step > s.number ? (
                                            <CheckCircle className="w-5 h-5" />
                                        ) : (
                                            <s.icon className="w-5 h-5" />
                                        )}
                                    </div>
                                    <span className="font-medium hidden sm:inline">{s.title}</span>
                                </div>
                                {index < steps.length - 1 && (
                                    <div className={`w-12 h-0.5 mx-2 ${
                                        step > s.number ? 'bg-slate-900' : 'bg-slate-200'
                                    }`} />
                                )}
                            </React.Fragment>
                        ))}
                    </div>
                )}

                {/* Step 1: File Upload */}
                {step === 1 && (
                    <div className="space-y-6">
                        <Card>
                            <CardHeader>
                                <CardTitle>Upload Input File</CardTitle>
                                <CardDescription>
                                    Upload an Excel file with columns: Owner, Economy, Legal basis, Question, Topic
                                </CardDescription>
                            </CardHeader>
                            <CardContent>
                                <FileUploader 
                                    onFileProcessed={setParsedFile}
                                    parsedData={parsedFile}
                                />
                            </CardContent>
                        </Card>

                        <div className="flex justify-end">
                            <Button
                                onClick={() => setStep(2)}
                                disabled={!parsedFile}
                                className="gap-2 bg-slate-900 hover:bg-slate-800"
                            >
                                Next: Configure API
                                <ArrowRight className="w-4 h-4" />
                            </Button>
                        </div>
                    </div>
                )}

                {/* Step 2: API Configuration */}
                {step === 2 && (
                    <div className="space-y-6">
                        <Card>
                            <CardHeader>
                                <CardTitle>API Connection</CardTitle>
                                <CardDescription>
                                    Select or create an API connection for the LLM
                                </CardDescription>
                            </CardHeader>
                            <CardContent>
                                <ConnectionManager 
                                    selectedId={selectedConnection}
                                    onSelect={setSelectedConnection}
                                    allowManagement={canManageConnections}
                                />
                            </CardContent>
                        </Card>

                        <Card>
                            <CardHeader>
                                <CardTitle>Model & Web Search</CardTitle>
                                <CardDescription>
                                    Choose the model and web search options
                                </CardDescription>
                            </CardHeader>
                            <CardContent>
                                <ModelSelector
                                    connectionId={selectedConnection}
                                    selectedModel={selectedModel}
                                    onSelectModel={setSelectedModel}
                                    selectedWebSearch={selectedWebSearch}
                                    onSelectWebSearch={setSelectedWebSearch}
                                />
                            </CardContent>
                        </Card>

                        <div className="flex justify-between">
                            <Button variant="outline" onClick={() => setStep(1)}>
                                <ArrowLeft className="w-4 h-4 mr-2" />
                                Back
                            </Button>
                            <Button
                                onClick={() => setStep(3)}
                                disabled={!canProceedToStep2}
                                className="gap-2 bg-slate-900 hover:bg-slate-800"
                            >
                                Next: Review
                                <ArrowRight className="w-4 h-4" />
                            </Button>
                        </div>
                    </div>
                )}

                {/* Step 3: Review & Submit */}
                {step === 3 && (
                    <div className="space-y-6">
                        <Card>
                            <CardHeader>
                                <CardTitle>Review Configuration</CardTitle>
                                <CardDescription>
                                    Confirm your settings before starting the extraction
                                </CardDescription>
                            </CardHeader>
                            <CardContent className="space-y-4">
                                <div className="grid md:grid-cols-2 gap-4">
                                    <div className="p-4 bg-slate-50 rounded-lg">
                                        <div className="flex items-center gap-2 text-slate-500 mb-2">
                                            <FileSpreadsheet className="w-4 h-4" />
                                            <span className="text-sm">Input File</span>
                                        </div>
                                        <p className="font-medium">{parsedFile?.file_name}</p>
                                        <p className="text-sm text-slate-500">{parsedFile?.total_rows} rows</p>
                                    </div>

                                    <div className="p-4 bg-slate-50 rounded-lg">
                                        <div className="flex items-center gap-2 text-slate-500 mb-2">
                                            <Cpu className="w-4 h-4" />
                                            <span className="text-sm">Model</span>
                                        </div>
                                        <p className="font-medium">{selectedModel}</p>
                                        <p className="text-sm text-slate-500">
                                            Web Search: {selectedWebSearch === 'none' ? 'Disabled' : selectedWebSearch}
                                        </p>
                                    </div>
                                </div>

                                <div className="p-4 bg-slate-50 rounded-lg">
                                    <label className="block text-sm font-medium text-slate-700 mb-2">
                                        Task Name (optional)
                                    </label>
                                    <Input
                                        value={taskName}
                                        onChange={(e) => setTaskName(e.target.value)}
                                        placeholder="e.g. Slovenia batch Q1 2026"
                                        className="max-w-md"
                                    />
                                </div>

                                <div className="p-4 bg-green-50 rounded-lg border border-green-200">
                                    <div className="flex items-center gap-2">
                                        <CheckCircle className="w-5 h-5 text-green-600" />
                                        <div>
                                            <p className="font-medium text-green-900">
                                                Using Spec Version {specVersion?.number}
                                            </p>
                                            <p className="text-sm text-green-700">
                                                The latest active specification will be used for extraction
                                            </p>
                                        </div>
                                    </div>
                                </div>
                            </CardContent>
                        </Card>

                        <div className="flex justify-between">
                            <Button variant="outline" onClick={() => setStep(2)}>
                                <ArrowLeft className="w-4 h-4 mr-2" />
                                Back
                            </Button>
                            <Button
                                onClick={handleSubmit}
                                disabled={creating || !canSubmit}
                                className="gap-2 bg-slate-900 hover:bg-slate-800"
                            >
                                {creating ? (
                                    <>
                                        <Loader2 className="w-4 h-4 animate-spin" />
                                        Starting...
                                    </>
                                ) : (
                                    <>
                                        <Play className="w-4 h-4" />
                                        Start Extraction
                                    </>
                                )}
                            </Button>
                        </div>
                    </div>
                )}

                {/* Step 4: Job Progress */}
                {step === 4 && createdJobId && (
                    <div className="space-y-6">
                        <JobProgress 
                            jobId={createdJobId}
                            onComplete={(job) => {
                                toast.success('Extraction completed!');
                            }}
                        />

                        <div className="flex justify-center gap-4">
                            <Link to={createPageUrl('NewRun')}>
                                <Button variant="outline" onClick={() => {
                                    setStep(1);
                                    setParsedFile(null);
                                    setCreatedJobId(null);
                                }}>
                                    Start New Run
                                </Button>
                            </Link>
                            <Link to={createPageUrl('History')}>
                                <Button variant="outline">
                                    View All Jobs
                                </Button>
                            </Link>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}
