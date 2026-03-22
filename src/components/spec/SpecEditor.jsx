import React, { useState, useEffect } from 'react';
import { base44 } from '@/api/base44Client';
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Save, RotateCcw, History, FileText, Loader2, Check, AlertCircle, ShieldAlert, Upload } from 'lucide-react';
import { format } from 'date-fns';
import { toast } from 'sonner';
import ReactMarkdown from 'react-markdown';

export default function SpecEditor() {
    const [spec, setSpec] = useState(null);
    const [editedText, setEditedText] = useState('');
    const [changeNote, setChangeNote] = useState('');
    const [versions, setVersions] = useState([]);
    const [loading, setLoading] = useState(true);
    const [saving, setSaving] = useState(false);
    const [activeTab, setActiveTab] = useState('edit');
    const [hasChanges, setHasChanges] = useState(false);
    const [isAdmin, setIsAdmin] = useState(false);
    const [importingFile, setImportingFile] = useState(false);

    useEffect(() => {
        loadSpec();
        loadVersions();
        base44.auth.me().then(u => setIsAdmin(u?.role === 'admin')).catch(() => {});
    }, []);

    useEffect(() => {
        if (spec) {
            setHasChanges(editedText !== spec.current_text);
        }
    }, [editedText, spec]);

    const loadSpec = async () => {
        try {
            const response = await base44.functions.invoke('specManager', { action: 'getActive' });
            setSpec(response.data.spec);
            setEditedText(response.data.spec?.current_text || '');
        } catch (error) {
            toast.error('Failed to load spec');
        } finally {
            setLoading(false);
        }
    };

    const loadVersions = async () => {
        try {
            const response = await base44.functions.invoke('specManager', { action: 'getVersions' });
            setVersions(response.data.versions || []);
        } catch (error) {
            console.error('Failed to load versions:', error);
        }
    };

    const handleSave = async () => {
        if (!editedText.trim()) {
            toast.error('Spec text cannot be empty');
            return;
        }

        setSaving(true);
        try {
            const response = await base44.functions.invoke('specManager', {
                action: 'save',
                spec_text: editedText,
                change_note: changeNote || 'Updated specification'
            });
            setSpec(response.data.spec);
            setChangeNote('');
            setHasChanges(false);
            toast.success('Spec saved successfully');
            loadVersions();
        } catch (error) {
            toast.error('Failed to save spec');
        } finally {
            setSaving(false);
        }
    };

    const handleRestoreDefault = async () => {
        if (!confirm('Are you sure you want to restore the default specification? This will overwrite your current changes.')) {
            return;
        }

        setSaving(true);
        try {
            const response = await base44.functions.invoke('specManager', { action: 'restoreDefault' });
            setSpec(response.data.spec);
            setEditedText(response.data.spec?.current_text || '');
            setHasChanges(false);
            toast.success('Default spec restored');
            loadVersions();
        } catch (error) {
            toast.error('Failed to restore default');
        } finally {
            setSaving(false);
        }
    };

    const handleRestoreVersion = async (version) => {
        setEditedText(version.spec_text);
        setActiveTab('edit');
        toast.info(`Loaded version ${version.version_number}. Click Save to apply.`);
    };

    const handleImportFile = async (e) => {
        const file = e.target.files?.[0];
        if (!file) return;

        const name = file.name.toLowerCase();
        const isPlainText = name.endsWith('.txt') || name.endsWith('.md');

        setImportingFile(true);
        try {
            if (isPlainText) {
                // Plain text files: read directly in the browser, load into editor
                const text = await file.text();
                if (!text.trim()) {
                    toast.error('File is empty');
                    return;
                }
                setEditedText(text);
                setActiveTab('edit');
                toast.success(`Loaded "${file.name}" into editor. Review and click Save to apply.`);
            } else {
                // Binary files (docx, pdf): upload and extract via backend
                const { file_url } = await base44.integrations.Core.UploadFile({ file });
                const response = await base44.functions.invoke('specManager', {
                    action: 'restoreFromFile',
                    file_url,
                });
                const updatedSpec = response.data.spec;
                setSpec(updatedSpec);
                setEditedText(updatedSpec?.current_text || '');
                setHasChanges(false);
                setActiveTab('edit');
                loadVersions();
                toast.success(`Spec loaded from "${file.name}" and saved as a new version.`);
            }
        } catch (error) {
            const msg = error?.response?.data?.error || 'Failed to import spec from file';
            toast.error(msg);
        } finally {
            setImportingFile(false);
            e.target.value = '';
        }
    };

    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <Loader2 className="w-8 h-8 animate-spin text-slate-400" />
            </div>
        );
    }

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                    <div>
                        <h2 className="text-2xl font-semibold text-slate-900">Specification Editor</h2>
                        <p className="text-slate-500 mt-1">
                            Define the rules for legal metadata extraction
                        </p>
                    </div>
                    {!isAdmin && (
                        <Badge className="bg-slate-100 text-slate-600 gap-1">
                            <ShieldAlert className="w-3 h-3" />
                            Admin only
                        </Badge>
                    )}
                </div>
                {spec && (
                    <div className="text-right text-sm text-slate-500">
                        <div>Last updated: {format(new Date(spec.updated_date), 'PPp')}</div>
                        <div>By: {spec.updated_by_email}</div>
                    </div>
                )}
            </div>

            <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
                <TabsList className="bg-slate-100">
                    <TabsTrigger value="edit" className="gap-2">
                        <FileText className="w-4 h-4" />
                        Edit Spec
                    </TabsTrigger>
                    <TabsTrigger value="preview" className="gap-2">
                        <FileText className="w-4 h-4" />
                        Preview
                    </TabsTrigger>
                    <TabsTrigger value="history" className="gap-2">
                        <History className="w-4 h-4" />
                        Version History
                    </TabsTrigger>
                </TabsList>

                <TabsContent value="edit" className="mt-4">
                    <Card>
                        <CardContent className="pt-6">
                            <div className="space-y-4">
                                <div className="relative">
                                    <Textarea
                                        value={editedText}
                                        onChange={(e) => setEditedText(e.target.value)}
                                        className="min-h-[500px] font-mono text-sm resize-y"
                                        placeholder="Enter specification text..."
                                        readOnly={!isAdmin}
                                    />
                                    {hasChanges && (
                                        <Badge className="absolute top-2 right-2 bg-amber-100 text-amber-800">
                                            Unsaved changes
                                        </Badge>
                                    )}
                                </div>

                                {isAdmin && (
                                    <div className="flex items-center gap-4">
                                        <Input
                                            value={changeNote}
                                            onChange={(e) => setChangeNote(e.target.value)}
                                            placeholder="Change note (optional)"
                                            className="flex-1"
                                        />
                                        <Button
                                            onClick={handleSave}
                                            disabled={saving || !hasChanges}
                                            className="gap-2 bg-slate-900 hover:bg-slate-800"
                                        >
                                            {saving ? (
                                                <Loader2 className="w-4 h-4 animate-spin" />
                                            ) : (
                                                <Save className="w-4 h-4" />
                                            )}
                                            Save Changes
                                        </Button>
                                        <Button
                                            variant="outline"
                                            onClick={handleRestoreDefault}
                                            disabled={saving || importingFile}
                                            className="gap-2"
                                        >
                                            <RotateCcw className="w-4 h-4" />
                                            Restore Default
                                        </Button>
                                        <div>
                                            <input
                                                type="file"
                                                accept=".txt,.md,.docx,.pdf"
                                                onChange={handleImportFile}
                                                className="hidden"
                                                id="spec-file-upload"
                                            />
                                            <label htmlFor="spec-file-upload">
                                                <Button
                                                    variant="outline"
                                                    className="gap-2 cursor-pointer"
                                                    disabled={saving || importingFile}
                                                    asChild
                                                >
                                                    <span>
                                                        {importingFile ? (
                                                            <Loader2 className="w-4 h-4 animate-spin" />
                                                        ) : (
                                                            <Upload className="w-4 h-4" />
                                                        )}
                                                        Load From File
                                                    </span>
                                                </Button>
                                            </label>
                                        </div>
                                    </div>
                                )}
                            </div>
                        </CardContent>
                    </Card>
                </TabsContent>

                <TabsContent value="preview" className="mt-4">
                    <Card>
                        <CardContent className="pt-6">
                            <div className="prose prose-slate max-w-none">
                                <ReactMarkdown>{editedText}</ReactMarkdown>
                            </div>
                        </CardContent>
                    </Card>
                </TabsContent>

                <TabsContent value="history" className="mt-4">
                    <Card>
                        <CardHeader>
                            <CardTitle className="text-lg">Version History</CardTitle>
                        </CardHeader>
                        <CardContent>
                            {versions.length === 0 ? (
                                <p className="text-slate-500 text-center py-8">No version history available</p>
                            ) : (
                                <div className="space-y-3">
                                    {versions.map((version, index) => (
                                        <div
                                            key={version.id}
                                            className="flex items-center justify-between p-4 bg-slate-50 rounded-lg hover:bg-slate-100 transition-colors"
                                        >
                                            <div className="flex items-center gap-4">
                                                <div className="w-10 h-10 rounded-full bg-slate-200 flex items-center justify-center font-semibold text-slate-600">
                                                    v{version.version_number}
                                                </div>
                                                <div>
                                                    <p className="font-medium text-slate-900">
                                                        {version.change_note || 'No description'}
                                                    </p>
                                                    <p className="text-sm text-slate-500">
                                                        {format(new Date(version.created_date), 'PPp')} by {version.created_by_email}
                                                    </p>
                                                </div>
                                            </div>
                                            <div className="flex items-center gap-2">
                                                {index === 0 && (
                                                    <Badge className="bg-green-100 text-green-800">Current</Badge>
                                                )}
                                                <Button
                                                    variant="ghost"
                                                    size="sm"
                                                    onClick={() => handleRestoreVersion(version)}
                                                    className="text-slate-600 hover:text-slate-900"
                                                >
                                                    Load
                                                </Button>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            )}
                        </CardContent>
                    </Card>
                </TabsContent>
            </Tabs>
        </div>
    );
}