import React, { useState, useEffect, useCallback } from 'react';
import { base44 } from '@/api/base44Client';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Plus, Trash2, RefreshCw, Check, Loader2, Server, Globe, AlertTriangle, Shield } from 'lucide-react';
import { toast } from 'sonner';

const PROVIDER_COLORS = {
    openai:           { bg: 'bg-emerald-100', text: 'text-emerald-800' },
    openrouter:       { bg: 'bg-violet-100',  text: 'text-violet-800' },
    anthropic:        { bg: 'bg-orange-100',  text: 'text-orange-800' },
    azure_openai:     { bg: 'bg-blue-100',    text: 'text-blue-800' },
    groq:             { bg: 'bg-yellow-100',  text: 'text-yellow-800' },
    together:         { bg: 'bg-pink-100',    text: 'text-pink-800' },
    mistral:          { bg: 'bg-cyan-100',    text: 'text-cyan-800' },
    perplexity:       { bg: 'bg-indigo-100',  text: 'text-indigo-800' },
    google:           { bg: 'bg-sky-100',     text: 'text-sky-800' },
    openai_compatible:{ bg: 'bg-slate-100',   text: 'text-slate-800' },
};

const PROVIDER_LABELS = {
    openai: 'OpenAI', openrouter: 'OpenRouter', anthropic: 'Anthropic',
    azure_openai: 'Azure OpenAI', groq: 'Groq', together: 'Together AI',
    mistral: 'Mistral', perplexity: 'Perplexity', google: 'Google AI',
    openai_compatible: 'OpenAI-Compatible',
};

const PRESET_URLS = {
    openai:     'https://api.openai.com',
    openrouter: 'https://openrouter.ai/api',
    anthropic:  'https://api.anthropic.com',
    groq:       'https://api.groq.com',
    together:   'https://api.together.xyz',
    mistral:    'https://api.mistral.ai',
    perplexity: 'https://api.perplexity.ai',
    google:     'https://generativelanguage.googleapis.com',
};

export default function ConnectionManager({ onSelect, selectedId }) {
    const [connections, setConnections] = useState([]);
    const [loading, setLoading] = useState(true);
    const [showCreate, setShowCreate] = useState(false);
    const [testing, setTesting] = useState(null);
    const [creating, setCreating] = useState(false);
    const [newConn, setNewConn] = useState({ name: '', base_url: '', api_key: '' });
    const [detectedProvider, setDetectedProvider] = useState(null);
    const [testResult, setTestResult] = useState(null);

    useEffect(() => { loadConnections(); }, []);

    const detectProvider = useCallback(async (url, key) => {
        if (!url && !key) { setDetectedProvider(null); return; }
        try {
            const resp = await base44.functions.invoke('apiConnections', {
                action: 'detectProvider', base_url: url, api_key: key
            });
            setDetectedProvider(resp.data);
        } catch { setDetectedProvider(null); }
    }, []);

    useEffect(() => {
        const t = setTimeout(() => detectProvider(newConn.base_url, newConn.api_key), 300);
        return () => clearTimeout(t);
    }, [newConn.base_url, newConn.api_key, detectProvider]);

    const loadConnections = async () => {
        try {
            const resp = await base44.functions.invoke('apiConnections', { action: 'list' });
            setConnections(resp.data.connections || []);
        } catch { toast.error('Failed to load connections'); }
        finally { setLoading(false); }
    };

    const handleCreate = async () => {
        if (!newConn.name || !newConn.base_url || !newConn.api_key) {
            toast.error('Please fill in all fields'); return;
        }
        setCreating(true);
        setTestResult(null);
        try {
            const testResp = await base44.functions.invoke('apiConnections', {
                action: 'testNew', base_url: newConn.base_url, api_key: newConn.api_key
            });
            if (!testResp.data.success) {
                const err = testResp.data.error || 'Unknown error';
                setTestResult({
                    success: false, error: err,
                    isCloudflare: err.includes('CLOUDFLARE_BLOCKED'),
                    provider: testResp.data.label || 'Unknown',
                });
                setCreating(false); return;
            }
            await base44.functions.invoke('apiConnections', { action: 'create', ...newConn });
            toast.success(`${testResp.data.label || 'Connection'} added — ${testResp.data.models?.length || 0} models found`);
            setShowCreate(false);
            setNewConn({ name: '', base_url: '', api_key: '' });
            setDetectedProvider(null);
            setTestResult(null);
            loadConnections();
        } catch { toast.error('Failed to create connection'); }
        finally { setCreating(false); }
    };

    const handleTest = async (connectionId) => {
        setTesting(connectionId);
        try {
            const resp = await base44.functions.invoke('apiConnections', {
                action: 'testExisting', connection_id: connectionId
            });
            if (resp.data.success) { toast.success('Connection is working'); loadConnections(); }
            else { toast.error(`Test failed: ${resp.data.error?.slice(0, 120)}`); }
        } catch { toast.error('Test failed'); }
        finally { setTesting(null); }
    };

    const handleDelete = async (connectionId) => {
        if (!confirm('Delete this connection and all its cached models?')) return;
        try {
            const resp = await base44.functions.invoke('apiConnections', { action: 'delete', connection_id: connectionId });
            if (resp.data?.error) {
                toast.error(resp.data.error);
            } else {
                toast.success('Connection deleted');
                loadConnections();
                if (selectedId === connectionId) onSelect?.(null);
            }
        } catch (err) {
            const msg = err?.response?.data?.error || err?.message || 'Unknown error';
            toast.error(`Failed to delete: ${msg}`);
        }
    };

    const applyPreset = (key) => {
        setNewConn(prev => ({
            ...prev,
            base_url: PRESET_URLS[key] || prev.base_url,
            name: prev.name || PROVIDER_LABELS[key] || '',
        }));
    };

    const ProviderBadge = ({ providerType, small }) => {
        const colors = PROVIDER_COLORS[providerType] || PROVIDER_COLORS.openai_compatible;
        const label  = PROVIDER_LABELS[providerType]  || providerType;
        return (
            <Badge className={`${colors.bg} ${colors.text} ${small ? 'text-[10px] px-1.5 py-0' : 'text-xs'}`}>
                {label}
            </Badge>
        );
    };

    if (loading) {
        return <div className="flex items-center justify-center h-32"><Loader2 className="w-6 h-6 animate-spin text-slate-400" /></div>;
    }

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <div>
                    <h3 className="text-lg font-semibold text-slate-900">API Connections</h3>
                    <p className="text-sm text-slate-500">Connect to any LLM provider</p>
                </div>
                <Dialog open={showCreate} onOpenChange={(open) => { setShowCreate(open); if (!open) { setTestResult(null); setDetectedProvider(null); } }}>
                    <DialogTrigger asChild>
                        <Button className="gap-2 bg-slate-900 hover:bg-slate-800">
                            <Plus className="w-4 h-4" /> Add Connection
                        </Button>
                    </DialogTrigger>
                    <DialogContent className="sm:max-w-lg">
                        <DialogHeader><DialogTitle>New API Connection</DialogTitle></DialogHeader>
                        <div className="space-y-4 py-2">
                            <div>
                                <Label className="text-xs text-slate-500 mb-2 block">Quick presets</Label>
                                <div className="flex flex-wrap gap-1.5">
                                    {Object.entries(PRESET_URLS).map(([key]) => (
                                        <button key={key} onClick={() => applyPreset(key)}
                                            className="text-xs px-2 py-1 rounded-md border border-slate-200 hover:bg-slate-50 transition-colors">
                                            {PROVIDER_LABELS[key]}
                                        </button>
                                    ))}
                                </div>
                            </div>
                            <div className="space-y-1.5">
                                <Label>Connection Name</Label>
                                <Input value={newConn.name} onChange={(e) => setNewConn({ ...newConn, name: e.target.value })} placeholder="My API Connection" />
                            </div>
                            <div className="space-y-1.5">
                                <Label>API Base URL</Label>
                                <Input value={newConn.base_url} onChange={(e) => setNewConn({ ...newConn, base_url: e.target.value })} placeholder="https://api.openai.com" />
                            </div>
                            <div className="space-y-1.5">
                                <Label>API Key</Label>
                                <Input type="password" value={newConn.api_key} onChange={(e) => setNewConn({ ...newConn, api_key: e.target.value })} placeholder="sk-..." />
                            </div>
                            {detectedProvider && (
                                <div className="flex items-center gap-2 p-3 bg-slate-50 rounded-lg">
                                    <Shield className="w-4 h-4 text-slate-500" />
                                    <span className="text-sm text-slate-600">Detected: <strong>{detectedProvider.label}</strong></span>
                                    <ProviderBadge providerType={detectedProvider.provider_type} small />
                                    {detectedProvider.cloudflareRisk && (
                                        <Badge className="bg-amber-100 text-amber-800 text-[10px]">
                                            <AlertTriangle className="w-3 h-3 mr-0.5" /> CF risk
                                        </Badge>
                                    )}
                                </div>
                            )}
                            {detectedProvider?.cloudflareRisk && (
                                <Alert className="border-amber-200 bg-amber-50">
                                    <AlertTriangle className="w-4 h-4 text-amber-600" />
                                    <AlertDescription className="text-sm text-amber-800">
                                        <strong>{detectedProvider.label}</strong> may block requests from cloud servers (Cloudflare bot protection).
                                        If this fails, use <button onClick={() => applyPreset('openrouter')} className="underline font-medium">OpenRouter</button> instead — it proxies to {detectedProvider.label} models without Cloudflare blocks.
                                    </AlertDescription>
                                </Alert>
                            )}
                            {testResult && !testResult.success && (
                                <Alert className="border-red-200 bg-red-50">
                                    <AlertTriangle className="w-4 h-4 text-red-600" />
                                    <AlertDescription className="text-sm text-red-800">
                                        {testResult.isCloudflare ? (
                                            <><strong>Cloudflare blocked this request.</strong> The {testResult.provider} API rejected the server-side call. Use <button onClick={() => applyPreset('openrouter')} className="underline font-semibold">OpenRouter</button> as a proxy to access {testResult.provider} models.</>
                                        ) : (
                                            <>Connection test failed: {testResult.error?.slice(0, 200)}</>
                                        )}
                                    </AlertDescription>
                                </Alert>
                            )}
                            <Button onClick={handleCreate} disabled={creating} className="w-full gap-2">
                                {creating ? <Loader2 className="w-4 h-4 animate-spin" /> : <Check className="w-4 h-4" />}
                                Test & Create
                            </Button>
                        </div>
                    </DialogContent>
                </Dialog>
            </div>
            {connections.length === 0 ? (
                <Card className="border-dashed">
                    <CardContent className="flex flex-col items-center justify-center py-12">
                        <Server className="w-12 h-12 text-slate-300 mb-4" />
                        <p className="text-slate-500 text-center">No API connections yet. Add one to get started.</p>
                    </CardContent>
                </Card>
            ) : (
                <div className="grid gap-3">
                    {connections.map((conn) => {
                        const colors = PROVIDER_COLORS[conn.provider_type] || PROVIDER_COLORS.openai_compatible;
                        return (
                            <Card key={conn.id}
                                className={`cursor-pointer transition-all ${selectedId === conn.id ? 'ring-2 ring-slate-900 bg-slate-50' : 'hover:bg-slate-50'}`}
                                onClick={() => onSelect?.(conn.id)}>
                                <CardContent className="p-4">
                                    <div className="flex items-center justify-between">
                                        <div className="flex items-center gap-3">
                                            <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${conn.is_valid ? colors.bg : 'bg-slate-100'}`}>
                                                <Globe className={`w-5 h-5 ${conn.is_valid ? colors.text : 'text-slate-400'}`} />
                                            </div>
                                            <div>
                                                <div className="flex items-center gap-2">
                                                    <p className="font-medium text-slate-900">{conn.name}</p>
                                                    <ProviderBadge providerType={conn.provider_type} small />
                                                </div>
                                                <p className="text-sm text-slate-500">
                                                    {conn.base_url}
                                                    {conn.model_count > 0 && (
                                                        <span className="ml-2 text-xs text-slate-400">
                                                            • {conn.model_count} models
                                                            {conn.web_search_model_count > 0 && (
                                                                <span className="text-blue-500"> ({conn.web_search_model_count} with web search)</span>
                                                            )}
                                                        </span>
                                                    )}
                                                </p>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-2">
                                            {conn.is_valid ? (
                                                <Badge className="bg-green-100 text-green-800">Valid</Badge>
                                            ) : (
                                                <Badge variant="secondary">Untested</Badge>
                                            )}
                                            <Button variant="ghost" size="icon"
                                                onClick={(e) => { e.stopPropagation(); handleTest(conn.id); }}
                                                disabled={testing === conn.id}>
                                                {testing === conn.id ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                                            </Button>
                                            <Button variant="ghost" size="icon"
                                                onClick={(e) => { e.stopPropagation(); handleDelete(conn.id); }}
                                                className="text-red-500 hover:text-red-600 hover:bg-red-50">
                                                <Trash2 className="w-4 h-4" />
                                            </Button>
                                        </div>
                                    </div>
                                </CardContent>
                            </Card>
                        );
                    })}
                </div>
            )}
        </div>
    );
}