import React, { useState, useEffect, useCallback } from 'react';
import { base44 } from '@/api/base44Client';
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { RefreshCw, Loader2, Search, CheckCircle, XCircle, HelpCircle, Cpu, Globe, ShieldCheck, ShieldX } from 'lucide-react';
import { toast } from 'sonner';

const PROVIDER_LABELS = {
    openai: 'OpenAI', openrouter: 'Legacy (Removed)', anthropic: 'Anthropic',
    azure_openai: 'Azure OpenAI', groq: 'Groq', together: 'Together AI',
    mistral: 'Mistral', perplexity: 'Perplexity', google: 'Google AI',
    openai_compatible: 'OpenAI-Compatible',
};

export default function ModelSelector({ connectionId, selectedModel, onSelectModel, selectedWebSearch, onSelectWebSearch }) {
    const [models, setModels] = useState([]);
    const [loading, setLoading] = useState(false);
    const [refreshing, setRefreshing] = useState(false);
    const [verifying, setVerifying] = useState(false);
    const [providerType, setProviderType] = useState(null);
    const [filterText, setFilterText] = useState('');

    useEffect(() => {
        if (connectionId) { loadCachedModels(); }
        else { setModels([]); setProviderType(null); }
    }, [connectionId]);

    // ── Fast path: read cached models from DB (no external API call) ──
    const loadCachedModels = async () => {
        if (!connectionId) return;
        setLoading(true);
        try {
            const resp = await base44.functions.invoke('apiConnections', {
                action: 'getModels', connection_id: connectionId
            });
            setModels(resp.data.models || []);
            setProviderType(resp.data.provider_type || null);
        } catch {
            toast.error('Failed to load models');
        } finally {
            setLoading(false);
        }
    };

    // ── Slow path: live fetch from provider API (for manual refresh) ──
    const refreshModelsLive = async () => {
        if (!connectionId) return;
        setRefreshing(true);
        try {
            const resp = await base44.functions.invoke('apiConnections', {
                action: 'fetchModels', connection_id: connectionId
            });
            setModels(resp.data.models || []);
            setProviderType(resp.data.provider_type || null);
            const count = resp.data.models?.length || 0;
            const wsCount = (resp.data.models || []).filter(m => m.supports_web_search === true).length;
            toast.success(`Refreshed: ${count} models found${wsCount > 0 ? `, ${wsCount} with web search` : ''}`);
        } catch {
            toast.error('Failed to refresh models from provider');
        } finally {
            setRefreshing(false);
        }
    };

    // ── Auto-enable web search when user selects a model with verified search ──
    useEffect(() => {
        if (!selectedModel || models.length === 0) return;
        const model = models.find(m => m.model_id === selectedModel);
        const isVerified = model?.search_mode && model.search_mode !== 'none';
        if (isVerified && model?.web_search_options?.length > 0) {
            onSelectWebSearch(model.web_search_options[0]);
        } else {
            onSelectWebSearch('none');
        }
    }, [selectedModel, models]);

    const verifyWebSearch = async () => {
        if (!connectionId || !selectedModel) return;
        setVerifying(true);
        try {
            const resp = await base44.functions.invoke('apiConnections', {
                action: 'verifyWebSearch',
                connection_id: connectionId,
                model_id: selectedModel,
            });
            const result = resp.data;
            if (result.verified) {
                toast.success(`Web search verified: ${result.search_mode}`);
            } else {
                toast.info(`Web search not available: ${result.notes}`);
            }
            // Reload models to get updated search_mode
            await loadCachedModels();
        } catch {
            toast.error('Verification failed');
        } finally {
            setVerifying(false);
        }
    };

    const selectedModelData = models.find(m => m.model_id === selectedModel);
    const webSearchOptions = selectedModelData?.web_search_options || [];

    const filteredModels = models.filter(m => {
        if (!filterText) return true;
        const q = filterText.toLowerCase();
        return (m.model_id || '').toLowerCase().includes(q) || (m.display_name || '').toLowerCase().includes(q);
    });

    // Sort: web-search-capable models first, then alphabetical
    const sortedModels = [...filteredModels].sort((a, b) => {
        const aWs = a.supports_web_search === true ? 0 : 1;
        const bWs = b.supports_web_search === true ? 0 : 1;
        if (aWs !== bWs) return aWs - bWs;
        return (a.display_name || a.model_id || '').localeCompare(b.display_name || b.model_id || '');
    });

    if (!connectionId) {
        return (
            <Card className="border-dashed">
                <CardContent className="flex flex-col items-center justify-center py-8">
                    <Cpu className="w-10 h-10 text-slate-300 mb-3" />
                    <p className="text-slate-500 text-center text-sm">Select an API connection first</p>
                </CardContent>
            </Card>
        );
    }

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <div>
                    <h3 className="text-lg font-semibold text-slate-900">Model Selection</h3>
                    <div className="flex items-center gap-2 mt-0.5">
                        <p className="text-sm text-slate-500">Choose a model for extraction</p>
                        {providerType && (
                            <Badge className="bg-slate-100 text-slate-600 text-[10px]">
                                {PROVIDER_LABELS[providerType] || providerType}
                            </Badge>
                        )}
                        {models.length > 0 && (
                            <Badge variant="outline" className="text-[10px]">
                                {models.length} models
                            </Badge>
                        )}
                    </div>
                </div>
                <Button variant="outline" size="sm" onClick={refreshModelsLive}
                    disabled={loading || refreshing} className="gap-2">
                    {refreshing ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                    {refreshing ? 'Refreshing...' : 'Refresh'}
                </Button>
            </div>

            {loading ? (
                <div className="flex items-center justify-center py-8">
                    <Loader2 className="w-6 h-6 animate-spin text-slate-400" />
                </div>
            ) : models.length === 0 ? (
                <Card className="border-dashed">
                    <CardContent className="flex flex-col items-center justify-center py-8">
                        <Cpu className="w-10 h-10 text-slate-300 mb-3" />
                        <p className="text-slate-500 text-center text-sm mb-3">
                            No models cached for this connection.
                        </p>
                        <Button variant="outline" size="sm" onClick={refreshModelsLive}
                            disabled={refreshing} className="gap-2">
                            {refreshing ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                            Fetch Models from Provider
                        </Button>
                    </CardContent>
                </Card>
            ) : (
                <div className="space-y-4">
                    {models.length > 10 && (
                        <div className="relative">
                            <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
                            <Input value={filterText} onChange={(e) => setFilterText(e.target.value)}
                                placeholder={`Search ${models.length} models...`} className="pl-10" />
                        </div>
                    )}
                    <div className="space-y-2">
                        <Label>Select Model</Label>
                        <Select value={selectedModel || ''} onValueChange={onSelectModel}>
                            <SelectTrigger><SelectValue placeholder="Choose a model..." /></SelectTrigger>
                            <SelectContent className="max-h-80">
                                {sortedModels.map((model) => (
                                    <SelectItem key={model.id} value={model.model_id}>
                                        <div className="flex items-center gap-2">
                                            <span className="truncate">{model.display_name || model.model_id}</span>
                                            {model.supports_web_search === true && (
                                                <Badge className="bg-blue-100 text-blue-800 text-[10px] shrink-0">
                                                    <Globe className="w-2.5 h-2.5 mr-0.5" />Web
                                                </Badge>
                                            )}
                                        </div>
                                    </SelectItem>
                                ))}
                                {sortedModels.length === 0 && (
                                    <div className="px-2 py-4 text-sm text-slate-500 text-center">No models match "{filterText}"</div>
                                )}
                            </SelectContent>
                        </Select>
                        {models.length > 10 && (
                            <p className="text-xs text-slate-400">{sortedModels.length} of {models.length} models shown</p>
                        )}
                    </div>

                    {/* Selected model details */}
                    {selectedModel && (
                        <Card className="bg-slate-50">
                            <CardContent className="p-4">
                                <div className="flex items-center justify-between mb-2">
                                    <span className="font-medium text-slate-900 text-sm truncate mr-2">
                                        {selectedModelData?.display_name || selectedModel}
                                    </span>
                                    {selectedModelData?.search_mode && selectedModelData.search_mode !== 'none' ? (
                                        <Badge className="bg-green-100 text-green-800 text-[10px] shrink-0">
                                            <ShieldCheck className="w-2.5 h-2.5 mr-0.5" />Verified
                                        </Badge>
                                    ) : (
                                        <Badge className="bg-slate-200 text-slate-600 text-[10px] shrink-0">
                                            <ShieldX className="w-2.5 h-2.5 mr-0.5" />Unverified
                                        </Badge>
                                    )}
                                </div>
                                <div className="flex items-center gap-2 mb-2">
                                    <span className="text-sm text-slate-500">Web Search:</span>
                                    {selectedModelData?.search_mode && selectedModelData.search_mode !== 'none' ? (
                                        <div className="flex items-center gap-1 text-green-600">
                                            <CheckCircle className="w-4 h-4" />
                                            <span className="text-sm">
                                                Verified ({selectedModelData.search_mode.replace(/_/g, ' ')})
                                            </span>
                                        </div>
                                    ) : selectedModelData?.supports_web_search === true ? (
                                        <div className="flex items-center gap-1 text-amber-500">
                                            <HelpCircle className="w-4 h-4" />
                                            <span className="text-sm">Detected but not verified</span>
                                        </div>
                                    ) : selectedModelData?.supports_web_search === false ? (
                                        <div className="flex items-center gap-1 text-red-500">
                                            <XCircle className="w-4 h-4" /><span className="text-sm">Not supported</span>
                                        </div>
                                    ) : (
                                        <div className="flex items-center gap-1 text-slate-400">
                                            <HelpCircle className="w-4 h-4" /><span className="text-sm">Unknown</span>
                                        </div>
                                    )}
                                </div>
                                {selectedModelData?.search_verified_at && (
                                    <p className="text-xs text-slate-400 mb-1">
                                        Last verified: {new Date(selectedModelData.search_verified_at).toLocaleDateString()}
                                        {selectedModelData.search_verification_notes && ` — ${selectedModelData.search_verification_notes.slice(0, 80)}`}
                                    </p>
                                )}
                                {(selectedModelData?.supports_web_search === true || selectedModelData?.supports_web_search === null) && (!selectedModelData?.search_mode || selectedModelData.search_mode === 'none') && (
                                    <Button variant="outline" size="sm" onClick={verifyWebSearch} disabled={verifying} className="gap-2 mt-1">
                                        {verifying ? <Loader2 className="w-3 h-3 animate-spin" /> : <ShieldCheck className="w-3 h-3" />}
                                        {verifying ? 'Verifying...' : 'Verify Web Search'}
                                    </Button>
                                )}
                                {providerType === 'perplexity' && (
                                    <p className="text-xs text-indigo-600 mt-2">All Perplexity models include built-in web search automatically.</p>
                                )}
                            </CardContent>
                        </Card>
                    )}

                    {/* Web search override — only shown when model has verified search */}
                    {selectedModel && selectedModelData?.search_mode && selectedModelData.search_mode !== 'none' && webSearchOptions.length > 0 && (
                        <div className="space-y-2">
                            <Label>Web Search Tool</Label>
                            <Select value={selectedWebSearch || 'none'} onValueChange={onSelectWebSearch}>
                                <SelectTrigger><SelectValue placeholder="Select web search option..." /></SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="none">None (disable web search)</SelectItem>
                                    {webSearchOptions.map((opt) => (
                                        <SelectItem key={opt} value={opt}>
                                            {opt === 'builtin' ? 'Built-in Web Search' : opt.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                                        </SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                            <p className="text-xs text-slate-400">Web search was auto-enabled because this model is verified. You can disable it here.</p>
                        </div>
                    )}

                    {/* Web search disabled for unverified models */}
                    {selectedModel && (!selectedModelData?.search_mode || selectedModelData.search_mode === 'none') && (
                        <div className="space-y-2">
                            <Label>Web Search Tool</Label>
                            <Select value="none" disabled>
                                <SelectTrigger className="opacity-50"><SelectValue placeholder="Not verified" /></SelectTrigger>
                                <SelectContent><SelectItem value="none">Not verified</SelectItem></SelectContent>
                            </Select>
                            <p className="text-xs text-slate-500">
                                {selectedModelData?.supports_web_search === true
                                    ? 'Web search detected but must be verified first. Click "Verify Web Search" above.'
                                    : "This model doesn't support web search tools."}
                            </p>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}