import React, { useState, useEffect } from 'react';
import { base44 } from '@/api/base44Client';
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { RefreshCw, Loader2, Search, CheckCircle, XCircle, HelpCircle, Cpu } from 'lucide-react';
import { toast } from 'sonner';

const PROVIDER_LABELS = {
    openai: 'OpenAI', openrouter: 'OpenRouter', anthropic: 'Anthropic',
    azure_openai: 'Azure OpenAI', groq: 'Groq', together: 'Together AI',
    mistral: 'Mistral', perplexity: 'Perplexity', google: 'Google AI',
    openai_compatible: 'OpenAI-Compatible',
};

export default function ModelSelector({ connectionId, selectedModel, onSelectModel, selectedWebSearch, onSelectWebSearch }) {
    const [models, setModels] = useState([]);
    const [loading, setLoading] = useState(false);
    const [probing, setProbing] = useState(null);
    const [providerType, setProviderType] = useState(null);
    const [filterText, setFilterText] = useState('');

    useEffect(() => {
        if (connectionId) { loadModels(); }
        else { setModels([]); setProviderType(null); }
    }, [connectionId]);

    const loadModels = async () => {
        if (!connectionId) return;
        setLoading(true);
        try {
            const resp = await base44.functions.invoke('apiConnections', {
                action: 'fetchModels', connection_id: connectionId
            });
            setModels(resp.data.models || []);
            setProviderType(resp.data.provider_type || null);
        } catch { toast.error('Failed to fetch models'); }
        finally { setLoading(false); }
    };

    const probeWebSearch = async (modelId) => {
        setProbing(modelId);
        try {
            const resp = await base44.functions.invoke('apiConnections', {
                action: 'probeWebSearch', connection_id: connectionId, model_id: modelId
            });
            await loadModels();
            if (resp.data.supports_web_search) {
                toast.success(`Web search supported: ${resp.data.web_search_options.join(', ')}`);
            } else {
                toast.info('Web search not supported for this model');
            }
        } catch { toast.error('Failed to probe web search'); }
        finally { setProbing(null); }
    };

    const selectedModelData = models.find(m => m.model_id === selectedModel);
    const webSearchOptions = selectedModelData?.web_search_options || [];

    const filteredModels = models.filter(m => {
        if (!filterText) return true;
        const q = filterText.toLowerCase();
        return (m.model_id || '').toLowerCase().includes(q) || (m.display_name || '').toLowerCase().includes(q);
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
                    </div>
                </div>
                <Button variant="outline" size="sm" onClick={loadModels} disabled={loading} className="gap-2">
                    {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                    Refresh
                </Button>
            </div>

            {loading ? (
                <div className="flex items-center justify-center py-8">
                    <Loader2 className="w-6 h-6 animate-spin text-slate-400" />
                </div>
            ) : models.length === 0 ? (
                <Card className="border-dashed">
                    <CardContent className="flex flex-col items-center justify-center py-8">
                        <p className="text-slate-500 text-center text-sm">No models found. Click Refresh to load.</p>
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
                                {filteredModels.map((model) => (
                                    <SelectItem key={model.id} value={model.model_id}>
                                        <div className="flex items-center gap-2">
                                            <span className="truncate">{model.display_name || model.model_id}</span>
                                            {model.supports_web_search === true && (
                                                <Badge className="bg-blue-100 text-blue-800 text-[10px] shrink-0">Web</Badge>
                                            )}
                                        </div>
                                    </SelectItem>
                                ))}
                                {filteredModels.length === 0 && (
                                    <div className="px-2 py-4 text-sm text-slate-500 text-center">No models match "{filterText}"</div>
                                )}
                            </SelectContent>
                        </Select>
                        {models.length > 10 && (
                            <p className="text-xs text-slate-400">{filteredModels.length} of {models.length} models shown</p>
                        )}
                    </div>

                    {selectedModel && (
                        <Card className="bg-slate-50">
                            <CardContent className="p-4">
                                <div className="flex items-center justify-between mb-3">
                                    <span className="font-medium text-slate-900 text-sm truncate mr-2">
                                        {selectedModelData?.display_name || selectedModel}
                                    </span>
                                    <Button variant="ghost" size="sm" onClick={() => probeWebSearch(selectedModel)}
                                        disabled={probing === selectedModel} className="gap-1.5 text-xs shrink-0">
                                        {probing === selectedModel ? <Loader2 className="w-3 h-3 animate-spin" /> : <Search className="w-3 h-3" />}
                                        Check Web Search
                                    </Button>
                                </div>
                                <div className="flex items-center gap-2">
                                    <span className="text-sm text-slate-500">Web Search:</span>
                                    {selectedModelData?.supports_web_search === true ? (
                                        <div className="flex items-center gap-1 text-green-600">
                                            <CheckCircle className="w-4 h-4" />
                                            <span className="text-sm">Supported {webSearchOptions.length > 0 && <span className="text-slate-400 ml-1">({webSearchOptions.join(', ')})</span>}</span>
                                        </div>
                                    ) : selectedModelData?.supports_web_search === false ? (
                                        <div className="flex items-center gap-1 text-red-500">
                                            <XCircle className="w-4 h-4" /><span className="text-sm">Not supported</span>
                                        </div>
                                    ) : (
                                        <div className="flex items-center gap-1 text-amber-500">
                                            <HelpCircle className="w-4 h-4" /><span className="text-sm">Unknown — click Check</span>
                                        </div>
                                    )}
                                </div>
                                {providerType === 'perplexity' && (
                                    <p className="text-xs text-indigo-600 mt-2">All Perplexity models include built-in web search automatically.</p>
                                )}
                            </CardContent>
                        </Card>
                    )}

                    {selectedModel && webSearchOptions.length > 0 && (
                        <div className="space-y-2">
                            <Label>Web Search Tool</Label>
                            <Select value={selectedWebSearch || 'none'} onValueChange={onSelectWebSearch}>
                                <SelectTrigger><SelectValue placeholder="Select web search option..." /></SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="none">None (no web search)</SelectItem>
                                    {webSearchOptions.map((opt) => (
                                        <SelectItem key={opt} value={opt}>
                                            {opt === 'builtin' ? 'Built-in Web Search' : opt.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                                        </SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                        </div>
                    )}

                    {selectedModel && selectedModelData?.supports_web_search === false && (
                        <div className="space-y-2">
                            <Label>Web Search Tool</Label>
                            <Select value="none" disabled>
                                <SelectTrigger className="opacity-50"><SelectValue placeholder="Not available for this model" /></SelectTrigger>
                                <SelectContent><SelectItem value="none">Not available</SelectItem></SelectContent>
                            </Select>
                            <p className="text-xs text-slate-500">This model doesn't support web search tools</p>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}