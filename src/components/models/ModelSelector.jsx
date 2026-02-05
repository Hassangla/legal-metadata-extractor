import React, { useState, useEffect } from 'react';
import { base44 } from '@/api/base44Client';
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { RefreshCw, Loader2, Search, CheckCircle, XCircle, HelpCircle, Cpu } from 'lucide-react';
import { toast } from 'sonner';

export default function ModelSelector({ 
    connectionId, 
    selectedModel, 
    onSelectModel,
    selectedWebSearch,
    onSelectWebSearch 
}) {
    const [models, setModels] = useState([]);
    const [loading, setLoading] = useState(false);
    const [probing, setProbing] = useState(null);

    useEffect(() => {
        if (connectionId) {
            loadModels();
        } else {
            setModels([]);
        }
    }, [connectionId]);

    const loadModels = async () => {
        if (!connectionId) return;

        setLoading(true);
        try {
            const response = await base44.functions.invoke('apiConnections', {
                action: 'fetchModels',
                connection_id: connectionId
            });
            setModels(response.data.models || []);
        } catch (error) {
            toast.error('Failed to fetch models');
        } finally {
            setLoading(false);
        }
    };

    const probeWebSearch = async (modelId) => {
        setProbing(modelId);
        try {
            const response = await base44.functions.invoke('apiConnections', {
                action: 'probeWebSearch',
                connection_id: connectionId,
                model_id: modelId
            });
            
            // Refresh models to get updated capabilities
            await loadModels();
            
            if (response.data.supports_web_search) {
                toast.success('Web search supported!');
            } else {
                toast.info('Web search not supported for this model');
            }
        } catch (error) {
            toast.error('Failed to probe web search capability');
        } finally {
            setProbing(null);
        }
    };

    const selectedModelData = models.find(m => m.model_id === selectedModel);
    const webSearchOptions = selectedModelData?.web_search_options || [];

    if (!connectionId) {
        return (
            <Card className="border-dashed">
                <CardContent className="flex flex-col items-center justify-center py-8">
                    <Cpu className="w-10 h-10 text-slate-300 mb-3" />
                    <p className="text-slate-500 text-center text-sm">
                        Select an API connection first
                    </p>
                </CardContent>
            </Card>
        );
    }

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <div>
                    <h3 className="text-lg font-semibold text-slate-900">Model Selection</h3>
                    <p className="text-sm text-slate-500">Choose a model for extraction</p>
                </div>
                <Button
                    variant="outline"
                    size="sm"
                    onClick={loadModels}
                    disabled={loading}
                    className="gap-2"
                >
                    {loading ? (
                        <Loader2 className="w-4 h-4 animate-spin" />
                    ) : (
                        <RefreshCw className="w-4 h-4" />
                    )}
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
                        <p className="text-slate-500 text-center text-sm">
                            No models found. Click refresh to load.
                        </p>
                    </CardContent>
                </Card>
            ) : (
                <div className="space-y-4">
                    <div className="space-y-2">
                        <Label>Select Model</Label>
                        <Select value={selectedModel || ''} onValueChange={onSelectModel}>
                            <SelectTrigger>
                                <SelectValue placeholder="Choose a model..." />
                            </SelectTrigger>
                            <SelectContent>
                                {models.map((model) => (
                                    <SelectItem key={model.id} value={model.model_id}>
                                        <div className="flex items-center gap-2">
                                            <span>{model.display_name || model.model_id}</span>
                                            {model.supports_web_search === true && (
                                                <Badge className="bg-blue-100 text-blue-800 text-xs">
                                                    Web Search
                                                </Badge>
                                            )}
                                        </div>
                                    </SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                    </div>

                    {selectedModel && (
                        <Card className="bg-slate-50">
                            <CardContent className="p-4">
                                <div className="flex items-center justify-between mb-3">
                                    <span className="font-medium text-slate-900">
                                        {selectedModelData?.display_name || selectedModel}
                                    </span>
                                    <Button
                                        variant="ghost"
                                        size="sm"
                                        onClick={() => probeWebSearch(selectedModel)}
                                        disabled={probing === selectedModel}
                                        className="gap-2 text-xs"
                                    >
                                        {probing === selectedModel ? (
                                            <Loader2 className="w-3 h-3 animate-spin" />
                                        ) : (
                                            <Search className="w-3 h-3" />
                                        )}
                                        Check Web Search
                                    </Button>
                                </div>
                                
                                <div className="flex items-center gap-2">
                                    <span className="text-sm text-slate-500">Web Search:</span>
                                    {selectedModelData?.supports_web_search === true ? (
                                        <div className="flex items-center gap-1 text-green-600">
                                            <CheckCircle className="w-4 h-4" />
                                            <span className="text-sm">Supported</span>
                                        </div>
                                    ) : selectedModelData?.supports_web_search === false ? (
                                        <div className="flex items-center gap-1 text-red-500">
                                            <XCircle className="w-4 h-4" />
                                            <span className="text-sm">Not supported</span>
                                        </div>
                                    ) : (
                                        <div className="flex items-center gap-1 text-amber-500">
                                            <HelpCircle className="w-4 h-4" />
                                            <span className="text-sm">Unknown (click Check)</span>
                                        </div>
                                    )}
                                </div>
                            </CardContent>
                        </Card>
                    )}

                    {selectedModel && webSearchOptions.length > 0 && (
                        <div className="space-y-2">
                            <Label>Web Search Tool</Label>
                            <Select value={selectedWebSearch || 'none'} onValueChange={onSelectWebSearch}>
                                <SelectTrigger>
                                    <SelectValue placeholder="Select web search option..." />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="none">None (no web search)</SelectItem>
                                    {webSearchOptions.map((option) => (
                                        <SelectItem key={option} value={option}>
                                            {option.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
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
                                <SelectTrigger className="opacity-50">
                                    <SelectValue placeholder="Not available for this model" />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="none">Not available</SelectItem>
                                </SelectContent>
                            </Select>
                            <p className="text-xs text-slate-500">
                                This model doesn't support web search tools
                            </p>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}