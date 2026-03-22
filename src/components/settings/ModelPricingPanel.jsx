import React, { useState, useEffect } from 'react';
import { base44 } from '@/api/base44Client';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Loader2, RefreshCw, Pencil, Check, X } from 'lucide-react';
import { toast } from 'sonner';

export default function ModelPricingPanel() {
    const [models, setModels] = useState([]);
    const [loading, setLoading] = useState(true);
    const [fetchingLive, setFetchingLive] = useState(false);
    const [editingId, setEditingId] = useState(null);
    const [editInput, setEditInput] = useState('');
    const [editOutput, setEditOutput] = useState('');

    const loadPricing = async () => {
        try {
            const resp = await base44.functions.invoke('apiConnections', {
                action: 'getModelPricing',
            });
            setModels(resp.data.models || []);
        } catch (e) {
            toast.error('Failed to load model pricing');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => { loadPricing(); }, []);

    const handleFetchLive = async () => {
        setFetchingLive(true);
        try {
            const resp = await base44.functions.invoke('apiConnections', {
                action: 'fetchPricing',
            });
            const r = resp.data;
            toast.success(`Updated ${r.updated} of ${r.total} models (${r.live_pricing_models} live prices found)`);
            await loadPricing();
        } catch (e) {
            toast.error('Failed to fetch pricing');
        } finally {
            setFetchingLive(false);
        }
    };

    const handleSavePrice = async (modelCatalogId) => {
        try {
            await base44.functions.invoke('apiConnections', {
                action: 'updateModelPrice',
                model_catalog_id: modelCatalogId,
                input_price: editInput,
                output_price: editOutput,
            });
            toast.success('Price updated');
            setEditingId(null);
            await loadPricing();
        } catch (e) {
            toast.error('Failed to update price');
        }
    };

    if (loading) {
        return <div className="flex justify-center py-8"><Loader2 className="w-6 h-6 animate-spin text-slate-400" /></div>;
    }

    return (
        <Card>
            <CardHeader>
                <div className="flex items-center justify-between">
                    <div>
                        <CardTitle className="text-lg">Model Pricing</CardTitle>
                        <p className="text-sm text-slate-500 mt-1">
                            Prices in USD per million tokens. Used to estimate task costs.
                        </p>
                    </div>
                    <Button onClick={handleFetchLive} disabled={fetchingLive} variant="outline" className="gap-2">
                        {fetchingLive ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                        Fetch Latest Prices
                    </Button>
                </div>
            </CardHeader>
            <CardContent>
                {models.length === 0 ? (
                    <p className="text-sm text-slate-400 py-4 text-center">No models found. Add an API connection and refresh models first.</p>
                ) : (
                    <div className="border rounded-lg overflow-hidden">
                        <Table>
                            <TableHeader>
                                <TableRow>
                                    <TableHead>Model</TableHead>
                                    <TableHead>Connection</TableHead>
                                    <TableHead className="text-right">Input $/M</TableHead>
                                    <TableHead className="text-right">Output $/M</TableHead>
                                    <TableHead className="text-center">Source</TableHead>
                                    <TableHead className="w-20">Actions</TableHead>
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {models.map(m => (
                                    <TableRow key={m.id}>
                                        <TableCell className="font-medium text-sm">{m.display_name || m.model_id}</TableCell>
                                        <TableCell className="text-sm text-slate-500">{m.connection_name}</TableCell>
                                        {editingId === m.id ? (
                                            <>
                                                <TableCell className="text-right">
                                                    <Input
                                                        type="number"
                                                        step="0.01"
                                                        value={editInput}
                                                        onChange={e => setEditInput(e.target.value)}
                                                        className="w-24 text-right ml-auto h-8"
                                                    />
                                                </TableCell>
                                                <TableCell className="text-right">
                                                    <Input
                                                        type="number"
                                                        step="0.01"
                                                        value={editOutput}
                                                        onChange={e => setEditOutput(e.target.value)}
                                                        className="w-24 text-right ml-auto h-8"
                                                    />
                                                </TableCell>
                                                <TableCell />
                                                <TableCell>
                                                    <div className="flex gap-1">
                                                        <Button size="icon" variant="ghost" className="h-7 w-7" onClick={() => handleSavePrice(m.id)}>
                                                            <Check className="w-3 h-3 text-green-600" />
                                                        </Button>
                                                        <Button size="icon" variant="ghost" className="h-7 w-7" onClick={() => setEditingId(null)}>
                                                            <X className="w-3 h-3" />
                                                        </Button>
                                                    </div>
                                                </TableCell>
                                            </>
                                        ) : (
                                            <>
                                                <TableCell className="text-right text-sm">
                                                    {m.input_price_per_million > 0 ? `$${m.input_price_per_million.toFixed(2)}` : <span className="text-slate-300">—</span>}
                                                </TableCell>
                                                <TableCell className="text-right text-sm">
                                                    {m.output_price_per_million > 0 ? `$${m.output_price_per_million.toFixed(2)}` : <span className="text-slate-300">—</span>}
                                                </TableCell>
                                                <TableCell className="text-center">
                                                    {m.pricing_source && (
                                                        <Badge variant="outline" className="text-xs">
                                                            {m.pricing_source}
                                                        </Badge>
                                                    )}
                                                </TableCell>
                                                <TableCell>
                                                    <Button
                                                        size="icon"
                                                        variant="ghost"
                                                        className="h-7 w-7"
                                                        onClick={() => {
                                                            setEditingId(m.id);
                                                            setEditInput(String(m.input_price_per_million || ''));
                                                            setEditOutput(String(m.output_price_per_million || ''));
                                                        }}
                                                    >
                                                        <Pencil className="w-3 h-3" />
                                                    </Button>
                                                </TableCell>
                                            </>
                                        )}
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </div>
                )}
            </CardContent>
        </Card>
    );
}