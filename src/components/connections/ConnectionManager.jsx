import React, { useState, useEffect } from 'react';
import { base44 } from '@/api/base44Client';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Plus, Trash2, RefreshCw, Check, X, Loader2, Server, Key, Globe } from 'lucide-react';
import { format } from 'date-fns';
import { toast } from 'sonner';

export default function ConnectionManager({ onSelect, selectedId }) {
    const [connections, setConnections] = useState([]);
    const [loading, setLoading] = useState(true);
    const [showCreate, setShowCreate] = useState(false);
    const [testing, setTesting] = useState(null);
    const [creating, setCreating] = useState(false);

    const [newConnection, setNewConnection] = useState({
        name: '',
        base_url: '',
        api_key: ''
    });

    useEffect(() => {
        loadConnections();
    }, []);

    const loadConnections = async () => {
        try {
            const response = await base44.functions.invoke('apiConnections', { action: 'list' });
            setConnections(response.data.connections || []);
        } catch (error) {
            toast.error('Failed to load connections');
        } finally {
            setLoading(false);
        }
    };

    const handleCreate = async () => {
        if (!newConnection.name || !newConnection.base_url || !newConnection.api_key) {
            toast.error('Please fill in all fields');
            return;
        }

        setCreating(true);
        try {
            // First test the connection
            const testResponse = await base44.functions.invoke('apiConnections', {
                action: 'test',
                base_url: newConnection.base_url,
                api_key: newConnection.api_key
            });

            if (!testResponse.data.success) {
                toast.error(`Connection test failed: ${testResponse.data.error}`);
                setCreating(false);
                return;
            }

            // Create the connection
            const response = await base44.functions.invoke('apiConnections', {
                action: 'create',
                ...newConnection
            });

            toast.success('Connection created successfully');
            setShowCreate(false);
            setNewConnection({ name: '', base_url: '', api_key: '' });
            loadConnections();
        } catch (error) {
            toast.error('Failed to create connection');
        } finally {
            setCreating(false);
        }
    };

    const handleTest = async (connectionId) => {
        setTesting(connectionId);
        try {
            const response = await base44.functions.invoke('apiConnections', {
                action: 'test',
                connection_id: connectionId
            });

            if (response.data.success) {
                toast.success('Connection is working');
                loadConnections();
            } else {
                toast.error(`Test failed: ${response.data.error}`);
            }
        } catch (error) {
            toast.error('Test failed');
        } finally {
            setTesting(null);
        }
    };

    const handleDelete = async (connectionId) => {
        if (!confirm('Are you sure you want to delete this connection?')) return;

        try {
            await base44.functions.invoke('apiConnections', {
                action: 'delete',
                connection_id: connectionId
            });
            toast.success('Connection deleted');
            loadConnections();
            if (selectedId === connectionId) {
                onSelect?.(null);
            }
        } catch (error) {
            toast.error('Failed to delete connection');
        }
    };

    if (loading) {
        return (
            <div className="flex items-center justify-center h-32">
                <Loader2 className="w-6 h-6 animate-spin text-slate-400" />
            </div>
        );
    }

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <div>
                    <h3 className="text-lg font-semibold text-slate-900">API Connections</h3>
                    <p className="text-sm text-slate-500">Connect to OpenAI-compatible APIs</p>
                </div>
                <Dialog open={showCreate} onOpenChange={setShowCreate}>
                    <DialogTrigger asChild>
                        <Button className="gap-2 bg-slate-900 hover:bg-slate-800">
                            <Plus className="w-4 h-4" />
                            Add Connection
                        </Button>
                    </DialogTrigger>
                    <DialogContent className="sm:max-w-md">
                        <DialogHeader>
                            <DialogTitle>New API Connection</DialogTitle>
                        </DialogHeader>
                        <div className="space-y-4 py-4">
                            <div className="space-y-2">
                                <Label htmlFor="name">Connection Name</Label>
                                <Input
                                    id="name"
                                    value={newConnection.name}
                                    onChange={(e) => setNewConnection({ ...newConnection, name: e.target.value })}
                                    placeholder="My OpenAI Connection"
                                />
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="base_url">API Base URL</Label>
                                <Input
                                    id="base_url"
                                    value={newConnection.base_url}
                                    onChange={(e) => setNewConnection({ ...newConnection, base_url: e.target.value })}
                                    placeholder="https://api.openai.com"
                                />
                                <p className="text-xs text-slate-500">
                                    Use https://api.openai.com for OpenAI, or your custom endpoint
                                </p>
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="api_key">API Key</Label>
                                <Input
                                    id="api_key"
                                    type="password"
                                    value={newConnection.api_key}
                                    onChange={(e) => setNewConnection({ ...newConnection, api_key: e.target.value })}
                                    placeholder="sk-..."
                                />
                            </div>
                            <Button
                                onClick={handleCreate}
                                disabled={creating}
                                className="w-full gap-2"
                            >
                                {creating ? (
                                    <Loader2 className="w-4 h-4 animate-spin" />
                                ) : (
                                    <Check className="w-4 h-4" />
                                )}
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
                        <p className="text-slate-500 text-center">
                            No API connections yet.<br />
                            Add one to get started.
                        </p>
                    </CardContent>
                </Card>
            ) : (
                <div className="grid gap-3">
                    {connections.map((conn) => (
                        <Card
                            key={conn.id}
                            className={`cursor-pointer transition-all ${
                                selectedId === conn.id
                                    ? 'ring-2 ring-slate-900 bg-slate-50'
                                    : 'hover:bg-slate-50'
                            }`}
                            onClick={() => onSelect?.(conn.id)}
                        >
                            <CardContent className="p-4">
                                <div className="flex items-center justify-between">
                                    <div className="flex items-center gap-3">
                                        <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${
                                            conn.is_valid ? 'bg-green-100' : 'bg-slate-100'
                                        }`}>
                                            <Globe className={`w-5 h-5 ${
                                                conn.is_valid ? 'text-green-600' : 'text-slate-400'
                                            }`} />
                                        </div>
                                        <div>
                                            <p className="font-medium text-slate-900">{conn.name}</p>
                                            <p className="text-sm text-slate-500">{conn.base_url}</p>
                                        </div>
                                    </div>
                                    <div className="flex items-center gap-2">
                                        {conn.is_valid ? (
                                            <Badge className="bg-green-100 text-green-800">Valid</Badge>
                                        ) : (
                                            <Badge variant="secondary">Untested</Badge>
                                        )}
                                        <Button
                                            variant="ghost"
                                            size="icon"
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                handleTest(conn.id);
                                            }}
                                            disabled={testing === conn.id}
                                        >
                                            {testing === conn.id ? (
                                                <Loader2 className="w-4 h-4 animate-spin" />
                                            ) : (
                                                <RefreshCw className="w-4 h-4" />
                                            )}
                                        </Button>
                                        <Button
                                            variant="ghost"
                                            size="icon"
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                handleDelete(conn.id);
                                            }}
                                            className="text-red-500 hover:text-red-600 hover:bg-red-50"
                                        >
                                            <Trash2 className="w-4 h-4" />
                                        </Button>
                                    </div>
                                </div>
                            </CardContent>
                        </Card>
                    ))}
                </div>
            )}
        </div>
    );
}