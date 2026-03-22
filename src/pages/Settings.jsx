import React, { useState, useEffect } from 'react';
import { base44 } from '@/api/base44Client';
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft, Server, Globe, Upload, Loader2, Search, ChevronLeft, ChevronRight, Pencil, Check, X, Trash2, DollarSign } from 'lucide-react';
import { toast } from 'sonner';

import ConnectionManager from '@/components/connections/ConnectionManager';
import ModelPricingPanel from '@/components/settings/ModelPricingPanel';

const PAGE_SIZE = 30;

export default function Settings() {
    const [user, setUser] = useState(null);
    const [economyCodes, setEconomyCodes] = useState([]);
    const [loading, setLoading] = useState(true);
    const [importing, setImporting] = useState(false);
    const [searchTerm, setSearchTerm] = useState('');
    const [page, setPage] = useState(0);
    const [editingId, setEditingId] = useState(null);
    const [editEconomy, setEditEconomy] = useState('');
    const [editCode, setEditCode] = useState('');

    useEffect(() => {
        base44.auth.me().then(u => setUser(u)).catch(() => {});
        loadEconomyCodes();
    }, []);

    const isAdmin = user?.role === 'admin';

    const loadEconomyCodes = async () => {
        try {
            const response = await base44.functions.invoke('economyCodes', { action: 'list' });
            setEconomyCodes(response.data.codes || []);
        } catch (error) {
            console.error('Failed to load economy codes:', error);
        } finally {
            setLoading(false);
        }
    };

    const handleImportFile = async (e) => {
        const file = e.target.files?.[0];
        if (!file) return;

        setImporting(true);
        try {
            const name = file.name.toLowerCase();

            if (name.endsWith('.xlsx') || name.endsWith('.xls')) {
                const { file_url } = await base44.integrations.Core.UploadFile({ file });
                const response = await base44.functions.invoke('economyCodes', {
                    action: 'importFromFile',
                    file_url,
                    file_name: file.name
                });
                toast.success(`Imported ${response.data.imported} new, updated ${response.data.updated}, skipped ${response.data.skipped} unchanged`);
            } else {
                const text = await file.text();
                const lines = text.split(/\r?\n/).filter(line => line.trim().length > 0);
                if (lines.length < 2) {
                    toast.error('File is empty or has no data rows');
                    return;
                }

                const delimiter = lines[0].includes(';') && !lines[0].includes(',') ? ';' : ',';
                const headers = lines[0].split(delimiter).map(h => h.trim().replace(/^["']|["']$/g, '').toLowerCase());
                const economyIdx = headers.findIndex(h => ['economy', 'economy_name', 'name', 'country', 'country_name'].includes(h));
                const codeIdx = headers.findIndex(h => ['economy_code', 'code', 'iso_code', 'iso3', 'country_code'].includes(h));

                if (economyIdx === -1 || codeIdx === -1) {
                    toast.error('File must have "economy" and "economy_code" columns');
                    return;
                }

                const data = [];
                for (let i = 1; i < lines.length; i++) {
                    const values = lines[i].split(delimiter).map(v => v.trim().replace(/^["']|["']$/g, ''));
                    if (values[economyIdx] && values[codeIdx]) {
                        data.push({ economy: values[economyIdx], economy_code: values[codeIdx] });
                    }
                }

                const response = await base44.functions.invoke('economyCodes', {
                    action: 'import',
                    data
                });
                toast.success(`Imported ${response.data.imported} new, updated ${response.data.updated}, skipped ${response.data.skipped} unchanged`);
            }
            loadEconomyCodes();
        } catch (error) {
            toast.error(error?.response?.data?.error || 'Failed to import');
        } finally {
            setImporting(false);
            e.target.value = '';
        }
    };

    const startEditing = (code) => {
        setEditingId(code.id);
        setEditEconomy(code.economy);
        setEditCode(code.economy_code);
    };

    const cancelEditing = () => {
        setEditingId(null);
        setEditEconomy('');
        setEditCode('');
    };

    const saveEdit = async () => {
        if (!editEconomy.trim() || !editCode.trim()) {
            toast.error('Both fields are required');
            return;
        }
        try {
            await base44.functions.invoke('economyCodes', {
                action: 'update',
                id: editingId,
                economy: editEconomy,
                economy_code: editCode
            });
            toast.success('Updated');
            cancelEditing();
            loadEconomyCodes();
        } catch (error) {
            toast.error('Failed to update');
        }
    };

    const handleDelete = async (id) => {
        if (!confirm('Delete this economy code?')) return;
        try {
            await base44.functions.invoke('economyCodes', { action: 'delete', id });
            toast.success('Deleted');
            loadEconomyCodes();
        } catch (error) {
            toast.error('Failed to delete');
        }
    };

    const filteredCodes = economyCodes.filter(code =>
        code.economy.toLowerCase().includes(searchTerm.toLowerCase()) ||
        code.economy_code.toLowerCase().includes(searchTerm.toLowerCase())
    );

    const totalPages = Math.max(1, Math.ceil(filteredCodes.length / PAGE_SIZE));
    const currentPage = Math.min(page, totalPages - 1);
    const pagedCodes = filteredCodes.slice(currentPage * PAGE_SIZE, (currentPage + 1) * PAGE_SIZE);

    useEffect(() => { setPage(0); }, [searchTerm]);

    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
            <div className="max-w-5xl mx-auto px-6 py-12">
                <Link to={createPageUrl('Dashboard')} className="inline-flex items-center text-slate-500 hover:text-slate-700 mb-8">
                    <ArrowLeft className="w-4 h-4 mr-2" />
                    Back to Dashboard
                </Link>

                <div className="mb-8">
                    <h1 className="text-3xl font-light text-slate-900 mb-2">Settings</h1>
                    <p className="text-slate-500">
                        {isAdmin ? 'Manage API connections and configuration' : 'View and manage economy codes'}
                    </p>
                </div>

                <Tabs defaultValue={isAdmin ? "connections" : "economy"} className="space-y-6">
                    <TabsList className="bg-slate-100">
                        {isAdmin && (
                            <TabsTrigger value="connections" className="gap-2">
                                <Server className="w-4 h-4" />
                                API Connections
                            </TabsTrigger>
                        )}
                        <TabsTrigger value="economy" className="gap-2">
                            <Globe className="w-4 h-4" />
                            Economy Codes
                        </TabsTrigger>
                        {isAdmin && (
                            <TabsTrigger value="pricing" className="gap-2">
                                <DollarSign className="w-4 h-4" />
                                Model Pricing
                            </TabsTrigger>
                        )}
                    </TabsList>

                    {isAdmin && (
                        <TabsContent value="connections">
                            <Card>
                                <CardContent className="pt-6">
                                    <ConnectionManager />
                                </CardContent>
                            </Card>
                        </TabsContent>
                    )}

                    <TabsContent value="economy">
                        <Card>
                            <CardHeader>
                                <div className="flex items-center justify-between">
                                    <div>
                                        <CardTitle>Economy Codes</CardTitle>
                                        <CardDescription>
                                            {economyCodes.length} economy codes loaded. Upload CSV or Excel to add more.
                                        </CardDescription>
                                    </div>
                                    {isAdmin && (
                                        <div>
                                            <input
                                                type="file"
                                                accept=".csv,.xlsx,.xls"
                                                onChange={handleImportFile}
                                                className="hidden"
                                                id="economy-upload"
                                            />
                                            <label htmlFor="economy-upload">
                                                <Button
                                                    variant="outline"
                                                    className="gap-2 cursor-pointer"
                                                    disabled={importing}
                                                    asChild
                                                >
                                                    <span>
                                                        {importing ? (
                                                            <Loader2 className="w-4 h-4 animate-spin" />
                                                        ) : (
                                                            <Upload className="w-4 h-4" />
                                                        )}
                                                        Import CSV / Excel
                                                    </span>
                                                </Button>
                                            </label>
                                        </div>
                                    )}
                                </div>
                            </CardHeader>
                            <CardContent>
                                <div className="mb-4">
                                    <div className="relative">
                                        <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
                                        <Input
                                            value={searchTerm}
                                            onChange={(e) => setSearchTerm(e.target.value)}
                                            placeholder="Search economies..."
                                            className="pl-10"
                                        />
                                    </div>
                                </div>

                                {loading ? (
                                    <div className="flex items-center justify-center py-8">
                                        <Loader2 className="w-6 h-6 animate-spin text-slate-400" />
                                    </div>
                                ) : economyCodes.length === 0 ? (
                                    <div className="text-center py-12">
                                        <Globe className="w-12 h-12 text-slate-300 mx-auto mb-4" />
                                        <p className="text-slate-500 mb-4">No economy codes loaded</p>
                                        <p className="text-sm text-slate-400 mb-4">
                                            Upload a CSV or Excel file with 'economy' and 'economy_code' columns
                                        </p>
                                    </div>
                                ) : (
                                    <>
                                        <div className="border rounded-lg overflow-hidden">
                                            <Table>
                                                <TableHeader>
                                                    <TableRow>
                                                        <TableHead>Economy</TableHead>
                                                        <TableHead>Code</TableHead>
                                                        {isAdmin && <TableHead className="w-24">Actions</TableHead>}
                                                    </TableRow>
                                                </TableHeader>
                                                <TableBody>
                                                    {pagedCodes.map((code) => (
                                                        <TableRow key={code.id}>
                                                            {editingId === code.id ? (
                                                                <>
                                                                    <TableCell>
                                                                        <Input
                                                                            value={editEconomy}
                                                                            onChange={(e) => setEditEconomy(e.target.value)}
                                                                            className="h-8"
                                                                        />
                                                                    </TableCell>
                                                                    <TableCell>
                                                                        <Input
                                                                            value={editCode}
                                                                            onChange={(e) => setEditCode(e.target.value)}
                                                                            className="h-8 w-24"
                                                                        />
                                                                    </TableCell>
                                                                    <TableCell>
                                                                        <div className="flex gap-1">
                                                                            <Button variant="ghost" size="icon" className="h-7 w-7" onClick={saveEdit}>
                                                                                <Check className="w-3 h-3 text-green-600" />
                                                                            </Button>
                                                                            <Button variant="ghost" size="icon" className="h-7 w-7" onClick={cancelEditing}>
                                                                                <X className="w-3 h-3 text-slate-400" />
                                                                            </Button>
                                                                        </div>
                                                                    </TableCell>
                                                                </>
                                                            ) : (
                                                                <>
                                                                    <TableCell className="font-medium capitalize">
                                                                        {code.economy}
                                                                    </TableCell>
                                                                    <TableCell>
                                                                        <Badge variant="outline">
                                                                            {code.economy_code}
                                                                        </Badge>
                                                                    </TableCell>
                                                                    {isAdmin && (
                                                                        <TableCell>
                                                                            <div className="flex gap-1">
                                                                                <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => startEditing(code)}>
                                                                                    <Pencil className="w-3 h-3" />
                                                                                </Button>
                                                                                <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => handleDelete(code.id)}>
                                                                                    <Trash2 className="w-3 h-3 text-red-400" />
                                                                                </Button>
                                                                            </div>
                                                                        </TableCell>
                                                                    )}
                                                                </>
                                                            )}
                                                        </TableRow>
                                                    ))}
                                                </TableBody>
                                            </Table>
                                        </div>

                                        {/* Pagination */}
                                        <div className="flex items-center justify-between mt-4">
                                            <div className="text-sm text-slate-500">
                                                Showing {currentPage * PAGE_SIZE + 1}–{Math.min((currentPage + 1) * PAGE_SIZE, filteredCodes.length)} of {filteredCodes.length}
                                            </div>
                                            <div className="flex items-center gap-2">
                                                <Button
                                                    variant="outline"
                                                    size="sm"
                                                    onClick={() => setPage(p => Math.max(0, p - 1))}
                                                    disabled={currentPage === 0}
                                                >
                                                    <ChevronLeft className="w-4 h-4 mr-1" />
                                                    Previous
                                                </Button>
                                                <span className="text-sm text-slate-500">
                                                    Page {currentPage + 1} of {totalPages}
                                                </span>
                                                <Button
                                                    variant="outline"
                                                    size="sm"
                                                    onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
                                                    disabled={currentPage >= totalPages - 1}
                                                >
                                                    Next
                                                    <ChevronRight className="w-4 h-4 ml-1" />
                                                </Button>
                                            </div>
                                        </div>
                                    </>
                                )}
                            </CardContent>
                        </Card>
                    </TabsContent>
                    {isAdmin && (
                        <TabsContent value="pricing">
                            <ModelPricingPanel />
                        </TabsContent>
                    )}
                </Tabs>
            </div>
        </div>
    );
}