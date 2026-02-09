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
import { ArrowLeft, Server, Globe, Upload, Loader2, Search } from 'lucide-react';
import { toast } from 'sonner';

import ConnectionManager from '@/components/connections/ConnectionManager';

export default function Settings() {
    const [economyCodes, setEconomyCodes] = useState([]);
    const [loading, setLoading] = useState(true);
    const [importing, setImporting] = useState(false);
    const [searchTerm, setSearchTerm] = useState('');

    useEffect(() => {
        loadEconomyCodes();
    }, []);

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

    // Fix 12: Deterministic CSV import via backend — no AI extraction
    const handleImportCodes = async (e) => {
        const file = e.target.files?.[0];
        if (!file) return;

        setImporting(true);
        try {
            const { file_url } = await base44.integrations.Core.UploadFile({ file });
            
            const response = await base44.functions.invoke('economyCodes', {
                action: 'importFromCsv',
                file_url
            });

            toast.success(`Imported ${response.data.imported} economy codes`);
            loadEconomyCodes();
        } catch (error) {
            toast.error(error?.response?.data?.error || 'Failed to import economy codes');
        } finally {
            setImporting(false);
            // Reset file input
            e.target.value = '';
        }
    };

    const filteredCodes = economyCodes.filter(code =>
        code.economy.toLowerCase().includes(searchTerm.toLowerCase()) ||
        code.economy_code.toLowerCase().includes(searchTerm.toLowerCase())
    );

    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
            <div className="max-w-5xl mx-auto px-6 py-12">
                <Link to={createPageUrl('Dashboard')} className="inline-flex items-center text-slate-500 hover:text-slate-700 mb-8">
                    <ArrowLeft className="w-4 h-4 mr-2" />
                    Back to Dashboard
                </Link>

                <div className="mb-8">
                    <h1 className="text-3xl font-light text-slate-900 mb-2">Settings</h1>
                    <p className="text-slate-500">Manage API connections and configuration</p>
                </div>

                <Tabs defaultValue="connections" className="space-y-6">
                    <TabsList className="bg-slate-100">
                        <TabsTrigger value="connections" className="gap-2">
                            <Server className="w-4 h-4" />
                            API Connections
                        </TabsTrigger>
                        <TabsTrigger value="economy" className="gap-2">
                            <Globe className="w-4 h-4" />
                            Economy Codes
                        </TabsTrigger>
                    </TabsList>

                    <TabsContent value="connections">
                        <Card>
                            <CardContent className="pt-6">
                                <ConnectionManager />
                            </CardContent>
                        </Card>
                    </TabsContent>

                    <TabsContent value="economy">
                        <Card>
                            <CardHeader>
                                <div className="flex items-center justify-between">
                                    <div>
                                        <CardTitle>Economy Codes</CardTitle>
                                        <CardDescription>
                                            Mapping of economy names to codes for the extraction
                                        </CardDescription>
                                    </div>
                                    <div>
                                        <input
                                            type="file"
                                            accept=".csv"
                                            onChange={handleImportCodes}
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
                                                    Import CSV
                                                </span>
                                            </Button>
                                        </label>
                                    </div>
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
                                            Upload a CSV file with 'economy' and 'economy_code' columns
                                        </p>
                                    </div>
                                ) : (
                                    <div className="border rounded-lg overflow-hidden">
                                        <Table>
                                            <TableHeader>
                                                <TableRow>
                                                    <TableHead>Economy</TableHead>
                                                    <TableHead>Code</TableHead>
                                                </TableRow>
                                            </TableHeader>
                                            <TableBody>
                                                {filteredCodes.slice(0, 50).map((code) => (
                                                    <TableRow key={code.id}>
                                                        <TableCell className="font-medium">
                                                            {code.economy}
                                                        </TableCell>
                                                        <TableCell>
                                                            <Badge variant="outline">
                                                                {code.economy_code}
                                                            </Badge>
                                                        </TableCell>
                                                    </TableRow>
                                                ))}
                                            </TableBody>
                                        </Table>
                                        {filteredCodes.length > 50 && (
                                            <div className="p-4 text-center text-sm text-slate-500 border-t">
                                                Showing 50 of {filteredCodes.length} results
                                            </div>
                                        )}
                                    </div>
                                )}

                                <div className="mt-4 text-sm text-slate-500">
                                    Total: {economyCodes.length} economy codes
                                </div>
                            </CardContent>
                        </Card>
                    </TabsContent>
                </Tabs>
            </div>
        </div>
    );
}