import React, { useState, useCallback } from 'react';
import { base44 } from '@/api/base44Client';
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Upload, FileSpreadsheet, Check, AlertCircle, Loader2, X } from 'lucide-react';
import { toast } from 'sonner';

export default function FileUploader({ onFileProcessed, parsedData }) {
    const [uploading, setUploading] = useState(false);
    const [parsing, setParsing] = useState(false);
    const [dragActive, setDragActive] = useState(false);

    const processFile = async (file) => {
        if (!file) return;

        // Validate file type
        const validTypes = [
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'application/vnd.ms-excel',
            'application/octet-stream'
        ];
        const isExcel = validTypes.includes(file.type) || 
            file.name.endsWith('.xlsx') || 
            file.name.endsWith('.xls');

        if (!isExcel) {
            toast.error('Please upload an Excel file (.xlsx or .xls)');
            return;
        }

        setUploading(true);
        try {
            // Upload file
            const { file_url } = await base44.integrations.Core.UploadFile({ file });

            setParsing(true);
            setUploading(false);

            // Parse the file
            const response = await base44.functions.invoke('parseInputFile', {
                file_url
            });

            if (response.data.error) {
                toast.error(response.data.error);
                onFileProcessed(null);
                return;
            }

            toast.success(`File parsed: ${response.data.total_rows} rows found`);
            onFileProcessed({
                file_url,
                file_name: file.name,
                rows: response.data.rows,
                total_rows: response.data.total_rows,
                columns: response.data.columns
            });

        } catch (error) {
            toast.error('Failed to process file');
            onFileProcessed(null);
        } finally {
            setUploading(false);
            setParsing(false);
        }
    };

    const handleDrag = useCallback((e) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.type === 'dragenter' || e.type === 'dragover') {
            setDragActive(true);
        } else if (e.type === 'dragleave') {
            setDragActive(false);
        }
    }, []);

    const handleDrop = useCallback((e) => {
        e.preventDefault();
        e.stopPropagation();
        setDragActive(false);

        if (e.dataTransfer.files && e.dataTransfer.files[0]) {
            processFile(e.dataTransfer.files[0]);
        }
    }, []);

    const handleFileInput = (e) => {
        if (e.target.files && e.target.files[0]) {
            processFile(e.target.files[0]);
        }
    };

    const clearFile = () => {
        onFileProcessed(null);
    };

    if (parsedData) {
        return (
            <Card className="bg-green-50 border-green-200">
                <CardContent className="p-4">
                    <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                            <div className="w-10 h-10 rounded-lg bg-green-100 flex items-center justify-center">
                                <FileSpreadsheet className="w-5 h-5 text-green-600" />
                            </div>
                            <div>
                                <p className="font-medium text-slate-900">{parsedData.file_name}</p>
                                <p className="text-sm text-slate-500">
                                    {parsedData.total_rows} rows • {parsedData.columns?.length || 0} columns
                                </p>
                            </div>
                        </div>
                        <div className="flex items-center gap-2">
                            <Badge className="bg-green-100 text-green-800">
                                <Check className="w-3 h-3 mr-1" />
                                Ready
                            </Badge>
                            <Button
                                variant="ghost"
                                size="icon"
                                onClick={clearFile}
                                className="text-slate-400 hover:text-slate-600"
                            >
                                <X className="w-4 h-4" />
                            </Button>
                        </div>
                    </div>
                    
                    {/* Preview columns */}
                    <div className="mt-3 pt-3 border-t border-green-200">
                        <p className="text-xs text-slate-500 mb-2">Detected columns:</p>
                        <div className="flex flex-wrap gap-1">
                            {parsedData.columns?.map((col, i) => (
                                <Badge key={i} variant="outline" className="text-xs">
                                    {col}
                                </Badge>
                            ))}
                        </div>
                    </div>
                </CardContent>
            </Card>
        );
    }

    return (
        <Card
            className={`border-2 border-dashed transition-colors ${
                dragActive ? 'border-slate-400 bg-slate-50' : 'border-slate-200'
            }`}
            onDragEnter={handleDrag}
            onDragLeave={handleDrag}
            onDragOver={handleDrag}
            onDrop={handleDrop}
        >
            <CardContent className="flex flex-col items-center justify-center py-12">
                {uploading || parsing ? (
                    <>
                        <Loader2 className="w-10 h-10 text-slate-400 animate-spin mb-4" />
                        <p className="text-slate-500">
                            {uploading ? 'Uploading...' : 'Parsing file...'}
                        </p>
                    </>
                ) : (
                    <>
                        <Upload className="w-10 h-10 text-slate-300 mb-4" />
                        <p className="text-slate-900 font-medium mb-1">
                            Drop your Excel file here
                        </p>
                        <p className="text-sm text-slate-500 mb-4">
                            or click to browse
                        </p>
                        <input
                            type="file"
                            accept=".xlsx,.xls"
                            onChange={handleFileInput}
                            className="hidden"
                            id="file-upload"
                        />
                        <label htmlFor="file-upload">
                            <Button
                                variant="outline"
                                className="cursor-pointer"
                                asChild
                            >
                                <span>Select File</span>
                            </Button>
                        </label>
                        <p className="text-xs text-slate-400 mt-4">
                            Required columns: Owner, Economy, Legal basis, Question, Topic
                        </p>
                    </>
                )}
            </CardContent>
        </Card>
    );
}