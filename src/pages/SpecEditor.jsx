import React from 'react';
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import { ArrowLeft } from 'lucide-react';
import SpecEditor from '@/components/spec/SpecEditor';

export default function SpecEditorPage() {
    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
            <div className="max-w-5xl mx-auto px-6 py-12">
                <Link to={createPageUrl('Dashboard')} className="inline-flex items-center text-slate-500 hover:text-slate-700 mb-8">
                    <ArrowLeft className="w-4 h-4 mr-2" />
                    Back to Dashboard
                </Link>

                <SpecEditor />
            </div>
        </div>
    );
}