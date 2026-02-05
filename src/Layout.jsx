import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import { 
    LayoutDashboard, PlayCircle, FileText, Settings, History, 
    Menu, X, Scale
} from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Toaster } from "@/components/ui/sonner";

export default function Layout({ children }) {
    const [mobileMenuOpen, setMobileMenuOpen] = React.useState(false);
    const location = useLocation();

    const navigation = [
        { name: 'Dashboard', href: 'Dashboard', icon: LayoutDashboard },
        { name: 'New Run', href: 'NewRun', icon: PlayCircle },
        { name: 'Specification', href: 'SpecEditor', icon: FileText },
        { name: 'History', href: 'History', icon: History },
        { name: 'Settings', href: 'Settings', icon: Settings },
    ];

    const isActive = (href) => {
        const url = createPageUrl(href);
        return location.pathname === url || location.pathname === url + '/';
    };

    return (
        <div className="min-h-screen bg-slate-50">
            {/* Desktop Sidebar */}
            <aside className="hidden lg:fixed lg:inset-y-0 lg:z-50 lg:flex lg:w-64 lg:flex-col">
                <div className="flex grow flex-col gap-y-5 overflow-y-auto bg-white border-r border-slate-200 px-6 pb-4">
                    {/* Logo */}
                    <div className="flex h-16 shrink-0 items-center gap-3">
                        <div className="w-9 h-9 rounded-lg bg-slate-900 flex items-center justify-center">
                            <Scale className="w-5 h-5 text-white" />
                        </div>
                        <div>
                            <span className="font-semibold text-slate-900">Legal Extractor</span>
                            <p className="text-xs text-slate-500">Metadata Tool</p>
                        </div>
                    </div>

                    {/* Navigation */}
                    <nav className="flex flex-1 flex-col">
                        <ul className="flex flex-1 flex-col gap-y-1">
                            {navigation.map((item) => (
                                <li key={item.name}>
                                    <Link
                                        to={createPageUrl(item.href)}
                                        className={`
                                            group flex gap-x-3 rounded-lg p-3 text-sm font-medium transition-all
                                            ${isActive(item.href)
                                                ? 'bg-slate-100 text-slate-900'
                                                : 'text-slate-600 hover:bg-slate-50 hover:text-slate-900'
                                            }
                                        `}
                                    >
                                        <item.icon className={`h-5 w-5 shrink-0 ${
                                            isActive(item.href) ? 'text-slate-900' : 'text-slate-400 group-hover:text-slate-600'
                                        }`} />
                                        {item.name}
                                    </Link>
                                </li>
                            ))}
                        </ul>
                    </nav>

                    {/* Footer */}
                    <div className="border-t border-slate-200 pt-4">
                        <p className="text-xs text-slate-400 text-center">
                            Base44 Legal Metadata Extractor
                        </p>
                    </div>
                </div>
            </aside>

            {/* Mobile Header */}
            <div className="sticky top-0 z-40 flex items-center gap-x-6 bg-white px-4 py-4 shadow-sm lg:hidden">
                <button
                    type="button"
                    className="-m-2.5 p-2.5 text-slate-700"
                    onClick={() => setMobileMenuOpen(true)}
                >
                    <Menu className="h-6 w-6" />
                </button>
                <div className="flex items-center gap-2">
                    <div className="w-8 h-8 rounded-lg bg-slate-900 flex items-center justify-center">
                        <Scale className="w-4 h-4 text-white" />
                    </div>
                    <span className="font-semibold text-slate-900">Legal Extractor</span>
                </div>
            </div>

            {/* Mobile Menu */}
            {mobileMenuOpen && (
                <div className="relative z-50 lg:hidden">
                    <div className="fixed inset-0 bg-slate-900/80" onClick={() => setMobileMenuOpen(false)} />
                    <div className="fixed inset-y-0 left-0 z-50 w-full max-w-xs overflow-y-auto bg-white px-6 py-4">
                        <div className="flex items-center justify-between mb-6">
                            <div className="flex items-center gap-2">
                                <div className="w-8 h-8 rounded-lg bg-slate-900 flex items-center justify-center">
                                    <Scale className="w-4 h-4 text-white" />
                                </div>
                                <span className="font-semibold text-slate-900">Legal Extractor</span>
                            </div>
                            <button
                                type="button"
                                className="-m-2.5 p-2.5 text-slate-700"
                                onClick={() => setMobileMenuOpen(false)}
                            >
                                <X className="h-6 w-6" />
                            </button>
                        </div>
                        <nav className="flex flex-col gap-y-1">
                            {navigation.map((item) => (
                                <Link
                                    key={item.name}
                                    to={createPageUrl(item.href)}
                                    onClick={() => setMobileMenuOpen(false)}
                                    className={`
                                        flex gap-x-3 rounded-lg p-3 text-sm font-medium transition-all
                                        ${isActive(item.href)
                                            ? 'bg-slate-100 text-slate-900'
                                            : 'text-slate-600 hover:bg-slate-50'
                                        }
                                    `}
                                >
                                    <item.icon className={`h-5 w-5 shrink-0 ${
                                        isActive(item.href) ? 'text-slate-900' : 'text-slate-400'
                                    }`} />
                                    {item.name}
                                </Link>
                            ))}
                        </nav>
                    </div>
                </div>
            )}

            {/* Main Content */}
            <main className="lg:pl-64">
                {children}
            </main>

            <Toaster position="bottom-right" />
        </div>
    );
}