/**
 * Format a date string to Washington DC timezone (America/New_York)
 * @param {string} dateStr - ISO date string
 * @param {string} fmt - 'short' for "MMM d, yyyy HH:mm" style
 * @returns {string} formatted date string in ET
 */
export function formatDCTime(dateStr, fmt = 'short') {
    if (!dateStr) return '';
    // Ensure the string is treated as UTC if it has no timezone offset
    const normalized = String(dateStr).replace(/(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?)$/, '$1Z');
    const date = new Date(normalized);
    
    if (fmt === 'short') {
        return date.toLocaleString('en-US', {
            timeZone: 'America/New_York',
            month: 'short',
            day: 'numeric',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            hour12: true,
        });
    }
    
    return date.toLocaleString('en-US', {
        timeZone: 'America/New_York',
    });
}