/**
 * Format a date string to Washington DC timezone (America/New_York)
 * @param {string} dateStr - ISO date string
 * @param {string} fmt - 'short' for "MMM d, yyyy HH:mm" style
 * @returns {string} formatted date string in ET
 */
export function formatDCTime(dateStr, fmt = 'short') {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    
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