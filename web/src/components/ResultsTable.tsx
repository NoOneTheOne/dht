import { TorrentItem } from '../types';

interface ResultsTableProps {
  items: TorrentItem[];
  total: number;
  query: string;
}

function formatSize(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatTime(timeStr: string): string {
  const date = new Date(timeStr);
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function truncateHash(hash: string): string {
  return hash.slice(0, 8) + '...' + hash.slice(-8);
}

function copyToClipboard(text: string) {
  navigator.clipboard.writeText(text);
}

export default function ResultsTable({ items, total, query }: ResultsTableProps) {
  if (!query) {
    return (
      <div className="text-center text-gray-400 mt-20 animate-fade-in">
        <svg xmlns="http://www.w3.org/2000/svg" className="h-16 w-16 mx-auto mb-4 opacity-30" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
        </svg>
        <p className="text-xl">输入关键词开始搜索</p>
      </div>
    );
  }

  if (items.length === 0) {
    return (
      <div className="text-center text-gray-400 mt-20 animate-fade-in">
        <p className="text-xl">未找到相关结果</p>
        <p className="mt-2 text-sm">试试其他关键词？</p>
      </div>
    );
  }

  return (
    <div className="w-full max-w-5xl mx-auto mt-8 animate-fade-in">
      <div className="mb-4 text-gray-300 text-sm">
        找到 <span className="text-purple-400 font-bold">{total}</span> 条结果
      </div>
      <div className="glass rounded-2xl overflow-hidden">
        <table className="w-full">
          <thead>
            <tr className="border-b border-white/10 text-left text-gray-400 text-sm">
              <th className="px-6 py-4 font-medium">名称</th>
              <th className="px-6 py-4 font-medium">大小</th>
              <th className="px-6 py-4 font-medium">时间</th>
              <th className="px-6 py-4 font-medium">热度</th>
              <th className="px-6 py-4 font-medium">InfoHash</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item, index) => (
              <tr
                key={item.infohash}
                className="result-row border-b border-white/5 animate-slide-up"
                style={{ animationDelay: `${index * 50}ms` }}
              >
                <td className="px-6 py-4 text-white max-w-md">
                  <div className="truncate font-medium" title={item.name}>
                    {item.name}
                  </div>
                </td>
                <td className="px-6 py-4 text-gray-300 whitespace-nowrap">
                  {formatSize(item.length)}
                </td>
                <td className="px-6 py-4 text-gray-400 whitespace-nowrap">
                  {formatTime(item.time)}
                </td>
                <td className="px-6 py-4">
                  <span className="inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium bg-gradient-to-r from-orange-500/20 to-red-500/20 text-orange-300 border border-orange-500/30">
                    <svg xmlns="http://www.w3.org/2000/svg" className="h-3 w-3" viewBox="0 0 20 20" fill="currentColor">
                      <path fillRule="evenodd" d="M12.395 2.553a1 1 0 00-1.45-.385c-.345.23-.614.558-.822.88-.214.33-.403.713-.57 1.116-.334.804-.614 1.768-.84 2.734a31.365 31.365 0 00-.613 3.58 2.64 2.64 0 01-.945-1.067c-.328-.68-.398-1.534-.398-2.654A1 1 0 005.05 6.05 6.981 6.981 0 003 11a7 7 0 1011.95-4.95c-.592-.591-.98-.985-1.348-1.467-.363-.476-.724-1.063-1.207-2.03zM12.12 15.12A3 3 0 017 13s.879.5 2.5.5c0-1 .5-4 1.25-4.5.5 1 .786 1.293 1.371 1.879A2.99 2.99 0 0113 13a2.99 2.99 0 01-.879 2.121z" clipRule="evenodd" />
                    </svg>
                    {item.count}
                  </span>
                </td>
                <td className="px-6 py-4">
                  <button
                    onClick={() => copyToClipboard(item.infohash)}
                    className="hash-text text-xs text-purple-400 hover:text-purple-300 transition-colors cursor-pointer"
                    title={item.infohash}
                  >
                    {truncateHash(item.infohash)}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
