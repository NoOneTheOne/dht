import { useState } from 'react';
import { TorrentItem } from '../types';

interface ResultsTableProps {
  items: TorrentItem[];
  total: number;
  query: string;
}

type SortField = 'name' | 'length' | 'time' | 'count';
type SortOrder = 'asc' | 'desc';

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

function SortIcon({ field, sortField, sortOrder }: { field: SortField; sortField: SortField; sortOrder: SortOrder }) {
  if (field !== sortField) {
    return (
      <svg className="w-3 h-3 ml-1 opacity-30" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16V4m0 0L3 8m4-4l4 4m6 0v12m0 0l4-4m-4 4l-4-4" />
      </svg>
    );
  }
  return sortOrder === 'asc' ? (
    <svg className="w-3 h-3 ml-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 15l7-7 7 7" />
    </svg>
  ) : (
    <svg className="w-3 h-3 ml-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
    </svg>
  );
}

export default function ResultsTable({ items, total, query }: ResultsTableProps) {
  const [sortField, setSortField] = useState<SortField>('count');
  const [sortOrder, setSortOrder] = useState<SortOrder>('desc');
  const [copiedHash, setCopiedHash] = useState<string | null>(null);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortOrder('desc');
    }
  };

  const sortedItems = [...items].sort((a, b) => {
    let comparison = 0;
    switch (sortField) {
      case 'name':
        comparison = a.name.localeCompare(b.name);
        break;
      case 'length':
        comparison = a.length - b.length;
        break;
      case 'time':
        comparison = new Date(a.time).getTime() - new Date(b.time).getTime();
        break;
      case 'count':
        comparison = a.count - b.count;
        break;
    }
    return sortOrder === 'asc' ? comparison : -comparison;
  });

  const copyToClipboard = async (text: string, hash: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopiedHash(hash);
      setTimeout(() => setCopiedHash(null), 2000);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  };

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
    <div className="w-full max-w-6xl mx-auto mt-8 animate-fade-in">
      <div className="mb-4 text-gray-300 text-sm">
        找到 <span className="text-purple-400 font-bold">{total}</span> 条结果
      </div>
      <div className="glass rounded-2xl overflow-hidden overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="border-b border-white/10 text-left text-gray-400 text-sm">
              <th 
                className="px-6 py-4 font-medium cursor-pointer hover:text-white transition-colors select-none"
                onClick={() => handleSort('name')}
              >
                <div className="flex items-center">
                  名称
                  <SortIcon field="name" sortField={sortField} sortOrder={sortOrder} />
                </div>
              </th>
              <th 
                className="px-6 py-4 font-medium cursor-pointer hover:text-white transition-colors select-none"
                onClick={() => handleSort('length')}
              >
                <div className="flex items-center">
                  大小
                  <SortIcon field="length" sortField={sortField} sortOrder={sortOrder} />
                </div>
              </th>
              <th 
                className="px-6 py-4 font-medium cursor-pointer hover:text-white transition-colors select-none"
                onClick={() => handleSort('time')}
              >
                <div className="flex items-center">
                  时间
                  <SortIcon field="time" sortField={sortField} sortOrder={sortOrder} />
                </div>
              </th>
              <th 
                className="px-6 py-4 font-medium cursor-pointer hover:text-white transition-colors select-none"
                onClick={() => handleSort('count')}
              >
                <div className="flex items-center">
                  热度
                  <SortIcon field="count" sortField={sortField} sortOrder={sortOrder} />
                </div>
              </th>
              <th className="px-6 py-4 font-medium">InfoHash</th>
            </tr>
          </thead>
          <tbody>
            {sortedItems.map((item, index) => (
              <tr
                key={item.infohash}
                className="result-row border-b border-white/5 animate-slide-up"
                style={{ animationDelay: `${index * 30}ms` }}
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
                  <div className="flex items-center gap-2">
                    <span className="hash-text text-xs text-purple-400 font-mono" title={item.infohash}>
                      {item.infohash}
                    </span>
                    <button
                      onClick={() => copyToClipboard(item.infohash, item.infohash)}
                      className="p-1 rounded hover:bg-white/10 transition-colors"
                      title="复制 InfoHash"
                    >
                      {copiedHash === item.infohash ? (
                        <svg className="w-4 h-4 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                        </svg>
                      ) : (
                        <svg className="w-4 h-4 text-gray-400 hover:text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                        </svg>
                      )}
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
