import { useEffect, useMemo, useRef, useState } from 'react';
import { TorrentItem } from '../types';

interface ResultsTableProps {
  items: TorrentItem[];
  total: number;
  query: string;
  loading: boolean;
}

type SortField = 'name' | 'length' | 'time' | 'count';
type SortOrder = 'asc' | 'desc';
const dateFormatter = new Intl.DateTimeFormat('zh-CN', { year: 'numeric', month: '2-digit', day: '2-digit' });
const numberFormatter = new Intl.NumberFormat('zh-CN');
const nameCollator = new Intl.Collator('zh-CN', { numeric: true });
const fields: { key: SortField; label: string }[] = [
  { key: 'name', label: '资源名称' }, { key: 'length', label: '文件大小' },
  { key: 'time', label: '收录时间' }, { key: 'count', label: '采集次数' },
];

function formatSize(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB'];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  return `${Number((bytes / 1024 ** index).toFixed(2))} ${units[index]}`;
}

export default function ResultsTable({ items, total, query, loading }: ResultsTableProps) {
  const [sortField, setSortField] = useState<SortField>('time');
  const [sortOrder, setSortOrder] = useState<SortOrder>('desc');
  const [notice, setNotice] = useState('');
  const noticeTimer = useRef<ReturnType<typeof setTimeout>>();
  useEffect(() => () => clearTimeout(noticeTimer.current), []);
  useEffect(() => { setNotice(''); clearTimeout(noticeTimer.current); }, [query]);

  const preparedItems = useMemo(() => items.map(item => {
    const timestamp = Date.parse(item.time);
    return { ...item, timestamp: Number.isFinite(timestamp) ? timestamp : 0,
      formattedTime: Number.isFinite(timestamp) ? dateFormatter.format(timestamp) : '时间未知',
      formattedSize: formatSize(item.length),
      magnet: `magnet:?xt=urn:btih:${item.infohash}&dn=${encodeURIComponent(item.name)}` };
  }), [items]);
  const sortedItems = useMemo(() => [...preparedItems].sort((a, b) => {
    const comparison = sortField === 'name' ? nameCollator.compare(a.name, b.name) :
      sortField === 'time' ? a.timestamp - b.timestamp : a[sortField] - b[sortField];
    return (sortOrder === 'asc' ? comparison : -comparison) || a.infohash.localeCompare(b.infohash);
  }), [preparedItems, sortField, sortOrder]);

  const handleSort = (field: SortField) => {
    if (sortField === field) setSortOrder(order => order === 'asc' ? 'desc' : 'asc');
    else { setSortField(field); setSortOrder(field === 'name' ? 'asc' : 'desc'); }
  };

  const copyText = async (text: string, label: string) => {
    clearTimeout(noticeTimer.current);
    try {
      if (!navigator.clipboard) throw new Error('Clipboard unavailable');
      await navigator.clipboard.writeText(text);
      setNotice(`${label}已复制`);
    } catch {
      setNotice('复制失败，请使用 HTTPS 或 localhost 访问，也可手动选择 InfoHash 复制。');
    }
    noticeTimer.current = setTimeout(() => setNotice(''), 4000);
  };

  if (loading) return (
    <section className="state-panel" aria-busy="true" role="status"><span className="loading-ring" aria-hidden="true" /><h2>正在检索资源</h2><p>正在搜索「{query}」，请稍候…</p></section>
  );
  if (!query) return (
    <section className="state-panel welcome-panel"><span className="state-symbol" aria-hidden="true">⌕</span><h2>让发现从这里开始</h2><p>输入资源名称，快速查看大小与收录信息。<br />找到结果后，可复制磁力链接到支持的客户端。</p><span className="state-caption">KEYWORD → METADATA → MAGNET</span></section>
  );
  if (items.length === 0) return (
    <section className="state-panel" role="status"><span className="state-symbol" aria-hidden="true">⌕</span><h2>没有找到「{query}」</h2><p>尝试更短的关键词、其他拼写，或去掉不必要的符号。</p></section>
  );

  return (
    <section className="results-section" aria-labelledby="results-title">
      <div className="results-heading"><div><span className="eyebrow">SEARCH RESULTS</span><h2 id="results-title">「{query}」<span className="result-count">{numberFormatter.format(total)} 条匹配</span></h2></div><span className="results-limit">展示 {items.length} 条 · 排序仅作用于当前结果</span></div>
      <div className="mobile-sort"><label htmlFor="result-sort">排序方式</label><select id="result-sort" value={sortField} onChange={event => { setSortField(event.target.value as SortField); setSortOrder('desc'); }}>{fields.map(field => <option key={field.key} value={field.key}>{field.label}</option>)}</select><button className="text-button" onClick={() => setSortOrder(order => order === 'asc' ? 'desc' : 'asc')}>{sortOrder === 'asc' ? '升序 ↑' : '降序 ↓'}</button></div>
      <div className="results-surface"><table className="results-table"><caption className="sr-only">{query} 的搜索结果，最多展示 50 条</caption><thead><tr>{fields.map(field => <th key={field.key} scope="col" aria-sort={sortField === field.key ? sortOrder === 'asc' ? 'ascending' : 'descending' : 'none'}><button onClick={() => handleSort(field.key)}>{field.label}<span aria-hidden="true" className={sortField === field.key ? 'sort-active' : 'sort-idle'}>{sortField === field.key && sortOrder === 'asc' ? '↑' : '↓'}</span></button></th>)}<th scope="col" className="action-heading">操作</th></tr></thead><tbody>
        {sortedItems.map(item => <tr key={item.infohash}>
          <td className="resource-cell"><a className="resource-name" href={item.magnet}>{item.name}</a><div className="hash-line"><span className="hash-label">HASH</span><span className="hash-value">{item.infohash}</span><button className="hash-copy" aria-label={`复制 ${item.name} 的 InfoHash`} title="复制 InfoHash" onClick={() => void copyText(item.infohash, 'InfoHash')}>复制</button></div></td>
          <td data-label="大小" className="size-cell">{item.formattedSize}</td>
          <td data-label="收录" className="time-cell"><time dateTime={item.time}>{item.formattedTime}</time></td>
          <td data-label="采集" className="count-cell"><span className="count-badge">{numberFormatter.format(item.count)}<span className="mobile-only"> 次</span></span></td>
          <td className="action-cell"><button className="magnet-button" aria-label={`复制 ${item.name} 的磁力链接`} onClick={() => void copyText(item.magnet, '磁力链接')}>复制磁力<span aria-hidden="true"> ↗</span></button></td>
        </tr>)}
      </tbody></table></div>
      <p className="results-footnote">{total > items.length ? `当前展示最近收录的 ${items.length} 条匹配，请细化关键词以缩小范围。` : '已展示本次搜索的全部匹配。'} 采集次数不代表在线 Peer 数。</p>
      <div className="copy-notice" role="status" aria-live="polite">{notice}</div>
    </section>
  );
}
