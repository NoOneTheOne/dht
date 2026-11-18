import { FormEvent, useEffect, useRef, useState } from 'react';
import { TorrentItem } from '../types';
import { formatSize } from '../format';

const periods = [{ value: '1h', label: '过去 1 小时' }, { value: '24h', label: '过去 24 小时' }, { value: '7d', label: '过去 7 天' }];
interface TrendingResponse { items: TorrentItem[]; window: string; limit: number; since: string; until: string }

function isTrending(value: unknown, period: string, limit: number): value is TrendingResponse {
  if (!value || typeof value !== 'object') return false;
  const data = value as TrendingResponse;
  return data.window === period && data.limit === limit && Number.isFinite(Date.parse(data.since)) && Number.isFinite(Date.parse(data.until)) &&
    Array.isArray(data.items) && data.items.length <= limit && data.items.every(item => item && typeof item.name === 'string' &&
      typeof item.infohash === 'string' && /^[a-f\d]{40}$/i.test(item.infohash) && typeof item.time === 'string' &&
      Number.isFinite(item.length) && item.length >= 0 && Number.isSafeInteger(item.count) && item.count > 0 && typeof item.has_files === 'boolean');
}

export default function Trending() {
  const [period, setPeriod] = useState('24h');
  const [limit, setLimit] = useState(10);
  const [draftLimit, setDraftLimit] = useState('10');
  const [validation, setValidation] = useState('');
  const [data, setData] = useState<TrendingResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [attempt, setAttempt] = useState(0);
  const [notice, setNotice] = useState('');
  const noticeTimer = useRef<ReturnType<typeof setTimeout>>();
  useEffect(() => () => clearTimeout(noticeTimer.current), []);
  useEffect(() => {
    const controller = new AbortController();
    let timedOut = false;
    const timeout = window.setTimeout(() => { timedOut = true; controller.abort(); }, 15000);
    setLoading(true); setError(''); setData(null);
    void (async () => {
      try {
        const response = await fetch(`/api/trending?window=${period}&limit=${limit}`, { signal: controller.signal });
        if (!response.ok) throw new Error('热榜服务暂时不可用，请稍后重试。');
        const result: unknown = await response.json();
        if (!isTrending(result, period, limit)) throw new Error('热榜数据格式不正确，请稍后重试。');
        if (!controller.signal.aborted) setData(result);
      } catch (err) {
        if (timedOut) setError('热榜加载超时，请稍后重试。');
        else if (!controller.signal.aborted) setError(err instanceof Error ? err.message : '热榜加载失败。');
      } finally {
        window.clearTimeout(timeout);
        if (!controller.signal.aborted || timedOut) setLoading(false);
      }
    })();
    return () => { window.clearTimeout(timeout); controller.abort(); };
  }, [period, limit, attempt]);

  const applyLimit = (event: FormEvent) => {
    event.preventDefault();
    const next = Number(draftLimit);
    if (!/^\d+$/.test(draftLimit) || !Number.isInteger(next) || next < 1 || next > 100) { setValidation('请输入 1–100 之间的整数。'); return; }
    setValidation(''); setLimit(next); setDraftLimit(String(next));
  };
  const copyMagnet = async (item: TorrentItem) => {
    clearTimeout(noticeTimer.current);
    try {
      await navigator.clipboard.writeText(`magnet:?xt=urn:btih:${item.infohash}&dn=${encodeURIComponent(item.name)}`);
      setNotice('磁力链接已复制');
    } catch { setNotice('复制失败，可手动选择 InfoHash 复制。'); }
    noticeTimer.current = setTimeout(() => setNotice(''), 4000);
  };

  return <section className="trending-section" aria-labelledby="trending-title">
    <a className="detail-back" href="#">← 返回搜索</a>
    <div className="trending-heading"><div><span className="eyebrow">TRENDING / DHT NETWORK</span><h1 id="trending-title">资源热榜<span className="trending-top">TOP {limit}</span></h1><p>发现最近被频繁采集的资源，按所选时间范围内的命中次数排序。</p></div><span className="trending-indicator">滚动时间窗口</span></div>
    <div className="trending-toolbar">
      <div className="trending-periods" role="group" aria-label="热榜时间范围">{periods.map(option => <button key={option.value} aria-pressed={period === option.value} onClick={() => setPeriod(option.value)}>{option.label}</button>)}</div>
      <form className="trending-limit" onSubmit={applyLimit} noValidate><label htmlFor="trending-limit">显示条数</label><input id="trending-limit" type="number" inputMode="numeric" min="1" max="100" step="1" value={draftLimit} aria-invalid={!!validation} aria-describedby="trending-limit-hint" onChange={event => { setDraftLimit(event.target.value); setValidation(''); }} /><button className="page-button" type="submit">应用</button></form>
    </div>
    <p id="trending-limit-hint" className={validation ? 'validation-error' : 'trending-hint'}>{validation || '默认 10 条，可自定义 1–100 条；相同次数按 InfoHash 排序。'}</p>
    {loading ? <div className="state-panel" role="status" aria-busy="true"><span className="loading-ring" aria-hidden="true" /><h2>正在整理热榜</h2><p>统计所选时间窗口中的采集命中次数…</p></div> : error ? <div className="state-panel error-panel" role="alert"><h2>热榜加载失败</h2><p>{error}</p><button className="secondary-button" onClick={() => setAttempt(value => value + 1)}>重试</button></div> : data && data.items.length === 0 ? <div className="state-panel" role="status"><h2>这个时间段暂无热榜数据</h2><p>尝试切换更长的时间范围，或等待新的采集记录。</p></div> : data && <>
      <div className="trending-meta"><span>{periods.find(option => option.value === period)?.label} · {data.items.length} 条资源</span><span>统计截至 <time dateTime={data.until}>{new Intl.DateTimeFormat('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }).format(Date.parse(data.until))}</time></span></div>
      <ol className="trending-list">{data.items.map((item, index) => <li key={item.infohash} className="trending-row">
        <span className={`trending-rank ${index < 3 ? 'trending-rank-leading' : ''}`} aria-label={`第 ${index + 1} 名`}>{String(index + 1).padStart(2, '0')}</span>
        <div className="trending-resource">{item.has_files ? <a className="resource-name" href={`#/torrent/${item.infohash}`}>{item.name}</a> : <span className="resource-name">{item.name}</span>}<div className="hash-line"><span className="hash-label">HASH</span><span className="hash-value">{item.infohash}</span></div><span className="trending-size">{formatSize(item.length)}</span></div>
        <div className="trending-hits"><strong>{item.count.toLocaleString('zh-CN')}</strong><span>窗口内命中</span></div>
        <button className="magnet-button" onClick={() => void copyMagnet(item)} aria-label={`复制 ${item.name} 的磁力链接`}>复制磁力 ↗</button>
      </li>)}</ol>
    </>}
    <p className="results-footnote">命中次数指成功采集并落库的记录，不是网页浏览量或下载次数；仅统计已有观察记录，旧版未记录的时间段无法补算。</p>
    <div className="copy-notice" role="status" aria-live="polite">{notice}</div>
  </section>;
}
