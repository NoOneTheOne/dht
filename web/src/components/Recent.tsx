import { FormEvent, useEffect, useRef, useState } from 'react';
import { TorrentItem } from '../types';
import { formatRelative, formatSize } from '../format';

interface RecentResponse { items: TorrentItem[]; limit: number }

const REFRESH_MS = 30000;

function isRecent(value: unknown, limit: number): value is RecentResponse {
  if (!value || typeof value !== 'object') return false;
  const data = value as RecentResponse;
  return data.limit === limit && Array.isArray(data.items) && data.items.length <= limit &&
    data.items.every(item => item && typeof item.name === 'string' && typeof item.infohash === 'string' &&
      /^[a-f\d]{40}$/i.test(item.infohash) && typeof item.time === 'string' && Number.isFinite(item.length) &&
      item.length >= 0 && Number.isSafeInteger(item.count) && item.count >= 0 && typeof item.has_files === 'boolean');
}

const clock = new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });

// Recent is the "最近收录" page: the newest collected resources, refreshed live.
export default function Recent() {
  const [limit, setLimit] = useState(10);
  const [draftLimit, setDraftLimit] = useState('10');
  const [validation, setValidation] = useState('');
  const [items, setItems] = useState<TorrentItem[] | null>(null);
  const [error, setError] = useState('');
  const [attempt, setAttempt] = useState(0);
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [notice, setNotice] = useState('');
  const noticeTimer = useRef<ReturnType<typeof setTimeout>>();
  const refreshNow = useRef<() => void>(() => {});
  useEffect(() => () => clearTimeout(noticeTimer.current), []);

  useEffect(() => {
    let active: AbortController | null = null;
    let first = true;
    const load = async () => {
      active?.abort();
      const controller = new AbortController();
      active = controller;
      setRefreshing(true);
      const timeout = window.setTimeout(() => controller.abort(), 60000);
      try {
        const response = await fetch(`/api/recent?limit=${limit}`, { signal: controller.signal });
        if (!response.ok) throw new Error('最近收录暂时不可用，请稍后重试。');
        const result: unknown = await response.json();
        if (!isRecent(result, limit)) throw new Error('最近收录数据格式不正确。');
        if (active === controller) { setItems(result.items); setError(''); setUpdatedAt(Date.now()); }
      } catch (err) {
        // Background refreshes keep the last good list; only the first load reports errors.
        if (active === controller && first) {
          setError(controller.signal.aborted ? '最近收录加载超时，请稍后重试。' : err instanceof Error ? err.message : '最近收录加载失败。');
        }
      } finally {
        window.clearTimeout(timeout);
        if (active === controller) { first = false; setRefreshing(false); }
      }
    };
    refreshNow.current = () => { void load(); }; // Manual refresh keeps the current list.
    setItems(null);
    setError('');
    void load();
    const timer = window.setInterval(() => { if (!document.hidden) void load(); }, REFRESH_MS);
    return () => { window.clearInterval(timer); active?.abort(); active = null; };
  }, [limit, attempt]);

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
  const now = updatedAt ?? Date.now();

  return <section className="trending-section" aria-labelledby="recent-title">
    <a className="detail-back" href="#">← 返回搜索</a>
    <div className="trending-heading">
      <div><span className="eyebrow">LIVE / RECENTLY COLLECTED</span><h1 id="recent-title">最近收录<span className="trending-top">{limit} 条</span></h1><p>按首次收录时间倒序展示刚采集到的资源，页面打开时每 30 秒自动刷新。</p></div>
      <span className="recent-live"><span className="recent-dot" aria-hidden="true" />实时更新</span>
    </div>
    <div className="trending-toolbar">
      <div className="recent-status">
        <span>{updatedAt ? <>更新于 <time dateTime={new Date(updatedAt).toISOString()}>{clock.format(updatedAt)}</time></> : '正在读取…'}</span>
        <button className="page-button" onClick={() => refreshNow.current()} disabled={refreshing}>{refreshing ? '刷新中…' : '立即刷新'}</button>
      </div>
      <form className="trending-limit" onSubmit={applyLimit} noValidate>
        <label htmlFor="recent-limit">显示条数</label>
        <input id="recent-limit" type="number" inputMode="numeric" min="1" max="100" step="1" value={draftLimit} aria-invalid={!!validation} aria-describedby="recent-limit-hint" onChange={event => { setDraftLimit(event.target.value); setValidation(''); }} />
        <button className="page-button" type="submit">应用</button>
      </form>
    </div>
    <p id="recent-limit-hint" className={validation ? 'validation-error' : 'trending-hint'}>{validation || '默认 10 条，可自定义 1–100 条；时间为首次收录时间。'}</p>
    {error ? <div className="state-panel error-panel" role="alert"><h2>最近收录加载失败</h2><p>{error}</p><button className="secondary-button" onClick={() => setAttempt(value => value + 1)}>重试</button></div>
      : items === null ? <div className="state-panel" role="status" aria-busy="true"><span className="loading-ring" aria-hidden="true" /><h2>正在读取最近收录</h2><p>获取最新采集到的资源…</p></div>
      : items.length === 0 ? <div className="state-panel" role="status"><h2>暂无收录</h2><p>采集到的新资源会出现在这里。</p></div>
      : <ol className="trending-list">{items.map(item => <li key={item.infohash} className="trending-row recent-row">
        <time className="recent-age" dateTime={item.time} title={new Date(item.time).toLocaleString('zh-CN', { hour12: false })}>{formatRelative(item.time, now)}</time>
        <div className="trending-resource">{item.has_files ? <a className="resource-name" href={`#/torrent/${item.infohash}`}>{item.name}</a> : <span className="resource-name">{item.name}</span>}<div className="hash-line"><span className="hash-label">HASH</span><span className="hash-value">{item.infohash}</span></div><span className="trending-size">{formatSize(item.length)}</span></div>
        <button className="magnet-button" onClick={() => void copyMagnet(item)} aria-label={`复制 ${item.name} 的磁力链接`}>复制磁力 ↗</button>
      </li>)}</ol>}
    <p className="results-footnote">仅展示成功获取元数据并落库的资源；同一资源再次被采集时不会重新排到前面。</p>
    <div className="copy-notice" role="status" aria-live="polite">{notice}</div>
  </section>;
}
