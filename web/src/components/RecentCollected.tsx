import { FormEvent, useEffect, useState } from 'react';
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

// RecentCollected lists the newest collected resources and refreshes itself.
export default function RecentCollected() {
  const [limit, setLimit] = useState(10);
  const [draftLimit, setDraftLimit] = useState('10');
  const [validation, setValidation] = useState('');
  const [items, setItems] = useState<TorrentItem[] | null>(null);
  const [error, setError] = useState('');
  const [attempt, setAttempt] = useState(0);
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    let active: AbortController | null = null;
    let first = true;
    const load = async () => {
      active?.abort();
      const controller = new AbortController();
      active = controller;
      const timeout = window.setTimeout(() => controller.abort(), 15000);
      try {
        const response = await fetch(`/api/recent?limit=${limit}`, { signal: controller.signal });
        if (!response.ok) throw new Error('最近收录暂时不可用，请稍后重试。');
        const result: unknown = await response.json();
        if (!isRecent(result, limit)) throw new Error('最近收录数据格式不正确。');
        if (active === controller) { setItems(result.items); setError(''); setNow(Date.now()); }
      } catch (err) {
        // Background refreshes keep the last good list; only the first load reports errors.
        if (active === controller && first) {
          setError(controller.signal.aborted ? '最近收录加载超时，请稍后重试。' : err instanceof Error ? err.message : '最近收录加载失败。');
        }
      } finally {
        window.clearTimeout(timeout);
        if (active === controller) first = false;
      }
    };
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

  return <aside className="recent-panel" aria-labelledby="recent-title">
    <div className="recent-header">
      <div><span className="eyebrow">LIVE / RECENT</span><h2 id="recent-title">最近收录</h2></div>
      <span className="recent-live"><span className="recent-dot" aria-hidden="true" />每 30 秒更新</span>
    </div>
    <form className="trending-limit recent-limit" onSubmit={applyLimit} noValidate>
      <label htmlFor="recent-limit">显示条数</label>
      <input id="recent-limit" type="number" inputMode="numeric" min="1" max="100" step="1" value={draftLimit} aria-invalid={!!validation} aria-describedby="recent-limit-hint" onChange={event => { setDraftLimit(event.target.value); setValidation(''); }} />
      <button className="page-button" type="submit">应用</button>
    </form>
    <p id="recent-limit-hint" className={validation ? 'validation-error' : 'trending-hint'}>{validation || '默认 10 条，可自定义 1–100 条；按首次收录时间排序。'}</p>
    {error ? <div className="state-panel error-panel" role="alert"><h2>加载失败</h2><p>{error}</p><button className="secondary-button" onClick={() => setAttempt(value => value + 1)}>重试</button></div>
      : items === null ? <div className="state-panel" role="status" aria-busy="true"><span className="loading-ring" aria-hidden="true" /><p>正在读取最近收录…</p></div>
      : items.length === 0 ? <div className="state-panel" role="status"><h2>暂无收录</h2><p>采集到的新资源会出现在这里。</p></div>
      : <ol className="recent-list">{items.map(item => <li key={item.infohash} className="recent-row">
        {item.has_files ? <a className="resource-name" href={`#/torrent/${item.infohash}`}>{item.name}</a> : <span className="resource-name">{item.name}</span>}
        <div className="hash-line"><span className="hash-label">HASH</span><span className="hash-value">{item.infohash}</span></div>
        <div className="recent-meta"><time dateTime={item.time} title={new Date(item.time).toLocaleString('zh-CN', { hour12: false })}>{formatRelative(item.time, now)}</time><span>{formatSize(item.length)}</span></div>
      </li>)}</ol>}
  </aside>;
}
