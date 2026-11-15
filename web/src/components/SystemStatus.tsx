import { useEffect, useState } from 'react';

type Rate = { succeeded: number; failed: number; success_rate: number | null };
type Pool = 'packet' | 'query' | 'metadata';
type Crawler = {
  started_at: string; uptime_seconds: number; collected: number; connected_peers: number;
  rejected_peers: number; dropped_peers: number; workers: Record<Pool, number>; worker_limits: Record<Pool, number>;
  dht_query: Rate; metadata_fetch: Rate; nat: string; reachability: string; ready: boolean;
};
type Status = {
  status: 'operational' | 'degraded' | 'outage'; issues: string[]; updated_at: string;
  crawler: Crawler | null; total_collected: number | null; storage: string;
};

const number = (value: number | null | undefined) => value == null ? '—' : value.toLocaleString('zh-CN');
const date = (value: string | undefined) => value ? new Date(value).toLocaleString('zh-CN', { hour12: false }) : '—';
function duration(seconds: number | undefined) {
  if (seconds == null) return '—';
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor(seconds % 86400 / 3600);
  const minutes = Math.floor(seconds % 3600 / 60);
  return `${days ? `${days} 天 ` : ''}${hours} 小时 ${minutes} 分`;
}
function isStatus(value: unknown): value is Status {
  if (!value || typeof value !== 'object') return false;
  const s = value as Status;
  const nonnegative = (n: unknown) => typeof n === 'number' && Number.isFinite(n) && n >= 0;
  const rate = (r: Rate) => r && nonnegative(r.succeeded) && nonnegative(r.failed) &&
    (r.success_rate === null || nonnegative(r.success_rate) && r.success_rate <= 100);
  const c = s.crawler;
  return ['operational', 'degraded', 'outage'].includes(s.status) && Array.isArray(s.issues) && s.issues.every(i => typeof i === 'string') &&
    Number.isFinite(Date.parse(s.updated_at)) && ['connected', 'unavailable'].includes(s.storage) &&
    (s.total_collected === null || nonnegative(s.total_collected)) && (c === null || !!c &&
      Number.isFinite(Date.parse(c.started_at)) && [c.uptime_seconds, c.collected, c.connected_peers, c.rejected_peers, c.dropped_peers].every(nonnegative) &&
      typeof c.nat === 'string' && typeof c.reachability === 'string' && typeof c.ready === 'boolean' && rate(c.dht_query) && rate(c.metadata_fetch) &&
      !!c.workers && !!c.worker_limits && (['packet', 'query', 'metadata'] as Pool[]).every(pool => nonnegative(c.workers[pool]) && nonnegative(c.worker_limits[pool])));
}

function RateCard({ label, subtitle, rate }: { label: string; subtitle: string; rate: Rate | undefined }) {
  const value = rate?.success_rate;
  return <article className="status-card rate-card"><span className="metric-label">{label}</span>
    <strong className="metric-value">{value == null ? '—' : value.toFixed(1)}{value != null && <small>%</small>}</strong>
    <div className="rate-track" aria-hidden="true"><span style={{ width: `${value ?? 0}%` }} /></div>
    <div className="rate-counts"><span>成功 <b>{number(rate?.succeeded)}</b></span><span>失败 <b>{number(rate?.failed)}</b></span></div>
    <p className="metric-note">{value == null ? '等待完成首次尝试' : subtitle}</p>
  </article>;
}

export default function SystemStatus() {
  const [data, setData] = useState<Status | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    let disposed = false;
    let timer: number;
    let controller: AbortController;
    const load = async () => {
      controller = new AbortController();
      setLoading(true);
      const timeout = window.setTimeout(() => controller.abort(), 10000);
      try {
        const response = await fetch('/api/status', { signal: controller.signal, cache: 'no-store' });
        if (!response.ok) throw new Error('状态服务暂时无法连接，请稍后重试。');
        const value: unknown = await response.json();
        if (!isStatus(value)) throw new Error('状态数据格式异常，请稍后重试。');
        if (!disposed) { setData(value); setError(''); }
      } catch (err) {
        if (!disposed) setError(err instanceof Error && err.name !== 'AbortError' && !(err instanceof TypeError) ? err.message : '状态更新失败或超时，请检查服务连接。');
      } finally {
        window.clearTimeout(timeout);
        if (!disposed) { setLoading(false); timer = window.setTimeout(() => void load(), 15000); }
      }
    };
    void load();
    return () => { disposed = true; window.clearTimeout(timer); controller?.abort(); };
  }, [refresh]);
  const c = data?.crawler;
  const state = error ? 'outage' : data?.status ?? 'degraded';
  const title = error ? '状态更新中断' : !data ? '正在检查系统状态' : { operational: '所有系统运行正常', degraded: '部分服务需要关注', outage: '部分服务暂时不可用' }[state];
  const natLabels: Record<string, string> = { manual: '手动端口转发', discovering: '正在发现网关', mapped: '自动映射已建立', unavailable: '未发现 NAT 网关', mapping_failed: '自动映射失败' };
  const reachLabels: Record<string, string> = { checking: '正在检测', reachable: '已观察到公网入站', unknown: '尚未确认' };
  return <section className="status-section" aria-labelledby="status-title">
    <div className={`status-banner ${state}`} role="status" aria-live="polite"><span className="banner-icon" aria-hidden="true">{state === 'operational' ? '✓' : state === 'outage' ? '!' : '−'}</span>
      <div><strong>{title}</strong><p>{error ? `${error}${data ? ' 下方为上次成功获取的数据，可能已过期。' : ''}` : data ? '持续观测采集、存储与网络连接状态。' : '正在连接采集进程与资源数据库。'}</p></div>
      <span className="banner-badge">{state === 'operational' ? 'OPERATIONAL' : state === 'outage' ? 'DISRUPTION' : 'ATTENTION'}</span>
    </div>
    <div className="status-heading"><div><span className="eyebrow">LIVE SYSTEM OVERVIEW</span><h1 id="status-title">系统运行状态<span className="status-title-dot">.</span></h1><p>分布式网络的每一次连接，都在这里汇成可见的脉搏。</p></div>
      <button className="page-button refresh-button" disabled={loading} onClick={() => setRefresh(v => v + 1)}><svg className={loading ? 'refresh-spinning' : ''} width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" aria-hidden="true"><path d="M20 7v5h-5M4 17v-5h5"/><path d="M6.1 6.1A8 8 0 0 1 20 12M4 12a8 8 0 0 0 13.9 5.9"/></svg>{loading ? '更新中' : '刷新状态'}</button>
    </div>
    <div className="status-meta"><span><i className={`status-dot ${error ? 'outage' : loading ? 'degraded' : data?.status ?? 'degraded'}`} />每 15 秒自动更新</span><span>最后更新 {date(data?.updated_at)}</span></div>
    {data && data.issues.length > 0 && <div className="status-issues"><strong>需要关注</strong><ul>{data.issues.map(issue => <li key={issue}>{issue}</li>)}</ul></div>}
    <div className={`status-content${error && data ? ' stale-data' : ''}`} aria-busy={loading}>
      <div className="status-grid overview-grid">
        <article className="status-card"><span className="metric-label">运行时间 <span>UPTIME</span></span><strong className="metric-value uptime-value">{duration(c?.uptime_seconds)}</strong><p className="metric-note">采集进程 · 自本次启动</p></article>
        <article className="status-card"><span className="metric-label">启动时间 <span>STARTED AT</span></span><strong className="metric-value start-value">{date(c?.started_at)}</strong><p className="metric-note">按浏览器本地时区显示</p></article>
        <article className="status-card"><span className="metric-label">本次运行收集 <span>THIS RUN</span></span><strong className="metric-value">{number(c?.collected)}<small>条</small></strong><p className="metric-note">已通过哈希校验的元数据抓取次数</p></article>
        <article className="status-card"><span className="metric-label">总收集条目 <span>ALL TIME</span></span><strong className="metric-value">{number(data?.total_collected)}<small>条</small></strong><p className="metric-note">数据库已收录的去重资源数</p></article>
      </div>
      <div className="status-group-heading"><h2>采集与连接</h2><span>CRAWLER & CONNECTIONS</span></div>
      <div className="status-grid connections-grid">
        <article className="status-card peers-card"><div className="peer-stat"><span className="metric-label">当前已建联 Peer</span><strong className="metric-value">{number(c?.connected_peers)}<small>个</small></strong><p className="metric-note">已建立的元数据 TCP 连接</p></div><div className="peer-stat"><span className="metric-label">拒绝的 Peer</span><strong className="metric-value">{number(c?.rejected_peers)}<small>次</small></strong><p className="metric-note">远端明确拒绝元数据请求</p></div><div className="peer-local">本地队列拒绝 <b>{number(c?.dropped_peers)}</b><span>含重复、限流与无效请求</span></div></article>
        <RateCard label="DHT 查询成功率" subtitle="成功响应 / 已完成查询" rate={c?.dht_query} />
        <RateCard label="元数据抓取成功率" subtitle="校验成功 / 已完成抓取" rate={c?.metadata_fetch} />
      </div>
      <div className="status-group-heading"><h2>基础服务</h2><span>INFRASTRUCTURE</span></div>
      <div className="status-grid infrastructure-grid">
        <article className="status-card worker-card"><span className="metric-label">工作线程数 <span>活动 / 上限</span></span>{(['packet', 'query', 'metadata'] as Pool[]).map(pool => <div className="worker-row" key={pool}><div><span>{{ packet: '数据包处理', query: 'DHT 查询', metadata: '元数据抓取' }[pool]}</span><b>{number(c?.workers[pool])}<span> / {number(c?.worker_limits[pool])}</span></b></div><div className="rate-track" aria-hidden="true"><span style={{ width: `${c && c.worker_limits[pool] ? Math.min(100, c.workers[pool] / c.worker_limits[pool] * 100) : 0}%` }} /></div></div>)}<p className="metric-note">并发任务数，非操作系统线程数</p></article>
        <article className="status-card network-card"><div className="service-row"><div><span className="metric-label">NAT 状态</span><strong>{c ? natLabels[c.nat] ?? '未知' : '—'}</strong></div><i className={`status-dot ${c?.nat === 'mapped' ? 'operational' : 'degraded'}`} /></div><div className="service-row"><div><span className="metric-label">公网可达性</span><strong>{c ? reachLabels[c.reachability] ?? '未知' : '—'}</strong></div><i className={`status-dot ${c?.reachability === 'reachable' ? 'operational' : 'degraded'}`} /></div><p className="metric-note">基于公网主动 DHT 入站请求；仅有出站响应不能证明可达，未观测到不等于不可达。</p></article>
        <article className="status-card network-card"><div className="service-row"><div><span className="metric-label">资源数据库</span><strong>{!data ? '等待检查' : data.storage === 'connected' ? '连接正常' : '暂时不可用'}</strong></div><i className={`status-dot ${!data ? 'degraded' : data.storage === 'connected' ? 'operational' : 'outage'}`} /></div><div className="service-row"><div><span className="metric-label">DHT 网络</span><strong>{!c ? '采集进程未连接' : c.ready ? '已加入网络' : '正在初始化'}</strong></div><i className={`status-dot ${!data ? 'degraded' : !c ? 'outage' : c.ready ? 'operational' : 'degraded'}`} /></div><p className="metric-note">绿色正常 · 黄色待确认或需关注 · 红色服务不可用</p></article>
      </div>
    </div>
    <p className="status-footnote">成功率按采集进程本次运行累计统计，不包含主动停止的任务；重启后重新计数。状态观测不代表历史可用率。</p>
  </section>;
}
