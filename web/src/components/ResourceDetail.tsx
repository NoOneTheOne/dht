import { useEffect, useState } from 'react';
import { TorrentDetail } from '../types';

function isDetail(value: unknown, infohash: string): value is TorrentDetail {
  if (!value || typeof value !== 'object') return false;
  const data = value as TorrentDetail;
  return typeof data.name === 'string' && typeof data.infohash === 'string' &&
    data.infohash.toLowerCase() === infohash.toLowerCase() && typeof data.time === 'string' &&
    Number.isFinite(data.length) && data.length >= 0 && Number.isFinite(data.count) && data.count >= 0 &&
    Array.isArray(data.files) && data.files.length > 0 && data.files.every(file =>
      file && Array.isArray(file.path) && file.path.length > 0 &&
      file.path.every(part => typeof part === 'string' && part.length > 0) &&
      Number.isFinite(file.length) && file.length >= 0);
}

function formatSize(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB'];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  return `${Number((bytes / 1024 ** index).toFixed(2))} ${units[index]}`;
}

export default function ResourceDetail({ infohash }: { infohash: string }) {
  const [detail, setDetail] = useState<TorrentDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [notFound, setNotFound] = useState(false);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    let timedOut = false;
    setLoading(true);
    setError('');
    setNotFound(false);
    setDetail(null);
    const timeout = window.setTimeout(() => { timedOut = true; controller.abort(); }, 15000);
    const load = async () => {
      try {
        const response = await fetch(`/api/torrents/${infohash}`, { signal: controller.signal });
        if (response.status === 404) {
          if (!controller.signal.aborted) setNotFound(true);
          return;
        }
        if (!response.ok) throw new Error('详情服务暂时不可用，请稍后重试。');
        const data: unknown = await response.json();
        if (!isDetail(data, infohash)) throw new Error('服务返回的数据格式不正确，请稍后重试。');
        if (!controller.signal.aborted) setDetail(data);
      } catch (err) {
        if (timedOut) setError('详情加载超时，请稍后重试。');
        else if (!controller.signal.aborted) setError(err instanceof TypeError ? '无法连接详情服务，请检查网络。' : err instanceof Error ? err.message : '详情加载失败。');
      } finally {
        window.clearTimeout(timeout);
        if (!controller.signal.aborted || timedOut) setLoading(false);
      }
    };
    void load();
    return () => { window.clearTimeout(timeout); controller.abort(); };
  }, [infohash, attempt]);

  const timestamp = detail ? Date.parse(detail.time) : NaN;
  return (
    <section className="detail-section" aria-labelledby="detail-title">
      <a className="detail-back" href="#">← 返回搜索结果</a>
      {loading ? <div className="state-panel" role="status" aria-busy="true"><span className="loading-ring" aria-hidden="true" /><h1 id="detail-title">正在加载文件列表</h1></div> :
        notFound ? <div className="state-panel" role="status"><h1 id="detail-title">暂无文件列表</h1><p>该资源不存在，或尚未保存文件列表，无法提供详情。</p></div> :
        error ? <div className="state-panel error-panel" role="alert"><h1 id="detail-title">详情加载失败</h1><p>{error}</p><button className="secondary-button" onClick={() => setAttempt(value => value + 1)}>重试</button></div> : detail && <>
          <div className="detail-header"><span className="eyebrow">RESOURCE DETAILS</span><h1 id="detail-title">{detail.name}</h1><p className="detail-hash">{detail.infohash}</p>
            <dl className="detail-summary"><div><dt>文件总大小</dt><dd>{formatSize(detail.length)}</dd></div><div><dt>文件数量</dt><dd>{detail.files.length.toLocaleString('zh-CN')}</dd></div><div><dt>收录时间</dt><dd><time dateTime={detail.time}>{Number.isFinite(timestamp) ? new Intl.DateTimeFormat('zh-CN').format(timestamp) : '时间未知'}</time></dd></div><div><dt>采集次数</dt><dd>{detail.count.toLocaleString('zh-CN')}</dd></div></dl>
            <a className="magnet-button detail-magnet" href={`magnet:?xt=urn:btih:${detail.infohash}&dn=${encodeURIComponent(detail.name)}`}>打开磁力链接 ↗</a>
          </div>
          <div className="file-list-surface"><table className="file-list"><caption>文件列表 · {detail.files.length.toLocaleString('zh-CN')} 个文件</caption><thead><tr><th scope="col">文件路径</th><th scope="col">大小</th></tr></thead><tbody>{detail.files.map((file, index) => <tr key={index}><td>{file.path.join(' / ')}</td><td>{formatSize(file.length)}</td></tr>)}</tbody></table></div>
          <p className="results-footnote">仅展示已收录的元数据，不托管或直接提供资源文件。</p>
        </>}
    </section>
  );
}
