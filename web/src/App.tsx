import { useEffect, useRef, useState } from 'react';
import SearchBar from './components/SearchBar';
import ResultsTable from './components/ResultsTable';
import Trending from './components/Trending';
import ResourceDetail from './components/ResourceDetail';
import SystemStatus from './components/SystemStatus';
import { TorrentItem, SearchResponse } from './types';

function isSearchResponse(value: unknown): value is SearchResponse {
  if (!value || typeof value !== 'object') return false;
  const data = value as SearchResponse;
  return Number.isSafeInteger(data.total) && data.total >= 0 &&
    Number.isSafeInteger(data.page) && data.page > 0 && data.page_size === 50 &&
    Array.isArray(data.items) && data.items.length <= data.page_size &&
    data.items.every(item => item && typeof item.name === 'string' &&
      typeof item.infohash === 'string' && /^[a-f\d]{40}$/i.test(item.infohash) &&
      typeof item.time === 'string' && Number.isFinite(item.length) && item.length >= 0 &&
      Number.isFinite(item.count) && item.count >= 0 && typeof item.has_files === 'boolean');
}

function App() {
  const [theme, setTheme] = useState(() => document.documentElement.dataset.theme === 'dark' ? 'dark' : 'light');
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.querySelector('meta[name="theme-color"]')?.setAttribute('content', theme === 'dark' ? '#0b1b18' : '#e2f8ed');
    try { localStorage.setItem('dht-theme', theme); } catch {}
  }, [theme]);
  const [items, setItems] = useState<TorrentItem[]>([]);
  const [total, setTotal] = useState(0);
  const [query, setQuery] = useState('');
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const activeRequest = useRef<AbortController | null>(null);

  const [route, setRoute] = useState(() => window.location.hash);
  const detailHash = /^#\/torrent\/([a-f\d]{40})$/i.exec(route)?.[1];
  useEffect(() => {
    const onHashChange = () => setRoute(window.location.hash);
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, []);
  useEffect(() => () => activeRequest.current?.abort(), []);

  const handleSearch = async (searchQuery: string, requestedPage = 1): Promise<void> => {
    activeRequest.current?.abort();
    const controller = new AbortController();
    activeRequest.current = controller;
    setQuery(searchQuery);
    setPage(requestedPage);
    setLoading(true);
    setError('');
    setItems([]);
    setTotal(0);
    let timedOut = false;
    const timeout = window.setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, 15000);

    try {
      const response = await fetch(`/search?name=${encodeURIComponent(searchQuery)}&page=${requestedPage}`, { signal: controller.signal });
      if (!response.ok) {
        throw new Error(response.status === 400 ? '关键词不符合要求，请缩短后重试。' : '搜索服务暂时不可用，请稍后重试。');
      }
      const data: unknown = await response.json();
      if (!isSearchResponse(data) || data.page !== requestedPage) throw new Error('服务返回的数据格式不正确，请稍后重试。');
      if (activeRequest.current !== controller) return;
      const lastPage = Math.max(1, Math.ceil(data.total / data.page_size));
      if (requestedPage > lastPage) {
        void handleSearch(searchQuery, lastPage);
        return;
      }
      setItems(data.items);
      setTotal(data.total);
      setPageSize(data.page_size);
    } catch (err) {
      if (activeRequest.current !== controller) return;
      if (timedOut) setError('搜索超时，请缩短关键词或稍后重试。');
      else if (!controller.signal.aborted) setError(err instanceof TypeError ? '无法连接搜索服务，请检查网络后重试。' : err instanceof Error ? err.message : '搜索失败，请重试。');
    } finally {
      window.clearTimeout(timeout);
      if (activeRequest.current === controller) {
        activeRequest.current = null;
        setLoading(false);
      }
    }
  };

  return (
    <div className="app-shell">
      <header className="site-header">
        <a className="brand" href="/" aria-label="DHT 首页"><img className="brand-mark" src="/search-icon.svg" alt="" width="33" height="33" /><span>DHT</span></a>
        <nav className="header-nav" aria-label="主要导航">
          <a href="#" aria-current={!detailHash && !['#/trending', '#/status'].includes(route) ? 'page' : undefined}>搜索服务<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><circle cx="10.5" cy="10.5" r="6.5" /><path d="m16 16 4 4" /></svg></a>
          <a href="#/trending" aria-current={route === '#/trending' ? 'page' : undefined}>实时热榜<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M8.5 14.5A6 6 0 0 1 14 3a6 6 0 0 0 3 5c3 2 4 5 4 7a9 9 0 0 1-18 0c0-1.2.4-3 1-4a6 6 0 0 0 4.5 3.5Z" /></svg></a>
          <a href="#/status" aria-current={route === '#/status' ? 'page' : undefined}>系统状态<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><rect x="3" y="4" width="18" height="14" rx="2" /><path d="M6 11h3l2-4 3 8 2-4h2M9 21h6m-3-3v3" /></svg></a>
          <button className="theme-toggle" onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')} aria-label={theme === 'dark' ? '切换到日间模式' : '切换到夜间模式'} title={theme === 'dark' ? '日间模式' : '夜间模式'}><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden="true">{theme === 'dark' ? <><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.5 1.5m11 11L19 19M5 19l1.5-1.5m11-11L19 5"/></> : <path d="M20.5 13A8.5 8.5 0 0 1 11 3.5 8.5 8.5 0 1 0 20.5 13Z"/>}</svg></button>
        </nav>
      </header>
      <main id="main-content" tabIndex={-1}>
        {route === '#/status' ? <SystemStatus /> : detailHash ? <ResourceDetail key={detailHash} infohash={detailHash} /> : route === '#/trending' ? <Trending /> : <>
        <section className="hero" aria-labelledby="page-title">
          <span className="eyebrow" lang="en">DISCOVER THE DISTRIBUTED WEB</span>
          <h1 id="page-title">万千微光，<span>自成星海。</span></h1>
          <SearchBar onSearch={handleSearch} loading={loading} initialQuery={query} />
          <div className="search-notes"><span>按名称关键词匹配</span><span>每页 50 条，支持翻页</span><span>不托管资源文件</span></div>
        </section>
        {error ? (
          <section className="state-panel error-panel" role="alert">
            <span className="state-symbol" aria-hidden="true">!</span>
            <h2>这次搜索没有完成</h2><p>{error}</p>
            <button className="secondary-button" onClick={() => void handleSearch(query, page)}>重试第 {page} 页</button>
          </section>
        ) : <ResultsTable items={items} total={total} query={query} loading={loading} page={page} pageSize={pageSize} onPageChange={nextPage => void handleSearch(query, nextPage)} />}
        </>}
      </main>
      <footer className="site-footer"><span>DHT SEARCH <span className="footer-divider">/</span> 基于分布式网络</span><span>仅展示公开采集的元数据，不提供资源内容。请遵守当地法律法规。</span></footer>
    </div>
  );
}

export default App;
