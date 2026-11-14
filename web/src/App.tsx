import { useEffect, useRef, useState } from 'react';
import SearchBar from './components/SearchBar';
import ResultsTable from './components/ResultsTable';
import ResourceDetail from './components/ResourceDetail';
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
      <a className="skip-link" href="#main-content" onClick={event => { event.preventDefault(); document.getElementById('main-content')?.focus(); }}>跳到主要内容</a>
      <header className="site-header">
        <a className="brand" href="/" aria-label="DHT Search 首页"><img className="brand-mark" src="/search-icon.svg" alt="" width="33" height="33" /><span>DHT<span className="brand-light"> / SEARCH</span></span></a>
        <span className="header-note">BitTorrent 元数据检索</span>
      </header>
      <main id="main-content" tabIndex={-1}>
        {detailHash ? <ResourceDetail key={detailHash} infohash={detailHash} /> : <>
        <section className="hero" aria-labelledby="page-title">
          <span className="eyebrow">DISCOVER THE DISTRIBUTED WEB</span>
          <h1 id="page-title">从关键词，找到<span>你的下一份发现。</span></h1>
          <p className="hero-description">检索 DHT 网络中的资源元数据，查看文件大小、收录时间与采集次数。</p>
          <SearchBar onSearch={handleSearch} loading={loading} />
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
      <footer className="site-footer"><span>DHT SEARCH <span className="footer-divider">/</span> 基于分布式网络</span><span>请遵守当地法律，仅获取有权访问的内容。</span></footer>
    </div>
  );
}

export default App;
