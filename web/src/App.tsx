import { useState } from 'react';
import SearchBar from './components/SearchBar';
import ResultsTable from './components/ResultsTable';
import { TorrentItem, SearchResponse } from './types';

function App() {
  const [items, setItems] = useState<TorrentItem[]>([]);
  const [total, setTotal] = useState(0);
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSearch = async (searchQuery: string) => {
    setQuery(searchQuery);
    setLoading(true);
    setError('');

    try {
      const response = await fetch(`/search?name=${encodeURIComponent(searchQuery)}`);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const data: SearchResponse = await response.json();
      setItems(data.items || []);
      setTotal(data.total || 0);
    } catch (err) {
      setError(err instanceof Error ? err.message : '搜索失败');
      setItems([]);
      setTotal(0);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex flex-col items-center px-4 py-12">
      <header className="text-center mb-12 animate-fade-in">
        <h1 className="text-5xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-purple-400 via-pink-400 to-indigo-400 mb-3">
          DHT Search
        </h1>
        <p className="text-gray-400 text-lg">BitTorrent 磁力搜索引擎</p>
      </header>

      <SearchBar onSearch={handleSearch} loading={loading} />

      {error && (
        <div className="mt-6 px-4 py-2 rounded-lg bg-red-500/20 border border-red-500/30 text-red-300 animate-fade-in">
          {error}
        </div>
      )}

      <ResultsTable items={items} total={total} query={query} />

      <footer className="mt-auto pt-12 text-gray-500 text-sm">
        Powered by DHT Network
      </footer>
    </div>
  );
}

export default App;
