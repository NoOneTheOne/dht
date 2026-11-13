import { useState } from 'react';

interface SearchBarProps {
  onSearch: (query: string) => void;
  loading: boolean;
}

export default function SearchBar({ onSearch, loading }: SearchBarProps) {
  const [query, setQuery] = useState('');
  const [error, setError] = useState('');

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    const value = query.trim();
    if (!value) return;
    if (new TextEncoder().encode(value).length > 200) {
      setError('关键词不能超过 200 字节，中文约 66 个字。');
      return;
    }
    setError('');
    onSearch(value);
  };

  return (
    <form onSubmit={handleSubmit} className="search-form" role="search">
      <label className="sr-only" htmlFor="search-input">资源名称关键词</label>
      <div className="search-box">
        <svg aria-hidden="true" width="22" height="22" fill="none" viewBox="0 0 24 24" stroke="currentColor"><circle cx="10.5" cy="10.5" r="6.5" strokeWidth="1.8" /><path d="m16 16 4 4" strokeWidth="1.8" strokeLinecap="round" /></svg>
        <input id="search-input" type="search" value={query} onChange={event => { setQuery(event.target.value); setError(''); }} placeholder="输入资源名称或关键词…" autoComplete="off" spellCheck={false} aria-describedby={error ? 'search-validation' : undefined} aria-invalid={!!error} />
        <button className="primary-button" type="submit" disabled={!query.trim()}>
          {loading ? '重新搜索' : '搜索资源'}<span aria-hidden="true">↗</span>
        </button>
      </div>
      {error && <p id="search-validation" className="validation-error" role="alert">{error}</p>}
    </form>
  );
}
