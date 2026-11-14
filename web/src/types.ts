export interface TorrentItem {
  name: string;
  infohash: string;
  time: string;
  length: number;
  count: number;
}

export interface SearchResponse {
  total: number;
  items: TorrentItem[];
  page: number;
  page_size: number;
}
