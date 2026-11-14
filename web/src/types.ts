export interface TorrentItem {
  name: string;
  infohash: string;
  time: string;
  length: number;
  count: number;
  has_files: boolean;
}

export interface TorrentDetail extends TorrentItem {
  files: { path: string[]; length: number }[];
}

export interface SearchResponse {
  total: number;
  items: TorrentItem[];
  page: number;
  page_size: number;
}
