export interface TorrentItem {
  name: string;
  infohash: string;
  time: string;
  length: number;
  count: number;
  has_files: boolean;
  /** Search only: matched in file names but not in the name. */
  matched_files?: boolean;
}

export interface TorrentDetail extends TorrentItem {
  files: { path: string[]; length: number }[];
}

export interface SearchResponse {
  total: number;
  items: TorrentItem[];
  page: number;
  page_size: number;
  /** False when only names were searched (file index not ready, or words too common). */
  files_searched: boolean;
  /** True when more rows match than were counted; total is then a lower bound. */
  total_capped?: boolean;
}
