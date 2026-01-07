import { config } from "../config";
import { getEpisodeNumber } from "../episode";
import { getReadableSize } from "../file";
import { getFormats } from "../format";
import { getLanguage } from "../language";

export enum TorrentCategory {
	Movie = "movie",
	Series = "series",
}

export type StremioStream = {
	name: string;
	title?: string;
	description?: string;
	url?: string;
	infoHash?: string;
	fileIdx?: number;
	sources?: string[];
	subtitles?: {
		id: string;
		url: string;
		lang: string;
	}[];
	behaviorHints?: {
		bingeGroup?: string;
		filename?: string;
		videoSize?: number;
	};
};

type TorrentSearchResultParams = Pick<
	TorrentSearchResult,
	| "name"
	| "fileName"
	| "fileIndex"
	| "imdb"
	| "season"
	| "episode"
	| "tracker"
	| "category"
	| "size"
	| "seeds"
	| "peers"
	| "torrent"
	| "magnet"
>;

export class TorrentSearchResult {
	name: string;
	fileName?: string;
	fileIndex?: number;
	imdb?: string;
	season?: number;
	episode?: number;
	tracker: string;
	category?: string;
	size?: number;
	seeds?: number;
	peers?: number;
	torrent?: string;
	magnet?: string;

	constructor(params: TorrentSearchResultParams) {
		this.name = params.name;
		this.fileName = params.fileName;
		this.fileIndex = params.fileIndex;
		this.imdb = params.imdb;
		this.season = params.season;
		this.episode = params.episode;
		this.tracker = params.tracker;
		this.category = params.category;
		this.size = params.size;
		this.seeds = params.seeds;
		this.peers = params.peers;
		this.torrent = params.torrent;
		this.magnet = params.magnet;
	}

	get language() {
		return getLanguage(this.name, this.category);
	}

	get formats() {
		return getFormats(this.name);
	}

	get readableSize() {
		if (this.size === undefined) return "Unknown size";
		return getReadableSize(this.size);
	}

	isCorrectEpisode(season: number, episode: number) {
		if (this.season === season && this.episode === episode) return true;
		const guess = getEpisodeNumber(this.name);
		if (guess.completeSeries) return true;
		if (guess.seasons?.includes(season)) return true;
		if (guess.season === season && guess.episode === episode) return true;
		if (season === 0) {
			if (this.name.toLowerCase().includes("special")) return true;
			if (guess.season === undefined && guess.seasons === undefined)
				return true;
		}
		return false;
	}
}

export abstract class TorrentSearchProvider {
	abstract id: string;
	abstract name: string;

	abstract searchTorrentsByCategory(
		query: string,
		category: TorrentCategory,
		season?: number,
		episode?: number,
	): Promise<TorrentSearchResult[]>;

	isEnabled() {
		const { torrentProviders } = config;
		if (torrentProviders.length === 0) return true;
		return torrentProviders.includes(this.id);
	}
}
