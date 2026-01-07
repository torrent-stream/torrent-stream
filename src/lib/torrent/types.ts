import type { Torrent, TorrentFile } from "webtorrent";
import { getEpisodeNumber } from "../episode";
import {
	getReadableProgress,
	getReadableSize,
	isSubtitleFile,
	isVideoFile,
} from "../file";
import { getFormats, type TorrentFormat } from "../format";
import { getHistoricalSpeeds, getStreams } from "./streams";

export class TorrentInfo {
	name: string;
	infoHash: string;
	size: number;
	readableSize: string;
	progress: number;
	readableProgress: string;
	downloaded: number;
	readableDownloaded: string;
	uploaded: number;
	readableUploaded: string;
	downloadSpeed: number;
	readableDownloadSpeed: string;
	uploadSpeed: number;
	readableUploadSpeed: string;
	historicalSpeeds: { date: Date; download: number; upload: number }[];
	peers: number;
	streams: number;
	files: TorrentFileInfo[];

	constructor(torrent: Torrent) {
		this.name = torrent.name;
		this.infoHash = torrent.infoHash;
		this.size = torrent.length;
		this.readableSize = getReadableSize(this.size);
		this.progress = torrent.progress;
		this.readableProgress = getReadableProgress(this.progress);
		this.downloaded = torrent.downloaded;
		this.readableDownloaded = getReadableSize(this.downloaded);
		this.uploaded = torrent.uploaded;
		this.readableUploaded = getReadableSize(this.uploaded);
		this.downloadSpeed = torrent.downloadSpeed;
		this.readableDownloadSpeed = `${getReadableSize(this.downloadSpeed)}/s`;
		this.uploadSpeed = torrent.uploadSpeed;
		this.readableUploadSpeed = `${getReadableSize(this.uploadSpeed)}/s`;
		this.historicalSpeeds = getHistoricalSpeeds(this.infoHash);
		this.peers = torrent.numPeers;
		this.streams = getStreams(torrent).length;
		this.files = torrent.files.map(
			(file) => new TorrentFileInfo(torrent, file),
		);
	}
}

export class TorrentFileInfo {
	name: string;
	index: number;
	path: string;
	size: number;
	readableSize: string;
	progress: number;
	readableProgress: string;
	downloaded: number;
	readableDownloaded: string;
	formats: { formats: TorrentFormat[]; quality: string; score: number };
	streams: number;
	isVideo: boolean;
	isSubtitle: boolean;

	constructor(torrent: Torrent, file: TorrentFile) {
		this.name = file.name;
		this.index = torrent.files.indexOf(file);
		this.path = file.path;
		this.size = file.length;
		this.readableSize = getReadableSize(this.size);
		this.progress = file.progress;
		this.readableProgress = getReadableProgress(this.progress);
		this.downloaded = file.downloaded;
		this.readableDownloaded = getReadableSize(this.downloaded);
		this.formats = getFormats(this.name);
		this.streams = getStreams(torrent).filter((stream) =>
			stream.files.has(this.path),
		).length;
		this.isVideo = isVideoFile(this.name);
		this.isSubtitle = isSubtitleFile(this.name);
	}

	isCorrectEpisode(season: number, episode: number) {
		const guess = getEpisodeNumber(this.name);
		if (guess.season === season && guess.episode === episode) return true;
		if (season === 0) return true;
		return false;
	}
}
