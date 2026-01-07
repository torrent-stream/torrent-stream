import type { Torrent, TorrentFile } from "webtorrent";
import { config } from "../config";
import { logger } from "../logger";
import { LRU } from "../lru";
import { torrentClient } from "./clients";
import { TorrentStreamChunkStore } from "./store";

declare global {
	var torrentDataMap:
		| Map<
				string,
				{
					streams: Map<string, TorrentStream>;
					speeds: LRU<number, { date: Date; upload: number; download: number }>;
				}
		  >
		| undefined;
}

if (!global.torrentDataMap) {
	const map = new Map<
		string,
		{
			streams: Map<string, TorrentStream>;
			speeds: LRU<number, { date: Date; upload: number; download: number }>;
		}
	>();

	global.torrentDataMap = map;

	setInterval(() => {
		torrentClient.torrents.forEach((torrent) => {
			const data = map.get(torrent.infoHash);
			if (!data) return;
			const now = new Date();
			data.speeds.put(now.getTime(), {
				date: now,
				download: torrent.downloadSpeed,
				upload: torrent.uploadSpeed,
			});
		});
	}, 1000);
}

const torrentData = global.torrentDataMap;

export class TorrentStream {
	id: string;
	torrent: Torrent;
	files = new Map<string, TorrentFile>();
	timeout?: NodeJS.Timeout;

	constructor(id: string, torrent: Torrent) {
		this.id = id;
		this.torrent = torrent;
	}

	get reservedPieces() {
		return this.files
			.values()
			.toArray()
			.flatMap((file) => [
				Math.floor(file.offset / this.torrent.pieceLength),
				Math.floor((file.offset + file.length - 1) / this.torrent.pieceLength),
			]);
	}

	refreshTimeout() {
		clearTimeout(this.timeout);
		this.timeout = setTimeout(() => {
			unregisterStream(this.id, this.torrent);
		}, config.torrentIdleTimeout);
	}
}

export function registerTorrent(torrent: Torrent) {
	if (torrentData.has(torrent.infoHash)) return;
	torrent.store = new TorrentStreamChunkStore(torrent);
	torrentData.set(torrent.infoHash, {
		streams: new Map<string, TorrentStream>(),
		speeds: new LRU<number, { date: Date; upload: number; download: number }>(
			300,
		),
	});
	logger.info(`Torrent added: ${torrent.name} (${torrent.infoHash})`);
}

export function unregisterTorrent(torrent: Torrent) {
	torrentData.delete(torrent.infoHash);
	logger.info(`Torrent removed: ${torrent.name} (${torrent.infoHash})`);
}

export function registerStream(
	id: string,
	torrent: Torrent,
	file: TorrentFile,
) {
	const data = torrentData.get(torrent.infoHash);
	if (!data) throw new Error("Torrent not registered");

	let stream = data.streams.get(id);
	if (!stream) {
		stream = new TorrentStream(id, torrent);
		data.streams.set(id, stream);

		logger.info(`Stream started: ${torrent.name} (${id})`);

		const store = torrent.store as TorrentStreamChunkStore;
		store.refreshCapacity();
	}

	stream.files.set(file.path, file);

	return stream;
}

export function unregisterStream(id: string, torrent: Torrent) {
	if (!torrentData.get(torrent.infoHash)?.streams.delete(id)) {
		return;
	}

	logger.info(`Stream ended: ${torrent.name} (${id})`);

	const store = torrent.store as TorrentStreamChunkStore;
	store.refreshCapacity();

	if (getStreams(torrent).length === 0) {
		setTimeout(() => {
			if (getStreams(torrent).length > 0) return;
			torrent.destroy(undefined, () => {
				unregisterTorrent(torrent);
			});
		}, config.torrentRemoveTimeout);
	}
}

export function getStreams(torrent: Torrent | string) {
	const infoHash = typeof torrent === "string" ? torrent : torrent.infoHash;
	return torrentData.get(infoHash)?.streams.values().toArray() ?? [];
}

export function getHistoricalSpeeds(torrent: Torrent | string) {
	const infoHash = typeof torrent === "string" ? torrent : torrent.infoHash;
	return (
		torrentData
			.get(infoHash)
			?.speeds.map.entries()
			.map(([, { date, download, upload }]) => ({
				date,
				download,
				upload,
			}))
			.toArray() ?? []
	);
}
