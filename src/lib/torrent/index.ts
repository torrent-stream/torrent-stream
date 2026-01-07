import MemoryChunkStore from "memory-chunk-store";
import type { Torrent, TorrentFile } from "webtorrent";
import { config } from "../config";
import { logger } from "../logger";
import { infoClient, torrentClient } from "./clients";
import { registerStream, registerTorrent, type TorrentStream } from "./streams";
import { TorrentInfo } from "./types";

export function getTorrentInfo(uri: string) {
	return new Promise<TorrentInfo | undefined>((resolve) => {
		const torrent = infoClient.add(
			uri,
			{
				store: MemoryChunkStore,
				destroyStoreOnDestroy: true,
			},
			(torrent) => {
				clearTimeout(timeout);
				const info = new TorrentInfo(torrent);
				torrent.destroy();
				resolve(info);
			},
		);

		const timeout = setTimeout(() => {
			torrent.destroy();
			resolve(undefined);
		}, config.torrentAddTimeout);
	});
}

export function getOrAddTorrent(uri: string) {
	return new Promise<Torrent | undefined>((resolve) => {
		const torrent = torrentClient.add(
			uri,
			{
				store: MemoryChunkStore,
				destroyStoreOnDestroy: true,
				deselect: true,
			},
			(torrent) => {
				clearTimeout(timeout);
				registerTorrent(torrent);
				resolve(torrent);
			},
		);

		const timeout = setTimeout(() => {
			torrent.destroy();
			resolve(undefined);
		}, config.torrentAddTimeout);
	});
}

export function getReadableStream(
	id: string,
	torrent: Torrent,
	file: TorrentFile,
	start: number,
	end: number,
) {
	let stream: TorrentStream;
	let iterator: NodeJS.AsyncIterator<Uint8Array>;
	let cancelled = false;

	return new ReadableStream({
		start() {
			iterator = file[Symbol.asyncIterator]({ start, end });
			stream = registerStream(id, torrent, file);
		},
		async pull(controller) {
			try {
				stream.refreshTimeout();

				const { value, done } = await iterator.next();

				if (cancelled) return;

				if (done) {
					controller.close();
					return;
				}

				controller.enqueue(value);
			} catch (err) {
				logger.error(err);
				controller.error(err);
			}
		},
		cancel() {
			cancelled = true;
			iterator.return?.();
		},
	});
}
