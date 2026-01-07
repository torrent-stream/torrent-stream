"use server";

import { torrentClient } from "@/lib/torrent/clients";
import { TorrentInfo } from "@/lib/torrent/types";

export interface TorrentStats {
	name: string;
	infoHash: string;
	streams: number;
	peers: number;
	size: string;
	progress: string;
	downloaded: string;
	uploaded: string;
	downloadSpeed: string;
	uploadSpeed: string;
	historicalSpeeds: {
		date: Date;
		download: number;
		upload: number;
	}[];
	files: {
		name: string;
		path: string;
		size: string;
		progress: string;
		downloaded: string;
		isVideo: boolean;
		isSubtitle: boolean;
		streams: number;
	}[];
}

export async function getTorrents(): Promise<TorrentStats[]> {
	return torrentClient.torrents
		.filter((torrent) => torrent.ready)
		.toReversed()
		.map((torrent) => {
			const info = new TorrentInfo(torrent);
			return {
				name: info.name,
				infoHash: info.infoHash,
				streams: info.streams,
				peers: info.peers,
				size: info.readableSize,
				progress: info.readableProgress,
				downloaded: info.readableDownloaded,
				uploaded: info.readableUploaded,
				downloadSpeed: info.readableDownloadSpeed,
				uploadSpeed: info.readableUploadSpeed,
				historicalSpeeds: info.historicalSpeeds,
				files: info.files
					.map((file) => ({
						name: file.name,
						path: file.path,
						size: file.readableSize,
						progress: file.readableProgress,
						downloaded: file.readableDownloaded,
						isVideo: file.isVideo,
						isSubtitle: file.isSubtitle,
						streams: file.streams,
					}))
					.sort((a, b) => a.path.localeCompare(b.path)),
			};
		});
}
