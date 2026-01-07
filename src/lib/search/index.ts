import { randomBytes } from "node:crypto";
import { config } from "../config";
import { encryptText } from "../encryption";
import { getReadableSize } from "../file";
import { getFormats, TorrentFormat } from "../format";
import { logger } from "../logger";
import { getTorrentInfo } from "../torrent";
import type { TorrentInfo } from "../torrent/types";
import { InsaneProvider } from "./providers/insane";
import { NcoreProvider } from "./providers/ncore";
import { TorrentioProvider } from "./providers/torrentio";
import {
	type StremioStream,
	TorrentCategory,
	type TorrentSearchProvider,
	type TorrentSearchResult,
} from "./types";

export const providers: TorrentSearchProvider[] = [
	new TorrentioProvider(),
	new NcoreProvider(),
	new InsaneProvider(),
];

export async function getStremioStreams(
	endpoint: string,
	category: TorrentCategory,
	imdbId: string,
	season?: number,
	episode?: number,
) {
	let torrents = await searchTorrents(imdbId, category, season, episode);

	torrents = torrents.filter((torrent) => {
		if (!torrent.seeds) return false;
		if (!isAllowedLanguage(torrent.language.code)) return false;
		if (!isAllowedFormat(torrent.formats.formats)) return false;
		if (
			season &&
			episode &&
			!torrent.isCorrectEpisode(Number(season), Number(episode))
		)
			return false;

		return true;
	});

	let streams = (
		await Promise.all(
			torrents.map((torrent) =>
				getStreamsFromTorrent(endpoint, torrent, season, episode),
			),
		)
	).flat();

	streams = streams.filter((stream) => isAllowedFormat(stream.formats));

	logger.info(
		`Search found ${streams.length} streams for ${category === TorrentCategory.Movie ? imdbId : `${imdbId} season ${season} episode ${episode}`}`,
	);

	return streams.sort((a, b) => b.score - a.score).map(({ stream }) => stream);
}

async function searchTorrents(
	query: string,
	category: TorrentCategory,
	season?: number,
	episode?: number,
) {
	const promises = providers.map((provider) =>
		provider.isEnabled()
			? provider.searchTorrentsByCategory(query, category, season, episode)
			: Promise.resolve([]),
	);

	let results = (await Promise.all(promises)).flat();

	results = new Map(
		results.map((torrent) => [`${torrent.tracker}:${torrent.name}`, torrent]),
	)
		.values()
		.toArray();

	return results;
}

async function getStreamsFromTorrent(
	endpoint: string,
	torrent: TorrentSearchResult,
	season?: number,
	episode?: number,
) {
	const uri = torrent.torrent || torrent.magnet;
	if (!uri) return [];

	let info: TorrentInfo | undefined;

	if (torrent.magnet && torrent.fileName && torrent.fileIndex !== undefined) {
		info = {
			files: [
				{
					name: torrent.fileName,
					index: torrent.fileIndex,
					size: torrent.size || 0,
					readableSize: getReadableSize(torrent.size || 0),
					formats: getFormats(torrent.name),
					isVideo: true,
					isSubtitle: false,
					isCorrectEpisode: (_s, _e) => true,
				},
			],
		} as TorrentInfo;
	} else {
		info = await getTorrentInfo(uri);
	}

	if (!info) return [];

	const uriSegment = encodeURIComponent(encryptText(uri));

	const {
		formats: torrentFormats,
		quality: torrentQuality,
		score: torrentScore,
	} = torrent.formats;

	const { flag, language } = torrent.language;

	let videos = info.files.filter((file) => file.isVideo);
	if (season && episode) {
		videos = videos.filter((file) =>
			file.isCorrectEpisode(Number(season), Number(episode)),
		);
	}

	const videosSize = videos.reduce((acc, file) => acc + file.size, 0);

	videos = videos.filter(
		(file) => file.size > videosSize / (videos.length + 1),
	);

	const subs = info.files.filter((file) => file.isSubtitle);

	return videos.map((file) => {
		const {
			formats: fileFormats,
			quality: fileQuality,
			score: fileScore,
		} = file.formats;

		const score = Math.max(torrentScore, fileScore);
		const quality = fileScore > torrentScore ? fileQuality : torrentQuality;
		const formats = [
			...new Set([
				...torrentFormats,
				...fileFormats.filter((f) => f !== TorrentFormat.Unknown),
			]),
		];

		const streamId = randomBytes(8).toString("hex");

		const stream: StremioStream = {
			name: quality,
			description: [
				...(videos.length > 1 ? [torrent.name, file.name] : [torrent.name]),
				[
					...[torrent.size ? `💾 ${file.readableSize}` : []],
					...[torrent.seeds ? `⬆️ ${torrent.seeds}` : []],
					...[torrent.peers ? `⬇️ ${torrent.peers}` : []],
				].join(" "),
				[`${flag} ${language}`, `⚙️ ${torrent.tracker}`].join(" "),
			].join("\n"),
			url: [endpoint, `${uriSegment}?s=${streamId}&f=${file.index}`].join("/"),
			subtitles: subs.map((sub) => ({
				id: sub.index.toString(),
				url: [endpoint, `${uriSegment}?s=${streamId}&f=${sub.index}`].join("/"),
				lang: sub.name,
			})),
			behaviorHints: {
				bingeGroup: torrent.name,
				filename: file.name,
				videoSize: file.size,
			},
		};

		return {
			stream,
			torrentName: torrent.name,
			fileName: file.name,
			formats,
			quality,
			score,
		};
	});
}

function isAllowedLanguage(language: string) {
	const { torrentLanguages } = config;
	if (torrentLanguages.length === 0) return true;
	return torrentLanguages.includes(language);
}

function isAllowedFormat(formats: TorrentFormat[]) {
	const { torrentFormats } = config;
	if (torrentFormats.length === 0) return true;
	for (const format of formats) {
		if (!torrentFormats.includes(format)) {
			return false;
		}
	}
	return true;
}
