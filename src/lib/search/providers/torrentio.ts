import { config } from "../../config";
import { logger } from "../../logger";
import {
	type StremioStream,
	TorrentCategory,
	TorrentSearchProvider,
	TorrentSearchResult,
} from "../types";

const torrentioProviders = [
	"yts",
	"eztv",
	"rarbg",
	"1337x",
	"thepiratebay",
	"kickasstorrents",
	"torrentgalaxy",
	"magnetdl",
	"horriblesubs",
	"nyaasi",
	"tokyotosho",
	"anidex",
	"rutor",
	"rutracker",
	"comando",
	"bludv",
	"micoleaodublado",
	"torrent9",
	"ilcorsaronero",
	"mejortorrent",
	"wolfmax4k",
	"cinecalidad",
	"besttorrents",
];

export class TorrentioProvider extends TorrentSearchProvider {
	id = "torrentio";
	name = "Torrentio";

	async searchTorrentsByCategory(
		query: string,
		category: TorrentCategory,
		season?: number,
		episode?: number,
	) {
		const providers = this.enabledProviders();

		switch (category) {
			case TorrentCategory.Movie:
				return this.searchTorrents(providers, "movie", query);
			case TorrentCategory.Series:
				return this.searchTorrents(providers, "tv", query, season, episode);
		}
	}

	async searchTorrents(
		providers: string[],
		category: "movie" | "tv",
		imdb: string,
		season?: number,
		episode?: number,
	) {
		const torrents: TorrentSearchResult[] = [];

		try {
			const configParam = `providers=${providers.join(",")}`;
			const json = `${category === "movie" ? `${imdb}` : `${imdb}:${season}:${episode}`}.json`;

			const url = [
				`https://torrentio.strem.fun`,
				...[this.allProvidersEnabled() ? [configParam] : []],
				`stream`,
				category,
				json,
			].join("/");

			const response = await fetch(url, {
				signal: AbortSignal.timeout(config.webRequestTimeout),
			});
			const responseJson = (await response.json()) as {
				streams: StremioStream[];
			};

			for (const stream of responseJson.streams) {
				const name =
					stream.title?.split("\n")[0].replace("⭐", "") ||
					stream.behaviorHints?.filename ||
					stream.name;

				const fileName = stream.behaviorHints?.filename;

				const fileIndex = stream.fileIdx;

				const tracker =
					stream.title?.split("⚙️ ")[1].split("\n")[0] || "Torrentio";

				const category = stream.name.split("\n")[1] || undefined;

				const size = this.parseSize(
					stream.title?.split("💾 ")[1].split(" ⚙️")[0],
				);

				const seeds =
					Number(stream.title?.split("👤 ")[1].split(" 💾")[0]) || undefined;

				const trackers = (stream.sources || [])
					.map((tr) => `&tr=${encodeURIComponent(tr)}`)
					.join("");

				const magnet = `magnet:?xt=urn:btih:${stream.infoHash}${trackers}`;

				torrents.push(
					new TorrentSearchResult({
						name,
						fileName,
						fileIndex,
						imdb,
						season,
						episode,
						tracker,
						category,
						size,
						seeds,
						magnet,
					}),
				);
			}
		} catch (error) {
			logger.error(error);
		}

		return torrents;
	}

	override isEnabled(): boolean {
		return this.allProvidersEnabled() || this.enabledProviders().length > 0;
	}

	private allProvidersEnabled() {
		const { torrentProviders } = config;
		return torrentProviders.includes("torrentio");
	}

	private enabledProviders() {
		const { torrentProviders } = config;
		return torrentioProviders.filter((provider) =>
			torrentProviders.includes(provider),
		);
	}

	private parseSize(size: string | undefined) {
		if (!size) return 0;

		const units = {
			TB: 1024 ** 4,
			GB: 1024 ** 3,
			MB: 1024 ** 2,
			KB: 1024,
			B: 1,
		};

		const [sizeStr, unit] = size.split(" ");
		const sizeNum = Number(sizeStr);

		if (!sizeNum || !units[unit]) return 0;

		return Math.ceil(sizeNum * units[unit]);
	}
}
