import * as cheerio from "cheerio";
import makeFetchCookie from "fetch-cookie";
import { config } from "../../config";
import { isImdbId } from "../../imdb";
import { logger } from "../../logger";
import {
	TorrentCategory,
	TorrentSearchProvider,
	TorrentSearchResult,
} from "../types";

enum NcoreCategory {
	Film_SD_HU = "xvid_hun",
	Film_SD_EN = "xvid",
	Film_HD_HU = "hd_hun",
	Film_HD_EN = "hd",
	Sorozat_SD_HU = "xvidser_hun",
	Sorozat_SD_EN = "xvidser",
	Sorozat_HD_HU = "hdser_hun",
	Sorozat_HD_EN = "hdser",
}

type NcoreCredentials = {
	username: string;
	password: string;
};

export class NcoreProvider extends TorrentSearchProvider {
	id = "ncore";
	name = "nCore";

	private fetch = makeFetchCookie(fetch);
	private lastLoginCredentials?: NcoreCredentials;
	private lastLoginDate?: number;

	async searchTorrentsByCategory(query: string, category: TorrentCategory) {
		switch (category) {
			case TorrentCategory.Movie:
				return this.searchTorrents(query, [
					NcoreCategory.Film_SD_HU,
					NcoreCategory.Film_SD_EN,
					NcoreCategory.Film_HD_HU,
					NcoreCategory.Film_HD_EN,
				]);
			case TorrentCategory.Series:
				return this.searchTorrents(query, [
					NcoreCategory.Sorozat_SD_HU,
					NcoreCategory.Sorozat_SD_EN,
					NcoreCategory.Sorozat_HD_HU,
					NcoreCategory.Sorozat_HD_EN,
				]);
		}
	}

	async searchTorrents(
		query: string,
		ncoreCategories: NcoreCategory[],
		ncoreCredentials?: NcoreCredentials,
	) {
		const torrents: TorrentSearchResult[] = [];

		try {
			const credentials = ncoreCredentials ?? this.getCredentials();
			await this.login(credentials);

			const imdb = isImdbId(query) ? query : undefined;

			let page = 0;

			while (page <= 5) {
				page++;

				let torrentsOnPage = 0;

				const params = new URLSearchParams({
					oldal: page.toString(),
					tipus: "kivalasztottak_kozott",
					kivalasztott_tipus: ncoreCategories.join(","),
					mire: query,
					miben: imdb ? "imdb" : "name",
					miszerint: "ctime",
					hogyan: "DESC",
				});

				const link = `https://ncore.pro/torrents.php?${params.toString()}`;
				const torrentsPage = await this.fetch(link, {
					signal: AbortSignal.timeout(config.webRequestTimeout),
				});
				const $ = cheerio.load(await torrentsPage.text());

				const rssUrl = $("link[rel=alternate]").attr("href");
				const downloadKey = rssUrl?.split("=")[1];
				if (!downloadKey) throw new Error("Failed to get nCore download key");

				for (const el of $("div.box_torrent")) {
					torrentsOnPage++;

					const name = $(el).find("div.torrent_txt > a").attr("title");

					const categoryHref = $(el)
						.find("a > img.categ_link")
						.parent()
						.attr("href");

					const tracker = this.name;
					const category = this.parseCategory(categoryHref?.split("=")[1]);
					const size = this.parseSize($(el).find("div.box_meret2").text());
					const seeds = Number($(el).find("div.box_s2").text());
					const peers = Number($(el).find("div.box_l2").text());
					const torrentId = $(el).next().next().attr("id");
					const torrent = `https://ncore.pro/torrents.php?action=download&id=${torrentId}&key=${downloadKey}`;

					if (!name || !torrentId) continue;

					torrents.push(
						new TorrentSearchResult({
							name,
							imdb,
							tracker,
							category,
							size,
							seeds,
							peers,
							torrent,
						}),
					);
				}

				if (torrentsOnPage < 50) break;
			}
		} catch (error) {
			logger.error(error);
		}

		return torrents;
	}

	private async login(credentials: NcoreCredentials) {
		const sessionTimeout = 15 * 60 * 1000; // 15 minutes

		// Check if we need to re-login
		if (
			this.lastLoginCredentials &&
			this.lastLoginCredentials.username === credentials.username &&
			this.lastLoginCredentials.password === credentials.password &&
			this.lastLoginDate &&
			Date.now() - this.lastLoginDate < sessionTimeout
		) {
			return;
		}

		this.fetch = makeFetchCookie(fetch);

		const formData = new FormData();
		formData.append("nev", credentials.username);
		formData.append("pass", credentials.password);
		formData.append("set_lang", "hu");
		formData.append("submitted", "1");

		await this.fetch("https://ncore.pro/login.php", {
			method: "POST",
			body: formData,
			signal: AbortSignal.timeout(config.webRequestTimeout),
		});

		this.lastLoginCredentials = credentials;
		this.lastLoginDate = Date.now();
	}

	private getCredentials() {
		const { ncoreUser, ncorePass } = config;

		if (!ncoreUser || !ncorePass) {
			throw new Error("nCore credentials are not set in environment variables");
		}

		return {
			username: ncoreUser,
			password: ncorePass,
		};
	}

	private parseCategory(category: string | undefined) {
		if (!category) return undefined;

		return {
			[NcoreCategory.Film_SD_HU]: "Movies/SD/HU",
			[NcoreCategory.Film_SD_EN]: "Movies/SD/EN",
			[NcoreCategory.Film_HD_HU]: "Movies/HD/HU",
			[NcoreCategory.Film_HD_EN]: "Movies/HD/EN",
			[NcoreCategory.Sorozat_SD_HU]: "TV/SD/HU",
			[NcoreCategory.Sorozat_SD_EN]: "TV/SD/EN",
			[NcoreCategory.Sorozat_HD_HU]: "TV/HD/HU",
			[NcoreCategory.Sorozat_HD_EN]: "TV/HD/EN",
		}[category];
	}

	private parseSize(size: string | undefined) {
		if (!size) return 0;

		const units = {
			TiB: 1024 ** 4,
			GiB: 1024 ** 3,
			MiB: 1024 ** 2,
			KiB: 1024,
			B: 1,
		};

		const [sizeStr, unit] = size.split(" ");
		const sizeNum = Number(sizeStr);

		if (!sizeNum || !units[unit]) return 0;

		return Math.ceil(sizeNum * units[unit]);
	}
}
