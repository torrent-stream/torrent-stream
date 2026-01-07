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

enum InsaneCategory {
	Film_Hun_SD = 41,
	Film_Hun_HD = 27,
	Film_Hun_UHD = 44,
	Film_Eng_SD = 42,
	Film_Eng_HD = 25,
	Film_Eng_UHD = 45,
	Sorozat_Hun = 8,
	Sorozat_Hun_HD = 40,
	Sorozat_Hun_UHD = 47,
	Sorozat_Eng = 7,
	Sorozat_Eng_HD = 39,
	Sorozat_Eng_UHD = 46,
}

type InsaneCredentials = {
	username: string;
	password: string;
};

export class InsaneProvider extends TorrentSearchProvider {
	id = "insane";
	name = "iNSANE";

	private fetch = makeFetchCookie(fetch);
	private lastLoginCredentials?: InsaneCredentials;
	private lastLoginDate?: number;

	async searchTorrentsByCategory(query: string, category: TorrentCategory) {
		switch (category) {
			case TorrentCategory.Movie:
				return this.searchTorrents(query, [
					InsaneCategory.Film_Hun_SD,
					InsaneCategory.Film_Eng_SD,
					InsaneCategory.Film_Hun_HD,
					InsaneCategory.Film_Eng_HD,
					InsaneCategory.Film_Hun_UHD,
					InsaneCategory.Film_Eng_UHD,
				]);
			case TorrentCategory.Series:
				return this.searchTorrents(query, [
					InsaneCategory.Sorozat_Hun,
					InsaneCategory.Sorozat_Eng,
					InsaneCategory.Sorozat_Hun_HD,
					InsaneCategory.Sorozat_Eng_HD,
					InsaneCategory.Sorozat_Hun_UHD,
					InsaneCategory.Sorozat_Eng_UHD,
				]);
		}
	}

	async searchTorrents(
		query: string,
		insaneCategories: InsaneCategory[],
		insaneCredentials?: InsaneCredentials,
	) {
		const torrents: TorrentSearchResult[] = [];

		try {
			const credentials = insaneCredentials ?? this.getCredentials();
			await this.login(credentials);

			const imdb = isImdbId(query) ? query : undefined;

			let page = 0;

			while (page <= 5) {
				let torrentsOnPage = 0;

				const params = new URLSearchParams({
					page: page.toString(),
					search: query,
					searchsort: "normal",
					searchtype: "desc",
					torart: "tor",
				});

				for (const category of insaneCategories) {
					params.append("cat[]", category.toString());
				}

				const link = `https://newinsane.info/browse.php?${params.toString()}}`;
				const torrentsPage = await this.fetch(link, {
					signal: AbortSignal.timeout(config.webRequestTimeout),
				});
				const $ = cheerio.load(await torrentsPage.text());

				for (const el of $("tr.torrentrow")) {
					torrentsOnPage++;

					const tracker = "iNSANE";
					const name = $(el).find("a.torrentname").attr("title");
					const category = this.parseCategory(
						$(el).find("td.caticon > a > img").attr("title"),
					);
					const size = this.parseSize($(el).find("td.size").text());
					const seeds = Number($(el).find("td.data > a:nth-of-type(1)").text());
					const peers = Number($(el).find("td.data > a:nth-of-type(2)").text());
					const torrent = $(el).find("a.downloadicon").attr("href");

					if (!name || !torrent) continue;

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

				if (torrentsOnPage < 25) break;

				page++;
			}
		} catch (error) {
			logger.error(error);
		}

		return torrents;
	}

	private async login(credentials: InsaneCredentials) {
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
		formData.append("username", credentials.username);
		formData.append("password", credentials.password);

		await this.fetch("https://newinsane.info/login.php", {
			method: "POST",
			body: formData,
			signal: AbortSignal.timeout(config.webRequestTimeout),
		});

		this.lastLoginCredentials = credentials;
		this.lastLoginDate = Date.now();
	}

	private getCredentials() {
		const { insaneUser, insanePass } = config;

		if (!insaneUser || !insanePass) {
			throw new Error(
				"iNSANE credentials are not set in environment variables",
			);
		}

		return {
			username: insaneUser,
			password: insanePass,
		};
	}

	private parseCategory(category: string | undefined) {
		if (!category) return undefined;
		const categories: Record<string, string> = {
			"Film/Hun/SD": "Movies/SD/HU",
			"Film/Hun/HD": "Movies/HD/HU",
			"Film/Hun/UHD": "Movies/UHD/HU",
			"Film/Eng/SD": "Movies/SD/EN",
			"Film/Eng/HD": "Movies/HD/EN",
			"Film/Eng/UHD": "Movies/UHD/EN",
			"Sorozat/Hun": "TV/SD/HU",
			"Sorozat/Hun/HD": "TV/HD/HU",
			"Sorozat/Hun/UHD": "TV/UHD/HU",
			"Sorozat/Eng": "TV/SD/EN",
			"Sorozat/Eng/HD": "TV/HD/EN",
			"Sorozat/Eng/UHD": "TV/UHD/EN",
		};
		return categories[category];
	}

	private parseSize(sizeStr: string) {
		const size = sizeStr.replace(",", ".").trim();
		let bytes = 0;
		if (size.endsWith("TiB"))
			bytes = (Number(size.replace("TiB", "")) || 0) * 1024 ** 4;
		else if (size.endsWith("GiB"))
			bytes = (Number(size.replace("GiB", "")) || 0) * 1024 ** 3;
		else if (size.endsWith("MiB"))
			bytes = (Number(size.replace("MiB", "")) || 0) * 1024 ** 2;
		else if (size.endsWith("KiB"))
			bytes = (Number(size.replace("KiB", "")) || 0) * 1024;
		else if (size.endsWith("B")) bytes = Number(size.replace("B", "")) || 0;
		return Math.ceil(bytes);
	}
}
