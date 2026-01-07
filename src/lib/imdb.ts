import { config } from "./config";

export function isImdbId(str: string) {
	return /ev\d{7}\/\d{4}(-\d)?|(ch|co|ev|nm|tt)\d{7}/.test(str);
}

export async function getTitle(imdbId: string, language?: string) {
	try {
		const response = await fetch(`https://www.imdb.com/title/${imdbId}`, {
			headers: {
				"Accept-Language": language || "",
			},
			signal: AbortSignal.timeout(config.webRequestTimeout),
		});
		const data = await response.text();
		const title = data.match(/<title>(.*?)<\/title>/)?.[1];
		return title?.split(" (")[0];
	} catch {
		return undefined;
	}
}

export async function getTitles(imdbId: string) {
	const titles = new Set<string>();
	(await Promise.all([getTitle(imdbId), getTitle(imdbId, "en")])).forEach(
		(title) => {
			if (title) titles.add(title);
		},
	);
	return [...titles];
}
