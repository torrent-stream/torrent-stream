import { type NextRequest, NextResponse } from "next/server";
import { tryParseEnum } from "../../../../../lib/enum";
import { isImdbId } from "../../../../../lib/imdb";
import { getStremioStreams } from "../../../../../lib/search";
import { TorrentCategory } from "../../../../../lib/search/types";

export async function GET(
	request: NextRequest,
	context: RouteContext<"/api/stream/[category]/[id]">,
) {
	const { category: categoryString, id } = await context.params;

	const category = tryParseEnum(TorrentCategory, categoryString);

	if (!category) {
		return NextResponse.json({ error: "Invalid category." }, { status: 400 });
	}

	const [imdbId, seasonStr, episodeStr] = id.replace(/.json$/, "").split(":");

	if (!isImdbId(imdbId)) {
		return NextResponse.json(
			{ error: "Invalid IMDb ID format." },
			{ status: 400 },
		);
	}

	const season = seasonStr ? Number(seasonStr) : undefined;
	if (season !== undefined && (Number.isNaN(season) || season < 1)) {
		return NextResponse.json(
			{ error: "Invalid season number." },
			{ status: 400 },
		);
	}

	const episode = episodeStr ? Number(episodeStr) : undefined;
	if (episode !== undefined && (Number.isNaN(episode) || episode < 1)) {
		return NextResponse.json(
			{ error: "Invalid episode number." },
			{ status: 400 },
		);
	}

	const protocol =
		request.headers.get("x-forwarded-proto") ||
		request.nextUrl.protocol.replace(":", "");
	const host = request.headers.get("host");

	const endpoint = `${protocol}://${host}/api/file`;

	const streams = await getStremioStreams(
		endpoint,
		category,
		imdbId,
		season,
		episode,
	);

	return NextResponse.json({ streams });
}
