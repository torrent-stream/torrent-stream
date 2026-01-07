import { type NextRequest, NextResponse } from "next/server";

export function GET(_request: NextRequest) {
	return NextResponse.json({
		id: "community.torrent-stream",
		version: "2.0.0",
		name: "Torrent Stream",
		description:
			"This addon enables Stremio to stream movies and shows from torrents",
		//"icon": "URL to 256x256 monochrome png icon",
		//"background": "URL to 1024x786 png/jpg background",
		types: ["movie", "series"],
		resources: ["stream"],
		catalogs: [],
		idPrefixes: ["tt"],
	});
}
