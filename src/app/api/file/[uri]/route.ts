import { type NextRequest, NextResponse } from "next/server";
import { decryptText } from "../../../../lib/encryption";
import { getStreamingMimeType } from "../../../../lib/file";
import { getOrAddTorrent, getReadableStream } from "../../../../lib/torrent";

export async function GET(
	request: NextRequest,
	context: RouteContext<"/api/file/[uri]">,
) {
	const { uri } = await context.params;

	const streamId = request.nextUrl.searchParams.get("s");
	if (!streamId) {
		return NextResponse.json({ error: "Missing stream ID." }, { status: 400 });
	}

	const fileIndex = Number(request.nextUrl.searchParams.get("f"));
	if (Number.isNaN(fileIndex) || fileIndex < 0) {
		return NextResponse.json({ error: "Missing file index." }, { status: 400 });
	}

	const torrent = await getOrAddTorrent(decryptText(uri));
	if (!torrent) {
		return NextResponse.json(
			{ error: "Failed to add torrent." },
			{ status: 500 },
		);
	}

	const file = torrent.files[fileIndex];
	if (!file) {
		return NextResponse.json(
			{ error: "File not found in torrent." },
			{ status: 404 },
		);
	}

	const rangeHeader = request.headers.get("range");

	const rangeValue = rangeHeader
		? rangeHeader
				.replace(/bytes=/, "")
				.split(",")[0]
				.trim()
		: "0-";

	const positions = rangeValue.split("-");

	let start = Number(positions[0]);
	if (Number.isNaN(start)) {
		start = 0;
	}

	if (start >= file.length) {
		return new NextResponse(null, {
			status: 416,
			headers: { "Content-Range": `bytes */${file.length}` },
		});
	}

	let end = positions[1] ? Number(positions[1]) : file.length - 1;
	if (Number.isNaN(end) || end >= file.length) {
		end = file.length - 1;
	}

	if (end - start > torrent.pieceLength) {
		end = start + torrent.pieceLength;
	}

	if (request.signal.aborted) {
		return new NextResponse(null, { status: 499 });
	}

	const stream = getReadableStream(streamId, torrent, file, start, end);

	const headers = new Headers({
		Connection: "keep-alive",
		"Keep-Alive": `timeout=60, max=10000`,
		"Accept-Ranges": "bytes",
		"Content-Range": `bytes ${start}-${end}/${file.length}`,
		"Content-Length": `${end - start + 1}`,
		"Content-Type": getStreamingMimeType(file.name),
		"Content-Disposition": `inline; filename*=UTF-8''${encodeURIComponent(file.name)}`,
	});

	return new NextResponse(stream, { status: 206, headers });
}
