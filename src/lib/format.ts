export enum TorrentFormat {
	"4K" = "4k",
	"1080p" = "1080p",
	"720p" = "720p",
	DolbyVision = "dv",
	HDR = "hdr",
	BD50 = "bd50",
	BD25 = "bd25",
	UHDBluRay = "uhdbluray",
	BluRay = "bluray",
	Remux = "remux",
	Web = "web",
	DVD9 = "dvd9",
	DVD5 = "dvd5",
	DVD = "dvd",
	HDTV = "hdtv",
	SDTV = "sdtv",
	Screener = "screener",
	Cam = "cam",
	"3D" = "3d",
	Unknown = "unknown",
	AV1 = "av1",
	HEVC = "hevc",
	AVC = "avc",
	DivX = "divx",
	Xvid = "xvid",
}

const encodingFormats = new Set([
	TorrentFormat.AV1,
	TorrentFormat.HEVC,
	TorrentFormat.AVC,
	TorrentFormat.DivX,
	TorrentFormat.Xvid,
]);

const formatStrings: { [key in TorrentFormat]: string } = {
	[TorrentFormat["4K"]]: "4K",
	[TorrentFormat["1080p"]]: "1080p",
	[TorrentFormat["720p"]]: "720p",
	[TorrentFormat.DolbyVision]: "Dolby Vision",
	[TorrentFormat.HDR]: "HDR",
	[TorrentFormat.BD50]: "BD50",
	[TorrentFormat.BD25]: "BD25",
	[TorrentFormat.UHDBluRay]: "UHD BluRay",
	[TorrentFormat.BluRay]: "BluRay",
	[TorrentFormat.Remux]: "Remux",
	[TorrentFormat.Web]: "Web",
	[TorrentFormat.DVD9]: "DVD9",
	[TorrentFormat.DVD5]: "DVD5",
	[TorrentFormat.DVD]: "DVD",
	[TorrentFormat.HDTV]: "HDTV",
	[TorrentFormat.SDTV]: "SDTV",
	[TorrentFormat.Screener]: "Screener",
	[TorrentFormat.Cam]: "Cam",
	[TorrentFormat["3D"]]: "3D",
	[TorrentFormat.Unknown]: "Unknown",
	[TorrentFormat.AV1]: "AV1",
	[TorrentFormat.HEVC]: "HEVC",
	[TorrentFormat.AVC]: "AVC",
	[TorrentFormat.DivX]: "DivX",
	[TorrentFormat.Xvid]: "Xvid",
};

export function getFormats(name: string) {
	const normalized = name.replace(/\W/g, " ").toLowerCase();
	const split = normalized.split(" ");

	let score = 0;
	const formats: TorrentFormat[] = [];

	if (split.includes("2160p") || normalized.includes("bd50")) {
		formats.push(TorrentFormat["4K"]);
		score += 3000;
	} else if (split.includes("1080p") || normalized.includes("bd25")) {
		formats.push(TorrentFormat["1080p"]);
		score += 2000;
	} else if (split.includes("720p")) {
		formats.push(TorrentFormat["720p"]);
		score += 1000;
	}

	if (
		(split.includes("dolby") && split.includes("vision")) ||
		split.includes("dovi") ||
		split.includes("dv")
	) {
		formats.push(TorrentFormat.DolbyVision);
		score += 20;
	} else if (split.includes("hdr")) {
		formats.push(TorrentFormat.HDR);
		score += 10;
	}

	if (normalized.includes("bd50")) {
		formats.push(TorrentFormat.BD50);
		score += 650;
	} else if (normalized.includes("bd25")) {
		formats.push(TorrentFormat.BD25);
		score += 600;
	} else if (
		split.includes("bluray") ||
		(split.includes("blu") && split.includes("ray")) ||
		split.includes("bdrip") ||
		split.includes("brrip")
	) {
		if (!formats.includes(TorrentFormat["4K"]) && split.includes("uhd")) {
			formats.push(TorrentFormat.UHDBluRay);
			score += 510;
		} else {
			formats.push(TorrentFormat.BluRay);
			score += 500;
		}

		if (split.includes("remux")) {
			formats.push(TorrentFormat.Remux);
			score += 50;
		}
	} else if (
		split.includes("webrip") ||
		split.includes("webdl") ||
		split.includes("web")
	) {
		formats.push(TorrentFormat.Web);
		score += 400;
	} else if (normalized.includes("dvd9")) {
		formats.push(TorrentFormat.DVD9);
		score += 320;
	} else if (normalized.includes("dvd5") || split.includes("dvdr")) {
		formats.push(TorrentFormat.DVD5);
		score += 310;
	} else if (split.includes("dvdrip")) {
		formats.push(TorrentFormat.DVD);
		score += 300;
	} else if (split.includes("hdtv") || split.includes("hdtvrip")) {
		formats.push(TorrentFormat.HDTV);
		score += 200;
	} else if (split.includes("sdtv") || split.includes("tvrip")) {
		formats.push(TorrentFormat.SDTV);
		score += 100;
	} else if (
		split.includes("scr") ||
		split.includes("dvdscr") ||
		split.includes("webscr") ||
		split.includes("bdscr") ||
		split.includes("screener") ||
		split.includes("dvdscreener") ||
		split.includes("webscreener") ||
		split.includes("bdscreener")
	) {
		formats.push(TorrentFormat.Screener);
		score -= 4000;
	} else if (
		split.includes("cam") ||
		split.includes("hdcam") ||
		split.includes("camrip") ||
		split.includes("ts") ||
		split.includes("hdts") ||
		split.includes("telesync") ||
		split.includes("tc") ||
		split.includes("hdtc") ||
		split.includes("telecine")
	) {
		formats.push(TorrentFormat.Cam);
		score -= 5000;
	}

	if (split.includes("3d")) {
		formats.push(TorrentFormat["3D"]);
		score -= 1;
	}

	if (split.includes("av1")) {
		formats.push(TorrentFormat.AV1);
	}

	if (
		split.includes("hevc") ||
		split.includes("h265") ||
		split.includes("x265")
	) {
		formats.push(TorrentFormat.HEVC);
	}

	if (
		split.includes("avc") ||
		split.includes("h264") ||
		split.includes("x264")
	) {
		formats.push(TorrentFormat.AVC);
	}

	if (split.includes("divx")) {
		formats.push(TorrentFormat.DivX);
	}

	if (split.includes("xvid")) {
		formats.push(TorrentFormat.Xvid);
	}

	const qualityFormats = formats.filter(
		(format) => !encodingFormats.has(format),
	);

	if (qualityFormats.length === 0) {
		formats.push(TorrentFormat.Unknown);
		qualityFormats.push(TorrentFormat.Unknown);
		score = -Infinity;
	}

	const quality = qualityFormats.map((f) => formatStrings[f]).join(" ");

	return { formats, quality, score };
}
