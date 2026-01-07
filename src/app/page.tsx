import { TorrentList } from "@/components/torrent-list";
import { getTorrents } from "./actions";

export default async function TorrentsPage() {
	const torrents = await getTorrents();
	return <TorrentList torrents={torrents} />;
}
