import { createServer } from "node:http";
import { parse } from "node:url";
import next from "next";
import { config } from "./lib/config";
import { logger } from "./lib/logger";

const { port, dev } = config;
const app = next({ dev });
const handle = app.getRequestHandler();

app.prepare().then(() => {
	createServer((req, res) => {
		const parsedUrl = parse(req.url as string, true);
		handle(req, res, parsedUrl);
	}).listen(port);
	logger.info(`Server listening at http://localhost:${port}`);
});
