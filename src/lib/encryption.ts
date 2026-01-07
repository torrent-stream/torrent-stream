import { createCipheriv, createDecipheriv, createHash } from "node:crypto";
import { brotliCompressSync, brotliDecompressSync } from "node:zlib";
import { config } from "./config";

const { encryptionAlgo, encryptionKey } = config;

const key = createHash("sha256").update(encryptionKey).digest();
const iv = Buffer.alloc(16, 0);

export const encryptText = (text: string): string => {
	const compressed = brotliCompressSync(text);
	const cipher = createCipheriv(encryptionAlgo, key, iv);
	const encrypted = Buffer.concat([cipher.update(compressed), cipher.final()]);
	return encrypted.toString("base64");
};

export const decryptText = (encryptedBase64: string): string => {
	const encrypted = Buffer.from(encryptedBase64, "base64");
	const decipher = createDecipheriv(encryptionAlgo, key, iv);
	const decrypted = Buffer.concat([
		decipher.update(encrypted),
		decipher.final(),
	]);
	return brotliDecompressSync(decrypted).toString();
};
