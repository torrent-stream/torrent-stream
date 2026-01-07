export class LRU<K, V> {
	map = new Map<K, V>();
	capacity: number;

	constructor(capacity: number) {
		if (capacity < 0) {
			throw new Error("LRU capacity can't be negative");
		}

		this.capacity = capacity;
	}

	has(key: K) {
		return this.map.has(key);
	}

	put(key: K, value: V) {
		// If updating existing item, move to the end (Most Recently Used)
		if (this.map.has(key)) {
			this.map.delete(key);
			this.map.set(key, value);
			return undefined;
		}

		// Check if we need to evict an item
		let evicted: K | undefined;
		if (this.map.size >= this.capacity) {
			// The first key in a Map is the oldest (Least Recently Used)
			const { value, done } = this.map.keys().next();
			if (!done) {
				evicted = value;
				this.map.delete(evicted);
			}
		}

		// Add new chunk (goes to the end of the Map)
		this.map.set(key, value);

		return evicted;
	}

	peek(key: K) {
		return this.map.get(key);
	}

	get(key: K) {
		const value = this.peek(key);
		if (value === undefined) return undefined;

		// Delete and re-set to move this item to the end (Most Recently Used)
		this.map.delete(key);
		this.map.set(key, value);

		return value;
	}

	clear() {
		const evicted = this.map.keys().toArray();
		this.map.clear();
		return evicted;
	}

	updateCapacity(capacity: number) {
		if (capacity < 0) {
			throw new Error("LRU capacity can't be negative");
		}

		this.capacity = capacity;

		const evicted: K[] = [];

		while (this.map.size > this.capacity) {
			const { value, done } = this.map.keys().next();
			if (done) break;
			evicted.push(value);
			this.map.delete(value);
		}

		return evicted;
	}
}
