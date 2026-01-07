export function isEnumValid<T extends object>(
	enumObj: T,
	value: unknown,
): value is T[keyof T] {
	return Object.values(enumObj).includes(value);
}

export function tryParseEnum<T extends object>(
	enumObj: T,
	value: unknown,
	fallback?: T[keyof T],
) {
	if (isEnumValid(enumObj, value)) {
		return value;
	}

	if (fallback !== undefined) {
		return fallback;
	}
}

export function parseEnum<T extends object>(
	enumObj: T,
	value: unknown,
	fallback?: T[keyof T],
) {
	const parsedValue = tryParseEnum(enumObj, value, fallback);
	if (parsedValue !== undefined) {
		return parsedValue;
	}

	const allowedValues = Object.values(enumObj).join(", ");
	throw new Error(
		`Invalid value '${value}'. Expected one of: [${allowedValues}]`,
	);
}
