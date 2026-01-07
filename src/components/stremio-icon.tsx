export function StremioIcon() {
	return (
		<svg
			xmlns="http://www.w3.org/2000/svg"
			viewBox="0 0 124.926 124.926"
			width={124.926}
			height={124.926}
		>
			<title>Stremio Icon</title>
			<defs>
				<mask id="playButtonMask">
					<rect
						width={88.336}
						height={88.336}
						fill="white"
						rx={16}
						transform="rotate(45 31.231 75.4)"
					/>
					<path
						fill="black"
						d="M83.389 61.658a1 1 0 0 1 0 1.611L54.75 84.334a1 1 0 0 1-1.592-.806V41.399a1 1 0 0 1 1.592-.806Z"
					/>
				</mask>
			</defs>
			<g transform="translate(.001)" mask="url(#playButtonMask)">
				<rect
					width={88.336}
					height={88.336}
					fill="white"
					rx={6}
					transform="rotate(45 31.231 75.4)"
				/>
			</g>
		</svg>
	);
}
