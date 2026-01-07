"use client";

import { cn } from "@/lib/utils";

function RollingNumber({ digit }: { digit: string }) {
	const yOffset = Number(digit) * -10;
	return (
		<div className="relative inline-grid overflow-hidden">
			<span className="col-start-1 row-start-1 opacity-0 pointer-events-none">
				{digit}
			</span>
			<div className="col-start-1 row-start-1 absolute inset-0 flex flex-col items-center">
				<div
					className="flex flex-col w-full h-[1000%] transition-transform duration-500 ease-[cubic-bezier(0.25,1,0.5,1)]"
					style={{ transform: `translateY(${yOffset}%)` }}
				>
					{[0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map((num) => (
						<span
							key={num}
							className="h-[10%] flex items-center justify-center"
						>
							{num}
						</span>
					))}
				</div>
			</div>
		</div>
	);
}

export function FlipNumber({
	children,
	className,
}: {
	children: string | number;
	className?: string;
}) {
	const characters = children.toString().split("");
	return (
		<div className={cn("inline-flex items-baseline whitespace-pre", className)}>
			{characters.map((char, index) => {
				const isDigit = /^[0-9]$/.test(char);
				if (isDigit) {
					// biome-ignore lint/suspicious/noArrayIndexKey: it's a string
					return <RollingNumber key={index} digit={char} />;
				}
				// biome-ignore lint/suspicious/noArrayIndexKey: it's a string
				return <span key={index}>{char}</span>;
			})}
		</div>
	);
}
