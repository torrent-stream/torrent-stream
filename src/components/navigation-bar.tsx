import { Menu } from "lucide-react";
import Link from "next/link";
import type { ComponentProps } from "react";
import { Button } from "@/components/ui/button";
import {
	NavigationMenu,
	NavigationMenuItem,
	NavigationMenuLink,
	NavigationMenuList,
	navigationMenuTriggerStyle,
} from "@/components/ui/navigation-menu";
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet";
import { StremioIcon } from "./stremio-icon";

export function NavigationBar({ host }: { host: string | null }) {
	return (
		<nav className="bg-card border-b">
			<div className="container flex items-center justify-between mx-auto px-6 py-4">
				<Logo />

				<div className="flex items-center gap-3">
					{/* <NavMenu className="hidden md:block" /> */}

					{host && <AddonButton host={host} />}

					{/* <div className="md:hidden">
						<NavigationSheet />
					</div> */}
				</div>
			</div>
		</nav>
	);
}

function Logo() {
	return <div className="mx-2 font-extrabold text-xl">torrent-stream.</div>;
}

function AddonButton({ host }: { host: string }) {
	return (
		<Button
			className="bg-[#5a4cad] hover:bg-[#695dac] text-white rounded-full"
			nativeButton={false}
			render={
				<Link href={`stremio://${host}/api/manifest.json`}>
					<StremioIcon />
					<span className="hidden md:inline">Install Stremio Addon</span>
					<span className="md:hidden">Install</span>
				</Link>
			}
		/>
	);
}

function NavMenu(props: ComponentProps<typeof NavigationMenu>) {
	return (
		<NavigationMenu {...props} data-orientation={props.orientation}>
			<NavigationMenuList
				data-orientation={props.orientation}
				className="data-[orientation=vertical]:flex-col data-[orientation=vertical]:items-start data-[orientation=vertical]:justify-start data-[orientation=vertical]:gap-2"
			>
				<NavigationMenuItem>
					<NavigationMenuLink
						className={navigationMenuTriggerStyle()}
						render={<Link href="/torrents" />}
					>
						Torrents
					</NavigationMenuLink>
				</NavigationMenuItem>
				<NavigationMenuItem>
					<NavigationMenuLink
						className={navigationMenuTriggerStyle()}
						render={<Link href="/settings" />}
					>
						Settings
					</NavigationMenuLink>
				</NavigationMenuItem>
			</NavigationMenuList>
		</NavigationMenu>
	);
}

function NavigationSheet() {
	return (
		<Sheet>
			<SheetTrigger
				render={
					<Button variant="outline" size="icon" className="rounded-full" />
				}
			>
				<Menu />
			</SheetTrigger>
			<SheetContent className="px-6 py-4.5">
				<Logo />
				<NavMenu
					orientation="vertical"
					className="mt-6 flex-col [&>div]:h-full"
				/>
			</SheetContent>
		</Sheet>
	);
}
