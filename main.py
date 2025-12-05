import os
from urllib.parse import urlparse

from crawler import Crawler, determine_auto_threads
from db import DB_FILENAME


def main() -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, DB_FILENAME)

    print("=== Terminal Web Crawler ===")

    # Crawl delay
    while True:
        delay_input = input("Crawl delay between requests (seconds, default 1.0): ").strip()
        if not delay_input:
            delay = 1.0
            break
        try:
            delay = float(delay_input)
            if delay < 0:
                raise ValueError
            break
        except ValueError:
            print("Please enter a non-negative number.")

    # Worker count
    auto_threads = determine_auto_threads()
    while True:
        th_input = input(
            f"Number of worker threads (0 = auto [{auto_threads}]): "
        ).strip()
        if not th_input:
            num_workers = auto_threads
            break
        try:
            n = int(th_input)
            if n < 0:
                raise ValueError
            num_workers = auto_threads if n == 0 else max(1, n)
            break
        except ValueError:
            print("Please enter a non-negative integer.")

    crawler = Crawler(db_path=db_path, num_workers=num_workers, delay_seconds=delay)

    # Startup menu: let user decide what to do with seeds / pending/paused work
    while True:
        pending, visited, paused, error = crawler.get_status_counts()
        print(
            f"\nCurrent DB state -> Pending: {pending}, Visited: {visited}, "
            f"Paused: {paused}, Error: {error}"
        )
        print(
            "\nStartup options:\n"
            "  1) Add new seed URL(s)\n"
            "  2) Resume ALL paused URLs and start crawling\n"
            "  3) Resume specific paused URL(s) and start crawling\n"
            "  4) Pause a URL\n"
            "  5) List pending URLs by prefix\n"
            "  6) Pause URLs by prefix (before starting)\n"
            "  7) Show crawler stats\n"
            "  8) Exit\n"
        )
        choice = input("Select an option (1-8): ").strip()

        if choice == "1":
            # Add a single seed URL, then return to menu
            seed = input("Enter seed URL (leave blank to cancel): ").strip()
            if seed:
                crawler.add_seed_url(seed)
            # Loop back to show updated counts and options again
        elif choice == "2":
            # Resume paused URLs (only for main domain if detected)
            if getattr(crawler, 'main_domain', None):
                print(f"Resuming paused URLs for main domain: {crawler.main_domain}")
                resumed = crawler.resume_paused_for_domain(crawler.main_domain)
            else:
                resumed = crawler.resume_all_paused()
            if pending == 0 and resumed == 0:
                confirm = input(
                    "There are currently no pending or paused URLs. Start anyway? (y/N): "
                ).strip().lower()
                if confirm != "y":
                    continue
            crawler.start_workers()
            print("Crawler started. Type 'help' for commands.")
            break
        elif choice == "3":
            print("Note: 'Resume specific' will only resume URLs with status 'paused'. Pending URLs are already queued.")

            # Ensure we have a main domain to filter by. Try crawler.main_domain first,
            # then try to infer from DB earliest URL, otherwise prompt the user.
            md = getattr(crawler, 'main_domain', None)
            if not md:
                try:
                    first = crawler.db.get_earliest_url()
                    if first:
                        p = urlparse(first)
                        host = p.netloc.lower()
                        if host.startswith('www.'):
                            host = host[4:]
                        md = host
                        crawler.main_domain = md
                        print(f"Inferred main domain from DB: {md}")
                except Exception:
                    md = None

            if not md:
                ans = input("No main domain detected. Enter main domain to filter, or leave blank to show all paused: ").strip()
                if ans:
                    md = ans.lower()
                    if md.startswith('www.'):
                        md = md[4:]
                    crawler.main_domain = md

            try:
                all_paused = crawler.list_paused_urls()
            except Exception as e:
                print(f"Error retrieving paused URLs: {e}")
                continue

            paused_urls = all_paused

            # If we have a main_domain, filter paused URLs to that domain for display
            if md:
                filtered = []
                for u in all_paused:
                    try:
                        h = urlparse(u).netloc.lower()
                        if h.startswith('www.'):
                            h = h[4:]
                    except Exception:
                        continue
                    if h == md or h.endswith('.' + md):
                        filtered.append(u)
                if filtered:
                    paused_urls = filtered
                    print(f"(Showing paused URLs for main domain: {md})")
                else:
                    # No paused URLs for main domain — automatically show all paused URLs
                    # so the user can resume items they paused on other domains.
                    if all_paused:
                        paused_urls = all_paused
                        print("(No paused URLs for main domain; showing ALL paused URLs)")
                    else:
                        print("\nThere are no paused URLs to resume.")
                        print()
                        continue

            if not paused_urls:
                print("\nThere are no paused URLs to resume.")
                print()  # Add blank line for clarity
                continue

            print(f"\nFound {len(paused_urls)} paused URL(s):")
            print("Paused URLs:")
            for idx, url in enumerate(paused_urls, start=1):
                print(f"  {idx}) {url}")
            print()  # Add blank line before prompt

            sel = input(
                "\nEnter number(s) to resume (comma-separated, or 'all' to resume all): "
            ).strip().lower()

            resumed_count = 0
            if sel == "all":
                for url in paused_urls:
                    crawler.resume_url(url)
                    resumed_count += 1
                print(f"\nResumed {resumed_count} URL(s).")
            else:
                parts = [p.strip() for p in sel.split(",") if p.strip()]
                if not parts:
                    print("No valid selection entered. Returning to menu.")
                    continue
                for p in parts:
                    if not p.isdigit():
                        print(f"Skipping invalid selection: {p!r}")
                        continue
                    i = int(p)
                    if 1 <= i <= len(paused_urls):
                        crawler.resume_url(paused_urls[i - 1])
                        resumed_count += 1
                    else:
                        print(f"Index out of range: {i}")
                if resumed_count > 0:
                    print(f"\nResumed {resumed_count} URL(s).")

            if resumed_count > 0:
                crawler.start_workers()
                print("Crawler started. Type 'help' for commands.")
                break
            else:
                print("No URLs were resumed. Returning to menu.")
        elif choice == "4":
            # Pause a URL
            url_to_pause = input("Enter URL to pause (leave blank to cancel): ").strip()
            if url_to_pause:
                crawler.pause_url(url_to_pause)
                print()  # Add blank line for clarity
            # Loop back to show updated counts and options again
        elif choice == "5":
            # List pending URLs by prefix
            prefix = input("Enter URL prefix to list pending (leave blank to cancel): ").strip()
            if prefix:
                # Show a few examples and count
                try:
                    pending_urls = [u for u in crawler.db.load_pending_urls() if u.startswith(prefix)]
                except Exception:
                    pending_urls = []
                print(f"Found {len(pending_urls)} pending URL(s) with prefix {prefix!r}:")
                for u in pending_urls[:20]:
                    print(f"  {u}")
                if len(pending_urls) > 20:
                    print("  ... (truncated)")
                print()
            # Loop back
        elif choice == "6":
            # Pause URLs by prefix before starting
            prefix = input("Enter URL prefix to pause (leave blank to cancel): ").strip()
            if prefix:
                crawler.pause_prefix(prefix)
                print()  # blank line
            # Loop back
        elif choice == "7":
            crawler.print_stats()
            # Loop back to menu after showing stats
            continue
        elif choice == "8":
            print("Exiting without starting crawler.")
            return
        else:
            print("Invalid option, please choose a number between 1 and 7.")

    # Command loop (runs after workers have started)
    try:
        while True:
            try:
                cmd_line = input("> ").strip()
            except EOFError:
                # Treat EOF as stop
                break

            if not cmd_line:
                continue

            parts = cmd_line.split(maxsplit=1)
            cmd = parts[0].lower()
            arg = parts[1] if len(parts) > 1 else ""

            if cmd in ("quit", "stop", "exit"):
                crawler.stop()
                break
            elif cmd == "help":
                print(
                    "Commands:\n"
                    "  seed <url>            - add a new seed URL (page or sitemap)\n"
                    "  pause <url>           - pause a single URL\n"
                    "  pause-prefix <prefix> - pause all pending URLs with given prefix\n"
                    "  resume <url>          - resume a paused URL\n"
                    "  resume-prefix <prefix>- resume paused URLs with prefix\n"
                    "  stats                 - show crawler statistics and top paused domains\n"
                    "  status                - show worker & URL counts\n"
                    "  stop / quit           - save state and exit\n"
                    "  help                  - show this help\n"
                )
            elif cmd == "seed":
                if not arg:
                    print("Usage: seed <url>")
                    continue
                crawler.add_seed_url(arg)
            elif cmd == "pause":
                if not arg:
                    print("Usage: pause <url>")
                    continue
                crawler.pause_url(arg)
            elif cmd == "pause-prefix":
                if not arg:
                    print("Usage: pause-prefix <prefix>")
                    continue
                crawler.pause_prefix(arg)
            elif cmd == "resume":
                if not arg:
                    print("Usage: resume <url>")
                    continue
                crawler.resume_url(arg)
            elif cmd == "resume-prefix":
                if not arg:
                    print("Usage: resume-prefix <prefix>")
                    continue
                crawler.resume_prefix(arg)
            elif cmd == "stats":
                crawler.print_stats()
            elif cmd == "status":
                crawler.print_status()
            else:
                print(f"Unknown command: {cmd!r}. Type 'help' for list of commands.")

    except KeyboardInterrupt:
        print("\nKeyboard interrupt received.")
        crawler.stop()


if __name__ == "__main__":
    main()


