#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Google Maps Restaurant Review Removal Scanner
=============================================

Sucht Restaurants in Regensburg auf Google Maps und prueft,
ob Google einen Hinweis auf geloeschte Bewertungen anzeigt.

Methode fuer die Restaurantliste:
  Network-Interception - faengt Googles interne Such-API-Antworten ab
  und extrahiert Place-IDs direkt aus dem JSON. Kein fragiles DOM-Scraping
  der Suchliste, kein API-Key noetig.

Ablauf:
  - search_page: scrollt die Ergebnisliste, sammelt Place-IDs per Network-Interception
  - parse_page:  analysiert jede Place-ID sofort parallel zum Scrollen
  - Enter-Druck beendet das Scrollen; laufende Analysen werden noch abgeschlossen
  - Nach dem Scrollen: Duplikate entfernen, sortieren, CSV speichern

OUTPUT:
- <Datum>_<Suchbegriff>.csv

VORAUSSETZUNGEN:
----------------
Python 3.10+

INSTALLATION:
--------------
pip install playwright pandas
playwright install

START:
-------
python schoodle.py
"""

import asyncio
import json
import re
from datetime import datetime

import pandas as pd

from playwright.async_api import async_playwright

SEARCH_QUERY = "feuerwehr"

# Dateiname: Datum_Uhrzeit_Suchbegriff.csv
_query_slug = re.sub(r'[^\w\-]', '_', SEARCH_QUERY)[:50]
_ts = datetime.now().strftime('%Y-%m-%d_%H-%M')
OUTPUT_FILE = f"{_ts}_{_query_slug}.csv"
OUTPUT_FILE_REMOVED = f"{_ts}_{_query_slug}_entfernt.csv"
OUTPUT_FILE_CLEAN = f"{_ts}_{_query_slug}_keine_entfernung.csv"

GOOGLE_MAPS_URL = "https://www.google.com/maps"

# Deutsche Zahlwoerter fuer die Prozentberechnung
_DE_NUMBERS = {
    "ein": 1, "zwei": 2, "drei": 3, "vier": 4, "f\u00fcnf": 5,
    "sechs": 6, "sieben": 7, "acht": 8, "neun": 9, "zehn": 10,
    "elf": 11, "zw\u00f6lf": 12, "zwanzig": 20, "drei\u00dfig": 30,
    "vierzig": 40, "f\u00fcnfzig": 50, "hundert": 100, "zweihundert": 200,
}


def _parse_removed_bounds(removed_str):
    """
    Gibt (lo, hi) als Integer-Tuple zurueck.
    Gibt None zurueck wenn kein Bereich erkennbar.
    """
    if not removed_str or removed_str in ("NEIN", "JA", "ERROR", "KEIN_TAB"):
        return None

    # Numerisch: "151\u2013200" oder "151 bis 200"
    m = re.search(r"(\d[\d\.\,]*)\s*(?:\u2013|bis)\s*(\d[\d\.\,]*)", removed_str, re.IGNORECASE)
    if m:
        lo = int(m.group(1).replace(".", "").replace(",", ""))
        hi = int(m.group(2).replace(".", "").replace(",", ""))
        return (lo, hi)

    # "\u00fcber 250"
    m = re.search(r"(?:\u00fcber|over|more than)\s*(\d+)", removed_str, re.IGNORECASE)
    if m:
        val = int(m.group(1))
        return (val, val)

    # Textform: "Zwei bis f\u00fcnf"
    m = re.search(r"([A-Za-z\u00c4\u00d6\u00dc\u00e4\u00f6\u00fc\u00df]+)\s+bis\s+([A-Za-z\u00c4\u00d6\u00dc\u00e4\u00f6\u00fc\u00df]+)", removed_str, re.IGNORECASE)
    if m:
        lo = _DE_NUMBERS.get(m.group(1).lower())
        hi = _DE_NUMBERS.get(m.group(2).lower())
        if lo and hi:
            return (lo, hi)

    return None


def _save_intermediate(results):
    try:
        pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False, sep=";", encoding="utf-8-sig")
        print(f"    [Zwischenstand: {len(results)} Eintraege gespeichert]")
    except Exception as e:
        print(f"[!] Fehler beim Zwischenspeichern: {e}")


async def collect_place_ids_via_interception(page, place_id_queue, stop_event):
    """
    Pollt den DOM alle 0.5s nach Eintraegen im Google-Maps-Feed.
    Primaere Quelle: data-item-id Attribut (enthaelt direkt die Place-ID ChIJ...).
    Fallback: href-Attribute der Place-Links.
    Stoppt wenn der Nutzer Enter drueckt. Danach wird ein Sentinel (None) gelegt.
    """
    print("[+] Sammle Place-IDs aus dem DOM ...")
    print("[>] Scrolle manuell durch die Liste \u2013 Enter druecken wenn fertig ...")

    seen = set()
    count = 0

    async def scan_dom():
        nonlocal count
        new_this_round = 0
        try:
            # Google Maps Suchergebnis-Karten haben die Klasse "hfpxzc"
            # Die Place-ID steckt als !19s<ChIJ...> im href
            anchors = await page.locator('a.hfpxzc').all()
            for a in anchors:
                try:
                    href = await a.get_attribute("href") or ""
                    m = re.search(r'!19s(ChIJ[A-Za-z0-9_-]{10,})', href)
                    if m:
                        pid = m.group(1)
                        if pid not in seen:
                            seen.add(pid)
                            await place_id_queue.put(pid)
                            count += 1
                            new_this_round += 1
                except Exception:
                    pass
        except Exception:
            pass

        if new_this_round:
            print(f"\r    {count} Eintraege gesammelt ...", end="", flush=True)

    while not stop_event.is_set():
        await scan_dom()
        await asyncio.sleep(0.5)

    # Abschliessender Scan nach Enter-Druck
    await scan_dom()

    print(f"\n[+] {count} Eintraege gesammelt. Laufende Analysen werden abgeschlossen ...")
    await place_id_queue.put(None)  # Sentinel: Parse-Worker beenden


async def parse_restaurant(page, entry):
    """
    Oeffnet Restaurantseite per Place-ID oder direkter URL und extrahiert Infos.
    entry: "ChIJ..."-Place-ID  oder  "url:https://..."-URL
    Primaere Quelle: JSON-LD Structured Data (<script type="application/ld+json">)
    Fallback: DOM-Selektoren und Body-Text.
    """
    if entry.startswith("url:"):
        url = entry[4:]
        place_id = None
    else:
        place_id = entry
        url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"

    for attempt in range(3):
        try:
            await page.goto(url, timeout=60000)
            await page.wait_for_selector("h1", timeout=20000)
            break
        except Exception as e:
            if attempt == 2:
                print(f"[!] Fehler nach 3 Versuchen: {e}")
                return {
                    "place_id": place_id or url,
                    "name": "ERROR",
                    "rating": None,
                    "reviews_count": None,
                    "removed_reviews": None,
                    "removed_reviews_text": None,
                    "maps_url": page.url,
                }
            print(f"[!] Versuch {attempt + 1} fehlgeschlagen, Wiederholung ...")
            await asyncio.sleep(3)

    current_url = page.url

    try:
        # ── 1. Name, Rating, Bewertungsanzahl aus JSON-LD (zuverlaessigste Quelle) ──
        name, rating, reviews_count = None, None, None

        # Namen die auf eine fehlgeschlagene/unvollstaendige Ladung hinweisen
        _INVALID_NAMES = {
            "google maps", "google", "maps", "google maps - route planen",
            "google maps - routenplanung", "google maps - directions",
            "unknown", "",
        }

        def _is_valid_name(n):
            return bool(n) and n.strip().lower() not in _INVALID_NAMES

        try:
            for script in await page.locator('script[type="application/ld+json"]').all():
                try:
                    data = json.loads(await script.inner_text(timeout=3000))
                    candidate = data.get("name", "")
                    if not name and _is_valid_name(candidate):
                        name = candidate
                    if "aggregateRating" in data:
                        ar = data["aggregateRating"]
                        if not rating and ar.get("ratingValue"):
                            rating = str(ar["ratingValue"]).replace(".", ",")
                        if reviews_count is None and ar.get("reviewCount"):
                            reviews_count = int(ar["reviewCount"])
                except Exception:
                    pass
        except Exception:
            pass

        # ── 2. Fallback Name: Seitentitel ──
        if not name:
            try:
                title = await page.evaluate("document.title")
                candidate = title.replace(" - Google Maps", "").replace(" – Google Maps", "").strip()
                if _is_valid_name(candidate):
                    name = candidate
            except Exception:
                pass

        # ── 3. Fallback Name: h1 ──
        if not name:
            try:
                candidate = (await page.locator("h1").first.inner_text(timeout=5000)).strip()
                if _is_valid_name(candidate):
                    name = candidate
            except Exception:
                pass

        name = name or "UNKNOWN"

        # ── 4. Fallback Rating: aria-label beliebiger Sterne-Anzeige ──
        if not rating:
            try:
                for el in await page.locator('[role="img"][aria-label], [aria-label*="Stern"], [aria-label*="star"]').all():
                    label = await el.get_attribute("aria-label") or ""
                    m = re.search(r"([0-9][,\.][0-9])", label)
                    if m:
                        rating = m.group(1)
                        break
            except Exception:
                pass

        # ── 5. Rezensionen-Tab anklicken (fuer geloeschte Bewertungen) ──
        has_reviews_tab = False
        try:
            tab_locator = page.locator(
                'button:has-text("Rezensionen"), '
                'button:has-text("Reviews")'
            ).first
            # Pruefe ob der Tab ueberhaupt im DOM vorhanden ist (kein wait_for – wuerde werfen)
            if await tab_locator.count() > 0:
                has_reviews_tab = True
                await tab_locator.click()
                await page.wait_for_selector(
                    '[data-review-id], [jslog*="review"], .MyEned',
                    timeout=10000,
                )
            else:
                print(f"    [!] Kein Rezensionen-Tab fuer {place_id or url} \u2013 ueberspringe.")
        except Exception:
            pass

        body_text = await page.locator("body").inner_text(timeout=10000)

        # ── 6. Fallback Bewertungsanzahl aus Body-Text ──
        if reviews_count is None:
            try:
                m = re.findall(r"([0-9][0-9\.\,]*)\s+(?:Rezensionen|Bewertungen|Berichte)", body_text)
                if m:
                    reviews_count = int(m[0].replace(".", "").replace(",", ""))
            except Exception:
                pass

        # ── 7. Geloeschte Bewertungen erkennen ──
        removed_reviews = None
        removed_reviews_text = None
        if not has_reviews_tab:
            removed_reviews = "KEIN_TAB"
        else:
            try:
                removal_el = page.locator('[jscontroller="qTKEd"]')
                if await removal_el.count() > 0:
                    hint_text = await removal_el.locator(".fontBodyMedium").first.inner_text(timeout=3000)
                    removed_reviews_text = hint_text.strip()
                    removed_reviews = "JA"

                    m = re.search(r"(\d[\d\.\,]*)\s+bis\s+(\d[\d\.\,]*)", hint_text, re.IGNORECASE)
                    if m:
                        removed_reviews = f"{m.group(1)}\u2013{m.group(2)}"
                    else:
                        m = re.search(r"([A-Za-z\u00c4\u00d6\u00dc\u00e4\u00f6\u00fc\u00df]+)\s+bis\s+([A-Za-z\u00c4\u00d6\u00dc\u00e4\u00f6\u00fc\u00df]+)", hint_text, re.IGNORECASE)
                        if m:
                            removed_reviews = f"{m.group(1)} bis {m.group(2)}"
                        else:
                            m = re.search(r"(\u00fcber|more than|over)\s+(\d+)", hint_text, re.IGNORECASE)
                            if m:
                                removed_reviews = f"\u00fcber {m.group(2)}"
            except Exception:
                pass

        return {
            "place_id": place_id or current_url,
            "name": name,
            "rating": rating,
            "reviews_count": reviews_count,
            "removed_reviews": removed_reviews if removed_reviews else "NEIN",
            "removed_reviews_text": removed_reviews_text,
            "maps_url": current_url,
        }

    except Exception as e:
        print(f"[!] Fehler beim Parsen: {e}")
        return {
            "place_id": place_id or url,
            "name": "ERROR",
            "rating": None,
            "reviews_count": None,
            "removed_reviews": None,
            "removed_reviews_text": None,
            "maps_url": current_url,
        }


async def parse_worker(worker_id, parse_page, place_id_queue, results, results_lock, shutdown_event):
    """
    Liest Eintraege aus der Queue und analysiert sie.
    Beendet sich sofort wenn das Analyse-Fenster geschlossen wird.
    """
    page_closed = asyncio.Event()

    def _on_page_close(_page):
        page_closed.set()
        shutdown_event.set()
        # Sentinel einlegen damit queue.get() nicht blockiert
        try:
            place_id_queue.put_nowait(None)
        except Exception:
            pass

    parse_page.on("close", _on_page_close)

    while not page_closed.is_set():
        # Auf naechsten Eintrag warten, aber sofort reagieren wenn Fenster schliesst
        get_task   = asyncio.ensure_future(place_id_queue.get())
        close_task = asyncio.ensure_future(page_closed.wait())
        done, pending = await asyncio.wait(
            {get_task, close_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        if page_closed.is_set():
            print(f"\n[W{worker_id}] Analyse-Fenster geschlossen – Worker beendet.")
            break

        entry = get_task.result()
        if entry is None:
            break

        async with results_lock:
            idx = len(results) + 1
        print(f"\n[W{worker_id}|{idx}] Analysiere: {entry[:80]}")

        # Analyse gegen Fenster-Schliessen und Timeout absichern
        parse_task  = asyncio.ensure_future(parse_restaurant(parse_page, entry))
        close_task2 = asyncio.ensure_future(page_closed.wait())
        done2, pending2 = await asyncio.wait(
            {parse_task, close_task2},
            timeout=120,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending2:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        if page_closed.is_set():
            print(f"[W{worker_id}] Fenster waehrend Analyse geschlossen – Worker beendet.")
            break

        if not done2:  # Timeout
            print(f"[W{worker_id}] Timeout fuer {entry[:60]} – uebersprungen.")
            data = {
                "place_id": entry,
                "name": "TIMEOUT",
                "rating": None,
                "reviews_count": None,
                "removed_reviews": "ERROR",
                "removed_reviews_text": None,
                "maps_url": entry if entry.startswith("http") else "",
            }
            try:
                await parse_page.goto("about:blank", timeout=5000)
            except Exception:
                pass
        else:
            try:
                data = parse_task.result()
            except Exception as e:
                print(f"[W{worker_id}] Fehler: {e}")
                data = {
                    "place_id": entry,
                    "name": "ERROR",
                    "rating": None,
                    "reviews_count": None,
                    "removed_reviews": "ERROR",
                    "removed_reviews_text": None,
                    "maps_url": "",
                }

        async with results_lock:
            results.append(data)
            count = len(results)
        print(f"    -> {data.get('name', '?')} | Entfernt: {data.get('removed_reviews', '?')}")

        if count % 5 == 0:
            async with results_lock:
                _save_intermediate(results)

    print(f"[+] Parse-Worker {worker_id} fertig.")


async def wait_for_enter_async(stop_event, shutdown_event):
    """Setzt stop_event wenn Enter gedrueckt wird ODER alle Analyse-Fenster geschlossen wurden."""
    loop = asyncio.get_running_loop()
    enter_task    = asyncio.ensure_future(loop.run_in_executor(None, input, ""))
    shutdown_task = asyncio.ensure_future(shutdown_event.wait())
    await asyncio.wait(
        {enter_task, shutdown_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    stop_event.set()
    for t in (enter_task, shutdown_task):
        t.cancel()
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass


async def main():

    results = []

    async with async_playwright() as p:

        browser = await p.chromium.launch(
            headless=False,
            slow_mo=50,
        )

        search_page = await browser.new_page()
        parse_page_1 = await browser.new_page()
        parse_page_2 = await browser.new_page()

        async def abort_media(route):
            if route.request.resource_type in ("image", "media"):
                await route.abort()
            else:
                await route.continue_()

        await search_page.route("**/*", abort_media)
        await parse_page_1.route("**/*", abort_media)
        await parse_page_2.route("**/*", abort_media)

        print("[+] Oeffne Google Maps ...")
        await search_page.goto(GOOGLE_MAPS_URL)
        await search_page.wait_for_load_state("domcontentloaded")

        # Datenschutz-Banner auf search_page ablehnen
        try:
            reject_btn = search_page.locator(
                'button:has-text("Alle ablehnen"), '
                'button:has-text("Reject all"), '
                'button:has-text("Ablehnen")'
            ).first
            await reject_btn.wait_for(state="visible", timeout=8000)
            await reject_btn.click()
            await search_page.wait_for_load_state("domcontentloaded")
            print("[+] Datenschutzhinweis (Suche) abgelehnt.")
        except Exception:
            pass

        # Datenschutz-Banner auf parse_page_1 ablehnen
        await parse_page_1.goto(GOOGLE_MAPS_URL)
        await parse_page_1.wait_for_load_state("domcontentloaded")
        try:
            reject_btn2 = parse_page_1.locator(
                'button:has-text("Alle ablehnen"), '
                'button:has-text("Reject all"), '
                'button:has-text("Ablehnen")'
            ).first
            await reject_btn2.wait_for(state="visible", timeout=8000)
            await reject_btn2.click()
            await parse_page_1.wait_for_load_state("domcontentloaded")
            print("[+] Datenschutzhinweis (Analyse 1) abgelehnt.")
        except Exception:
            pass

        # Datenschutz-Banner auf parse_page_2 ablehnen
        await parse_page_2.goto(GOOGLE_MAPS_URL)
        await parse_page_2.wait_for_load_state("domcontentloaded")
        try:
            reject_btn3 = parse_page_2.locator(
                'button:has-text("Alle ablehnen"), '
                'button:has-text("Reject all"), '
                'button:has-text("Ablehnen")'
            ).first
            await reject_btn3.wait_for(state="visible", timeout=8000)
            await reject_btn3.click()
            await parse_page_2.wait_for_load_state("domcontentloaded")
            print("[+] Datenschutzhinweis (Analyse 2) abgelehnt.")
        except Exception:
            pass

        # Suche starten
        print(f"[+] Suche nach: {SEARCH_QUERY}")
        search_box = None
        for selector in [
            'input[id="searchboxinput"]',
            'input[aria-label*="Such"]',
            'input[aria-label*="Search"]',
            'input[name="q"]',
        ]:
            try:
                sb = search_page.locator(selector).first
                await sb.wait_for(state="visible", timeout=5000)
                search_box = sb
                break
            except Exception:
                continue

        if not search_box:
            print("[!] Suchfeld nicht gefunden.")
            await browser.close()
            return

        await search_box.fill(SEARCH_QUERY)
        await search_page.keyboard.press("Enter")

        try:
            await search_page.wait_for_selector('div[role="feed"]', timeout=30000)
        except Exception:
            print("[!] feed-Selektor nicht gefunden, warte weiter ...")
            await asyncio.sleep(8)

        # Queue, Lock und Events vorbereiten
        place_id_queue = asyncio.Queue()
        results_lock   = asyncio.Lock()
        stop_event     = asyncio.Event()
        shutdown_event = asyncio.Event()  # wird gesetzt wenn ein Analyse-Fenster geschlossen wird

        # Tasks parallel:
        #   1. Sammelt Place-IDs in die Queue (wartet auf Enter oder shutdown)
        #   2+3. Zwei Parse-Worker leeren die Queue gleichzeitig
        #   4. Wartet auf Enter oder shutdown und setzt stop_event
        #
        # collect legt am Ende zwei Sentinels (None) ein – einen pro Worker
        async def collect_with_two_sentinels():
            await collect_place_ids_via_interception(search_page, place_id_queue, stop_event)
            await place_id_queue.put(None)  # zweiter Sentinel fuer Worker 2

        await asyncio.gather(
            collect_with_two_sentinels(),
            parse_worker(1, parse_page_1, place_id_queue, results, results_lock, shutdown_event),
            parse_worker(2, parse_page_2, place_id_queue, results, results_lock, shutdown_event),
            wait_for_enter_async(stop_event, shutdown_event),
        )

        await browser.close()

    if not results:
        print("[!] Keine Ergebnisse.")
        return

    # ── Duplikate entfernen (nach kanonischer maps_url) ──
    seen_urls: set = set()
    deduped = []
    for r in results:
        key = r.get("maps_url") or r.get("place_id", "")
        if key not in seen_urls:
            seen_urls.add(key)
            deduped.append(r)
    removed_count = len(results) - len(deduped)
    if removed_count:
        print(f"[+] {removed_count} Duplikat(e) entfernt. Verbleibend: {len(deduped)}")
    results = deduped

    df = pd.DataFrame(results).drop(columns=["place_id"], errors="ignore")

    # Prozentanteil geloeschter Bewertungen berechnen (als Bereich falls noetig)
    def calc_pct(row):
        bounds = _parse_removed_bounds(row["removed_reviews"])
        if bounds is None or not row["reviews_count"]:
            return None
        lo, hi = bounds
        total = row["reviews_count"]
        lo_pct = round(lo / total * 100, 1)
        hi_pct = round(hi / total * 100, 1)
        if lo_pct == hi_pct:
            return str(lo_pct).replace(".", ",") + "%"
        lo_str = str(lo_pct).replace(".", ",")
        hi_str = str(hi_pct).replace(".", ",")
        return f"{lo_str}\u2013{hi_str}%"

    def calc_pct_midpoint(row):
        bounds = _parse_removed_bounds(row["removed_reviews"])
        if bounds is None or not row["reviews_count"]:
            return -1
        lo, hi = bounds
        return (lo + hi) / 2 / row["reviews_count"]

    df["removed_reviews_pct"] = df.apply(calc_pct, axis=1)
    df["_sort_pct"] = df.apply(calc_pct_midpoint, axis=1)

    # Sortierung: Eintraege mit geloeschten Bewertungen zuerst, dann nach Prozent, dann nach Anzahl
    df["_has_removed"] = df["removed_reviews"].apply(lambda x: 0 if x not in ("NEIN", "KEIN_TAB", None) else 1)
    df = df.sort_values(
        by=["_has_removed", "_sort_pct", "reviews_count"],
        ascending=[True, False, False],
    ).drop(columns=["_has_removed", "_sort_pct"])

    # Spaltenreihenfolge
    df = df[["name", "rating", "reviews_count", "removed_reviews", "removed_reviews_pct", "removed_reviews_text", "maps_url"]]

    df.to_csv(OUTPUT_FILE, index=False, sep=";", encoding="utf-8-sig")

    # ── Zwei Teillisten speichern ──
    df_removed = df[~df["removed_reviews"].isin(("NEIN", "KEIN_TAB", None))]
    df_clean   = df[ df["removed_reviews"].isin(("NEIN", "KEIN_TAB")) | df["removed_reviews"].isna()]

    df_removed.to_csv(OUTPUT_FILE_REMOVED, index=False, sep=";", encoding="utf-8-sig")
    df_clean.to_csv(OUTPUT_FILE_CLEAN, index=False, sep=";", encoding="utf-8-sig")

    print("\n===================================")
    print("[+] Fertig!")
    print(f"[+] Gesamt:            {OUTPUT_FILE}  ({len(df)} Eintraege)")
    print(f"[+] Entfernte Reviews: {OUTPUT_FILE_REMOVED}  ({len(df_removed)} Eintraege)")
    print(f"[+] Keine Entfernung:  {OUTPUT_FILE_CLEAN}  ({len(df_clean)} Eintraege)")
    print("===================================")


if __name__ == "__main__":
    asyncio.run(main())
