"""
daily_monitor.py — standalone daily scraper for monitored ASINs.

Запускається щодня через cron / GitHub Actions.
Читає monitored_asins з БД → дзвонить Apify → пише нові відгуки в amazon_reviews.

ENV змінні (обов'язкові):
  DATABASE_URL  — постgres connection string
  APIFY_TOKEN   — токен Apify

Запуск локально:
  DATABASE_URL='postgresql://...' APIFY_TOKEN='...' python daily_monitor.py
"""

import os
import sys
import time
import hashlib
import psycopg2
import requests
from urllib.parse import urlparse

DATABASE_URL      = os.getenv("DATABASE_URL", "")
APIFY_TOKEN       = os.getenv("APIFY_TOKEN", "")
BRIGHTDATA_TOKEN  = os.getenv("BRIGHTDATA_TOKEN", "") or os.getenv("BD_TOKEN", "")
BRIGHTDATA_DS_ID  = os.getenv("BRIGHTDATA_DATASET_ID", "gd_le8e811kzy4ggddlq")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    print("❌ DATABASE_URL not set"); sys.exit(1)
if not APIFY_TOKEN and not BRIGHTDATA_TOKEN:
    print("❌ Жодного токена не задано — потрібен APIFY_TOKEN або BRIGHTDATA_TOKEN"); sys.exit(1)

STARS_MAP = {5: "fiveStar", 4: "fourStar", 3: "threeStar", 2: "twoStar", 1: "oneStar"}

ACTOR_US = os.getenv("APIFY_ACTOR_US", "xmiso~amazon-reviews-scraper")
ACTOR_EU = os.getenv("APIFY_ACTOR_EU", "web_wanderer~amazon-reviews-scraper")
ACTOR_FALLBACK = os.getenv("APIFY_ACTOR_FALLBACK", "junglee~amazon-reviews-scraper")

US_DOMAINS = {"com"}
EU_DOMAINS = {"co.uk", "de", "fr", "it", "es", "nl", "pl", "se",
              "ca", "com.au", "com.mx", "co.jp"}


def apify_endpoint(domain: str) -> tuple:
    if domain in US_DOMAINS:
        actor = ACTOR_US
    elif domain in EU_DOMAINS:
        actor = ACTOR_EU
    else:
        actor = ACTOR_FALLBACK
    url = (f"https://api.apify.com/v2/acts/{actor}"
           f"/run-sync-get-dataset-items?token={APIFY_TOKEN}")
    return url, actor


def get_conn():
    r = urlparse(DATABASE_URL)
    return psycopg2.connect(
        database=r.path[1:], user=r.username, password=r.password,
        host=r.hostname, port=r.port, connect_timeout=30
    )


def load_monitored():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        SELECT id, asin, domain, stars_to_monitor,
               COALESCE(scraper_source, 'apify') AS scraper_source
        FROM monitored_asins
        WHERE is_active = TRUE
        ORDER BY added_at ASC
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def get_existing_review_ids(asin, domain, limit=500):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        SELECT review_id FROM amazon_reviews
        WHERE asin=%s AND domain=%s AND review_id IS NOT NULL
        ORDER BY created_at DESC LIMIT %s
    """, (asin, domain, limit))
    ids = [r[0] for r in cur.fetchall() if r[0]]
    cur.close(); conn.close()
    return ids


def save_reviews(reviews, asin, domain, default_source='apify'):
    conn = get_conn(); cur = conn.cursor()
    inserted = 0
    for rev in reviews:
        url = rev.get("reviewUrl", "")
        rid = rev.get("review_id") or ""
        if not rid:
            if url:
                rid = url.split("/")[-1] or url[-80:]
            else:
                _h_src = (
                    f"{asin}|{domain}|{rev.get('author','')}|{rev.get('date','')}|"
                    f"{rev.get('ratingScore',0)}|{(rev.get('reviewTitle','') or '')[:100]}|"
                    f"{(rev.get('reviewDescription','') or '')[:200]}"
                )
                rid = f"{asin}_{domain}_{hashlib.md5(_h_src.encode()).hexdigest()[:20]}"
        try:
            cur.execute("""
                INSERT INTO amazon_reviews
                (asin, domain, review_id, author, rating, title, content, is_verified,
                 product_attributes, review_date,
                 author_id, author_link, badge, is_amazon_vine, helpful_count,
                 variant_asin, variant_name, review_country, source)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s)
                ON CONFLICT (review_id) DO NOTHING
            """, (
                asin, domain, rid,
                rev.get("author", "Amazon User"),
                int(rev.get("ratingScore", 0)),
                rev.get("reviewTitle", ""),
                rev.get("reviewDescription", ""),
                bool(rev.get("isVerified", False)),
                rev.get("variant", ""),
                rev.get("date", ""),
                rev.get("author_id") or None,
                rev.get("author_link") or None,
                rev.get("badge") or None,
                bool(rev.get("is_amazon_vine", False)),
                int(rev.get("helpful_count", 0) or 0),
                rev.get("variant_asin") or None,
                rev.get("variant_name") or None,
                rev.get("review_country") or None,
                rev.get("source") or default_source,
            ))
            if cur.rowcount > 0:
                inserted += 1
        except Exception:
            pass
    conn.commit(); cur.close(); conn.close()
    return inserted


def save_product_rating_snapshot(asin, domain, rating_obj, avg_rating=None):
    if not rating_obj or not isinstance(rating_obj, dict):
        return False
    one   = int(rating_obj.get("one_star", 0) or 0)
    two   = int(rating_obj.get("two_star", 0) or 0)
    three = int(rating_obj.get("three_star", 0) or 0)
    four  = int(rating_obj.get("four_star", 0) or 0)
    five  = int(rating_obj.get("five_star", 0) or 0)
    total = one + two + three + four + five
    if total == 0:
        return False
    if avg_rating is None:
        avg_rating = (one*1 + two*2 + three*3 + four*4 + five*5) / total
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO amazon_product_ratings
            (asin, domain, snapshot_date, one_star, two_star, three_star, four_star, five_star, total_count, avg_rating)
            VALUES (%s, %s, CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (asin, domain, snapshot_date) DO UPDATE
              SET one_star=EXCLUDED.one_star, two_star=EXCLUDED.two_star,
                  three_star=EXCLUDED.three_star, four_star=EXCLUDED.four_star,
                  five_star=EXCLUDED.five_star, total_count=EXCLUDED.total_count,
                  avg_rating=EXCLUDED.avg_rating
        """, (asin, domain, one, two, three, four, five, total, round(float(avg_rating), 2)))
        conn.commit(); cur.close(); conn.close()
        return True
    except Exception:
        return False


def update_last_check(mon_id, new_count):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "UPDATE monitored_asins SET last_check=NOW(), last_new_count=%s WHERE id=%s",
        (new_count, mon_id)
    )
    conn.commit(); cur.close(); conn.close()


def scrape_via_apify(asin, domain, stars, max_per_star=30):
    url = f"https://www.amazon.{domain}/dp/{asin}"
    endpoint, actor = apify_endpoint(domain)
    total_new = 0
    print(f"    🟡 Apify actor: {actor}")

    for star_num in stars:
        star_text = STARS_MAP.get(star_num)
        if not star_text: continue
        payload = {
            "productUrls": [{"url": url}],
            "filterByRatings": [star_text],
            "maxReviews": max_per_star,
            "sort": "recent",
        }
        try:
            res = requests.post(endpoint, json=payload, timeout=360)
            if res.status_code in (200, 201):
                data = res.json()
                if data:
                    ins = save_reviews(data, asin, domain)
                    total_new += ins
                    print(f"    ✅ {star_num}★: {len(data)} received, {ins} NEW")
                else:
                    print(f"    ⚠️ {star_num}★: empty")
            else:
                print(f"    ❌ {star_num}★: HTTP {res.status_code}")
        except Exception as e:
            print(f"    ❌ {star_num}★: {e}")
        time.sleep(2)
    return total_new


def scrape_via_brightdata(asin, domain, stars):
    if not BRIGHTDATA_TOKEN:
        print("    ❌ BRIGHTDATA_TOKEN not set — skip")
        return 0

    existing = get_existing_review_ids(asin, domain, limit=500)
    print(f"    🟣 Bright Data · dedup {len(existing)} існуючих ID")
    url = f"https://www.amazon.{domain}/dp/{asin}"
    payload = [{"url": url, "reviews_to_not_include": existing[:500]}]
    headers = {"Authorization": f"Bearer {BRIGHTDATA_TOKEN}", "Content-Type": "application/json"}

    try:
        r = requests.post(
            f"https://api.brightdata.com/datasets/v3/trigger"
            f"?dataset_id={BRIGHTDATA_DS_ID}&format=json&uncompressed_webhook=true&include_errors=true",
            headers=headers, json=payload, timeout=60
        )
        if r.status_code != 200:
            print(f"    ❌ BD trigger {r.status_code}"); return 0
        snap_id = r.json().get("snapshot_id")
    except Exception as e:
        print(f"    ❌ BD trigger error: {e}"); return 0

    ready = False
    for i in range(18):
        time.sleep(10)
        try:
            s = requests.get(f"https://api.brightdata.com/datasets/v3/progress/{snap_id}",
                             headers=headers, timeout=30).json()
            if s.get("status") == "ready":
                ready = True; break
            if s.get("status") == "failed":
                print(f"    ❌ BD failed"); return 0
        except: pass

    if not ready:
        print(f"    ⚠️ BD timeout"); return 0

    try:
        r = requests.get(f"https://api.brightdata.com/datasets/v3/snapshot/{snap_id}?format=json",
                         headers=headers, timeout=90)
        data = r.json()
    except Exception as e:
        print(f"    ❌ BD download: {e}"); return 0

    if not isinstance(data, list): return 0

    converted = []
    _prod_saved = False
    for item in data:
        if not isinstance(item, dict): continue

        if not _prod_saved:
            _pr = item.get("product_rating_object") or item.get("product_ratings")
            if _pr and isinstance(_pr, dict):
                _avg = item.get("product_rating") or item.get("average_rating")
                try: _avg = float(_avg) if _avg else None
                except: _avg = None
                if save_product_rating_snapshot(asin, domain, _pr, _avg):
                    print(f"    📊 Product rating saved: {_pr}")
                    _prod_saved = True

        nested = item.get("reviews") if "reviews" in item else None
        src = nested if nested else [item]
        for rev in src:
            if not isinstance(rev, dict): continue
            rid  = rev.get("review_id") or rev.get("reviewId") or rev.get("id") or ""
            rurl = rev.get("review_url") or rev.get("reviewUrl") or rev.get("url") or ""
            if not rurl and rid: rurl = f"https://www.amazon.{domain}/review/{rid}"
            try: rating_val = int(float(rev.get("rating") or rev.get("stars") or rev.get("ratingScore") or 0))
            except: rating_val = 0

            _variant_attrs = (rev.get("variant_attributes") or rev.get("variant")
                              or rev.get("product_variant") or rev.get("product_attributes"))
            if isinstance(_variant_attrs, dict):
                _variant_name = ", ".join(f"{k}: {v}" for k, v in _variant_attrs.items() if v)
            elif isinstance(_variant_attrs, list):
                _variant_name = ", ".join(str(x) for x in _variant_attrs if x)
            else:
                _variant_name = str(_variant_attrs) if _variant_attrs else ""
            try:
                _helpful = int(rev.get("helpful_count") or rev.get("helpful_votes") or 0)
            except: _helpful = 0

            converted.append({
                "reviewUrl": rurl,
                "author": (rev.get("author_name") or rev.get("author")
                           or rev.get("reviewer_name") or "Amazon User"),
                "ratingScore": rating_val,
                "reviewTitle": (rev.get("review_header") or rev.get("title")
                                or rev.get("review_title") or rev.get("reviewTitle")
                                or rev.get("review_headline") or rev.get("headline") or ""),
                "reviewDescription": (rev.get("review_text") or rev.get("text") or rev.get("body")
                                      or rev.get("reviewDescription") or rev.get("review_body")
                                      or rev.get("content") or ""),
                "isVerified": bool(rev.get("verified") or rev.get("is_verified")
                                   or rev.get("verified_purchase") or False),
                "variant": _variant_name,
                "date": (rev.get("review_posted_date") or rev.get("date")
                         or rev.get("review_date") or rev.get("reviewed_at")
                         or rev.get("timestamp") or ""),
                "review_id": rid,
                "author_id":       rev.get("author_id") or rev.get("reviewer_id") or None,
                "author_link":     rev.get("author_link") or rev.get("reviewer_url") or None,
                "badge":           rev.get("badge") or rev.get("verified_badge") or None,
                "is_amazon_vine":  bool(rev.get("is_amazon_vine") or rev.get("vine") or False),
                "helpful_count":   _helpful,
                "variant_asin":    rev.get("variant_asin") or rev.get("child_asin") or None,
                "variant_name":    _variant_name or None,
                "review_country":  rev.get("review_country") or rev.get("country") or domain,
                "source":          "brightdata",
            })

    filtered = [r for r in converted if r.get("ratingScore", 0) in stars]
    if not filtered:
        print(f"    ⚠️ BD: {len(converted)} отримано, 0 у потрібних зірках")
        return 0
    ins = save_reviews(filtered, asin, domain)
    print(f"    ✅ BD: {len(converted)} got, {len(filtered)} за зірками, {ins} NEW")
    return ins


def scrape_asin(asin, domain, stars, source='apify', max_per_star=30):
    if source == 'brightdata':
        return scrape_via_brightdata(asin, domain, stars)
    return scrape_via_apify(asin, domain, stars, max_per_star)


def main():
    from datetime import datetime
    print(f"🚀 Daily monitor start: {datetime.now().isoformat()}")

    monitored = load_monitored()
    if not monitored:
        print("📭 No active ASINs in monitored_asins — exit"); return

    print(f"📊 Active ASINs: {len(monitored)}")
    _apify_cnt = sum(1 for m in monitored if m[4] == 'apify')
    _bd_cnt    = sum(1 for m in monitored if m[4] == 'brightdata')
    print(f"   🟡 Apify: {_apify_cnt} · 🟣 Bright Data: {_bd_cnt}")
    total_new_all = 0

    for mon_id, asin, domain, stars_str, source in monitored:
        try:
            stars = [int(s.strip()) for s in stars_str.split(",") if s.strip().isdigit()]
        except Exception:
            stars = [1, 2, 3, 4]

        _src_icon = "🟡" if source == 'apify' else "🟣"
        print(f"\n{_src_icon} {asin} ({domain}) · stars: {stars} · source: {source}")
        try:
            new_count = scrape_asin(asin, domain, stars, source=source)
            update_last_check(mon_id, new_count)
            print(f"  🎯 +{new_count} new reviews")
            total_new_all += new_count
        except Exception as e:
            print(f"  ❌ error scraping {asin}: {e}")

        time.sleep(3)

    print(f"\n🏁 Finished: {total_new_all} new reviews across {len(monitored)} ASINs")


if __name__ == "__main__":
    main()
