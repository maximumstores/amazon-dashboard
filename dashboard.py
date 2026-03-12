def _scr_worker(urls, max_per_star, log_q, progress_q, loop_mode, stop_event, apify_token):
    try:
        _scr_ensure_table()
    except Exception as e:
        log_q.put(f"❌ DB error: {e}"); progress_q.put({"done": True}); return

    apify_token = apify_token or os.getenv("APIFY_TOKEN", "")
    endpoint = (
        "https://api.apify.com/v2/acts/junglee~amazon-reviews-scraper"
        f"/run-sync-get-dataset-items?token={apify_token}"
    )
    cycle = 0
    while not stop_event.is_set():
        cycle += 1
        total_steps = len(urls) * 5
        step = 0
        cycle_total = 0

        if loop_mode:
            log_q.put(f"\n{'🔄'*20}")
            log_q.put(f"🔄  ЦИКЛ #{cycle} РОЗПОЧАТО")
            log_q.put(f"{'🔄'*20}")

        for url in urls:
            if stop_event.is_set(): break
            if not url.startswith("http"):
                url = "https://" + url
            domain, asin = _scr_parse_url(url)
            flag = DOMAIN_FLAGS.get(domain, "🌍")
            log_q.put(f"\n{'='*50}")
            log_q.put(f"{flag}  {asin}  ·  amazon.{domain}  (цикл #{cycle})")
            log_q.put(f"{'='*50}")

            url_new = 0
            for star_num, star_text in STARS_MAP.items():
                if stop_event.is_set(): break
                step += 1
                pct = int(step / total_steps * 100)
                log_q.put(f"  ⏳ {star_num}★ — збираємо (max {max_per_star})...")
                progress_q.put({"pct": pct, "label": f"Цикл #{cycle} · {asin} · {star_num}★"})
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
                            ins = _scr_save(data, asin, domain)
                            url_new     += ins
                            cycle_total += ins
                            log_q.put(f"  ✅ {star_num}★: отримано {len(data)}, нових: {ins}")
                        else:
                            log_q.put(f"  ⚠️ {star_num}★: відгуків не знайдено")
                    else:
                        try: err = res.json()
                        except: err = res.text[:200]
                        log_q.put(f"  ❌ {star_num}★: HTTP {res.status_code} → {err}")
                except Exception as e:
                    log_q.put(f"  ❌ {star_num}★: {e}")
                time.sleep(2)

            in_db = _scr_count(asin, domain)
            log_q.put(f"🎯 {asin}/{domain}: +{url_new} нових · в БД: {in_db}")
            time.sleep(3)

        if loop_mode and not stop_event.is_set():
            pause_min = 30
            log_q.put(f"\n🏁 Цикл #{cycle} завершено! +{cycle_total} нових.")
            log_q.put(f"⏸  Пауза {pause_min} хв перед наступним циклом...")
            progress_q.put({"pct": 100, "label": f"Цикл #{cycle} готово, пауза {pause_min} хв..."})
            for _ in range(pause_min * 12):
                if stop_event.is_set(): break
                time.sleep(5)
        else:
            break

    log_q.put(f"\n🏁 ЗБІР ЗУПИНЕНО після {cycle} цикл(ів)")
    progress_q.put({"pct": 100, "label": "Зупинено", "done": True, "total": cycle})
