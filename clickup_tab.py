# clickup_tab.py — вкладка "ClickUp" для dashboard.py (MERINO BI)
#
# Показывает задачи Maximum Stores из ClickUp (Spaces/Lists/Tasks),
# загруженные в Postgres отдельным процессом clickup_loader.py
# (полный обход при первом запуске, дальше -- инкрементальный синк).
#
# Источники (Postgres):
#   public.clickup_tasks            -- снепшот всех задач (upsert по task_id)
#   public.clickup_space_snapshots  -- история open/done/overdue по каждому space
#
# Подключается из dashboard.py:
#   from clickup_tab import show_clickup_tab
#   show_clickup_tab(get_engine())

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timezone


# ============================================================
# DATA LOADERS
# ============================================================

@st.cache_data(ttl=300)  # 5 мин -- данные и так обновляются loader'ом не чаще раза в N минут
def _load_tasks(_engine):
    sql = "SELECT * FROM clickup_tasks"
    df = pd.read_sql(sql, _engine)
    for col in ("due_date", "date_created", "date_updated", "date_closed", "loaded_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df


@st.cache_data(ttl=300)
def _load_snapshots(_engine):
    sql = "SELECT * FROM clickup_space_snapshots ORDER BY snapshot_at"
    df = pd.read_sql(sql, _engine)
    df["snapshot_at"] = pd.to_datetime(df["snapshot_at"], utc=True, errors="coerce")
    return df


# ============================================================
# MAIN TAB
# ============================================================

def show_clickup_tab(engine, ai_fn=None):
    st.header("📋 ClickUp — Maximum Stores")
    st.caption(
        "Задачи по всем Spaces Maximum Stores из ClickUp: кто чем занят, что "
        "просрочено, как загружены команды. Данные синкает clickup_loader.py "
        "по расписанию (полный обход + инкрементальный синк по date_updated)."
    )

    df = _load_tasks(engine)
    if df.empty:
        st.warning(
            "Нет данных в clickup_tasks. Проверь, что clickup_loader.py уже "
            "хотя бы раз отработал."
        )
        return

    now = datetime.now(timezone.utc)
    df["is_overdue"] = (~df["is_done"]) & df["due_date"].notna() & (df["due_date"] < now)

    loaded_at = df["loaded_at"].max()
    st.caption(f"Данные обновлены: {loaded_at.strftime('%Y-%m-%d %H:%M UTC')} (кэш вкладки — 5 мин)")

    # ---------- фильтры ----------
    spaces = sorted(df["space_name"].dropna().unique().tolist())
    selected_spaces = st.multiselect(
        "Команды / Spaces", spaces, default=spaces, key="clickup_spaces_filter"
    )

    all_assignees = sorted(set(
        name.strip()
        for cell in df["assignees"].dropna()
        for name in cell.split(",") if name.strip()
    ))
    selected_assignees = st.multiselect(
        "Исполнители", all_assignees, default=[], key="clickup_assignees_filter"
    )

    status_filter = st.radio(
        "Статус", ["Все", "Открытые", "Готово", "Просроченные"],
        horizontal=True, index=0, key="clickup_status_filter",
    )

    f = df[df["space_name"].isin(selected_spaces)]
    if selected_assignees:
        pattern = "|".join(selected_assignees)
        f = f[f["assignees"].str.contains(pattern, na=False)]
    if status_filter == "Открытые":
        f = f[~f["is_done"]]
    elif status_filter == "Готово":
        f = f[f["is_done"]]
    elif status_filter == "Просроченные":
        f = f[f["is_overdue"]]

    # ---------- метрики ----------
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Всего задач", len(f))
    c2.metric("✅ Готово", int(f["is_done"].sum()))
    c3.metric("🚧 В работе", int((~f["is_done"]).sum()))
    c4.metric("⚠️ Просрочено", int(f["is_overdue"].sum()))

    # ---------- по командам ----------
    st.divider()
    st.subheader("🏷 По командам")
    st.caption(
        "Что показывает: сколько открытых/готовых/просроченных задач в каждом "
        "Space. % overdue — доля просроченных среди открытых задач команды."
    )

    by_space = f.groupby("space_name").agg(
        open=("is_done", lambda s: (~s).sum()),
        done=("is_done", "sum"),
        overdue=("is_overdue", "sum"),
    ).reset_index().sort_values("open", ascending=False)
    by_space["overdue_rate_%"] = (
        by_space["overdue"] / by_space["open"].replace(0, pd.NA) * 100
    ).round(0)

    col_table, col_chart = st.columns([1, 1])
    with col_table:
        st.dataframe(
            by_space.rename(columns={
                "space_name": "Space", "open": "Open", "done": "Done",
                "overdue": "Overdue", "overdue_rate_%": "% Overdue",
            }),
            use_container_width=True, hide_index=True, key="clickup_tbl_by_space",
        )
    with col_chart:
        fig_space = px.bar(
            by_space, x="space_name", y=["open", "overdue"],
            barmode="group", labels={"space_name": "", "value": "Задач"},
        )
        fig_space.update_layout(
            margin=dict(l=0, r=0, t=10, b=0), height=320,
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            legend_title_text="",
        )
        st.plotly_chart(fig_space, use_container_width=True, key="clickup_chart_by_space")

    # ---------- тренд по времени ----------
    snaps = _load_snapshots(engine)
    if not snaps.empty:
        st.subheader("📈 Тренд open / overdue")
        st.caption(
            "Что показывает: как менялось количество открытых и просроченных "
            "задач в выбранном Space со временем (по снепшотам loader'а)."
        )
        trend_space = st.selectbox("Space для тренда", spaces, key="clickup_trend_space")
        ts = snaps[snaps["space_name"] == trend_space]
        if not ts.empty:
            fig_trend = px.line(
                ts, x="snapshot_at", y=["open_count", "overdue_count"],
                labels={"snapshot_at": "", "value": "Задач"},
            )
            fig_trend.update_layout(
                margin=dict(l=0, r=0, t=10, b=0), height=300,
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                legend_title_text="",
            )
            st.plotly_chart(fig_trend, use_container_width=True, key="clickup_chart_trend")
        else:
            st.info("Пока нет снепшотов для этого Space.")

    # ============================================================
    # РИСКИ
    # ============================================================
    st.divider()
    st.subheader("🚨 Риски")
    st.caption(
        "Сигналы, которые не видны в обычных метриках сверху: зависшие задачи, "
        "задачи без исполнителя, без дедлайна, срочные просрочки и перекос "
        "бэклога по командам."
    )

    STALE_DAYS = 14
    open_df = f[~f["is_done"]].copy()

    stale_mask = open_df["date_updated"].notna() & (
        (now - open_df["date_updated"]).dt.days > STALE_DAYS
    )
    no_due_mask = open_df["due_date"].isna()
    orphan_mask = open_df["assignees"].isna() | (open_df["assignees"].str.strip() == "")

    prio_overdue = f[f["is_overdue"] & f["priority"].isin(["urgent", "high"])]
    prio_urgent_overdue = int((prio_overdue["priority"] == "urgent").sum())
    prio_high_overdue = int((prio_overdue["priority"] == "high").sum())

    r1, r2, r3, r4 = st.columns(4)
    r1.metric(f"⏳ Зависли (>{STALE_DAYS}д без апдейта)", int(stale_mask.sum()))
    r2.metric("📅 Открытые без due date", int(no_due_mask.sum()))
    r3.metric("👤 Без исполнителя", int(orphan_mask.sum()))
    r4.metric("🔥 Просрочено urgent/high", f"{prio_urgent_overdue} / {prio_high_overdue}")

    def _task_table(sub_df, key, sort_col=None, ascending=True):
        cols = ["space_name", "list_name", "name", "status", "priority", "assignees", "due_date", "date_updated", "url"]
        show = sub_df[cols].copy()
        if sort_col:
            show = show.sort_values(sort_col, ascending=ascending)
        show["due_date"] = show["due_date"].dt.strftime("%Y-%m-%d")
        show["date_updated"] = show["date_updated"].dt.strftime("%Y-%m-%d")
        show = show.rename(columns={
            "space_name": "Space", "list_name": "List", "name": "Задача",
            "status": "Статус", "priority": "Приоритет", "assignees": "Кто",
            "due_date": "Due", "date_updated": "Обновлено", "url": "Ссылка",
        })
        st.dataframe(
            show.head(300), use_container_width=True, hide_index=True, key=key,
            column_config={"Ссылка": st.column_config.LinkColumn("Ссылка", display_text="Открыть")},
        )
        if len(sub_df) > 300:
            st.caption(f"Показаны первые 300 из {len(sub_df)}.")

    with st.expander(f"⏳ Зависшие задачи ({int(stale_mask.sum())})"):
        _task_table(open_df[stale_mask], "clickup_tbl_stale", sort_col="date_updated", ascending=True)

    with st.expander(f"📅 Открытые без due date ({int(no_due_mask.sum())})"):
        _task_table(open_df[no_due_mask], "clickup_tbl_no_due", sort_col="date_updated", ascending=True)

    with st.expander(f"👤 Без исполнителя ({int(orphan_mask.sum())})"):
        _task_table(open_df[orphan_mask], "clickup_tbl_orphan", sort_col="date_updated", ascending=True)

    with st.expander(f"🔥 Просрочено urgent/high ({len(prio_overdue)})"):
        _task_table(prio_overdue, "clickup_tbl_prio_overdue", sort_col="due_date", ascending=True)

    # ---------- разбивка по приоритету ----------
    st.markdown("**По приоритету**")
    st.caption("Открытые задачи по приоритету — где сконцентрирован urgent/high.")
    prio_open = open_df["priority"].fillna("не задан").value_counts().reset_index()
    prio_open.columns = ["Приоритет", "Открытых задач"]
    col_prio_table, col_prio_chart = st.columns([1, 1])
    with col_prio_table:
        st.dataframe(prio_open, use_container_width=True, hide_index=True, key="clickup_tbl_priority")
    with col_prio_chart:
        fig_prio = px.bar(prio_open, x="Приоритет", y="Открытых задач")
        fig_prio.update_layout(
            margin=dict(l=0, r=0, t=10, b=0), height=280,
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_prio, use_container_width=True, key="clickup_chart_priority")

    # ---------- давность просрочки ----------
    overdue_df = f[f["is_overdue"]].copy()
    if not overdue_df.empty:
        st.markdown("**Давность просрочки**")
        st.caption("Просрочено на 1 день и просрочено на 3 месяца — разный уровень срочности.")
        overdue_df["overdue_days"] = (now - overdue_df["due_date"]).dt.days
        bins = [-1, 3, 7, 30, 10_000]
        labels = ["0-3 дня", "4-7 дней", "8-30 дней", "30+ дней"]
        overdue_df["Просрочено"] = pd.cut(overdue_df["overdue_days"], bins=bins, labels=labels)
        overdue_bucket = overdue_df["Просрочено"].value_counts().reindex(labels).reset_index()
        overdue_bucket.columns = ["Просрочено", "Задач"]
        st.dataframe(overdue_bucket, use_container_width=True, hide_index=True, key="clickup_tbl_overdue_age")

        picked_bucket = st.selectbox("Показать задачи из бакета", labels, key="clickup_overdue_bucket_pick")
        with st.expander(f"Задачи: просрочено {picked_bucket}"):
            _task_table(
                overdue_df[overdue_df["Просрочено"] == picked_bucket],
                "clickup_tbl_overdue_bucket_tasks", sort_col="due_date", ascending=True,
            )

    # ---------- бэклог-риск по spaces ----------
    backlog = by_space[(by_space["open"] >= 20) & (by_space["open"] > 2 * by_space["done"].clip(lower=1))]
    if not backlog.empty:
        st.markdown("**📉 Бэклог-риск (open сильно больше done)**")
        st.caption("Команды, где открытых задач в 2+ раза больше закрытых — сигнал, что не разгребают быстрее, чем прилетает новое.")
        st.dataframe(
            backlog[["space_name", "open", "done"]].rename(columns={
                "space_name": "Space", "open": "Open", "done": "Done",
            }),
            use_container_width=True, hide_index=True, key="clickup_tbl_backlog_risk",
        )
    else:
        st.caption("Бэклог-риска по текущим фильтрам не обнаружено.")

    # ---------- по исполнителям ----------
    st.divider()
    st.subheader("👥 По исполнителям")
    st.caption("Кто сколько задач ведёт: готово / в работе / просрочено.")

    person_rows = []
    for _, row in f.iterrows():
        names = [n.strip() for n in (row["assignees"] or "Unassigned").split(",") if n.strip()] or ["Unassigned"]
        for n in names:
            person_rows.append({"person": n, "is_done": row["is_done"], "is_overdue": row["is_overdue"]})
    person_df = pd.DataFrame(person_rows)

    by_person = person_df.groupby("person").agg(
        done=("is_done", "sum"),
        open=("is_done", lambda s: (~s).sum()),
        overdue=("is_overdue", "sum"),
    ).reset_index().sort_values("overdue", ascending=False)

    st.dataframe(
        by_person.rename(columns={
            "person": "Исполнитель", "done": "Done", "open": "Open", "overdue": "Overdue",
        }),
        use_container_width=True, hide_index=True, key="clickup_tbl_by_person",
    )

    # ---------- drill-down: что именно ведёт выбранный исполнитель ----------
    person_options = by_person["person"].tolist()
    picked_person = st.selectbox(
        "Показать задачи исполнителя", person_options, key="clickup_person_pick"
    )
    if picked_person:
        pattern = rf"(^|,\s*){picked_person}(\s*,|$)" if picked_person != "Unassigned" else None
        if picked_person == "Unassigned":
            person_tasks = f[f["assignees"].isna() | (f["assignees"].str.strip() == "")]
        else:
            person_tasks = f[f["assignees"].str.contains(pattern, na=False, regex=True)]

        person_tasks = person_tasks.sort_values(["is_overdue", "due_date"], ascending=[False, True])
        st.caption(f"{picked_person}: {len(person_tasks)} задач под текущими фильтрами")

        show_cols = person_tasks[["space_name", "list_name", "name", "status", "due_date", "is_overdue", "url"]].copy()
        show_cols["due_date"] = show_cols["due_date"].dt.strftime("%Y-%m-%d")
        show_cols = show_cols.rename(columns={
            "space_name": "Space", "list_name": "List", "name": "Задача",
            "status": "Статус", "due_date": "Due", "is_overdue": "Overdue", "url": "Ссылка",
        })
        st.dataframe(
            show_cols.head(300), use_container_width=True, hide_index=True,
            key="clickup_tbl_person_tasks",
            column_config={"Ссылка": st.column_config.LinkColumn("Ссылка", display_text="Открыть")},
        )
        if len(person_tasks) > 300:
            st.caption(f"Показаны первые 300 из {len(person_tasks)} — сузь фильтры сверху для остальных.")

    # ---------- карточки задач по Spaces ----------
    st.divider()
    st.subheader("📌 Задачи")
    st.caption("Просроченные и ближайшие задачи по каждому Space, со ссылкой в ClickUp.")

    if selected_spaces:
        tabs = st.tabs(selected_spaces)
        for tab, space_name in zip(tabs, selected_spaces):
            with tab:
                sub = f[f["space_name"] == space_name].sort_values(
                    ["is_overdue", "due_date"], ascending=[False, True]
                )
                if sub.empty:
                    st.info("Нет задач под текущие фильтры.")
                    continue
                for _, r in sub.head(200).iterrows():
                    icon = "⚠️" if r["is_overdue"] else ("✅" if r["is_done"] else "🚧")
                    due_s = r["due_date"].strftime("%Y-%m-%d") if pd.notna(r["due_date"]) else "—"
                    with st.container(border=True):
                        st.markdown(f"**{icon} {r['name']}**  ·  {r['list_name']}")
                        st.caption(f"Кто: {r['assignees'] or '—'}  ·  Due: {due_s}  ·  Статус: {r['status']}")
                        if r["text_content"]:
                            st.write(r["text_content"][:300] + ("…" if len(r["text_content"]) > 300 else ""))
                        st.markdown(f"[Открыть в ClickUp]({r['url']})")
                if len(sub) > 200:
                    st.caption(f"Показаны первые 200 из {len(sub)} — сузь фильтры для остальных.")

    # ============================================================
    # AI-АНАЛИЗ (через call_ai из dashboard.py, передан как ai_fn)
    # ============================================================
    st.divider()
    st.subheader("🤖 AI-анализ загрузки команд")
    if ai_fn is None:
        st.caption("AI-анализ недоступен (ai_fn не передан из dashboard.py).")
    else:
        st.caption(
            "Нажми кнопку — AI посмотрит на загрузку команд, просрочки и "
            "бэклог-риски, и даст 3-4 конкретных вывода."
        )
        cur_provider = st.session_state.get("ai_provider", "")
        default_idx = 0 if cur_provider.startswith("Claude") else 1
        provider = st.radio(
            "AI-модель", ["Claude", "Gemini"],
            horizontal=True, index=default_idx, key="clickup_ai_provider",
            help="Выбор синхронизируется с остальными вкладками.",
        )
        st.session_state["ai_provider"] = provider

        if st.button("🧠 Сгенерировать AI-анализ", key="clickup_ai_btn"):
            space_summary = "; ".join(
                f"{r['space_name']}: open={int(r['open'])} overdue={int(r['overdue'])} done={int(r['done'])}"
                for _, r in by_space.iterrows()
            )
            top_overdue_people = "; ".join(
                f"{r['person']} ({int(r['overdue'])} overdue)"
                for _, r in by_person[by_person["overdue"] > 0].head(8).iterrows()
            ) or "нет"

            prompt = f"""Ты — операционный аналитик для Amazon FBA бренда Maximum Stores. Анализируй загрузку команд по данным ClickUp.

ДАННЫЕ:
- По Spaces (open/overdue/done): {space_summary}
- Люди с наибольшим числом просроченных задач: {top_overdue_people}
- Всего задач под текущим фильтром: {len(f)}, просрочено: {int(f['is_overdue'].sum())}
- Зависли без апдейта >{STALE_DAYS}д: {int(stale_mask.sum())}
- Открытые без due date: {int(no_due_mask.sum())}
- Без исполнителя: {int(orphan_mask.sum())}
- Просрочено с приоритетом urgent: {prio_urgent_overdue}, high: {prio_high_overdue}

Дай 3-4 конкретных вывода с действиями: где риск бэклога, кого разгрузить, что просрочено критично. Кратко, по делу, на русском."""

            with st.spinner("AI анализирует данные..."):
                result = ai_fn(prompt)
            st.markdown(result)

    # ---------- футер ----------
    st.divider()
    st.caption(
        "Данные синкает clickup_loader.py (полный обход при первом запуске, "
        "дальше инкрементально по date_updated_gt). Кэш вкладки — 5 мин."
    )
