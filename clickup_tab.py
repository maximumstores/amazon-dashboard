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
