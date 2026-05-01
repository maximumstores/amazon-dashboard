"""
customer_feedback_page.py
Страница 📣 Customer Feedback для Streamlit BI.
"""
import os
from datetime import date

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from dotenv import load_dotenv

load_dotenv()


# ═══════════════════════════════════════════════════════════════════════
# DATA ACCESS
# ═══════════════════════════════════════════════════════════════════════
def _get_conn():
    url = os.getenv("DATABASE_URL") or (
        st.secrets.get("DATABASE_URL", "") if hasattr(st, "secrets") else ""
    )
    return psycopg2.connect(url)


@st.cache_data(ttl=600, show_spinner=False)
def _query(sql: str, params: tuple = ()) -> pd.DataFrame:
    with _get_conn() as conn:
        return pd.read_sql(sql, conn, params=params)


@st.cache_data(ttl=600, show_spinner=False)
def _get_snapshots() -> list:
    df = _query("""
        SELECT snapshot_date FROM cf_item_topics
        UNION
        SELECT snapshot_date FROM cf_node_topics
        ORDER BY 1 DESC
    """)
    return df["snapshot_date"].tolist() if not df.empty else []


@st.cache_data(ttl=600, show_spinner=False)
def _get_covered_asins(snap: date) -> pd.DataFrame:
    return _query("""
        SELECT DISTINCT asin, item_name, browse_node_id
        FROM cf_item_topics
        WHERE snapshot_date = %s
        ORDER BY asin
    """, (snap,))


@st.cache_data(ttl=600, show_spinner=False)
def _get_nodes(snap: date) -> pd.DataFrame:
    return _query("""
        SELECT DISTINCT browse_node_id, node_name
        FROM cf_node_topics
        WHERE snapshot_date = %s
        ORDER BY node_name
    """, (snap,))


# ═══════════════════════════════════════════════════════════════════════
# RENDER HELPERS
# ═══════════════════════════════════════════════════════════════════════
def _star_color(v):
    if v is None or pd.isna(v):
        return "#888"
    if v < 0:
        a = min(abs(v) / 0.4, 1.0)
        return f"rgba(220, 60, 60, {0.3 + 0.5 * a:.2f})"
    a = min(v / 0.4, 1.0)
    return f"rgba(60, 180, 90, {0.3 + 0.5 * a:.2f})"


def _render_topic_card(row: dict, level: str = "item"):
    sentiment = row.get("sentiment", "")
    topic     = row.get("topic", "")
    snippets  = row.get("review_snippets") or []
    subtopics = row.get("subtopics") or []

    if level == "item":
        star     = row.get("parent_star_impact")
        occur    = row.get("parent_occurrence_pct")
        occur_bn = row.get("bn_occurrence_pct")
        metric_line = []
        if star is not None and not pd.isna(star):
            metric_line.append(f"★ **{star:+.2f}**")
        if occur is not None and not pd.isna(occur):
            metric_line.append(f"parent: {occur:.1f}%")
        if occur_bn is not None and not pd.isna(occur_bn):
            metric_line.append(f"category: {occur_bn:.1f}%")
    else:
        star  = row.get("all_products_star_impact")
        occur = row.get("all_products_occurrence_pct")
        top25 = row.get("top25_occurrence_pct")
        metric_line = []
        if star is not None and not pd.isna(star):
            metric_line.append(f"★ **{star:+.2f}**")
        if occur is not None and not pd.isna(occur):
            metric_line.append(f"all: {occur:.1f}%")
        if top25 is not None and not pd.isna(top25):
            metric_line.append(f"top25%: {top25:.1f}%")

    color  = _star_color(star)
    emoji  = "❌" if sentiment == "negative" else "✅"
    header = f"{emoji} **{topic}** — " + " · ".join(metric_line) if metric_line else f"{emoji} **{topic}**"

    with st.expander(header, expanded=False):
        if snippets:
            st.markdown("**🗣 Snippets:**")
            for s in snippets[:10]:
                st.markdown(
                    f"<div style='padding:6px 10px;margin:3px 0;border-left:3px solid {color};"
                    f"background:rgba(255,255,255,0.02);border-radius:3px;font-size:13px;'>"
                    f"« {s} »</div>",
                    unsafe_allow_html=True,
                )
        if subtopics:
            st.markdown("**🔬 Subtopics:**")
            sub_rows = []
            for st_item in subtopics:
                m = st_item.get("metrics") or {}
                sub_rows.append({
                    "Subtopic":   st_item.get("subtopic", ""),
                    "Mentions":   m.get("numberOfMentions"),
                    "% of topic": m.get("occurrencePercentage"),
                    "Snippets":   " · ".join((st_item.get("reviewSnippets") or [])[:3]),
                })
            if sub_rows:
                st.dataframe(pd.DataFrame(sub_rows), use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ═══════════════════════════════════════════════════════════════════════
def _tab_overview(snap: date):
    kpi = _query("""
        SELECT
            (SELECT COUNT(DISTINCT asin)  FROM cf_item_topics WHERE snapshot_date=%s) AS asins_with_topics,
            (SELECT COUNT(DISTINCT asin)  FROM cf_item_trends WHERE snapshot_date=%s) AS asins_with_trends,
            (SELECT COUNT(DISTINCT browse_node_id) FROM cf_node_topics WHERE snapshot_date=%s) AS nodes,
            (SELECT COUNT(*) FROM cf_item_topics WHERE snapshot_date=%s AND sentiment='negative') AS neg_item_topics,
            (SELECT COUNT(*) FROM cf_item_topics WHERE snapshot_date=%s AND sentiment='positive') AS pos_item_topics
    """, (snap, snap, snap, snap, snap)).iloc[0]

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("ASINs с топиками", int(kpi["asins_with_topics"]))
    c2.metric("ASINs с трендами", int(kpi["asins_with_trends"]))
    c3.metric("Категорий",        int(kpi["nodes"]))
    c4.metric("Негативных тем",   int(kpi["neg_item_topics"]))
    c5.metric("Позитивных тем",   int(kpi["pos_item_topics"]))

    st.markdown("---")
    st.markdown("### 🔥 Наши самые болезненные темы (негативный star impact)")
    top_neg = _query("""
        SELECT asin, item_name, topic,
               parent_star_impact AS star,
               parent_occurrence_pct AS parent_pct,
               bn_occurrence_pct AS category_pct,
               parent_mentions AS mentions
        FROM cf_item_topics
        WHERE snapshot_date = %s AND sentiment = 'negative'
          AND parent_star_impact IS NOT NULL
        ORDER BY parent_star_impact ASC
        LIMIT 15
    """, (snap,))

    if top_neg.empty:
        st.info("Нет данных")
    else:
        top_neg["item_name"] = top_neg["item_name"].str[:60]
        st.dataframe(
            top_neg.rename(columns={
                "asin": "ASIN", "item_name": "Item", "topic": "Topic",
                "star": "★ Impact", "parent_pct": "Parent %",
                "category_pct": "Category %", "mentions": "Mentions",
            }),
            use_container_width=True, hide_index=True,
            column_config={
                "★ Impact":   st.column_config.NumberColumn(format="%.2f"),
                "Parent %":   st.column_config.NumberColumn(format="%.1f"),
                "Category %": st.column_config.NumberColumn(format="%.1f"),
            },
        )

    st.markdown("---")
    st.markdown("### 💰 Причины возвратов в наших категориях")
    returns = _query("""
        SELECT r.browse_node_id, r.node_name, r.topic, r.occurrence_pct
        FROM cf_node_returns r
        WHERE r.snapshot_date = %s
        ORDER BY r.node_name, r.topic_rank
    """, (snap,))

    if returns.empty:
        st.info("Нет данных о возвратах")
    else:
        pivot = returns.pivot_table(
            index="topic", columns="node_name",
            values="occurrence_pct", aggfunc="first",
        ).fillna(0).round(1)
        pivot["_sum"] = pivot.sum(axis=1)
        pivot = pivot.sort_values("_sum", ascending=False).drop(columns=["_sum"])
        fig = px.imshow(
            pivot, text_auto=".1f", aspect="auto",
            color_continuous_scale="Reds",
            labels=dict(x="Категория", y="Причина", color="% возвратов"),
        )
        fig.update_layout(height=420, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 2 — BY ASIN
# ═══════════════════════════════════════════════════════════════════════
def _tab_by_asin(snap: date):
    asins = _get_covered_asins(snap)
    if asins.empty:
        st.info("За этот snapshot нет item-level данных")
        return

    asins["label"] = asins.apply(
        lambda r: f"{r['asin']} — {(r['item_name'] or '')[:55]}", axis=1,
    )
    pick = st.selectbox("Выбери ASIN", asins["label"].tolist(), key="cf_asin_pick")
    asin = pick.split(" — ")[0]
    info = asins[asins["asin"] == asin].iloc[0]
    st.markdown(f"**{info['item_name'] or ''}**")
    if info["browse_node_id"]:
        st.caption(f"browseNode: `{info['browse_node_id']}`")

    topics = _query("""
        SELECT sentiment, topic_rank, topic,
               asin_mentions, asin_occurrence_pct,
               parent_mentions, parent_occurrence_pct, parent_star_impact,
               bn_occurrence_pct, bn_star_impact,
               review_snippets, subtopics,
               most_mentions_asin, most_mentions_count,
               least_mentions_asin, least_mentions_count
        FROM cf_item_topics
        WHERE snapshot_date = %s AND asin = %s
        ORDER BY sentiment DESC, topic_rank
    """, (snap, asin))

    col_n, col_p = st.columns(2)
    with col_n:
        st.markdown("#### ❌ Negative")
        neg = topics[topics["sentiment"] == "negative"]
        if neg.empty:
            st.caption("_нет_")
        else:
            for _, row in neg.iterrows():
                _render_topic_card(row.to_dict(), level="item")
    with col_p:
        st.markdown("#### ✅ Positive")
        pos = topics[topics["sentiment"] == "positive"]
        if pos.empty:
            st.caption("_нет_")
        else:
            for _, row in pos.iterrows():
                _render_topic_card(row.to_dict(), level="item")

    st.markdown("---")
    st.markdown("### 📈 Месячные тренды")
    trends = _query("""
        SELECT sentiment, topic, period_start,
               asin_occurrence_pct, parent_occurrence_pct,
               bn_occurrence_pct, bn_top25_occurrence_pct
        FROM cf_item_trends
        WHERE snapshot_date = %s AND asin = %s
        ORDER BY period_start
    """, (snap, asin))

    if trends.empty:
        st.info("Нет трендов")
        return

    sent = st.radio("Sentiment", ["negative", "positive"],
                    horizontal=True, key="cf_asin_trend_sent")
    sel  = trends[trends["sentiment"] == sent]
    if sel.empty:
        st.caption(f"Нет трендов ({sent})")
        return

    chosen_topic = st.selectbox("Топик", sel["topic"].unique().tolist(),
                                key="cf_asin_trend_topic")
    t_df = sel[sel["topic"] == chosen_topic]
    fig  = go.Figure()
    fig.add_trace(go.Scatter(
        x=t_df["period_start"], y=t_df["parent_occurrence_pct"],
        mode="lines+markers", name="Parent ASIN (мы)",
        line=dict(width=3, color="#E54B4B"),
    ))
    fig.add_trace(go.Scatter(
        x=t_df["period_start"], y=t_df["bn_occurrence_pct"],
        mode="lines+markers", name="Категория (all)",
        line=dict(width=2, dash="dot", color="#888"),
    ))
    fig.add_trace(go.Scatter(
        x=t_df["period_start"], y=t_df["bn_top25_occurrence_pct"],
        mode="lines+markers", name="Категория (top 25%)",
        line=dict(width=2, dash="dash", color="#4A90E2"),
    ))
    fig.update_layout(
        height=380, yaxis_title="% упоминаний",
        hovermode="x unified", margin=dict(l=10, r=10, t=30, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 3 — BY CATEGORY
# ═══════════════════════════════════════════════════════════════════════
def _tab_by_node(snap: date):
    nodes = _get_nodes(snap)
    if nodes.empty:
        st.info("За этот snapshot нет node-level данных")
        return

    nodes["label"] = nodes.apply(
        lambda r: f"{r['node_name']} ({r['browse_node_id']})", axis=1,
    )
    pick    = st.selectbox("Выбери категорию", nodes["label"].tolist(), key="cf_node_pick")
    node_id = pick.split("(")[-1].rstrip(")")
    node_name = nodes[nodes["browse_node_id"] == node_id].iloc[0]["node_name"]
    st.caption(f"**{node_name}** · node_id `{node_id}`")

    st.markdown("### 💰 Причины возвратов в категории")
    returns = _query("""
        SELECT topic_rank, topic, occurrence_pct
        FROM cf_node_returns
        WHERE snapshot_date = %s AND browse_node_id = %s
        ORDER BY topic_rank
    """, (snap, node_id))

    if returns.empty:
        st.info("Нет данных о возвратах")
    else:
        fig = px.bar(
            returns, x="occurrence_pct", y="topic",
            orientation="h", text="occurrence_pct",
            color="occurrence_pct", color_continuous_scale="Reds",
        )
        fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig.update_layout(
            height=380, yaxis=dict(autorange="reversed"),
            xaxis_title="% возвратов", yaxis_title="",
            margin=dict(l=10, r=10, t=10, b=10),
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.markdown("### 🏷 Топики категории")
    topics = _query("""
        SELECT sentiment, topic_rank, topic,
               all_products_occurrence_pct, all_products_star_impact,
               top25_occurrence_pct, top25_star_impact,
               review_snippets, subtopics
        FROM cf_node_topics
        WHERE snapshot_date = %s AND browse_node_id = %s
        ORDER BY sentiment DESC, topic_rank
    """, (snap, node_id))

    col_n, col_p = st.columns(2)
    with col_n:
        st.markdown("#### ❌ Negative")
        neg = topics[topics["sentiment"] == "negative"]
        if neg.empty:
            st.caption("_нет_")
        else:
            for _, row in neg.iterrows():
                _render_topic_card(row.to_dict(), level="node")
    with col_p:
        st.markdown("#### ✅ Positive")
        pos = topics[topics["sentiment"] == "positive"]
        if pos.empty:
            st.caption("_нет_")
        else:
            for _, row in pos.iterrows():
                _render_topic_card(row.to_dict(), level="node")

    st.markdown("---")
    st.markdown("### 📈 Тренды категории")
    trends = _query("""
        SELECT sentiment, topic, period_start,
               all_products_occurrence_pct, top25_occurrence_pct
        FROM cf_node_review_trends
        WHERE snapshot_date = %s AND browse_node_id = %s
        ORDER BY period_start
    """, (snap, node_id))

    if trends.empty:
        st.info("Нет трендов")
        return

    sent = st.radio("Sentiment", ["negative", "positive"],
                    horizontal=True, key="cf_node_trend_sent")
    sel  = trends[trends["sentiment"] == sent]
    if sel.empty:
        st.caption(f"Нет ({sent})")
        return

    chosen = st.selectbox("Топик", sel["topic"].unique().tolist(),
                          key="cf_node_trend_topic")
    t_df = sel[sel["topic"] == chosen]
    fig  = go.Figure()
    fig.add_trace(go.Scatter(
        x=t_df["period_start"], y=t_df["all_products_occurrence_pct"],
        mode="lines+markers", name="Все товары",
        line=dict(width=2, color="#888"),
    ))
    fig.add_trace(go.Scatter(
        x=t_df["period_start"], y=t_df["top25_occurrence_pct"],
        mode="lines+markers", name="Top 25%",
        line=dict(width=3, color="#4A90E2"),
    ))
    fig.update_layout(
        height=360, yaxis_title="% упоминаний",
        hovermode="x unified", margin=dict(l=10, r=10, t=30, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 4 — CROSS TABLE: ASINs × Теми
# ═══════════════════════════════════════════════════════════════════════
def _tab_cross(snap: date):
    st.markdown("### 🔥 Всі ASINs × Теми — порівняння")

    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    with c1:
        sent = st.radio("Sentiment", ["negative", "positive"],
                        horizontal=True, key="cross_sent")
    with c2:
        view = st.radio("Вигляд", ["Ranked list", "Bubble chart"],
                        horizontal=True, key="cross_view")
    with c3:
        max_topics = st.slider("Топ N тем", 5, 30, 12, key="cross_n")
    with c4:
        metric = st.selectbox("Метрика", [
            "parent_occurrence_pct",
            "parent_star_impact",
            "asin_occurrence_pct",
        ], key="cross_metric", format_func=lambda x: {
            "parent_occurrence_pct": "Parent %",
            "parent_star_impact":    "★ Star impact",
            "asin_occurrence_pct":   "ASIN %",
        }[x])

    df = _query("""
        SELECT asin, item_name, topic, sentiment,
               parent_occurrence_pct, parent_star_impact,
               asin_occurrence_pct, bn_occurrence_pct
        FROM cf_item_topics
        WHERE snapshot_date = %s AND sentiment = %s
        ORDER BY topic_rank
    """, (snap, sent))

    if df.empty:
        st.info("Немає даних")
        return

    top_topics = (
        df.groupby("topic")[metric]
        .mean().dropna().abs()
        .sort_values(ascending=(sent == "positive"))
        .head(max_topics).index.tolist()
    )
    df = df[df["topic"].isin(top_topics)].copy()
    df["item_name"] = df["item_name"].str[:40]
    df["gap"] = (df["parent_occurrence_pct"] - df["bn_occurrence_pct"]).round(1)

    # ── RANKED LIST ─────────────────────────────────────────
    if view == "Ranked list":
        st.caption("Червона смуга = наш %, сіра = категорія. Δ = відхилення від benchmark.")

        df_sorted = df.sort_values(
            "parent_star_impact",
            ascending=(sent == "negative")
        ).reset_index(drop=True)

        max_our = df_sorted["parent_occurrence_pct"].max() or 1

        st.markdown("""
        <style>
        .rl-row{display:flex;align-items:center;gap:8px;padding:6px 0;
                border-bottom:0.5px solid var(--color-border-tertiary)}
        .rl-row:last-child{border-bottom:none}
        .rl-name{font-size:12px;color:var(--color-text-secondary);
                 width:170px;flex-shrink:0;white-space:nowrap;
                 overflow:hidden;text-overflow:ellipsis}
        .rl-topic{font-size:11px;padding:2px 8px;border-radius:12px;
                  flex-shrink:0;width:120px;white-space:nowrap;
                  overflow:hidden;text-overflow:ellipsis}
        .rl-neg{background:#FCEBEB;color:#A32D2D}
        .rl-pos{background:#EAF3DE;color:#3B6D11}
        .rl-bars{flex:1}
        .rl-bar-our{height:10px;border-radius:3px;background:#E24B4A;min-width:2px}
        .rl-bar-cat{height:5px;border-radius:3px;background:#888780;
                    opacity:.5;margin-top:2px;min-width:2px}
        .rl-pct{font-size:12px;font-weight:500;min-width:34px;text-align:right}
        .rl-delta{font-size:12px;min-width:42px;text-align:right}
        .rl-star{font-size:12px;font-weight:500;min-width:42px;text-align:right}
        </style>
        """, unsafe_allow_html=True)

        rows_html = ""
        for _, r in df_sorted.iterrows():
            our = r["parent_occurrence_pct"] or 0
            cat = r["bn_occurrence_pct"] or 0
            star = r["parent_star_impact"]
            gap = our - cat
            bar_w = int(our / max_our * 180)
            cat_w = int(cat / max_our * 180)
            tag_cls = "rl-neg" if sent == "negative" else "rl-pos"
            d_color = "#E24B4A" if gap > 0 else "#1D9E75"
            s_color = "#E24B4A" if (star or 0) < 0 else "#1D9E75"
            star_str = f"{star:+.2f}" if star is not None and pd.notna(star) else "—"
            gap_str  = f"{gap:+.1f}" if pd.notna(gap) else "—"
            rows_html += f"""
            <div class="rl-row">
              <div class="rl-name" title="{r['asin']} — {r['item_name']}"><span style="font-family:monospace;font-size:10px;color:var(--color-text-tertiary)">{r['asin']}</span><br>{r['item_name']}</div>
              <div class="rl-topic {tag_cls}">{r['topic']}</div>
              <div class="rl-bars">
                <div class="rl-bar-our" style="width:{bar_w}px"></div>
                <div class="rl-bar-cat" style="width:{cat_w}px"></div>
              </div>
              <div class="rl-pct">{our:.1f}%</div>
              <div class="rl-delta" style="color:{d_color}">{gap_str}</div>
              <div class="rl-star" style="color:{s_color}">{star_str}</div>
            </div>"""

        header = """
        <div style="display:flex;gap:8px;padding:0 0 4px;
                    font-size:11px;color:var(--color-text-secondary)">
          <div style="width:150px">ASIN</div>
          <div style="width:120px">Тема</div>
          <div style="flex:1">Наш % vs Категорія</div>
          <div style="min-width:34px;text-align:right">Наш</div>
          <div style="min-width:42px;text-align:right">Δ</div>
          <div style="min-width:42px;text-align:right">★</div>
        </div>"""
        st.markdown(header + rows_html, unsafe_allow_html=True)

    # ── BUBBLE CHART ────────────────────────────────────────
    else:
        st.caption("X = наш %, Y = star impact, розмір = Δ від категорії. Пунктир = середнє категорії.")

        df_b = df.dropna(subset=["parent_occurrence_pct", "parent_star_impact"]).copy()
        df_b["delta_abs"] = df_b["gap"].abs().clip(lower=2)
        df_b["color"] = df_b["gap"].apply(
            lambda g: "rgba(226,75,74,0.8)" if g > 0 else "rgba(29,158,117,0.8)"
        )
        df_b["label"] = df_b["item_name"].str[:20] + "<br>" + df_b["topic"]

        cat_avg = df_b["bn_occurrence_pct"].mean()

        fig = go.Figure()

        # Bubble
        fig.add_trace(go.Scatter(
            x=df_b["parent_occurrence_pct"],
            y=df_b["parent_star_impact"],
            mode="markers+text",
            text=df_b["topic"].str[:14],
            textposition="top center",
            textfont=dict(size=9),
            marker=dict(
                size=df_b["delta_abs"] * 2.5,
                color=df_b["color"].tolist(),
                line=dict(width=1, color="rgba(0,0,0,0.3)"),
            ),
            customdata=df_b[["item_name", "topic", "parent_occurrence_pct",
                              "bn_occurrence_pct", "gap", "parent_star_impact"]].values,
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Тема: %{customdata[1]}<br>"
                "Наш: %{customdata[2]:.1f}%<br>"
                "Категорія: %{customdata[3]:.1f}%<br>"
                "Δ: %{customdata[4]:+.1f}<br>"
                "★: %{customdata[5]:+.2f}<extra></extra>"
            ),
        ))

        # Лінія категорії
        fig.add_vline(
            x=cat_avg, line_dash="dot",
            line_color="rgba(226,75,74,0.4)", line_width=1.5,
            annotation_text=f"avg cat {cat_avg:.1f}%",
            annotation_position="top right",
            annotation_font_size=10,
        )
        # Нульова лінія star impact
        fig.add_hline(
            y=0, line_dash="dot",
            line_color="rgba(128,128,128,0.3)", line_width=1,
        )

        fig.update_layout(
            height=420,
            xaxis_title="Наш parent %",
            yaxis_title="★ Star impact",
            hovermode="closest",
            margin=dict(l=10, r=10, t=20, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#e2e8f0",
            xaxis=dict(gridcolor="rgba(128,128,128,0.15)"),
            yaxis=dict(gridcolor="rgba(128,128,128,0.15)"),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Червоний = ми вище категорії (проблема) · Зелений = ми нижче (добре) · Розмір = Δ")

    st.divider()

    # ── Таблиця відхилень (спільна для обох вглядів) ────────
    st.markdown("### 📊 Топ відхилень від категорії")
    gap_df = df.sort_values("gap", ascending=(sent == "positive")).head(20)
    st.dataframe(
        gap_df[["asin", "item_name", "topic",
                "parent_occurrence_pct", "bn_occurrence_pct",
                "gap", "parent_star_impact"]].rename(columns={
            "asin":                  "ASIN",
            "item_name":             "Item",
            "topic":                 "Тема",
            "parent_occurrence_pct": "Наш %",
            "bn_occurrence_pct":     "Категорія %",
            "gap":                   "Δ",
            "parent_star_impact":    "★ Impact",
        }),
        use_container_width=True, hide_index=True,
        column_config={
            "Наш %":       st.column_config.NumberColumn(format="%.1f"),
            "Категорія %": st.column_config.NumberColumn(format="%.1f"),
            "Δ":           st.column_config.NumberColumn(format="%.1f"),
            "★ Impact":    st.column_config.NumberColumn(format="%.2f"),
        },
    )


# ═══════════════════════════════════════════════════════════════════════
# MAIN ENTRY
# ═══════════════════════════════════════════════════════════════════════
def show_customer_feedback():
    st.markdown("## 📣 Customer Feedback")
    st.caption("SP-API Customer Feedback v2024-06-01 · обновляється щотижня · US marketplace only")

    snaps = _get_snapshots()
    if not snaps:
        st.warning("❌ Даних немає. Запусти `python 17_customer_feedback_loader.py`")
        return

    col1, _ = st.columns([2, 4])
    with col1:
        snap = st.selectbox(
            "📅 Snapshot", snaps, index=0,
            format_func=lambda d: f"{d}  ({(date.today() - d).days} дн назад)",
        )

    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Overview",
        "📦 По нашему ASIN",
        "🏷 По категории",
        "🔥 ASINs × Теми",
    ])
    with tab1:
        _tab_overview(snap)
    with tab2:
        _tab_by_asin(snap)
    with tab3:
        _tab_by_node(snap)
    with tab4:
        _tab_cross(snap)


if __name__ == "__main__":
    st.set_page_config(page_title="Customer Feedback", page_icon="📣", layout="wide")
    show_customer_feedback()
