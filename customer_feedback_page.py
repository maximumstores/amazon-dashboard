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
    st.caption("Червоний = ми вище категорії (проблема). Зелений = ми кращі.")

    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        sent = st.radio("Sentiment", ["negative", "positive"],
                        horizontal=True, key="cross_sent")
    with c2:
        metric = st.selectbox("Метрика", [
            "parent_occurrence_pct",
            "parent_star_impact",
            "asin_occurrence_pct",
        ], key="cross_metric", format_func=lambda x: {
            "parent_occurrence_pct": "Parent %",
            "parent_star_impact":    "★ Star impact",
            "asin_occurrence_pct":   "ASIN %",
        }[x])
    with c3:
        max_topics = st.slider("Топ N тем", 5, 30, 12, key="cross_n")

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

    # Топ N тем по середньому значенню метрику
    top_topics = (
        df.groupby("topic")[metric]
        .mean()
        .dropna()
        .abs()
        .sort_values(ascending=(sent == "positive"))
        .head(max_topics)
        .index.tolist()
    )
    df_filtered = df[df["topic"].isin(top_topics)].copy()

    # Pivot для heatmap
    pivot = df_filtered.pivot_table(
        index="asin", columns="topic",
        values=metric, aggfunc="first",
    ).round(2)

    # Підписи рядків — item_name скорочено
    name_map = (
        df_filtered.drop_duplicates("asin")
        .set_index("asin")["item_name"]
        .str[:45]
    )
    pivot.index = [name_map.get(a, a) for a in pivot.index]

    # Heatmap
    colorscale = "RdYlGn" if metric == "parent_star_impact" else (
        "Reds" if sent == "negative" else "Greens"
    )
    zmid = 0 if metric == "parent_star_impact" else None

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=list(pivot.columns),
        y=list(pivot.index),
        text=[[f"{v:.1f}" if pd.notna(v) else "" for v in row]
              for row in pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=11),
        colorscale=colorscale,
        zmid=zmid,
        hoverongaps=False,
        colorbar=dict(
            title={"parent_occurrence_pct": "Parent %",
                   "parent_star_impact": "★ Impact",
                   "asin_occurrence_pct": "ASIN %"}[metric],
            thickness=12,
        ),
    ))
    fig.update_layout(
        height=max(320, len(pivot) * 44 + 120),
        margin=dict(l=10, r=10, t=20, b=90),
        xaxis=dict(tickangle=-35, tickfont=dict(size=11)),
        yaxis=dict(tickfont=dict(size=11)),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#e2e8f0",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Таблиця відхилень: наш % vs категорія
    st.markdown("### 📊 Де найбільше відхилення від категорії")
    gap_df = df.copy()
    gap_df["gap"] = (gap_df["parent_occurrence_pct"] - gap_df["bn_occurrence_pct"]).round(1)
    gap_df = gap_df.dropna(subset=["gap"])
    gap_df = gap_df.sort_values("gap", ascending=(sent == "positive"))
    gap_df["item_name"] = gap_df["item_name"].str[:45]

    st.dataframe(
        gap_df[["asin", "item_name", "topic",
                "parent_occurrence_pct", "bn_occurrence_pct",
                "gap", "parent_star_impact"]].head(30).rename(columns={
            "asin":                  "ASIN",
            "item_name":             "Item",
            "topic":                 "Тема",
            "parent_occurrence_pct": "Наш %",
            "bn_occurrence_pct":     "Категорія %",
            "gap":                   "Δ (наш − кат)",
            "parent_star_impact":    "★ Impact",
        }),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Наш %":        st.column_config.NumberColumn(format="%.1f"),
            "Категорія %":  st.column_config.NumberColumn(format="%.1f"),
            "Δ (наш − кат)": st.column_config.NumberColumn(format="%.1f"),
            "★ Impact":     st.column_config.NumberColumn(format="%.2f"),
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
