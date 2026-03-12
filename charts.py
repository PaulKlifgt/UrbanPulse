# charts.py
import plotly.graph_objects as go

LAYOUT = dict(
    font=dict(family="system-ui, sans-serif", size=13, color="#212529"),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=16, r=16, t=48, b=16),
)

# ── 8 компонент ──
SCORE_COLS = [
    "infrastructure_score", "education_score", "healthcare_score",
    "transport_score", "ecology_score", "safety_score",
    "leisure_score", "social_score",
]

SCORE_LABELS = [
    "Инфраструктура", "Образование", "Здравоохранение",
    "Транспорт", "Экология", "Безопасность",
    "Досуг", "Соц. среда",
]

# Для таблиц сравнения и т.д.
SCORE_LABELS_MAP = dict(zip(SCORE_COLS, SCORE_LABELS))

SCORE_EMOJI = {
    "infrastructure_score": "🏪",
    "education_score":      "🎓",
    "healthcare_score":     "🏥",
    "transport_score":      "🚌",
    "ecology_score":        "🌿",
    "safety_score":         "🛡️",
    "leisure_score":        "🎭",
    "social_score":         "💬",
}


# В charts.py — обновить grade_color:

def grade_color(value):
    if value >= 75: return "#059669"   # Зелёный (emerald)
    if value >= 60: return "#6366f1"   # Индиго
    if value >= 45: return "#f59e0b"   # Янтарный
    if value >= 30: return "#ef4444"   # Красный
    return "#6b7280"                    # Серый


def radar(row, title=""):
    vals = [row.get(c, 50) for c in SCORE_COLS]
    name = row.get("district", row.get("microdistrict", ""))
    idx = row.get("total_index", 0)

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=vals + [vals[0]],
        theta=SCORE_LABELS + [SCORE_LABELS[0]],
        fill="toself",
        fillcolor="rgba(13,110,253,0.1)",
        line=dict(color="#0d6efd", width=2),
        marker=dict(size=5, color="#0d6efd"),
    ))
    fig.update_layout(
        **LAYOUT,
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100],
                            gridcolor="#dee2e6",
                            tickfont=dict(size=10, color="#6c757d")),
            angularaxis=dict(gridcolor="#dee2e6",
                             tickfont=dict(size=11, color="#212529")),
            bgcolor="rgba(0,0,0,0)",
        ),
        showlegend=False,
        title=dict(
            text=title or f"{name} — {idx:.0f}/100",
            font=dict(size=15, color="#212529")),
        height=420,
    )
    return fig


def radar_compare(rows):
    colors = ["#0d6efd", "#dc3545", "#198754", "#fd7e14"]
    fills = [
        "rgba(13,110,253,0.08)",
        "rgba(220,53,69,0.08)",
        "rgba(25,135,84,0.08)",
        "rgba(253,126,20,0.08)",
    ]

    fig = go.Figure()
    for i, row in enumerate(rows):
        vals = [row.get(c, 50) for c in SCORE_COLS]
        fig.add_trace(go.Scatterpolar(
            r=vals + [vals[0]],
            theta=SCORE_LABELS + [SCORE_LABELS[0]],
            fill="toself",
            fillcolor=fills[i % len(fills)],
            line=dict(color=colors[i % len(colors)], width=2),
            name=f"{row.get('district', '?')} ({row.get('total_index', 0):.0f})",
        ))
    fig.update_layout(
        **LAYOUT,
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100], gridcolor="#dee2e6",
                            tickfont=dict(color="#6c757d")),
            angularaxis=dict(gridcolor="#dee2e6",
                             tickfont=dict(color="#212529")),
            bgcolor="rgba(0,0,0,0)",
        ),
        showlegend=True,
        legend=dict(font=dict(size=12, color="#212529")),
        height=460,
    )
    return fig


def bar_rating(df):
    df_s = df.sort_values("total_index", ascending=False).reset_index(drop=True)
    colors = [grade_color(v) for v in df_s["total_index"]]

    fig = go.Figure(go.Bar(
        x=df_s["total_index"],
        y=df_s["district"],
        orientation="h",
        marker=dict(color=colors, cornerradius=4),
        text=df_s["total_index"].apply(lambda x: f"{x:.0f}"),
        textposition="outside",
        textfont=dict(size=12, color="#212529"),
    ))
    fig.update_layout(
        **LAYOUT,
        height=max(360, len(df) * 36),
        yaxis=dict(autorange="reversed",
                   tickfont=dict(size=12, color="#212529")),
        xaxis=dict(range=[0, 105], showgrid=False,
                   tickfont=dict(color="#6c757d")),
    )
    return fig