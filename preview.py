import streamlit as st
import os
import pandas as pd


def _fmt_int(value):
    try:
        return f"{int(value):,}".replace(",", " ")
    except Exception:
        return "0"


@st.cache_data(show_spinner=False)
def _load_preview_stats(data_dir="data"):
    stats = {
        "cities": 0,
        "districts": 0,
        "reviews": 0,
        "offers": 0,
        "infra_items": 0,
    }
    if not os.path.isdir(data_dir):
        return stats

    for city in os.listdir(data_dir):
        base = os.path.join(data_dir, city, "processed")
        districts_path = os.path.join(base, "districts_final.csv")
        if not os.path.exists(districts_path):
            continue

        stats["cities"] += 1
        try:
            df = pd.read_csv(districts_path)
            stats["districts"] += int(len(df))
        except Exception:
            pass

        reviews_path = os.path.join(base, "reviews_analyzed.csv")
        if os.path.exists(reviews_path):
            try:
                stats["reviews"] += int(len(pd.read_csv(reviews_path)))
            except Exception:
                pass

        offers_path = os.path.join(base, "realty_offers.csv")
        if os.path.exists(offers_path):
            try:
                stats["offers"] += int(len(pd.read_csv(offers_path)))
            except Exception:
                pass

        infra_path = os.path.join(base, "infrastructure.csv")
        if os.path.exists(infra_path):
            try:
                infra_df = pd.read_csv(infra_path)
                count_cols = [c for c in infra_df.columns if c.endswith("_count")]
                if count_cols:
                    stats["infra_items"] += int(infra_df[count_cols].sum().sum())
            except Exception:
                pass

    return stats


@st.cache_data(show_spinner=False)
def _available_preview_cities(data_dir="data"):
    cities = []
    if not os.path.isdir(data_dir):
        return cities
    for city in os.listdir(data_dir):
        path = os.path.join(data_dir, city, "processed", "districts_final.csv")
        if os.path.exists(path):
            cities.append(city)
    return sorted(cities)


@st.cache_data(show_spinner=False)
def _load_city_districts(city, data_dir="data"):
    path = os.path.join(data_dir, city, "processed", "districts_final.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _safe_row(df, name_col, name):
    if df is None or df.empty:
        return None
    try:
        return df[df[name_col] == name].iloc[0]
    except Exception:
        return None

def render_page():
    stats = _load_preview_stats()
    
    st.markdown(
        """
        <style>
        .up-preview-wrap {
            padding: 2.5rem 2rem;
            max-width: 1200px;
            margin: 0 auto;
        }
        .up-preview-header {
            text-align: center;
            margin-bottom: 4rem;
        }
        .up-preview-title {
            font-size: 3rem;
            font-weight: 800;
            color: var(--ink);
            letter-spacing: -0.04em;
            margin-bottom: 1rem;
        }
        .up-preview-subtitle {
            font-size: 1.25rem;
            color: var(--muted);
            line-height: 1.6;
            max-width: 800px;
            margin: 0 auto;
        }
        .up-preview-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 2rem;
            margin-top: 3rem;
        }
        .up-preview-card {
            background: #FFFFFF;
            border: 1px solid var(--line);
            border-radius: 16px;
            padding: 2rem;
            box-shadow: var(--shadow-sm);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        .up-preview-card:hover {
            transform: translateY(-2px);
            box-shadow: var(--shadow-md);
        }
        .up-preview-icon {
            width: 48px;
            height: 48px;
            border-radius: 12px;
            background: #F8FAFC;
            display: flex;
            align-items: center;
            justify-content: center;
            margin-bottom: 1.5rem;
            color: var(--blue);
        }
        .up-preview-card-title {
            font-size: 1.25rem;
            font-weight: 700;
            color: var(--ink);
            margin-bottom: 0.75rem;
        }
        .up-preview-card-text {
            color: var(--muted);
            line-height: 1.5;
            font-size: 0.95rem;
        }
        
        /* Sidebar styling for preview page */
        .up-side-kicker { display: none; }
        .up-side-title {
            font-size: 1.25rem !important;
            font-weight: 600 !important;
            margin-bottom: 2rem !important;
            color: var(--ink) !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        f"""
        <div class="up-preview-wrap">
            <div class="up-preview-header">
                <div class="up-preview-title">UrbanPulse Overview</div>
                <div class="up-preview-subtitle">Spatial tracking, real-time analytics, and localized real estate insights powered by data.</div>
            </div>

            <div class="up-preview-grid">
                <div class="up-preview-card">
                    <div class="up-preview-card-title">Города</div>
                    <div class="up-preview-card-text">{_fmt_int(stats["cities"])} городов с данными</div>
                </div>
                <div class="up-preview-card">
                    <div class="up-preview-card-title">Районы</div>
                    <div class="up-preview-card-text">{_fmt_int(stats["districts"])} районов</div>
                </div>
                <div class="up-preview-card">
                    <div class="up-preview-card-title">Отзывы</div>
                    <div class="up-preview-card-text">{_fmt_int(stats["reviews"])} отзывов проанализировано</div>
                </div>
                <div class="up-preview-card">
                    <div class="up-preview-card-title">Объекты</div>
                    <div class="up-preview-card-text">{_fmt_int(stats["infra_items"])} объектов инфраструктуры · {_fmt_int(stats["offers"])} объявлений</div>
                </div>
            </div>
            
            <div class="up-preview-grid">
                <div class="up-preview-card">
                    <div class="up-preview-icon">
                        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"></path><circle cx="12" cy="10" r="3"></circle></svg>
                    </div>
                    <div class="up-preview-card-title">Precise Boundaries</div>
                    <div class="up-preview-card-text">Automated ingestion of strict OSM relations ensures district bounds do not overlap, retaining extreme detail.</div>
                </div>
                
                <div class="up-preview-card">
                    <div class="up-preview-icon" style="color: var(--green);">
                        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"></polyline></svg>
                    </div>
                    <div class="up-preview-card-title">Live Metrics</div>
                    <div class="up-preview-card-text">Proprietary modeling grades local environments based on transport loops, eco-zones, and safety patterns.</div>
                </div>
                
                <div class="up-preview-card">
                    <div class="up-preview-icon" style="color: var(--purple);">
                        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path><polyline points="9 22 9 12 15 12 15 22"></polyline></svg>
                    </div>
                    <div class="up-preview-card-title">Realty Radar</div>
                    <div class="up-preview-card-text">Deep local scans integrate real-time listings to surface live property pricing aggregated at a granular spatial level.</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    with st.sidebar:
        st.markdown(
            """
            <div class="up-side-title">UrbanPulse</div>
            <div class="up-side-nav">
                <div class="up-side-nav-item">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7"></rect><rect x="14" y="3" width="7" height="7"></rect><rect x="14" y="14" width="7" height="7"></rect><rect x="3" y="14" width="7" height="7"></rect></svg>
                    Dashboard
                </div>
                <div class="up-side-nav-item is-active">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect><line x1="3" y1="9" x2="21" y2="9"></line><line x1="9" y1="21" x2="9" y2="9"></line></svg>
                    Preview Mode
                </div>
            </div>
            
            <div style="margin-top:auto"></div>
            <div class="up-side-user">
                <div class="up-side-avatar">JD</div>
                <div class="up-side-user-info">
                    <strong>John Doe</strong>
                    <span>Admin</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
