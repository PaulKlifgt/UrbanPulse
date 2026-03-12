CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

/* Hide default streamlit elements */
#MainMenu, footer { visibility: hidden; }

header {
    visibility: visible !important;
    background: transparent !important;
}

[data-testid="collapsedControl"] {
    display: flex !important;
    visibility: visible !important;
}

/* Hide Streamlit runtime toolbar/status ("Stop / Deploy"). */
[data-testid="stToolbar"],
[data-testid="stStatusWidget"],
[data-testid="stDecoration"] {
    display: none !important;
}

:root {
    --bg-page: #F8FAFC;
    --bg-panel: #FFFFFF;
    --line: #E2E8F0;
    --ink: #0F172A;
    --muted: #64748B;
    --primary: #0F172A;
    --active: #0F172A;
    --blue: #3B82F6;
    --green: #10B981;
    --orange: #F59E0B;
    --red: #EF4444;
    --purple: #8B5CF6;
    --shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
    --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
}

html, body, [class*="css"] {
    font-family: "Inter", sans-serif !important;
    background: var(--bg-page) !important;
    color: var(--ink) !important;
}

.stApp {
    background: var(--bg-page) !important;
}

/* Base structural adjustments */
[data-testid="stAppViewContainer"] > .main {
    padding-top: 1rem;
    background: var(--bg-page) !important;
}
[data-testid="stAppViewContainer"] > .main > div {
    max-width: 1500px; /* Use wide layout */
    padding: 0 1rem 2rem 1rem;
}

/* Sidebar styling */
[data-testid="stSidebar"] {
    background: #FFFFFF !important;
    border-right: 1px solid var(--line) !important;
    min-width: 320px !important;
    max-width: 320px !important;
}
[data-testid="stSidebar"] > div:first-child {
    padding: 1.5rem 1.25rem;
}
[data-testid="stSidebar"][aria-expanded="false"] {
    min-width: 0 !important;
    max-width: 0 !important;
}

/* Sidebar Brand */
.up-side-kicker {
    display: block;
    color: var(--muted) !important;
    font-size: 0.82rem !important;
    font-weight: 700 !important;
    margin-bottom: 0.35rem !important;
}
.up-side-title {
    font-size: 1.25rem !important;
    font-weight: 600 !important;
    margin-bottom: 0.35rem !important;
    color: var(--ink) !important;
}
.up-side-copy {
    display: block;
    margin-bottom: 1rem !important;
    color: var(--muted) !important;
    line-height: 1.5 !important;
}

/* Sidebar Nav */
.up-side-search-wrap {
    margin-bottom: 1.5rem;
}
.up-side-search {
    border: 1px solid var(--line);
    border-radius: 12px;
    background: var(--bg-page);
    padding: 0.95rem 1rem;
    color: #94A3B8;
    font-size: 0.95rem;
    font-weight: 600;
}
.up-side-nav {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    margin-bottom: 2rem;
}
.up-side-nav-item {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.65rem 1rem;
    border-radius: 8px;
    color: var(--muted);
    font-size: 0.9rem;
    font-weight: 500;
    cursor: default;
}
.up-side-nav-item.is-active {
    background: var(--active);
    color: #FFFFFF !important;
    box-shadow: var(--shadow-md);
}
.up-side-nav-item svg {
    width: 18px;
    height: 18px;
    opacity: 0.8;
}
.up-side-section-label {
    color: var(--muted);
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin: 1rem 0 0.9rem 0;
}

/* Filter sections inside sidebar */
[data-testid="stSidebar"] .stMarkdown p,
[data-testid="stSidebar"] label {
    color: var(--muted) !important;
    font-size: 0.8rem !important;
}
[data-testid="stSidebar"] .stSelectbox > div[data-baseweb="select"] > div {
    border-radius: 8px !important;
    border: 1px solid var(--line) !important;
    background: var(--bg-page) !important;
    padding-left: 0.5rem;
}

/* Sidebar User */
.up-side-user {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-top: auto;
    padding-top: 1.5rem;
    border-top: 1px solid var(--line);
}
.up-side-avatar {
    width: 36px;
    height: 36px;
    border-radius: 50%;
    background: var(--ink);
    color: #fff;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.85rem;
    font-weight: 600;
}
.up-side-user-info strong {
    display: block;
    font-size: 0.85rem;
    font-weight: 600;
    line-height: 1.2;
}
.up-side-user-info span {
    display: block;
    font-size: 0.75rem;
    color: var(--muted);
}

/* Main Map Area */
.up-map-summary {
    background: var(--bg-panel);
    border: 1px solid var(--line);
    border-radius: 12px;
    box-shadow: var(--shadow-sm);
    margin-bottom: 1.5rem;
    padding: 1.25rem 1.35rem;
}
.up-panel-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 1rem;
}
.up-panel-title {
    margin: 0;
    color: var(--ink);
    font-size: 1.35rem;
    font-weight: 700;
    line-height: 1.1;
}
.up-panel-meta {
    margin: 0.35rem 0 0 0;
    color: var(--muted);
    font-size: 0.95rem;
    line-height: 1.5;
}
.up-city-chip {
    flex-shrink: 0;
    background: #F8FAFC;
    border: 1px solid var(--line);
    border-radius: 14px;
    padding: 0.85rem 1rem;
    min-width: 210px;
}
.up-city-chip strong {
    display: block;
    color: var(--ink);
    font-size: 1rem;
    font-weight: 700;
}
.up-city-chip span {
    display: block;
    margin-top: 0.25rem;
    color: var(--muted);
    font-size: 0.85rem;
}


/* Special Offers (Below Map) */
.up-offers-wrap {
    background: #FFFFFF;
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 1.25rem;
    box-shadow: var(--shadow-sm);
}
.up-offers-head {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 0.9rem;
    font-weight: 600;
    color: var(--ink);
    margin-bottom: 1rem;
}
.up-offers-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1rem;
}
.up-offer-card {
    border: 1px solid var(--line);
    border-radius: 8px;
    padding: 1rem;
    background: #FFFFFF;
}
.up-offer-top {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 0.5rem;
}
.up-offer-title {
    font-size: 0.85rem;
    font-weight: 600;
    color: var(--ink);
    line-height: 1.2;
}
.up-offer-badge {
    background: var(--ink);
    color: #FFFFFF;
    font-size: 0.7rem;
    font-weight: 600;
    padding: 0.2rem 0.6rem;
    border-radius: 999px;
    white-space: nowrap;
}
.up-offer-subtitle {
    font-size: 0.75rem;
    color: var(--muted);
    margin-bottom: 0.5rem;
    display: flex;
    align-items: center;
    gap: 0.35rem;
}
.up-offer-subtitle svg {
    width: 12px;
    height: 12px;
}
.up-offer-bottom {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    margin-top: 1rem;
}
.up-offer-price {
    font-size: 0.9rem;
    font-weight: 600;
    color: var(--ink);
}
.up-offer-meta {
    font-size: 0.7rem;
    color: var(--muted);
    display: flex;
    align-items: center;
    gap: 0.25rem;
}
.up-offer-actions {
    margin-top: 0.85rem;
}
.up-offer-link {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-height: 36px;
    padding: 0.55rem 0.9rem;
    border-radius: 10px;
    background: #0F172A;
    color: #FFFFFF !important;
    font-size: 0.8rem;
    font-weight: 700;
    text-decoration: none !important;
}
.up-offer-link:hover {
    background: #1E293B;
}


/* Right Sidebar Stats Area */
.up-stats-header {
    font-size: 1rem;
    font-weight: 600;
    color: var(--ink);
    margin-bottom: 1.25rem;
}
.up-stats-grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 1rem;
    margin-bottom: 2rem;
}
.up-stat-card {
    background: #FFFFFF;
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 1rem;
    box-shadow: var(--shadow-sm);
}
.up-stat-top {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.75rem;
}
.up-stat-icon {
    width: 28px;
    height: 28px;
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: #F1F5F9;
    color: var(--blue);
}
.up-stat-icon svg {
    width: 16px;
    height: 16px;
    display: block;
}
.up-stat-icon.green { color: var(--green); }
.up-stat-icon.purple { color: var(--purple); }
.up-stat-icon.orange { color: var(--orange); }

.up-stat-delta {
    font-size: 0.75rem;
    font-weight: 500;
}
.up-stat-delta.positive { color: var(--green); }
.up-stat-delta.neutral { color: var(--muted); }

.up-stat-value {
    font-size: 1.5rem;
    font-weight: 600;
    color: var(--ink);
    line-height: 1;
    margin-bottom: 0.25rem;
}
.up-stat-label {
    font-size: 0.8rem;
    color: var(--muted);
}

.up-chart-container {
    background: #FFFFFF;
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 1rem;
    box-shadow: var(--shadow-sm);
    margin-bottom: 1.5rem;
}
.up-chart-title {
    font-size: 0.85rem;
    font-weight: 600;
    color: var(--muted);
    margin-bottom: 1rem;
    margin-top: 1rem;
}

.up-rating-wrap {
    background: #FFFFFF;
    border: 1px solid var(--line);
    border-radius: 12px;
    box-shadow: var(--shadow-sm);
    overflow: hidden;
}
.up-rating-head,
.up-rating-row {
    display: grid;
    grid-template-columns: minmax(0, 1.5fr) minmax(0, 1.3fr) 56px 56px;
    gap: 0.85rem;
    align-items: center;
    padding: 0.9rem 1rem;
}
.up-rating-head {
    background: #F8FAFC;
    border-bottom: 1px solid var(--line);
    color: var(--muted);
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.04em;
    text-transform: uppercase;
}
.up-rating-row {
    border-bottom: 1px solid #EEF2F7;
}
.up-rating-row:last-child {
    border-bottom: none;
}
.up-rating-name {
    color: var(--ink);
    font-weight: 600;
}
.up-score-cell {
    display: flex;
    align-items: center;
    gap: 0.65rem;
}
.up-score-track {
    flex: 1;
    height: 10px;
    border-radius: 999px;
    background: #E2E8F0;
    overflow: hidden;
}
.up-score-fill {
    height: 100%;
    border-radius: inherit;
    background: linear-gradient(90deg, #9DBDFF 0%, #2A61E8 100%);
}
.up-score-text,
.up-grade,
.up-eco {
    color: var(--ink);
    font-size: 0.85rem;
    font-weight: 600;
}


/* Detail Section (Bottom or other tab) */
.up-detail-panel {
    background: #FFFFFF;
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 1.5rem;
    margin-top: 2rem;
    box-shadow: var(--shadow-sm);
}
.up-detail-title {
    font-size: 1.5rem;
    font-weight: 600;
    color: var(--ink);
    margin-bottom: 0.25rem;
}
.up-detail-subtitle {
    font-size: 0.9rem;
    color: var(--muted);
}

[data-testid="stMetric"] {
    background: #FFFFFF;
    border: 1px solid var(--line);
    border-radius: 12px;
    box-shadow: var(--shadow-sm);
    padding: 0.9rem 1rem;
}

.stFolium {
    margin: 0 0 1.25rem 0 !important;
    background: #FFFFFF !important;
    border: 1px solid var(--line) !important;
    border-radius: 12px !important;
    box-shadow: var(--shadow-sm) !important;
    padding: 0.75rem !important;
}

.stFolium > div,
.stFolium iframe {
    border-radius: 12px !important;
    overflow: hidden !important;
}

[data-testid="stPlotlyChart"] {
    background: #FFFFFF !important;
    border: 1px solid var(--line) !important;
    border-radius: 12px !important;
    box-shadow: var(--shadow-sm) !important;
    padding: 0.75rem !important;
    margin-bottom: 1rem !important;
}

[data-testid="stExpander"] {
    background: #FFFFFF !important;
    border: 1px solid var(--line) !important;
    border-radius: 12px !important;
    box-shadow: var(--shadow-sm) !important;
    overflow: hidden !important;
}

.stTabs [data-baseweb="tab-list"] {
    background: #F8FAFC !important;
    border: 1px solid var(--line) !important;
    border-radius: 10px !important;
    padding: 4px !important;
    margin-bottom: 0.75rem !important;
}

.stTabs [data-baseweb="tab"] {
    border-radius: 8px !important;
    font-weight: 600 !important;
    color: var(--muted) !important;
}

.stTabs [aria-selected="true"] {
    background: #FFFFFF !important;
    color: var(--ink) !important;
}

.stTabs [data-baseweb="tab-highlight"],
.stTabs [data-baseweb="tab-border"] {
    display: none !important;
}

@media (max-width: 1180px) {
    .up-panel-head {
        flex-direction: column;
    }

    .up-city-chip {
        min-width: 0;
        width: 100%;
    }

    .up-offers-grid {
        grid-template-columns: repeat(2, 1fr);
    }

    .up-stats-grid {
        grid-template-columns: 1fr;
    }

    .up-rating-head,
    .up-rating-row {
        grid-template-columns: minmax(0, 1fr);
    }

    .up-rating-head div:nth-child(n+2),
    .up-rating-row div:nth-child(n+2) {
        justify-self: start;
    }
}

@media (max-width: 900px) {
    [data-testid="stAppViewContainer"] > .main > div {
        padding: 0 0.55rem 1rem 0.55rem !important;
    }

    [data-testid="stSidebar"] {
        min-width: 100% !important;
        max-width: 100% !important;
    }

    .up-topbar {
        padding: 12px 14px !important;
        margin-bottom: 10px !important;
    }

    .up-topbar-title {
        font-size: 1.85rem !important;
        line-height: 1.05 !important;
    }

    .up-map-summary {
        padding: 0.9rem 0.95rem !important;
    }

    .up-panel-title {
        font-size: 1.06rem !important;
    }

    .up-panel-meta {
        font-size: 0.88rem !important;
    }

    .up-detail-title {
        font-size: 1.45rem !important;
    }

    .up-offers-grid {
        grid-template-columns: 1fr !important;
    }

    .stTabs [data-baseweb="tab-list"] {
        overflow-x: auto !important;
        scrollbar-width: thin;
    }

    .stTabs [data-baseweb="tab"] {
        white-space: nowrap !important;
    }

    .stFolium iframe {
        min-height: 420px !important;
    }

    [data-testid="stDataFrame"] {
        overflow-x: auto !important;
    }
}

</style>
"""
