# streamlit_admin.py (финальная, исправленная версия)

import streamlit as st
import asyncpg
import pandas as pd
import asyncio
from datetime import date, timedelta
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
from funnel_logic import fetch_funnel_stats, _fetch_trial_retention_cohorts

st.set_page_config(layout="wide")

# === Настройки подключения к БД ===
DB_URL = st.secrets["DATABASE_URL"]

# --- УПРАВЛЕНИЕ ЦИКЛОМ ASYNCIO ---

@st.cache_resource
def get_event_loop():
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop

@st.cache_resource
def get_db_pool(_loop):
    return _loop.run_until_complete(asyncpg.create_pool(dsn=DB_URL, loop=_loop))

loop = get_event_loop()
pool = get_db_pool(loop)

# --- АСИНХРОННЫЕ ФУНКЦИИ ---
# (Они остаются без изменений)
async def _fetch_user_stats(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT date_trunc('day', created_at)::date AS day,
                   COUNT(*) FILTER (WHERE is_blocked IS DISTINCT FROM true) AS new_active,
                   COUNT(*) FILTER (WHERE is_blocked = true) AS new_blocked
            FROM users GROUP BY day ORDER BY day
        ''')
    df = pd.DataFrame(rows, columns=["day", "new_active", "new_blocked"])
    if not df.empty:
        df['day'] = pd.to_datetime(df['day'])
        df = df.sort_values("day").set_index("day")
        df = df.resample('D').sum().fillna(0).reset_index()
        df["active_users"] = df["new_active"].cumsum()
        df["blocked_users"] = -df["new_blocked"].cumsum()
    return df

async def _fetch_sources(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT source FROM users WHERE source IS NOT NULL ORDER BY source")
    return ["все"] + [row["source"] for row in rows]

async def _fetch_retention_cohorts(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            WITH user_cohorts AS (
                SELECT id AS user_id, date_trunc('week', created_at)::date AS cohort_week FROM users
            ), user_activity AS (
                SELECT DISTINCT user_id, date_trunc('week', created_at)::date AS activity_week FROM meals
            )
            SELECT
                c.cohort_week,
                FLOOR((a.activity_week - c.cohort_week) / 7) AS week_number,
                COUNT(DISTINCT c.user_id) AS retained_users
            FROM user_cohorts c JOIN user_activity a ON c.user_id = a.user_id AND a.activity_week >= c.cohort_week
            GROUP BY c.cohort_week, week_number ORDER BY c.cohort_week, week_number
        ''')
    return pd.DataFrame(rows, columns=['cohort_week', 'week_number', 'retained_users'])

async def _fetch_paid_growth(pool, start_date, end_date):
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT date_trunc('day', starts_at)::date AS day, COUNT(*) AS new_subs
            FROM subscriptions WHERE is_active = TRUE AND starts_at::date BETWEEN $1 AND $2
            GROUP BY day ORDER BY day
        ''', start_date, end_date)
    return pd.DataFrame(rows, columns=["day", "new_subs"])

async def _fetch_arppu(pool, start_date, end_date):
    async with pool.acquire() as conn:
        return await conn.fetchval('''
            SELECT AVG(amount_cents) FROM subscriptions
            WHERE is_active = TRUE AND starts_at::date BETWEEN $1 AND $2
        ''', start_date, end_date)

async def _fetch_active_subscriptions_over_time(pool):
    async with pool.acquire() as conn:
        has_subs = await conn.fetchval("SELECT EXISTS (SELECT 1 FROM subscriptions)")
        if not has_subs:
            return pd.DataFrame(columns=["day", "active_subs"])
        
        rows = await conn.fetch('''
            SELECT day::date, COUNT(*) AS active_subs
            FROM generate_series(
                     (SELECT MIN(starts_at)::date FROM subscriptions),
                     CURRENT_DATE, interval '1 day'
                 ) AS day
            JOIN subscriptions s ON day >= s.starts_at::date AND day < s.ends_at::date
            WHERE s.is_active = TRUE GROUP BY day ORDER BY day
        ''')
    return pd.DataFrame(rows, columns=["day", "active_subs"])

async def _fetch_active_users_by_period(pool, days):
    """Считает уникальных активных пользователей за последние N дней."""
    async with pool.acquire() as conn:
        query = '''
            SELECT COUNT(DISTINCT user_id)
            FROM meals
            WHERE created_at >= NOW() - INTERVAL '$1 days'
        '''
        # Примечание: $1 в строке INTERVAL - это особенность asyncpg
        # для безопасной вставки числовых значений в такие конструкции.
        return await conn.fetchval(query.replace('$1', str(days)))

async def _fetch_active_users_over_time(pool, period='day'):
    """Считает количество уникальных активных пользователей по периодам."""
    # Безопасно проверяем, что период - одно из допустимых значений
    if period not in ['day', 'week', 'month']:
        period = 'day'
        
    async with pool.acquire() as conn:
        rows = await conn.fetch(f'''
            SELECT
                date_trunc('{period}', created_at)::date AS period_start,
                COUNT(DISTINCT user_id) AS active_users
            FROM meals
            GROUP BY period_start
            ORDER BY period_start
        ''')
    df = pd.DataFrame(rows, columns=['period_start', 'active_users'])
    if not df.empty:
        df = df.set_index('period_start')
    return df

async def _fetch_meals_stats_over_time(pool, period='day'):
    """Считает общее и среднее количество приемов пищи по периодам."""
    if period not in ['day', 'week', 'month']:
        period = 'day'
        
    async with pool.acquire() as conn:
        rows = await conn.fetch(f'''
            SELECT
                date_trunc('{period}', created_at)::date AS period_start,
                COUNT(id) AS total_meals,
                COUNT(DISTINCT user_id) AS active_users
            FROM meals
            GROUP BY period_start
            ORDER BY period_start
        ''')
    df = pd.DataFrame(rows, columns=['period_start', 'total_meals', 'active_users'])
    if not df.empty:
        # Считаем среднее, избегая деления на ноль
        df['avg_meals_per_user'] = df.apply(
            lambda row: row['total_meals'] / row['active_users'] if row['active_users'] > 0 else 0,
            axis=1
        )
        df = df.set_index('period_start')
    return df

async def _fetch_total_active_users(pool):
    """Считает общее количество НЕ заблокированных пользователей."""
    async with pool.acquire() as conn:
        return await conn.fetchval('SELECT COUNT(*) FROM users WHERE is_blocked IS DISTINCT FROM TRUE')

async def _fetch_total_paying_users(pool):
    """Считает общее количество пользователей с активной подпиской."""
    async with pool.acquire() as conn:
        return await conn.fetchval('SELECT COUNT(*) FROM subscriptions WHERE is_active = TRUE')

# --- СИНХРОННЫЕ ОБЕРТКИ С КЕШИРОВАНИЕМ ---

@st.cache_data(ttl=600)
def fetch_user_stats_cached():
    return loop.run_until_complete(_fetch_user_stats(pool))

@st.cache_data(ttl=600)
def fetch_sources_cached():
    return loop.run_until_complete(_fetch_sources(pool))

@st.cache_data(ttl=600)
def fetch_funnel_stats_cached(sources, start_date, end_date):
    return loop.run_until_complete(fetch_funnel_stats(pool, sources, start_date, end_date))

@st.cache_data(ttl=600)
def fetch_retention_cohorts_cached():
    return loop.run_until_complete(_fetch_retention_cohorts(pool))

@st.cache_data(ttl=600)
def fetch_trial_retention_cohorts_cached():
    return loop.run_until_complete(_fetch_trial_retention_cohorts(pool))

@st.cache_data(ttl=600)
def fetch_paid_growth_cached(start_date, end_date):
    return loop.run_until_complete(_fetch_paid_growth(pool, start_date, end_date))

@st.cache_data(ttl=600)
def fetch_arppu_cached(start_date, end_date):
    return loop.run_until_complete(_fetch_arppu(pool, start_date, end_date))

@st.cache_data(ttl=600)
def fetch_active_subscriptions_over_time_cached():
    return loop.run_until_complete(_fetch_active_subscriptions_over_time(pool))

@st.cache_data(ttl=600)
def fetch_active_users_by_period_cached(days):
    return loop.run_until_complete(_fetch_active_users_by_period(pool, days))

@st.cache_data(ttl=600)
def fetch_active_users_over_time_cached(period='day'):
    return loop.run_until_complete(_fetch_active_users_over_time(pool, period))

@st.cache_data(ttl=600)
def fetch_meals_stats_over_time_cached(period='day'):
    return loop.run_until_complete(_fetch_meals_stats_over_time(pool, period))

@st.cache_data(ttl=60)
def fetch_total_active_users_cached():
    return loop.run_until_complete(_fetch_total_active_users(pool))

@st.cache_data(ttl=60)
def fetch_total_paying_users_cached():
    return loop.run_until_complete(_fetch_total_paying_users(pool))

# --- Функции для отрисовки графиков ---

def plot_funnel_plotly(data, avg_days):
    if not data or pd.DataFrame(data).empty:
        st.info("Нет данных для построения воронки за выбранный период.")
        return

    df = pd.DataFrame(data)

    # Безопасные расчёты процентов
    df = df.copy()
    total = float(df["count"].iloc[0]) if df["count"].iloc[0] else 0.0
    pct_total = []
    pct_prev = []
    prev = None
    for i, val in enumerate(df["count"].tolist()):
        # % от общего
        pct_total.append(round((val / total * 100.0), 1) if total > 0 else 0.0)
        # % от предыдущего (для первого шага считаем 100%)
        if i == 0:
            pct_prev.append(100.0)
        else:
            pct_prev.append(round((val / prev * 100.0), 1) if prev and prev > 0 else 0.0)
        prev = val

    df["% от общего"] = pct_total
    df["% от предыдущего"] = pct_prev

    fig = go.Figure(go.Funnel(
        y=df["stage"],
        x=df["count"],
        textposition="inside",
        constraintext='inside',
        textfont=dict(size=12, color='white'),
        # две метрики из customdata: [pct_prev, pct_total]
        customdata=df[["% от предыдущего", "% от общего"]],
        texttemplate="<b>%{value}</b><br>%{customdata[0]:.1f}% от предыдущего<br><i>%{customdata[1]:.1f}% от всех</i>",
    ))

    title = f"🔻 Воронка: путь пользователя (в среднем {avg_days} дн. до покупки)" if avg_days else "🔻 Воронка: путь пользователя"
    fig.update_layout(title_text=title, height=600, margin=dict(l=200, r=50, t=50, b=50))
    st.plotly_chart(fig, use_container_width=True)

def plot_retention_heatmap(df):
    import plotly.express as px

    if df.empty or 'cohort_week' not in df.columns:
        st.info("Недостаточно данных для построения когортного анализа.")
        return

    cohort_sizes = df[df['week_number'] == 0].set_index('cohort_week')['retained_users']
    if cohort_sizes.empty:
        st.info("Недостаточно данных для расчета размеров когорт.")
        return

    df = df.copy()
    df['cohort_size'] = df['cohort_week'].map(cohort_sizes)
    df['retention'] = (df['retained_users'] / df['cohort_size']) * 100

    # ПРОЦЕНТЫ для цвета
    retention_pivot = df.pivot_table(index='cohort_week', columns='week_number', values='retention')
    # АБСОЛЮТНЫЕ для текста
    absolute_pivot = df.pivot_table(index='cohort_week', columns='week_number', values='retained_users')

    # Подписи вида "12.3% (5)"
    text_matrix = [
        [
            "" if pd.isna(retention_pivot.loc[idx, col]) else f"{retention_pivot.loc[idx, col]:.1f}% ({int(absolute_pivot.loc[idx, col])})"
            for col in retention_pivot.columns
        ]
        for idx in retention_pivot.index
    ]

    # Красивые метки индекса
    try:
        retention_pivot.index = pd.to_datetime(retention_pivot.index).strftime('%Y-%m-%d')
    except Exception:
        pass  # если уже строки — оставим как есть

    fig = px.imshow(
        retention_pivot,
        labels=dict(x="Неделя после регистрации", y="Когорта (неделя регистрации)", color="Удержание, %"),
        title="🗺️ Когортный анализ удержания пользователей (по неделям)",
        color_continuous_scale='Blues',
        range_color=[0, 100],
        aspect="auto"
    )

    fig.update_traces(text=text_matrix, texttemplate="%{text}", textfont_size=9)

    # 🔑 динамическая высота
    num_rows = retention_pivot.shape[0]  # либо: df['cohort_week'].nunique()
    row_height = 30  # подстрой по вкусу (25–35)
    fig.update_layout(height=max(300, num_rows * row_height),
                      margin=dict(l=100, r=50, t=50, b=50))

    fig.update_xaxes(side="top")
    st.plotly_chart(fig, use_container_width=True)


def plot_trial_retention_heatmap(df):
    import plotly.express as px
    # ожидаем, что есть: import pandas as pd, import streamlit as st

    if df.empty or not {'cohort_day', 'day_number', 'retained_users'}.issubset(df.columns):
        st.info("Недостаточно данных для построения удержания по триалу.")
        return

    df = df.copy()

    # cohort_size как в старой логике: берем retained_users на day 0
    sizes = df[df['day_number'] == 0].set_index('cohort_day')['retained_users']
    if sizes.empty:
        # запасной вариант, чтобы график не падал, если day 0 отсутствует
        sizes = df.groupby('cohort_day')['retained_users'].max()

    df['cohort_size'] = df['cohort_day'].map(sizes).astype(float)
    df['retention'] = (df['retained_users'] / df['cohort_size']).replace([float('inf'), -float('inf')], 0).fillna(0) * 100

    # сводные: проценты для цвета, абсолюты для подписи
    retention_pivot = df.pivot_table(index='cohort_day', columns='day_number', values='retention')
    absolute_pivot  = df.pivot_table(index='cohort_day', columns='day_number', values='retained_users')

    # упорядочим колонки 0..4
    cols = sorted([c for c in retention_pivot.columns if pd.notna(c)])
    retention_pivot = retention_pivot.reindex(columns=cols)
    absolute_pivot  = absolute_pivot.reindex(columns=cols)

    # подписи "12.3% (5)"
    text_matrix = [
        [
            "" if pd.isna(retention_pivot.loc[idx, col])
            else f"{retention_pivot.loc[idx, col]:.1f}% ({int(absolute_pivot.loc[idx, col])})"
            for col in retention_pivot.columns
        ]
        for idx in retention_pivot.index
    ]

    # красивые метки дат
    try:
        retention_pivot.index = pd.to_datetime(retention_pivot.index).strftime('%b %d, %Y')
    except Exception:
        pass

    fig = px.imshow(
        retention_pivot,
        labels=dict(x="День после начала триала (0 = день активации)", y="Когорта (дата активации)", color="Удержание, %"),
        title="🎯 Удержание в рамках 5-дневного триала",
        color_continuous_scale='Greens',
        range_color=[0, 100],
        aspect="auto"
    )
    fig.update_traces(text=text_matrix, texttemplate="%{text}", textfont_size=9)
    fig.update_xaxes(side="top")

    # 🔑 авто-высота: одна строка ≈ 30 px
    num_rows = retention_pivot.shape[0]
    row_height = 30
    fig.update_layout(
        height=max(320, num_rows * row_height),
        margin=dict(l=110, r=50, t=60, b=50)
    )

    st.plotly_chart(fig, use_container_width=True)




# --- ГЛАВНЫЙ КОД ---

st.title("📊 Админ-панель: Аналитика бота")
with st.spinner("Загружаем ключевые метрики..."):
    total_active = fetch_total_active_users_cached()
    total_paying = fetch_total_paying_users_cached()

col1, col2 = st.columns(2)
col1.metric("Всего активных пользователей", total_active or 0)
col2.metric("Пользователей с подпиской", f"{total_paying or 0} 💳")

st.markdown("---")
tab1, tab2 = st.tabs(["💡 Пользователи и воронка", "💳 Монетизация"])

with tab1:
    st.header("📈 Динамика пользовательской базы")

    st.markdown("#### Активные пользователи (DAU/WAU/MAU)")
    col1, col2, col3 = st.columns(3)
    with col1:
        dau = fetch_active_users_by_period_cached(1)
        st.metric("За день (DAU)", dau or 0)
    with col2:
        wau = fetch_active_users_by_period_cached(7)
        st.metric("За неделю (WAU)", wau or 0)
    with col3:
        mau = fetch_active_users_by_period_cached(30)
        st.metric("За месяц (MAU)", mau or 0)

    with st.spinner("Загружаем данные о пользователях..."):
        users_df = fetch_user_stats_cached()
    
    if not users_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Накопительно")
            melted_cumulative = users_df.melt(id_vars="day", value_vars=["active_users", "blocked_users"], var_name="type", value_name="count")
            chart_cumulative = alt.Chart(melted_cumulative).mark_line().encode(x="day:T", y="count:Q", color="type:N", tooltip=['day', 'type', 'count']).interactive()
            st.altair_chart(chart_cumulative, use_container_width=True)
        with col2:
            st.markdown("#### Прирост по дням")
            melted_daily = users_df.melt(id_vars="day", value_vars=["new_active", "new_blocked"], var_name="type", value_name="count")
            chart_daily = alt.Chart(melted_daily).mark_bar().encode(x="day:T", y="count:Q", color="type:N", tooltip=['day', 'type', 'count']).interactive()
            st.altair_chart(chart_daily, use_container_width=True)
    else:
        st.info("Нет данных о пользователях.")

    st.markdown("<hr>", unsafe_allow_html=True)
    st.header("🍔 Динамика сохранения приемов пищи")

    meals_period_options = {'По дням': 'day', 'По неделям': 'week', 'По месяцам': 'month'}
    meals_selected_period_label = st.selectbox(
        "Выберите период агрегации:",
        options=list(meals_period_options.keys()),
        key="meals_period_selector" # Уникальный ключ для этого селектбокса
    )
    meals_selected_period = meals_period_options[meals_selected_period_label]

    with st.spinner(f"Загружаем данные о приемах пищи..."):
        meals_stats_df = fetch_meals_stats_over_time_cached(meals_selected_period)

    if not meals_stats_df.empty:
        # Выбор, что отображать на графике
        display_option = st.radio(
            "Показать на графике:",
            ["Общее количество", "Среднее на пользователя"],
            horizontal=True
        )

        if display_option == "Общее количество":
            st.line_chart(meals_stats_df['total_meals'])
        else:
            st.line_chart(meals_stats_df['avg_meals_per_user'])
    else:
        st.info("Нет данных о приемах пищи для построения графика.")

    st.markdown("<hr>", unsafe_allow_html=True)
    st.header("🔻 Воронка конверсии")
    
    sources = fetch_sources_cached()
    selected_sources = st.multiselect("Источники (source):", sources, default=["все"])
    query_sources = [] if "все" in selected_sources else selected_sources
    
    # --- ИЗМЕНЕНИЕ 2: Добавляем кнопки для быстрой фильтрации дат ---
    
    today = date.today()
    
    # Инициализируем session_state, если его нет
    if 'start_date' not in st.session_state:
        st.session_state.start_date = today - timedelta(days=6)
        st.session_state.end_date = today

    def set_date_range(start, end):
        st.session_state.start_date = start
        st.session_state.end_date = end

    # Рассчитываем даты для кнопок
    yesterday = today - timedelta(days=1)
    last_7_days_start = today - timedelta(days=6)
    this_month_start = today.replace(day=1)
    last_month_end = this_month_start - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    # Создаем кнопки
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.button("Сегодня", on_click=set_date_range, args=(today, today), use_container_width=True)
    with col2:
        st.button("Вчера", on_click=set_date_range, args=(yesterday, yesterday), use_container_width=True)
    with col3:
        st.button("7 дней", on_click=set_date_range, args=(last_7_days_start, today), use_container_width=True)
    with col4:
        st.button("Этот месяц", on_click=set_date_range, args=(this_month_start, today), use_container_width=True)
    with col5:
        st.button("Прошлый месяц", on_click=set_date_range, args=(last_month_start, last_month_end), use_container_width=True)
    
    # Виджеты для выбора дат, управляемые через session_state
    start_date = st.date_input("Начало периода", key="start_date")
    end_date = st.date_input("Конец периода", key="end_date")

    if start_date > end_date:
        st.error("Дата начала не может быть позже даты окончания.")
    else:
        with st.spinner("Строим воронку..."):
            funnel_result = fetch_funnel_stats_cached(query_sources, start_date, end_date)
        
        # Оборачиваем график в колонки, чтобы сузить его
        col1, col2, col3 = st.columns([0.5, 2, 0.5])
        with col2:
            plot_funnel_plotly(funnel_result["funnel"], funnel_result["avg_days"])

    st.markdown("<hr>", unsafe_allow_html=True)
    st.header("📈 Динамика Активных Пользователей")

    period_options = {'По дням (DAU)': 'day', 'По неделям (WAU)': 'week', 'По месяцам (MAU)': 'month'}
    selected_period_label = st.selectbox(
        "Выберите период агрегации:",
        options=list(period_options.keys())
    )
    selected_period = period_options[selected_period_label]

    with st.spinner(f"Загружаем данные об активных пользователях..."):
        active_users_df = fetch_active_users_over_time_cached(selected_period)

    if not active_users_df.empty:
        st.line_chart(active_users_df)
    else:
        st.info("Нет данных об активных пользователях для построения графика.")

    st.markdown("<hr>", unsafe_allow_html=True)
    st.header("🗺️ Удержание пользователей (Retention)")
    with st.spinner("Считаем когорты..."):
        retention_df = fetch_retention_cohorts_cached()
    plot_retention_heatmap(retention_df)

    st.markdown("<hr>", unsafe_allow_html=True)
    st.header("🎯 Удержание в рамках 5-дневного триала")
    with st.spinner("Считаем когорты триала..."):
        trial_retention_df = fetch_trial_retention_cohorts_cached()
    plot_trial_retention_heatmap(trial_retention_df)


with tab2:
    st.header("💸 Ключевые метрики монетизации")
    
    m_today = date.today()
    m_start_date = st.date_input("Начало периода", value=m_today - timedelta(days=29), key="m_start")
    m_end_date = st.date_input("Конец периода", value=m_today, key="m_end")
    
    if m_start_date > m_end_date:
        st.error("Дата начала не может быть позже даты окончания.")
    else:
        st.markdown("#### 💹 Юнит-экономика")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            marketing_spend = st.number_input("Введите затраты на маркетинг за период ($)", min_value=0.0, step=10.0)
        
        with st.spinner("Считаем юнит-экономику..."):
            df_growth = fetch_paid_growth_cached(m_start_date, m_end_date)
            new_subscribers = df_growth['new_subs'].sum()
            arppu_cents = fetch_arppu_cached(m_start_date, m_end_date)
        
        with col2:
            cac = (marketing_spend / new_subscribers) if new_subscribers > 0 else 0
            st.metric("CAC (Cost per Acquisition)", f"${cac:.2f}")

        with col3:
            arppu = (arppu_cents / 100) if arppu_cents else 0
            st.metric("ARPPU (Average Revenue Per Paying User)", f"${arppu:.2f}")

        st.markdown("<hr>", unsafe_allow_html=True)
        st.header("📈 Динамика платных подписок")

        with st.spinner("Загружаем данные о подписках..."):
            subs_df = fetch_active_subscriptions_over_time_cached()
        
        if not subs_df.empty:
            st.markdown("#### Общее количество активных подписок")
            st.line_chart(subs_df.set_index('day'))
            st.markdown("#### Прирост новых платных подписчиков по дням")
            st.bar_chart(df_growth.set_index('day'))
        else:
            st.info("Нет данных о подписках.")