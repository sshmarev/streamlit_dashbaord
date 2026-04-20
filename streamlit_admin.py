# streamlit_admin.py (финальная, исправленная версия)

import streamlit as st
import asyncpg
import pandas as pd
import asyncio
from datetime import date, timedelta
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
import funnel_logic

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
                SELECT id AS user_id, date_trunc('week', created_at)::date AS cohort_week
                FROM users
            ), cohort_sizes AS (
                SELECT cohort_week, COUNT(DISTINCT user_id) AS cohort_size
                FROM user_cohorts
                GROUP BY cohort_week
            ), user_activity AS (
                SELECT DISTINCT user_id, date_trunc('week', created_at)::date AS activity_week
                FROM meals
            )
            SELECT
                c.cohort_week,
                FLOOR((a.activity_week - c.cohort_week) / 7) AS week_number,
                cs.cohort_size,
                COUNT(DISTINCT c.user_id) AS retained_users
            FROM user_cohorts c
            JOIN cohort_sizes cs ON cs.cohort_week = c.cohort_week
            LEFT JOIN user_activity a ON c.user_id = a.user_id AND a.activity_week >= c.cohort_week
            WHERE a.activity_week IS NOT NULL
            GROUP BY c.cohort_week, week_number, cs.cohort_size
            ORDER BY c.cohort_week, week_number
        ''')
    return pd.DataFrame(rows, columns=['cohort_week', 'week_number', 'cohort_size', 'retained_users'])

async def _fetch_paid_growth(pool, start_date, end_date):
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT date_trunc('day', starts_at)::date AS day, COUNT(DISTINCT user_id) AS new_subs
            FROM subscriptions
            WHERE is_trial = FALSE AND starts_at::date BETWEEN $1 AND $2
            GROUP BY day ORDER BY day
        ''', start_date, end_date)
    return pd.DataFrame(rows, columns=["day", "new_subs"])

async def _fetch_arppu(pool, start_date, end_date):
    async with pool.acquire() as conn:
        return await conn.fetchval('''
            SELECT AVG(amount_cents) FROM subscriptions
            WHERE is_trial = FALSE AND starts_at::date BETWEEN $1 AND $2
        ''', start_date, end_date)

async def _fetch_active_subscriptions_over_time(pool):
    async with pool.acquire() as conn:
        has_subs = await conn.fetchval("SELECT EXISTS (SELECT 1 FROM subscriptions WHERE is_trial = FALSE)")
        if not has_subs:
            return pd.DataFrame(columns=["day", "active_subs"])
        
        rows = await conn.fetch('''
            SELECT day::date, COUNT(*) AS active_subs
            FROM generate_series(
                     (SELECT MIN(starts_at)::date FROM subscriptions WHERE is_trial = FALSE),
                     CURRENT_DATE, interval '1 day'
                 ) AS day
            JOIN subscriptions s ON day >= s.starts_at::date AND day < s.ends_at::date
            WHERE s.is_trial = FALSE
            GROUP BY day ORDER BY day
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
        return await conn.fetchval("""
            SELECT COUNT(DISTINCT user_id)
            FROM subscriptions
            WHERE is_active = TRUE
              AND is_trial = FALSE
              AND ends_at > (NOW() AT TIME ZONE 'UTC')
        """)

# --- СИНХРОННЫЕ ОБЕРТКИ С КЕШИРОВАНИЕМ ---

async def _fetch_activation_funnel(pool, sources, start_date, end_date):
    return await funnel_logic.fetch_activation_funnel(pool, sources, start_date, end_date)

async def _fetch_trial_retention_cohorts_local(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            WITH trial_cohorts AS (
                SELECT user_id, date_trunc('day', MIN(created_at))::date AS cohort_day
                FROM user_actions
                WHERE action IN ('trial_started', 'trial_activated')
                GROUP BY user_id
            ),
            user_activity AS (
                SELECT DISTINCT user_id, date_trunc('day', created_at)::date AS activity_day
                FROM meals
            )
            SELECT
                c.cohort_day,
                (a.activity_day - c.cohort_day) AS day_number,
                COUNT(DISTINCT c.user_id) AS retained_users
            FROM trial_cohorts c
            JOIN user_activity a ON c.user_id = a.user_id AND a.activity_day >= c.cohort_day
            WHERE (a.activity_day - c.cohort_day) BETWEEN 0 AND 4
            GROUP BY c.cohort_day, day_number
            ORDER BY c.cohort_day, day_number
        """)
    return pd.DataFrame(rows, columns=["cohort_day", "day_number", "retained_users"])

async def _fetch_trial_retention_cohorts(pool):
    fetcher = getattr(
        funnel_logic,
        "fetch_trial_retention_cohorts",
        getattr(funnel_logic, "_fetch_trial_retention_cohorts", None),
    )
    if fetcher is None:
        return await _fetch_trial_retention_cohorts_local(pool)
    return await fetcher(pool)

async def _fetch_day_retention(pool, anchor='registration'):
    if anchor not in {'registration', 'first_meal'}:
        anchor = 'registration'
    anchor_sql = (
        "SELECT id AS user_id, created_at::date AS cohort_day FROM users"
        if anchor == 'registration'
        else "SELECT user_id, MIN(created_at)::date AS cohort_day FROM meals GROUP BY user_id"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            WITH anchors AS ({anchor_sql}),
            cohort_sizes AS (
                SELECT cohort_day, COUNT(DISTINCT user_id) AS cohort_size
                FROM anchors
                GROUP BY cohort_day
            ),
            activity AS (
                SELECT DISTINCT user_id, created_at::date AS activity_day
                FROM meals
            )
            SELECT
                a.cohort_day,
                (act.activity_day - a.cohort_day) AS day_number,
                cs.cohort_size,
                COUNT(DISTINCT a.user_id) AS retained_users
            FROM anchors a
            JOIN cohort_sizes cs ON cs.cohort_day = a.cohort_day
            LEFT JOIN activity act ON act.user_id = a.user_id AND act.activity_day >= a.cohort_day
            WHERE (act.activity_day - a.cohort_day) IN (0, 1, 3, 7, 14, 30)
            GROUP BY a.cohort_day, day_number, cs.cohort_size
            ORDER BY a.cohort_day, day_number
        """)
    return pd.DataFrame(rows, columns=['cohort_day', 'day_number', 'cohort_size', 'retained_users'])

async def _fetch_quality_metrics(pool, start_date, end_date):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT action, COUNT(*) AS events, COUNT(DISTINCT user_id) AS users
            FROM user_actions
            WHERE created_at::date BETWEEN $1 AND $2
              AND (
                action IN ('clarify_clicked', 'delete_last', 'accept_clicked', 'accept_and_continue_clicked')
                OR action LIKE 'input_%'
              )
            GROUP BY action
            ORDER BY events DESC
        """, start_date, end_date)
    df = pd.DataFrame(rows, columns=['action', 'events', 'users'])
    if df.empty:
        return df, {}
    total_inputs = int(df[df['action'].str.startswith('input_')]['events'].sum())
    clarify = int(df.loc[df['action'] == 'clarify_clicked', 'events'].sum())
    delete_last = int(df.loc[df['action'] == 'delete_last', 'events'].sum())
    accepted = int(df[df['action'].isin(['accept_clicked', 'accept_and_continue_clicked'])]['events'].sum())
    return df, {
        'total_inputs': total_inputs,
        'clarify_rate': (clarify / total_inputs * 100) if total_inputs else 0,
        'delete_rate': (delete_last / total_inputs * 100) if total_inputs else 0,
        'accept_rate': (accepted / total_inputs * 100) if total_inputs else 0,
    }

async def _fetch_habit_metrics(pool, start_date, end_date):
    async with pool.acquire() as conn:
        summary = await conn.fetchrow("""
            WITH daily AS (
                SELECT user_id, created_at::date AS day, COUNT(*) AS meals
                FROM meals
                WHERE created_at::date BETWEEN $1 AND $2
                GROUP BY user_id, day
            )
            SELECT
                COUNT(DISTINCT user_id) AS active_users,
                COUNT(*) AS active_user_days,
                COALESCE(SUM(meals), 0) AS total_meals,
                COUNT(*) FILTER (WHERE meals >= 2) AS user_days_2plus,
                AVG(meals::float) AS avg_meals_per_active_day
            FROM daily
        """, start_date, end_date)
        streak_rows = await conn.fetch("""
            WITH active_days AS (
                SELECT user_id, COUNT(DISTINCT created_at::date) AS logging_days
                FROM meals
                WHERE created_at::date BETWEEN $1 AND $2
                GROUP BY user_id
            )
            SELECT logging_days, COUNT(*) AS users
            FROM active_days
            GROUP BY logging_days
            ORDER BY logging_days
        """, start_date, end_date)
        returned_after_reminder = await conn.fetchval("""
            WITH reminders AS (
                SELECT user_id, MIN(created_at) AS reminder_at
                FROM user_actions
                WHERE action IN ('reminder_2h_after_start', 'reminder_streak_risk', 'retention_day3_sent', 'retention_day7_sent')
                  AND created_at::date BETWEEN $1 AND $2
                GROUP BY user_id
            )
            SELECT COUNT(DISTINCT r.user_id)
            FROM reminders r
            JOIN meals m ON m.user_id = r.user_id
             AND m.created_at > r.reminder_at
             AND m.created_at <= r.reminder_at + INTERVAL '48 hours'
        """, start_date, end_date)
    return dict(summary) if summary else {}, pd.DataFrame(streak_rows, columns=['logging_days', 'users']), int(returned_after_reminder or 0)

async def _fetch_segment_metrics(pool, start_date, end_date):
    async with pool.acquire() as conn:
        source_rows = await conn.fetch("""
            SELECT
                COALESCE(u.source, 'unknown') AS source,
                COUNT(DISTINCT u.id) AS users,
                COUNT(DISTINCT m.user_id) AS meal_users,
                COUNT(DISTINCT s.user_id) FILTER (WHERE s.is_trial = FALSE) AS paid_users
            FROM users u
            LEFT JOIN meals m ON m.user_id = u.id
            LEFT JOIN subscriptions s ON s.user_id = u.id
            WHERE u.created_at::date BETWEEN $1 AND $2
            GROUP BY source
            ORDER BY users DESC
        """, start_date, end_date)
        geo_rows = await conn.fetch("""
            SELECT
                COALESCE(country_code, 'unknown') AS country,
                COALESCE(currency, 'unknown') AS currency,
                COALESCE(language, 'unknown') AS language,
                COUNT(*) AS users
            FROM users
            WHERE created_at::date BETWEEN $1 AND $2
            GROUP BY country, currency, language
            ORDER BY users DESC
            LIMIT 50
        """, start_date, end_date)
        provider_rows = await conn.fetch("""
            SELECT provider, currency, COUNT(DISTINCT user_id) AS users, SUM(amount_cents) AS amount
            FROM subscriptions
            WHERE is_trial = FALSE AND starts_at::date BETWEEN $1 AND $2
            GROUP BY provider, currency
            ORDER BY users DESC
        """, start_date, end_date)
    return (
        pd.DataFrame(source_rows, columns=['source', 'users', 'meal_users', 'paid_users']),
        pd.DataFrame(geo_rows, columns=['country', 'currency', 'language', 'users']),
        pd.DataFrame(provider_rows, columns=['provider', 'currency', 'users', 'amount']),
    )

async def _fetch_paywall_metrics(pool, start_date, end_date):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            WITH paywalls AS (
                SELECT
                    user_id,
                    CASE
                        WHEN action LIKE 'paywall_shown_limit_reached' THEN 'limit_reached'
                        WHEN action LIKE 'paywall_shown_trial_ended' THEN 'trial_ended'
                        WHEN action LIKE 'paywall_shown_value_moment' OR action = 'value_paywall_shown' THEN 'value_moment'
                        WHEN action LIKE '%daily_meal_limit' THEN 'daily_meal_limit'
                        ELSE action
                    END AS paywall_type,
                    created_at
                FROM user_actions
                WHERE created_at::date BETWEEN $1 AND $2
                  AND (action LIKE 'paywall_shown_%' OR action LIKE 'soft_paywall_shown_%' OR action = 'value_paywall_shown')
            ),
            checkouts AS (
                SELECT user_id, created_at
                FROM user_actions
                WHERE action IN ('stripe_checkout_created', 'stars_checkout_opened_1m', 'stars_invoice_shown')
            ),
            payments AS (
                SELECT user_id, starts_at AS created_at
                FROM subscriptions
                WHERE is_trial = FALSE
            )
            SELECT
                p.paywall_type,
                COUNT(*) AS shown_events,
                COUNT(DISTINCT p.user_id) AS shown_users,
                COUNT(DISTINCT c.user_id) AS checkout_users,
                COUNT(DISTINCT pay.user_id) AS paid_users
            FROM paywalls p
            LEFT JOIN checkouts c ON c.user_id = p.user_id AND c.created_at >= p.created_at AND c.created_at <= p.created_at + INTERVAL '7 days'
            LEFT JOIN payments pay ON pay.user_id = p.user_id AND pay.created_at >= p.created_at AND pay.created_at <= p.created_at + INTERVAL '14 days'
            GROUP BY p.paywall_type
            ORDER BY shown_users DESC
        """, start_date, end_date)
    df = pd.DataFrame(rows, columns=['paywall_type', 'shown_events', 'shown_users', 'checkout_users', 'paid_users'])
    if not df.empty:
        df['checkout_rate_%'] = df.apply(lambda r: (r['checkout_users'] / r['shown_users'] * 100) if r['shown_users'] else 0, axis=1)
        df['paid_rate_%'] = df.apply(lambda r: (r['paid_users'] / r['shown_users'] * 100) if r['shown_users'] else 0, axis=1)
    return df

async def _fetch_unit_economics(pool, start_date, end_date):
    async with pool.acquire() as conn:
        revenue_rows = await conn.fetch("""
            SELECT
                COALESCE(provider, 'unknown') AS provider,
                COALESCE(currency, 'unknown') AS currency,
                COUNT(*) AS payments,
                COUNT(DISTINCT user_id) AS paying_users,
                SUM(amount_cents) AS gross_amount,
                AVG(amount_cents) AS arppu_cents
            FROM subscriptions
            WHERE is_trial = FALSE AND starts_at::date BETWEEN $1 AND $2
            GROUP BY provider, currency
            ORDER BY gross_amount DESC NULLS LAST
        """, start_date, end_date)
        active_rows = await conn.fetch("""
            SELECT
                COALESCE(provider, 'unknown') AS provider,
                COALESCE(currency, 'unknown') AS currency,
                COUNT(DISTINCT user_id) AS active_payers,
                SUM(
                    CASE
                        WHEN plan ILIKE '%12%' OR plan ILIKE '%year%' THEN amount_cents / 12.0
                        WHEN plan ILIKE '%3%' THEN amount_cents / 3.0
                        ELSE amount_cents
                    END
                ) AS mrr_cents
            FROM subscriptions
            WHERE is_trial = FALSE
              AND is_active = TRUE
              AND ends_at > (NOW() AT TIME ZONE 'UTC')
            GROUP BY provider, currency
            ORDER BY mrr_cents DESC NULLS LAST
        """)
        churn = await conn.fetchval("""
            WITH previous_paid AS (
                SELECT DISTINCT user_id
                FROM subscriptions
                WHERE is_trial = FALSE
                  AND starts_at::date < $1
                  AND ends_at::date >= ($1 - INTERVAL '30 days')::date
            ),
            current_paid AS (
                SELECT DISTINCT user_id
                FROM subscriptions
                WHERE is_trial = FALSE
                  AND is_active = TRUE
                  AND ends_at > (NOW() AT TIME ZONE 'UTC')
            )
            SELECT COUNT(*)
            FROM previous_paid p
            LEFT JOIN current_paid c ON c.user_id = p.user_id
            WHERE c.user_id IS NULL
        """, start_date)
    return (
        pd.DataFrame(revenue_rows, columns=['provider', 'currency', 'payments', 'paying_users', 'gross_amount', 'arppu_cents']),
        pd.DataFrame(active_rows, columns=['provider', 'currency', 'active_payers', 'mrr_cents']),
        int(churn or 0),
    )

@st.cache_data(ttl=600)
def fetch_user_stats_cached():
    return loop.run_until_complete(_fetch_user_stats(pool))

@st.cache_data(ttl=600)
def fetch_sources_cached():
    return loop.run_until_complete(_fetch_sources(pool))

@st.cache_data(ttl=600)
def fetch_funnel_stats_cached(sources, start_date, end_date):
    return loop.run_until_complete(funnel_logic.fetch_funnel_stats(pool, sources, start_date, end_date))

@st.cache_data(ttl=600)
def fetch_activation_funnel_cached(sources, start_date, end_date):
    return loop.run_until_complete(_fetch_activation_funnel(pool, sources, start_date, end_date))

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

@st.cache_data(ttl=600)
def fetch_day_retention_cached(anchor):
    return loop.run_until_complete(_fetch_day_retention(pool, anchor))

@st.cache_data(ttl=600)
def fetch_quality_metrics_cached(start_date, end_date):
    return loop.run_until_complete(_fetch_quality_metrics(pool, start_date, end_date))

@st.cache_data(ttl=600)
def fetch_habit_metrics_cached(start_date, end_date):
    return loop.run_until_complete(_fetch_habit_metrics(pool, start_date, end_date))

@st.cache_data(ttl=600)
def fetch_segment_metrics_cached(start_date, end_date):
    return loop.run_until_complete(_fetch_segment_metrics(pool, start_date, end_date))

@st.cache_data(ttl=600)
def fetch_paywall_metrics_cached(start_date, end_date):
    return loop.run_until_complete(_fetch_paywall_metrics(pool, start_date, end_date))

@st.cache_data(ttl=600)
def fetch_unit_economics_cached(start_date, end_date):
    return loop.run_until_complete(_fetch_unit_economics(pool, start_date, end_date))

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

    if 'cohort_size' in df.columns:
        cohort_sizes = df.groupby('cohort_week')['cohort_size'].first()
    else:
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

def plot_day_retention_heatmap(df, title):
    if df.empty or not {'cohort_day', 'day_number', 'cohort_size', 'retained_users'}.issubset(df.columns):
        st.info("Недостаточно данных для retention-графика.")
        return

    df = df.copy()
    df['retention'] = (df['retained_users'] / df['cohort_size']).replace([float('inf'), -float('inf')], 0).fillna(0) * 100
    retention_pivot = df.pivot_table(index='cohort_day', columns='day_number', values='retention').reindex(columns=[0, 1, 3, 7, 14, 30])
    absolute_pivot = df.pivot_table(index='cohort_day', columns='day_number', values='retained_users').reindex(columns=[0, 1, 3, 7, 14, 30])
    text_matrix = [
        [
            "" if pd.isna(retention_pivot.loc[idx, col]) else f"{retention_pivot.loc[idx, col]:.1f}% ({int(absolute_pivot.loc[idx, col])})"
            for col in retention_pivot.columns
        ]
        for idx in retention_pivot.index
    ]
    try:
        retention_pivot.index = pd.to_datetime(retention_pivot.index).strftime('%Y-%m-%d')
    except Exception:
        pass

    fig = px.imshow(
        retention_pivot,
        labels=dict(x="День после старта", y="Когорта", color="Retention, %"),
        title=title,
        color_continuous_scale='Blues',
        range_color=[0, 100],
        aspect="auto",
    )
    fig.update_traces(text=text_matrix, texttemplate="%{text}", textfont_size=9)
    fig.update_xaxes(side="top")
    fig.update_layout(height=max(320, retention_pivot.shape[0] * 30), margin=dict(l=110, r=50, t=60, b=50))
    st.plotly_chart(fig, use_container_width=True)


def money_from_minor(amount, currency):
    value = float(amount or 0)
    if str(currency).upper() == 'XTR':
        return f"{value:.0f} XTR"
    return f"{value / 100:.2f} {str(currency).upper()}"


def add_conversion_columns(df, numerator, denominator, out_col):
    if df.empty:
        return df
    df = df.copy()
    df[out_col] = df.apply(lambda r: (r[numerator] / r[denominator] * 100) if r[denominator] else 0, axis=1)
    return df


st.title("📊 Админ-панель: Аналитика бота")
with st.spinner("Загружаем ключевые метрики..."):
    total_active = fetch_total_active_users_cached()
    total_paying = fetch_total_paying_users_cached()

col1, col2 = st.columns(2)
col1.metric("Всего активных пользователей", total_active or 0)
col2.metric("Пользователей с подпиской", f"{total_paying or 0} 💳")

st.markdown("---")
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Acquisition & Activation",
    "Engagement & Retention",
    "Quality",
    "Segments",
    "Paywall",
    "Monetization",
])

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
            st.caption("Когортная воронка: пользователи, зарегистрированные в выбранный период. Поздние действия этих пользователей тоже входят в следующие шаги.")
            plot_funnel_plotly(funnel_result["funnel"], funnel_result["avg_days"])

        with st.spinner("Строим event activation funnel..."):
            activation_funnel = fetch_activation_funnel_cached(query_sources, start_date, end_date)
        col1, col2, col3 = st.columns([0.5, 2, 0.5])
        with col2:
            st.caption("Event funnel: события, которые произошли именно в выбранный период.")
            plot_funnel_plotly(activation_funnel, None)

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
    st.header("Retention D1/D3/D7/D14/D30 и привычка")
    retention_anchor = st.radio(
        "Точка отсчета retention",
        ["registration", "first_meal"],
        horizontal=True,
        format_func=lambda x: "Регистрация" if x == "registration" else "Первый сохраненный прием",
    )
    with st.spinner("Считаем дневной retention..."):
        day_retention_df = fetch_day_retention_cached(retention_anchor)
    plot_day_retention_heatmap(
        day_retention_df,
        "Retention от регистрации" if retention_anchor == "registration" else "Retention от первого приема пищи",
    )

    st.markdown("#### Habit metrics")
    h_today = date.today()
    h_start = st.date_input("Начало периода", value=h_today - timedelta(days=29), key="habit_start")
    h_end = st.date_input("Конец периода", value=h_today, key="habit_end")
    if h_start <= h_end:
        habit_summary, streak_df, returned_after_reminder = fetch_habit_metrics_cached(h_start, h_end)
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Active users", habit_summary.get("active_users", 0) or 0)
        col2.metric("Meals", habit_summary.get("total_meals", 0) or 0)
        col3.metric("Avg meals / active day", f"{habit_summary.get('avg_meals_per_active_day') or 0:.2f}")
        col4.metric("User-days 2+ meals", habit_summary.get("user_days_2plus", 0) or 0)
        col5.metric("Returned after reminder", returned_after_reminder)
        if not streak_df.empty:
            st.bar_chart(streak_df.set_index("logging_days"))
    else:
        st.error("Начало периода не может быть позже конца.")

with tab3:
    st.header("Качество распознавания и UX")
    q_today = date.today()
    q_start = st.date_input("Начало периода", value=q_today - timedelta(days=29), key="quality_start")
    q_end = st.date_input("Конец периода", value=q_today, key="quality_end")
    if q_start <= q_end:
        quality_df, quality_summary = fetch_quality_metrics_cached(q_start, q_end)
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Inputs", quality_summary.get("total_inputs", 0))
        col2.metric("Clarify rate", f"{quality_summary.get('clarify_rate', 0):.1f}%")
        col3.metric("Delete-last rate", f"{quality_summary.get('delete_rate', 0):.1f}%")
        col4.metric("Accept rate", f"{quality_summary.get('accept_rate', 0):.1f}%")
        st.dataframe(quality_df, use_container_width=True)
    else:
        st.error("Начало периода не может быть позже конца.")

with tab4:
    st.header("Источники, география и провайдеры")
    s_today = date.today()
    s_start = st.date_input("Начало периода", value=s_today - timedelta(days=29), key="segments_start")
    s_end = st.date_input("Конец периода", value=s_today, key="segments_end")
    if s_start <= s_end:
        source_df, geo_df, provider_df = fetch_segment_metrics_cached(s_start, s_end)
        if not source_df.empty:
            source_df = add_conversion_columns(source_df, "meal_users", "users", "activation_%")
            source_df = add_conversion_columns(source_df, "paid_users", "users", "paid_%")
        st.markdown("#### Source / campaign")
        st.dataframe(source_df, use_container_width=True)
        st.markdown("#### Country / currency / language")
        st.dataframe(geo_df, use_container_width=True)
        st.markdown("#### Stripe vs Telegram Stars")
        st.dataframe(provider_df, use_container_width=True)
    else:
        st.error("Начало периода не может быть позже конца.")

with tab5:
    st.header("Paywall analytics")
    p_today = date.today()
    p_start = st.date_input("Начало периода", value=p_today - timedelta(days=29), key="paywall_start")
    p_end = st.date_input("Конец периода", value=p_today, key="paywall_end")
    if p_start <= p_end:
        paywall_df = fetch_paywall_metrics_cached(p_start, p_end)
        st.dataframe(paywall_df, use_container_width=True)
    else:
        st.error("Начало периода не может быть позже конца.")

with tab6:
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

        revenue_df, mrr_df, churned_users = fetch_unit_economics_cached(m_start_date, m_end_date)
        st.markdown("#### Unit economics by provider/currency")
        if not revenue_df.empty:
            revenue_view = revenue_df.copy()
            revenue_view["gross"] = revenue_view.apply(lambda r: money_from_minor(r["gross_amount"], r["currency"]), axis=1)
            revenue_view["arppu"] = revenue_view.apply(lambda r: money_from_minor(r["arppu_cents"], r["currency"]), axis=1)
            st.dataframe(revenue_view[["provider", "currency", "payments", "paying_users", "gross", "arppu"]], use_container_width=True)
        else:
            st.info("Нет оплат за выбранный период.")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Active MRR / ARR")
            if not mrr_df.empty:
                mrr_view = mrr_df.copy()
                mrr_view["mrr"] = mrr_view.apply(lambda r: money_from_minor(r["mrr_cents"], r["currency"]), axis=1)
                mrr_view["arr"] = mrr_view.apply(lambda r: money_from_minor((r["mrr_cents"] or 0) * 12, r["currency"]), axis=1)
                st.dataframe(mrr_view[["provider", "currency", "active_payers", "mrr", "arr"]], use_container_width=True)
            else:
                st.info("Нет активных платящих подписок.")
        with col2:
            st.metric("Churned paid users", churned_users)
            if marketing_spend and not revenue_df.empty:
                paying_users = int(revenue_df["paying_users"].sum() or 0)
                gross_major = float(revenue_df[revenue_df["currency"].str.upper() != "XTR"]["gross_amount"].sum() or 0) / 100
                cac_proxy = marketing_spend / paying_users if paying_users else 0
                ltv_proxy = gross_major / paying_users if paying_users else 0
                st.metric("LTV/CAC proxy", f"{(ltv_proxy / cac_proxy) if cac_proxy else 0:.2f}")

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
