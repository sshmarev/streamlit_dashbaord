from datetime import date
from typing import Dict, List

import asyncpg
import pandas as pd


cohort_steps = [
    ("Все пользователи когорты", """
        SELECT COUNT(DISTINCT u.id)
        FROM users u
        WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND u.created_at::date BETWEEN $2 AND $3
    """),
    ("Отправили первое сообщение", """
        SELECT COUNT(DISTINCT ua.user_id)
        FROM user_actions ua
        JOIN users u ON u.id = ua.user_id
        WHERE ua.action IN ('input_text', 'input_photo', 'input_voice')
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND u.created_at::date BETWEEN $2 AND $3
    """),
    ("Сохранили 1-й прием пищи", """
        SELECT COUNT(DISTINCT m.user_id)
        FROM meals m
        JOIN users u ON u.id = m.user_id
        WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND u.created_at::date BETWEEN $2 AND $3
    """),
    ("Сохранили 2-й прием пищи", """
        SELECT COUNT(*)
        FROM (
            SELECT u.id
            FROM users u
            JOIN meals m ON m.user_id = u.id
            WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
              AND u.created_at::date BETWEEN $2 AND $3
            GROUP BY u.id
            HAVING COUNT(m.id) >= 2
        ) q
    """),
    ("Сохранили 3-й прием пищи", """
        SELECT COUNT(*)
        FROM (
            SELECT u.id
            FROM users u
            JOIN meals m ON m.user_id = u.id
            WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
              AND u.created_at::date BETWEEN $2 AND $3
            GROUP BY u.id
            HAVING COUNT(m.id) >= 3
        ) q
    """),
    ("Сохранили 4-й прием пищи", """
        SELECT COUNT(*)
        FROM (
            SELECT u.id
            FROM users u
            JOIN meals m ON m.user_id = u.id
            WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
              AND u.created_at::date BETWEEN $2 AND $3
            GROUP BY u.id
            HAVING COUNT(m.id) >= 4
        ) q
    """),
    ("Сохранили 5-й прием пищи", """
        SELECT COUNT(*)
        FROM (
            SELECT u.id
            FROM users u
            JOIN meals m ON m.user_id = u.id
            WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
              AND u.created_at::date BETWEEN $2 AND $3
            GROUP BY u.id
            HAVING COUNT(m.id) >= 5
        ) q
    """),
    ("Активировали trial", """
        SELECT COUNT(DISTINCT ua.user_id)
        FROM user_actions ua
        JOIN users u ON u.id = ua.user_id
        WHERE ua.action = 'trial_activated'
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND u.created_at::date BETWEEN $2 AND $3
    """),
    ("Вызвали /subscribe", """
        SELECT COUNT(DISTINCT ua.user_id)
        FROM user_actions ua
        JOIN users u ON u.id = ua.user_id
        WHERE ua.action = 'subscribe_cmd'
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND u.created_at::date BETWEEN $2 AND $3
    """),
    ("Открыли оплату", """
        SELECT COUNT(DISTINCT ua.user_id)
        FROM user_actions ua
        JOIN users u ON u.id = ua.user_id
        WHERE (ua.action LIKE 'subscribe_opened%%' OR ua.action IN ('stripe_checkout_created', 'stars_checkout_opened_1m'))
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND u.created_at::date BETWEEN $2 AND $3
    """),
    ("Оплатили подписку", """
        SELECT COUNT(DISTINCT s.user_id)
        FROM subscriptions s
        JOIN users u ON u.id = s.user_id
        WHERE s.is_trial = FALSE
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND u.created_at::date BETWEEN $2 AND $3
    """),
]


avg_time_query = """
    SELECT AVG(s.starts_at - u.created_at) AS avg_duration
    FROM subscriptions s
    JOIN users u ON u.id = s.user_id
    WHERE s.is_trial = FALSE
      AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
      AND u.created_at::date BETWEEN $2 AND $3
"""


activation_steps = [
    ("start", """
        SELECT COUNT(DISTINCT ua.user_id)
        FROM user_actions ua
        JOIN users u ON u.id = ua.user_id
        WHERE ua.action = 'start'
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND ua.created_at::date BETWEEN $2 AND $3
    """),
    ("first_input", """
        WITH first_input AS (
            SELECT user_id, MIN(created_at)::date AS event_day
            FROM user_actions
            WHERE action IN ('input_text', 'input_photo', 'input_voice')
            GROUP BY user_id
        )
        SELECT COUNT(DISTINCT fi.user_id)
        FROM first_input fi
        JOIN users u ON u.id = fi.user_id
        WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND fi.event_day BETWEEN $2 AND $3
    """),
    ("first_successful_meal", """
        WITH first_meal AS (
            SELECT user_id, MIN(created_at)::date AS event_day
            FROM meals
            GROUP BY user_id
        )
        SELECT COUNT(DISTINCT fm.user_id)
        FROM first_meal fm
        JOIN users u ON u.id = fm.user_id
        WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND fm.event_day BETWEEN $2 AND $3
    """),
    ("accept_or_save", """
        SELECT COUNT(DISTINCT ua.user_id)
        FROM user_actions ua
        JOIN users u ON u.id = ua.user_id
        WHERE ua.action IN ('accept_clicked', 'accept_and_continue_clicked')
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND ua.created_at::date BETWEEN $2 AND $3
    """),
    ("second_meal", """
        WITH numbered_meals AS (
            SELECT user_id, created_at::date AS event_day,
                   ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at, id) AS meal_number
            FROM meals
        )
        SELECT COUNT(DISTINCT nm.user_id)
        FROM numbered_meals nm
        JOIN users u ON u.id = nm.user_id
        WHERE nm.meal_number = 2
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND nm.event_day BETWEEN $2 AND $3
    """),
    ("fifth_meal", """
        WITH numbered_meals AS (
            SELECT user_id, created_at::date AS event_day,
                   ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at, id) AS meal_number
            FROM meals
        )
        SELECT COUNT(DISTINCT nm.user_id)
        FROM numbered_meals nm
        JOIN users u ON u.id = nm.user_id
        WHERE nm.meal_number = 5
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND nm.event_day BETWEEN $2 AND $3
    """),
    ("trial_started", """
        SELECT COUNT(DISTINCT ua.user_id)
        FROM user_actions ua
        JOIN users u ON u.id = ua.user_id
        WHERE ua.action IN ('trial_started', 'trial_activated')
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND ua.created_at::date BETWEEN $2 AND $3
    """),
    ("paywall_seen", """
        SELECT COUNT(DISTINCT ua.user_id)
        FROM user_actions ua
        JOIN users u ON u.id = ua.user_id
        WHERE (
              ua.action LIKE 'paywall_shown_%'
              OR ua.action LIKE 'soft_paywall_shown_%'
              OR ua.action = 'value_paywall_shown'
          )
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND ua.created_at::date BETWEEN $2 AND $3
    """),
    ("checkout_opened", """
        SELECT COUNT(DISTINCT ua.user_id)
        FROM user_actions ua
        JOIN users u ON u.id = ua.user_id
        WHERE ua.action IN ('stripe_checkout_created', 'stars_checkout_opened_1m', 'stars_invoice_shown')
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND ua.created_at::date BETWEEN $2 AND $3
    """),
    ("payment_success", """
        SELECT COUNT(DISTINCT paid.user_id)
        FROM (
            SELECT user_id, starts_at::date AS event_day
            FROM subscriptions
            WHERE is_trial = FALSE
            UNION
            SELECT user_id, created_at::date AS event_day
            FROM user_actions
            WHERE action IN ('subscribe_success', 'subscribe_success_stars', 'stars_purchase_success')
        ) paid
        JOIN users u ON u.id = paid.user_id
        WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND paid.event_day BETWEEN $2 AND $3
    """),
]


async def fetch_funnel_stats(pool: asyncpg.Pool, sources: List[str], start_date: date, end_date: date) -> Dict:
    """Cohort funnel: users registered in the selected period."""
    sql_sources = sources if sources else None
    funnel = []
    async with pool.acquire() as conn:
        for stage, query in cohort_steps:
            count = await conn.fetchval(query, sql_sources, start_date, end_date)
            funnel.append({"stage": stage, "count": count or 0})

        avg_days = await conn.fetchval(avg_time_query, sql_sources, start_date, end_date)
        if avg_days is not None:
            avg_days = round(avg_days.total_seconds() / 86400, 2)

    return {"funnel": funnel, "avg_days": avg_days}


async def fetch_activation_funnel(pool: asyncpg.Pool, sources: List[str], start_date: date, end_date: date) -> List[Dict]:
    """Event funnel: actions whose event date is in the selected period."""
    sql_sources = sources if sources else None
    funnel = []
    async with pool.acquire() as conn:
        for stage, query in activation_steps:
            count = await conn.fetchval(query, sql_sources, start_date, end_date)
            funnel.append({"stage": stage, "count": count or 0})
    return funnel


async def _fetch_trial_retention_cohorts(pool: asyncpg.Pool) -> pd.DataFrame:
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
