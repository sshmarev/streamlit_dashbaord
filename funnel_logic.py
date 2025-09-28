# funnel_logic.py (исправленная версия)

from datetime import date
from typing import List, Dict
import asyncpg, pandas as pd

steps = [
    ("Все пользователи", """
        SELECT COUNT(DISTINCT u.id) FROM users u
        WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND u.created_at::date BETWEEN $2 AND $3
    """),
    ("Отправили 1-е сообщение", """
        SELECT COUNT(DISTINCT ua.user_id) FROM user_actions ua
        JOIN users u ON u.id = ua.user_id
        WHERE ua.action IN ('input_text', 'input_photo', 'input_voice')
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND u.created_at::date BETWEEN $2 AND $3
    """),
    ("Сохранили 1-й приём пищи", """
        SELECT COUNT(DISTINCT m.user_id) FROM meals m
        JOIN users u ON u.id = m.user_id
        WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND u.created_at::date BETWEEN $2 AND $3
    """),
    ("Сохранили 2-й приём пищи", """
        SELECT COUNT(q.id) FROM (
            SELECT u.id FROM users u JOIN meals m ON m.user_id = u.id
            WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
              AND u.created_at::date BETWEEN $2 AND $3
            GROUP BY u.id HAVING COUNT(m.id) >= 2
        ) q
    """),
    ("Сохранили 3-й приём пищи", """
        SELECT COUNT(q.id) FROM (
            SELECT u.id FROM users u JOIN meals m ON m.user_id = u.id
            WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
              AND u.created_at::date BETWEEN $2 AND $3
            GROUP BY u.id HAVING COUNT(m.id) >= 3
        ) q
    """),
    ("Сохранили 4-й приём пищи", """
        SELECT COUNT(q.id) FROM (
            SELECT u.id FROM users u JOIN meals m ON m.user_id = u.id
            WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
              AND u.created_at::date BETWEEN $2 AND $3
            GROUP BY u.id HAVING COUNT(m.id) >= 4
        ) q
    """),
    ("Сохранили 5-й приём пищи", """
        SELECT COUNT(q.id) FROM (
            SELECT u.id FROM users u JOIN meals m ON m.user_id = u.id
            WHERE (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
              AND u.created_at::date BETWEEN $2 AND $3
            GROUP BY u.id HAVING COUNT(m.id) >= 5
        ) q
    """),
    ("Активировали триал", """
        SELECT COUNT(DISTINCT ua.user_id) FROM user_actions ua
        JOIN users u ON u.id = ua.user_id
        WHERE ua.action = 'trial_activated'
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND u.created_at::date BETWEEN $2 AND $3
    """),
     ("Вызвали /subscribe", """
        SELECT COUNT(DISTINCT ua.user_id) FROM user_actions ua
        JOIN users u ON u.id = ua.user_id
        WHERE ua.action = 'subscribe_cmd'
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND u.created_at::date BETWEEN $2 AND $3
    """),
    ("Открыли оплату", """
        SELECT COUNT(DISTINCT ua.user_id) FROM user_actions ua
        JOIN users u ON u.id = ua.user_id
        WHERE ua.action LIKE 'subscribe_opened%%'
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND u.created_at::date BETWEEN $2 AND $3
    """),
    ("Оплатили подписку", """
        SELECT COUNT(DISTINCT s.user_id) FROM subscriptions s
        JOIN users u ON u.id = s.user_id
        WHERE s.is_active = TRUE
          AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
          AND u.created_at::date BETWEEN $2 AND $3
    """)
]

avg_time_query = """
    SELECT AVG(s.starts_at - u.created_at) AS avg_duration
    FROM subscriptions s
    JOIN users u ON u.id = s.user_id
    WHERE s.is_active = TRUE
      AND (array_length($1::text[], 1) IS NULL OR u.source = ANY($1::text[]))
      AND u.created_at::date BETWEEN $2 AND $3
"""

async def fetch_funnel_stats(pool: asyncpg.Pool, sources: List[str], start_date: date, end_date: date) -> Dict:
    sql_sources = sources if sources else None
    funnel = []
    async with pool.acquire() as conn:
        for stage, query in steps:
            count = await conn.fetchval(query, sql_sources, start_date, end_date)
            funnel.append({"stage": stage, "count": count or 0}) # Добавил 'or 0' на всякий случай

        avg_days = await conn.fetchval(avg_time_query, sql_sources, start_date, end_date)

        if avg_days is not None:
            avg_days = round(avg_days.total_seconds() / 86400, 2)

    return {
        "funnel": funnel,
        "avg_days": avg_days
    }

async def _fetch_trial_retention_cohorts(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            WITH trial_cohorts AS (
                SELECT
                    user_id,
                    date_trunc('day', created_at)::date AS cohort_day
                FROM user_actions
                WHERE action = 'trial_activated'
            ),
            user_activity AS (
                SELECT
                    DISTINCT user_id,
                    date_trunc('day', created_at)::date AS activity_day
                FROM meals
            )
            SELECT
                c.cohort_day,
                -- ВОЗВРАЩАЕНО: Считаем дни с 0 до 4
                (a.activity_day - c.cohort_day) + 1 AS day_number,
                COUNT(DISTINCT c.user_id) AS retained_users
            FROM trial_cohorts c
            JOIN user_activity a ON c.user_id = a.user_id AND a.activity_day >= c.cohort_day
            WHERE (a.activity_day - c.cohort_day) <= 4
            GROUP BY c.cohort_day, day_number
            ORDER BY c.cohort_day, day_number
        ''')
    return pd.DataFrame(rows, columns=['cohort_day', 'day_number', 'retained_users'])