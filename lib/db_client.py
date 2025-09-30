import os
import time
import pymysql
from dotenv import load_dotenv
from dbutils.pooled_db import PooledDB
from lib.log_helper import log_print


load_dotenv()


# 连接池设置
pool = PooledDB(
    creator=pymysql,
    maxconnections=20,
    mincached=3,
    maxcached=7,
    maxshared=5,
    blocking=True,
    maxusage=100,
    setsession=[],
    ping=1,
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWD"),
    database='rate_data',
    charset='utf8mb4'
)



def rate_clean(val):
    if val is None or val == '' or not val:
        return None
    else:
        return val



def insert_xchg_rate(publish_timestamp,
                     currency_name,
                     remittance_buy_price=None,
                     cash_buy_price=None,
                     remittance_sell_price=None,
                     cash_sell_price=None,
                     bank_conversion_price=None):

    insert_sql = ("INSERT IGNORE INTO exchange_rates("
                  "currency_name,"
                  "remittance_buy_price,"
                  "cash_buy_price,"
                  "remittance_sell_price,"
                  "cash_sell_price,"
                  "bank_conversion_price,"
                  "publish_timestamp,"
                  "fetched_at)"
                  "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)")


    conn = pool.connection()
    try:
        with conn.cursor() as cur:
            cur.execute(insert_sql,(
                currency_name,
                rate_clean(remittance_buy_price),
                rate_clean(cash_buy_price),
                rate_clean(remittance_sell_price),
                rate_clean(cash_sell_price),
                rate_clean(bank_conversion_price),
                publish_timestamp,
                int(time.time())
            ))
            conn.commit()
            log_print.debug(f"Inserted {currency_name} exchange data successful")
    except pymysql.MySQLError as e:
        conn.rollback()
        log_print.error(f"insert_xchg_rate insert error: {e}")
    finally:
        conn.close()



def get_threshold_sub():
    query_sql = "SELECT * FROM rate_threshold_subscriptions WHERE active = 1"

    conn = pool.connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query_sql)
            result = cur.fetchall()
            return result
    except pymysql.MySQLError as e:
        log_print.error(f"get_threshold_sub query error: {e}")
        return False
    finally:
        conn.close()



def get_ccy_xchg_rate_by_date(start_mode, interval_mode, sort_mode,currency_name, query_start_date, query_end_date):

    start_mode_map = {
        "curdate": "CURDATE()",
        "now": "NOW()"
    }
    interval_mode_map = {
        "ms": "MICROSECOND",
        "s": "SECOND",
        "m": "MINUTE",
        "h": "HOUR",
        "d": "DAY",
        "w": "WEEK",
        "M": "MONTH",
        "q": "QUARTER",
        "y": "YEAR",
    }
    sort_mode_map = {
        "asc": "ASC", # 从旧到新
        "desc": "DESC" # 从新到旧
    }



    if start_mode in start_mode_map:
        query_start_mode = start_mode_map[start_mode]
    else:
        log_print.error(f"start_mode key error")
        return False
    if interval_mode in interval_mode_map:
        query_interval_mode = interval_mode_map[interval_mode]
    else:
        log_print.error(f"interval_mode key error")
        return False
    if sort_mode in sort_mode_map:
        query_sort_mode = sort_mode_map[sort_mode]
    else:
        log_print.error(f"sort_mode key error")

    query_sql = f"""
                SELECT *
                FROM exchange_rates
                WHERE currency_name = %s
                  AND publish_timestamp >= 
                      UNIX_TIMESTAMP(DATE_SUB({query_start_mode}, INTERVAL %s {query_interval_mode})) -- (query_start_date=0 and query_end_date=-1) = today
                  AND publish_timestamp < 
                      UNIX_TIMESTAMP(DATE_SUB({query_start_mode}, INTERVAL %s {query_interval_mode})) -- (query_start_date=7 and query_end_date=6) = 7 天前 
                ORDER BY publish_timestamp {query_sort_mode};
                """

    conn = pool.connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query_sql, (currency_name, query_start_date, query_end_date))
            result = cur.fetchall()
            return result
    except pymysql.MySQLError as e:
        log_print.error(f"get_ccy_xchg_rate_by_date query error: {e}")
        return False
    finally:
        conn.close()



def update_threshold_sub(uid, last_triggered_at, trigger_count, active):
    update_sql = ("UPDATE rate_threshold_subscriptions "
                  "SET last_triggered_at = %s,"
                  "    trigger_count = %s,"
                  "    active = %s "
                  "WHERE uid = %s")

    conn = pool.connection()
    try:
        with conn.cursor() as cur:
            cur.execute(update_sql,(last_triggered_at, trigger_count, active, uid))
            conn.commit()
    except pymysql.MySQLError as e:
        log_print.error(f"update_threshold_sub update error: {e}")
    finally:
        conn.close()



def get_daily_list():
    query_sql = "SELECT * FROM daily_rate_subscriptions WHERE active = 1"
    conn = pool.connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query_sql)
            result = cur.fetchall()
            return result
    except pymysql.MySQLError as e:
        log_print.error(f"get_daily_list query error: {e}")
    finally:
        conn.close()

