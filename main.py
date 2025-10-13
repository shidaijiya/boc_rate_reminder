import os
import time
from dotenv import load_dotenv
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from datetime import timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from threading import Event
from zoneinfo import ZoneInfo
from apscheduler.schedulers.background import BackgroundScheduler
from lib.chart_helper import generate_line_chart
from lib.db_client import insert_xchg_rate, get_threshold_sub, get_ccy_xchg_rate_by_date, update_threshold_sub, \
    get_daily_list
from lib.html_helper import generate_daily_html, replace_threshold_template
from lib.log_helper import log_print
from lib.mail_helper import send_mail
from lib.spider import fetch_exchange_rates
from lib.utils import conv_to_float


load_dotenv()
TIME_ZONE = os.getenv("TIME_ZONE")
job_defaults = {
    'coalesce': True,
    'max_instances': 3,
    'misfire_grace_time': 600
}


executor = ThreadPoolExecutor(max_workers=4)
scheduler = BackgroundScheduler(timezone=ZoneInfo(TIME_ZONE), job_defaults=job_defaults)
stop_event = Event()



currency_map = {
    "阿联酋迪拉姆": "AED",
    "澳大利亚元": "AUD",
    "文莱元": "BND",
    "巴西里亚尔": "BRL",
    "加拿大元": "CAD",
    "瑞士法郎": "CHF",
    "捷克克朗": "CZK",
    "丹麦克朗": "DKK",
    "欧元": "EUR",
    "英镑": "GBP",
    "港币": "HKD",
    "匈牙利福林": "HUF",
    "印尼卢比": "IDR",
    "以色列谢克尔": "ILS",
    "印度卢比": "INR",
    "日元": "JPY",
    "柬埔寨瑞尔": "KHR",
    "韩国元": "KRW",
    "科威特第纳尔": "KWD",
    "蒙古图格里克": "MNT",
    "澳门元": "MOP",
    "墨西哥比索": "MXN",
    "林吉特": "MYR",
    "挪威克朗": "NOK",
    "尼泊尔卢比": "NPR",
    "新西兰元": "NZD",
    "菲律宾比索": "PHP",
    "巴基斯坦卢比": "PKR",
    "卡塔尔里亚尔": "QAR",
    "塞尔维亚第纳尔": "RSD",
    "卢布": "RUB",
    "沙特里亚尔": "SAR",
    "瑞典克朗": "SEK",
    "新加坡元": "SGD",
    "泰国铢": "THB",
    "土耳其里拉": "TRY",
    "新台币": "TWD",
    "美元": "USD",
    "越南盾": "VND",
    "南非兰特": "ZAR"
}

price_labels = {
    "remittance_buy_price": "现汇买入价",
    "cash_buy_price": "现钞买入价",
    "remittance_sell_price": "现汇卖出价",
    "cash_sell_price": "现钞卖出价",
    "bank_conversion_price": "中行折算价"
}

sql_num_map = {
    "currency_name": 0,
    "remittance_buy_price": 1,
    "cash_buy_price": 2,
    "remittance_sell_price": 3,
    "cash_sell_price": 4,
    "bank_conversion_price": 5,
    "publish_timestamp": 6,
    "fetched_at": 7
}


def hour_task():
    save_exchange_rates()
    time.sleep(30)
    process_threshold_reminder()



def save_exchange_rates():
    rates = fetch_exchange_rates()


    if not rates:
        log_print.error("no exchange rate data")
        return

    for rate in rates:
        publish_timestamp = int(datetime.strptime(rate[6], "%Y.%m.%d %H:%M:%S")
                                .replace(tzinfo=timezone(timedelta(hours=8)))
                                .timestamp())
        insert_xchg_rate(
            publish_timestamp,
            rate[0],  # currency_name
            rate[1],  # remittance_buy
            rate[2],  # cash_buy
            rate[3],  # remittance_sell
            rate[4],  # cash_sell
            rate[5]  # bank_conversion
        )



def process_threshold_reminder():
    sub_user_list = get_threshold_sub()

    for user_data in sub_user_list:
        user_email = user_data[0]
        uid = user_data[1]
        currency_name = user_data[2]
        target_exchange_rate = Decimal(user_data[3])
        target_type_str = user_data[4]
        last_triggered_at = user_data[5]
        trigger_count = user_data[6]

        if last_triggered_at:
            if int(time.time()) - int(last_triggered_at) < 86400:
                log_print.debug("triggered within 24 hours")
                continue

        if trigger_count:
            if trigger_count >= 3:
                continue


        today_data = get_ccy_xchg_rate_by_date("curdate","d","desc",currency_name,0, -1)


        if not today_data:
            log_print.info("no exchange rate data")
            continue

        last_exchange_rates = today_data[0]


        if target_type_str in sql_num_map:
            target_type_int = sql_num_map[target_type_str]
            if last_exchange_rates[target_type_int] <= target_exchange_rate:

                active = 1
                last_trigger_txt = ""
                if not trigger_count:
                    trigger_count = 0
                if trigger_count == 2:
                    active = 0
                    trigger_count = -1
                    last_trigger_txt = "提醒已达最大次数3次,如需继续提醒请重新启用"

                tz = ZoneInfo(TIME_ZONE)
                now = datetime.now(tz).strftime("%Y-%m-%d %H:%M")


                week_data = get_ccy_xchg_rate_by_date("curdate", "d", "desc", currency_name, 7, 6)
                month_data = get_ccy_xchg_rate_by_date("curdate", "d", "desc", currency_name, 30, 29)


                week_growth_result, week_growth_rate = calculate_growth_rate(last_exchange_rates[target_type_int], week_data[0][target_type_int])

                month_growth_result, month_growth_rate = calculate_growth_rate(last_exchange_rates[target_type_int], month_data[0][target_type_int])

                threshold_template = replace_threshold_template(currency_name,
                                                                price_labels[target_type_str],
                                                                target_exchange_rate,
                                                                last_exchange_rates[target_type_int].normalize(),
                                                                now, week_growth_rate,
                                                                month_growth_rate,
                                                                last_trigger_txt)

                update_threshold_sub(uid, int(time.time()), trigger_count + 1, active)
                executor.submit(send_mail(user_email, f"{currency_name}汇率已达阈值提醒",threshold_template))



def calculate_growth_rate(new_price, original_price):
    log_print.debug(f"new_price:{new_price}, original_price:{original_price}")

    growth_rate = ((new_price - original_price) / original_price) * 100
    growth_rate = Decimal(str(growth_rate)).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)

    if new_price > original_price:
        return "increase", f"{growth_rate}%"
    elif new_price < original_price:
        return "decline", f"{growth_rate}%"
    else:
        return "no_change", "No change"



def extract_row(raw, growth_rate):
    if not raw:
        return ["", "", "", "", "", "", ""]
    return [
        str(raw[sql_num_map["remittance_buy_price"]].normalize()),
        str(raw[sql_num_map["cash_buy_price"]].normalize()),
        str(raw[sql_num_map["cash_sell_price"]].normalize()),
        str(raw[sql_num_map["cash_sell_price"]].normalize()),
        str(raw[sql_num_map["bank_conversion_price"]].normalize()),
        str(growth_rate),
        datetime.fromtimestamp(
            raw[sql_num_map["publish_timestamp"]],
            tz=ZoneInfo(TIME_ZONE)
        ).strftime("%Y-%m-%d %H:%M"),

    ]



def process_daily_reminder():
    daily_list = get_daily_list()
    grouped = defaultdict(list)

    for user_email, currency_name, active in daily_list:
        grouped[user_email].append(currency_name)

    for user_email, currency_names in grouped.items():
        total_data = []
        for currency_name in currency_names:
            today_data = get_ccy_xchg_rate_by_date("curdate", "d", "desc", currency_name, 0, -1)
            week_data = get_ccy_xchg_rate_by_date("curdate", "d", "desc", currency_name, 7, 6)
            month_data = get_ccy_xchg_rate_by_date("curdate", "d", "desc", currency_name, 30, 29)

            last_today_data = today_data[0] if today_data else None
            last_week_data = week_data[0] if week_data else None
            last_month_data = month_data[0] if month_data else None

            week_growth_result, week_growth_rate = calculate_growth_rate(today_data[0][3], week_data[0][3])

            month_growth_result, month_growth_rate = calculate_growth_rate(today_data[0][3], month_data[0][3])

            tmp_list = [
                extract_row(last_today_data, "Unavailable"),
                extract_row(last_week_data, week_growth_rate),
                extract_row(last_month_data, month_growth_rate),
            ]

            total_data.append([currency_name,tmp_list])

        mail_html = str(generate_daily_html(total_data))
        executor.submit(send_mail(user_email, f"这是您订阅的汇率请查收📩", mail_html))



def collect_chart_data(currency_name, start_date, end_date):
    log_print.debug(f"collecting {currency_name} chart data")
    data = get_ccy_xchg_rate_by_date("curdate", "d", "desc", currency_name, start_date, end_date)


    grouped = defaultdict(list)
    for row in data:
        ts = row[6]
        dt_str = datetime.fromtimestamp(ts, tz=ZoneInfo(TIME_ZONE)).strftime("%Y-%m-%d")
        grouped[dt_str].append(row)

    remittance_buy_price_list = []
    cash_buy_price_list = []
    remittance_sell_price_list = []
    cash_sell_price_list = []
    bank_conversion_price_list = []
    dates = []

    for k, v in grouped.items():
        remittance_buy_price_list.append(v[0][sql_num_map["remittance_buy_price"]])
        cash_buy_price_list.append(v[0][sql_num_map["cash_buy_price"]])
        remittance_sell_price_list.append(v[0][sql_num_map["remittance_sell_price"]])
        cash_sell_price_list.append(v[0][sql_num_map["cash_sell_price"]])
        bank_conversion_price_list.append(v[0][sql_num_map["bank_conversion_price"]])
        dates.append(k)

    return {
        "dates": dates,
        "remittance_buy_price": conv_to_float(remittance_buy_price_list),
        "cash_buy_price": conv_to_float(cash_buy_price_list),
        "remittance_sell_price": conv_to_float(remittance_sell_price_list),
        "cash_sell_price": conv_to_float(cash_sell_price_list),
        "bank_conversion_price": conv_to_float(bank_conversion_price_list),
    }



def start_generate_chart(start_date, end_date):
    for v, k in currency_map.items():
        generate_line_chart(v, collect_chart_data(v, start_date, end_date))



if __name__ == "__main__":
    scheduler.add_job(hour_task,
                      trigger='cron',
                      minute=0)

    scheduler.add_job(process_daily_reminder,
                      trigger="cron",
                      hour=9,
                      minute=30)

    scheduler.add_job(start_generate_chart,
                      args=[6, -1],
                      trigger='cron',
                      day_of_week='mon',
                      hour=0,
                      minute=0)

    scheduler.add_job(start_generate_chart,
                      args=[29, 1],
                      trigger='cron',
                      day=1)

    scheduler.add_job(start_generate_chart,
                      args=[364, -1],
                      trigger='cron',
                      month=1,
                      day=1)

    scheduler.start()

    try:
        log_print.info("boc rate reminder started")
        stop_event.wait()
    except (KeyboardInterrupt, SystemExit):
        stop_event.set()
    finally:
        scheduler.shutdown()





