import os
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from jinja2 import Template
from lib.log_helper import log_print


load_dotenv()


def replace_threshold_template(currency_name, price_labels, target_exchange_rate, current_price, trigger_time, week_growth, month_growth, last_trigger=""):
    data = {
        "currency_name": currency_name,
        "price_label": price_labels,
        "target_exchange_rate": target_exchange_rate,
        "current_price": current_price,
        "trigger_time": trigger_time,
        "week_growth": week_growth,
        "month_growth": month_growth,
        "last_trigger": last_trigger
    }
    with open("html_template/threshold_template.html", "r", encoding="utf-8") as f:
        template = Template(f.read())
    return template.render(**data)




def build_daily_table(current_name, current_data):
    if not current_data:
        return False
    headers = ["现汇买入价", "现钞买入价", "现汇卖出价", "现钞卖出价", "中行折算价", "涨跌百分比", "发布时间"]
    soup = BeautifulSoup("", "lxml")

    div = soup.new_tag("div", **{"class": "current-data-div"})

    h2 = soup.new_tag("h2")
    h2.string = f"{current_name} - 人民币"

    div.append(h2)

    table = soup.new_tag("table", **{"class": "current-data-table"})

    header = soup.new_tag("tr")
    for header_name in headers:
        th = soup.new_tag("th")
        th.string = header_name
        header.append(th)
    table.append(header)

    for row_data in current_data:
        row = soup.new_tag("tr")
        for data in row_data:
            td = soup.new_tag("td")
            td.string = data
            row.append(td)
        table.append(row)

    div.append(table)
    return div




def generate_daily_html(data):
    in_path = Path("./html_template/daily_template.html")
    html_text = in_path.read_text(encoding="utf-8")
    soup = BeautifulSoup(html_text, "lxml")
    main_div = soup.find("div", class_="main-div")
    if not main_div:
        log_print.error(f"No main div in html template")
        return False
    for current_name, current_data in data:
        table = build_daily_table(current_name, current_data)
        if table:
            main_div.append(table)

    TIME_ZONE = os.getenv("TIME_ZONE")
    generated_at = datetime.now(ZoneInfo(TIME_ZONE)).strftime("%Y-%m-%d %H:%M")


    html = soup.prettify()
    html = html.replace("{{generated_at}}", str(generated_at)).replace("{{timezone}}", str(TIME_ZONE))
    return html

