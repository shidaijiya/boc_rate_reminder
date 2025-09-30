import datetime
import json
import os
import plotly.graph_objects as go
from lib.log_helper import log_print

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



def generate_line_chart(currency_name, data):
    fig = go.Figure()
    now = datetime.datetime.now().strftime("%Y-%m-%d")

    iso_name = currency_map[currency_name]
    dates = data["dates"]

    chart_map_path = "currency_chart/chart_map.json"
    folder_path = f"currency_chart/{iso_name}"

    file_path = f"{folder_path}/{now}-{len(dates)}days.html"
    os.makedirs(folder_path, exist_ok=True)


    if not None in data["remittance_buy_price"]:
        fig.add_trace(go.Scatter(x=dates, y=data["remittance_buy_price"], mode="lines+markers", name="现汇买入价"))

    if not None in data["cash_buy_price"]:
        fig.add_trace(go.Scatter(x=dates, y=data["cash_buy_price"], mode="lines+markers", name="现钞买入价"))

    if not None in data["remittance_sell_price"]:
        fig.add_trace(go.Scatter(x=dates, y=data["remittance_sell_price"], mode="lines+markers", name="现汇卖出价"))

    if not None in data["cash_sell_price"]:
        fig.add_trace(go.Scatter(x=dates, y=data["cash_sell_price"], mode="lines+markers", name="现钞卖出价"))

    if not None in data["bank_conversion_price"]:
        fig.add_trace(go.Scatter(x=dates, y=data["bank_conversion_price"], mode="lines+markers", name="中行折算价"))





    fig.update_layout(
        title=f"{currency_name} to 人民币 {dates[0]}-{dates[-1]}",
        xaxis_title="日期",
        yaxis_title="价格",
        xaxis=dict(tickformat="%Y-%m-%d")
    )


    fig.write_html(file_path, include_plotlyjs=True)


    if os.path.exists(chart_map_path):
        with open(chart_map_path, "r", encoding="utf-8") as f:
            try:
                chart_map = json.load(f)
            except json.decoder.JSONDecodeError:
                chart_map = {}

        chart_map.setdefault(iso_name, []).append(file_path)
        with open(chart_map_path, "w", encoding="utf-8") as f:
            json.dump(chart_map, f, ensure_ascii=False, indent=4)


    else:
        chart_map = {}
        chart_map.setdefault(iso_name, []).append(file_path)
        with open(chart_map_path, "w", encoding="utf-8") as f:
            json.dump(chart_map, f, ensure_ascii=False, indent=4)


    log_print.debug(f"generate {currency_name} completed")





