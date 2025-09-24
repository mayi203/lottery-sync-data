from lottery_db_manager import LotteryDBManager
from lottery_models import LotteryDraw
import requests
import json
from datetime import datetime, timedelta

# 获取当前时间
now = datetime.now()

# 格式化为 YYYY-MM-DD
current = now.strftime("%Y-%m-%d")


def fetch_lottery(start_date):
    # 先访问首页获取 cookie
    session = requests.Session()
    session.get("https://www.cwl.gov.cn/")  # 访问首页，服务器会返回 Set-Cookie
    # 目标接口
    url = "https://www.cwl.gov.cn/cwl_admin/front/cwlkj/search/kjxx/findDrawNotice"
    # 请求参数
    params = {
        "name": "ssq",
        "issueCount": "",
        "issueStart": "",
        "issueEnd": "",
        "dayStart": start_date,
        "dayEnd": current,
        "pageNo": "1",
        "pageSize": "200",
        "week": "",
        "systemType": "PC",
    }

    # 带着 cookie 请求接口
    resp = session.get(
        url,
        params=params,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        },
    )

    if resp.status_code != 200:
        print(f"error occured: {resp.status_code}")
        return None
    data = json.loads(resp.text)
    results = data["result"]
    if len(results) < 1:
        return None
    path = f"./{current}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    return path


db = LotteryDBManager()
latest: LotteryDraw = db.query_latest()
if latest:
    next_day = (
        datetime.strptime(latest.date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")
    path = fetch_lottery(start_date=next_day)
    if path:
        db.insert_data_from_json(json_file=path)
