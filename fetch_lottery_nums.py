import requests
import time
import json

# 先访问首页获取 cookie
session = requests.Session()
session.get("https://www.cwl.gov.cn/")  # 访问首页，服务器会返回 Set-Cookie

# 目标接口
url = "https://www.cwl.gov.cn/cwl_admin/front/cwlkj/search/kjxx/findDrawNotice"

year: int = 2025

while year <= 2025:
    # 请求参数
    params = {
        "name": "ssq",
        "issueCount": "",
        "issueStart": "",
        "issueEnd": "",
        "dayStart": f"{year}-01-01",
        "dayEnd": f"{year}-12-31",
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
        break
    data = json.loads(resp.text)
    results = data["result"]
    with open(f"./{year}.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

    year = year + 1
    time.sleep(2)
