#get_comments_level_one.py
import requests
import pandas as pd
import json
from dateutil import parser
import logging
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

logging.basicConfig(level=logging.INFO)

def get_buildComments_level_one_response(uid, mid, cookie, the_first=True, max_id=None):
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "sec-ch-ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Microsoft Edge";v="116"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.69",
        "x-requested-with": "XMLHttpRequest",
    }

    params = {
        "is_reload": "1",
        "id": f"{mid}",
        "is_show_bulletin": "2",
        "is_mix": "0",
        "count": "20",
        "uid": f"{uid}",
        "fetch_level": "0",
        "locale": "zh-CN",
    }

    if not the_first:
        params["flow"] = 0
        params["max_id"] = max_id

    # 设置重试策略
    session = requests.Session()
    retry = Retry(
        total=5,             # 重试次数
        backoff_factor=1,    # 等待时间指数增长
        status_forcelist=[500, 502, 503, 504]  # 针对哪些状态码进行重试
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)

    try:
        response = session.get(
            "https://weibo.com/ajax/statuses/buildComments",
            params=params,
            headers=headers,
            cookies=cookie  # 确保 cookie 是一个字典
        )
        response.raise_for_status()  # 检查请求是否成功
        return response
    except requests.exceptions.RequestException as e:
        logging.error(f"请求失败: {e}")
        logging.error(f"响应内容: {response.text if response else '无响应内容'}")
        raise ValueError("解析页面失败，请检查你的cookie是否正确！")

def get_all_level_one(uid, mid, cookie, max_times=15):
    max_id = ""
    data_lst = []
    max_times = max_times

    try:
        for current_times in range(1, max_times):
            if current_times == 0:
                response = get_buildComments_level_one_response(uid, mid, cookie)
            else:
                response = get_buildComments_level_one_response(
                    uid, mid, cookie, the_first=False, max_id=max_id
                )

            data = pd.DataFrame(response.json()["data"])  # 解析 JSON 数据
            max_id = response.json()["max_id"]

            if data.shape[0] != 0:
                data_lst.append(data)
            if max_id == 0:
                break

        if data_lst:
            data = pd.concat(data_lst).reset_index(drop=True)
            data.insert(0, "main_body_uid", uid)
            data.insert(0, "main_body_mid", mid)
            return data
        else:
            return pd.DataFrame()

    except Exception as e:
        logging.error(f"解析一级评论失败: {e}")
        raise ValueError("解析页面失败，请检查你的cookie是否正确！")

if __name__ == "__main__":
    uid = "1006550592"
    mid = "5046560175424086"
    cookie = ""  # 设置你的cookie
    data = get_all_level_one(uid, mid, cookie)
    data.to_csv("demo_comments.csv", encoding="utf_8_sig")
