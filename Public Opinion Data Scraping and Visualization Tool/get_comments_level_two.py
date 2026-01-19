#get_comments_level_two.py
import requests
import pandas as pd
import json
from dateutil import parser
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO)

def create_retry_session(retries=5, backoff_factor=1, status_forcelist=(500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]  # 替换 `method_whitelist` 为 `allowed_methods`
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def get_buildComments_level_two_response(uid, mid, cookie, the_first=True, max_id=None):
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
        "is_mix": "1",
        "fetch_level": "1",
        "max_id": "0",
        "count": "20",
        "uid": f"{uid}",
        "locale": "zh-CN",
    }

    if not the_first:
        params["flow"] = 0
        params["max_id"] = max_id

    cookie_dict = dict(item.split("=") for item in cookie.split("; ")) if isinstance(cookie, str) else cookie

    session = create_retry_session()
    try:
        response = session.get(
            "https://weibo.com/ajax/statuses/buildComments",
            params=params,
            headers=headers,
            cookies=cookie_dict,
            verify=False  # 忽略 SSL 验证
        )
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logging.error(f"请求失败: {e}")
        raise ValueError("解析页面失败，请检查你的cookie是否正确！")

def get_rum_level_two_response(buildComments_url, cookie):
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
    entry = {"name": buildComments_url}
    files = {
        "entry": (None, json.dumps(entry)),
        "request_id": (None, ""),
    }

    cookie_dict = dict(item.split("=") for item in cookie.split("; ")) if isinstance(cookie, str) else cookie
    session = create_retry_session()
    session.post("https://weibo.com/ajax/log/rum", headers=headers, files=files, cookies=cookie_dict)

def get_level_two_response(uid, mid, cookie, the_first=True, max_id=None):  
    buildComments_resp = get_buildComments_level_two_response(uid, mid, cookie, the_first, max_id)  
    buildComments_url = buildComments_resp.url  
    get_rum_level_two_response(buildComments_url, cookie)  
    data = pd.DataFrame(buildComments_resp.json()["data"])  
    max_id = buildComments_resp.json()["max_id"]  
    return max_id, data  

def process_time(publish_time):  
    publish_time = parser.parse(publish_time)  
    publish_time = publish_time.strftime("%y年%m月%d日 %H:%M")  
    return publish_time  

def process_data(data):
    data_user = pd.json_normalize(data["user"])
    data_user_col_map = {
        "id": "uid",
        "screen_name": "用户昵称",
        "profile_url": "用户主页",
        "description": "用户描述",
        "location": "用户地理位置",
        "gender": "用户性别",
        "followers_count": "用户粉丝数量",
        "friends_count": "用户关注数量",
        "statuses_count": "用户全部微博",
        "status_total_counter.comment_cnt": "用户累计评论",
        "status_total_counter.repost_cnt": "用户累计转发",
        "status_total_counter.like_cnt": "用户累计获赞",
        "status_total_counter.total_cnt": "用户转评赞",
        "verified_reason": "用户认证信息",
    }
    
    # 只保留在 data_user_col_map 中定义的列
    data_user = data_user[[col for col in data_user if col in data_user_col_map]]
    data_user = data_user.rename(columns=data_user_col_map)

    data_main_col_map = {
        "created_at": "发布时间",
        "text": "处理内容",
        "source": "评论地点",
        "mid": "mid",
        "total_number": "回复数量",
        "like_counts": "点赞数量",
        "text_raw": "原生内容",
    }

    # 检查是否缺少列并填充缺失的列
    for col in data_main_col_map:
        if col not in data.columns:
            data[col] = pd.NA  # 填充 NaN 或者可以用其他默认值

    # 只保留在 data_main_col_map 中定义的列
    data_main = data[[col for col in data if col in data_main_col_map]]
    data_main = data_main.rename(columns=data_main_col_map)

    # 合并主数据和用户数据
    data = pd.concat([data_main, data_user], axis=1)
    data["发布时间"] = data["发布时间"].map(process_time)
    data["用户主页"] = "https://weibo.com" + data["用户主页"]
    return data

def get_all_level_two(uid, mid, cookie, max_times=15):  
    max_id = ""  
    data_lst = []  
    try:  
        for _ in range(max_times):  
            max_id, data = get_level_two_response(uid, mid, cookie, max_id != "")
            if data.shape[0] != 0:  
                data_lst.append(data)  
            if max_id == 0:  
                break  

        if data_lst:  
            data = pd.concat(data_lst).reset_index(drop=True)  
            data = process_data(data)  
            data.insert(0, "main_body_uid", uid)  
            data.insert(0, "comments_level_1_mid", mid)  
            return data  
        else:  
            return pd.DataFrame()  
    except Exception as e:  
        logging.error(f"获取二级评论失败: {e}")
        raise ValueError("解析页面失败，请检查你的cookie是否正确！")  

if __name__ == "__main__":  
    main_body_uid = "2656274875"  
    mid = "5046022789400609"  
    cookie = ""  # 设置你的cookie
    data = get_all_level_two(main_body_uid, mid, cookie)  
    data.to_csv("demo_comments_two.csv", encoding="utf_8_sig")
