# get_main_body.py  
  
import requests  
from urllib import parse  
# from weibo.parse_html import get_dataframe_from_html_text
from parse_html import get_dataframe_from_html_text
import logging  
from rich.progress import track  
import pandas as pd  
  
logging.basicConfig(level=logging.INFO)  

  
def get_the_main_body_response(q, kind, p, cookie, timescope):  
    """  
    q表示的是话题；  
    kind表示的是类别：综合，实时，热门，高级；  
    p表示的页码；  
    timescope表示日期范围  
    """    
    kind_params_url = {  
        "热门": [  
            {  
                "q": q,  
                "xsort": "hot",  
                "suball": "1",  
                "timescope": f"custom:{timescope}",  
                "Refer": "g",  
                "page": p,  
            },  
            "https://s.weibo.com/weibo",  
        ],  
        "普通": [  
            {  
                "q": q, 
                "suball": "1",  
                "timescope": f"custom:{timescope}",  
                "Refer": "g",  
                "page": p,  
            },  
            "https://s.weibo.com/weibo",  
        ],  
    }  
    
    params, url = kind_params_url[kind]  
    full_url = f"{url}?{parse.urlencode(params)}"
    logging.info("请求的 URL: %s", full_url)

    headers = {  
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.69",  
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, cookies=cookie, timeout=10)
        response.raise_for_status()  # 检查请求是否成功
        return response
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP错误: {http_err} - 状态码: {response.status_code} - URL: {full_url}")
        return None
    except requests.exceptions.RequestException as req_err:
        logging.error(f"请求错误: {req_err} - URL: {full_url}")
        return None
  
  
def get_all_main_body(q, kind, cookie, timescope):
    # 初始化数据
    data_list = []

    # 初始请求第一页以获得总页数
    resp = get_the_main_body_response(q, kind, 1, cookie, timescope)
    if resp is None:
        logging.warning("第一页请求失败，无法获取数据。")
        return pd.DataFrame()  # 返回空的 DataFrame

    html_text = resp.text

    try:
        data, total_page = get_dataframe_from_html_text(html_text)
        data_list.append(data)
        logging.info(f"话题：{q}，类型：{kind}，解析成功，一共有{total_page:2d}页，准备开始解析...")

        max_no_data_pages = 5  # 允许连续无数据的最大页数
        no_data_count = 0      # 当前连续无数据页面计数

        for current_page in track(range(2, total_page + 1), description="解析中..."):
            response = get_the_main_body_response(q, kind, current_page, cookie, timescope)
            if response is None:
                logging.warning(f"第 {current_page} 页请求失败，跳过该页。")
                no_data_count += 1
                if no_data_count >= max_no_data_pages:
                    logging.info("连续多页无数据或请求失败，停止该天的数据抓取。")
                    break
                continue

            html_text = response.text
            data, _ = get_dataframe_from_html_text(html_text)

            # 如果页面无数据，增加计数器
            if data.empty:
                no_data_count += 1
                logging.warning(f"第 {current_page} 页无有效数据，跳过该页。")
                if no_data_count >= max_no_data_pages:
                    logging.info("连续多页无数据，停止该天的数据抓取。")
                    break
            else:
                no_data_count = 0  # 重置计数器，因为此页有数据
                data_list.append(data)

        # 合并并返回所有数据
        if data_list:
            data = pd.concat(data_list).reset_index(drop=True)
            logging.info(f"话题：{q}，类型：{kind}，共解析了 {len(data_list)} 页数据，解析完毕！")
            return data
        else:
            logging.warning("没有爬取到任何数据！")
            return pd.DataFrame()

    except Exception as e:
        logging.warning("解析页面失败，请检查你的cookie是否正确！")
        raise ValueError("解析页面失败，请检查你的cookie是否正确！")
  
  
if __name__ == "__main__":  
    q = ""  # 话题  
    kind = ""  # 综合，实时，热门，高级  
    cookie = ""  # 设置你的cookie
    timescope = ""
    data = get_all_main_body(q, kind, cookie, timescope)  
    data.to_csv("demo.csv", encoding="utf_8_sig")
