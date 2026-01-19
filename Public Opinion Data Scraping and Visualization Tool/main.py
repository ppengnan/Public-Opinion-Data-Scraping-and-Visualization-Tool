#main.py
import os  
import pandas as pd  
from rich.progress import track  
import time
import random
# from weibo.get_main_body import get_all_main_body
# from weibo.get_comments_level_one import get_all_level_one
# from weibo.get_comments_level_two import get_all_level_two
from get_main_body import get_all_main_body
from get_comments_level_one import get_all_level_one
from get_comments_level_two import get_all_level_two
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import logging
from datetime import datetime, timedelta  # 新增
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO)  

class WBParser:  
    def __init__(self, cookie=None):  
        self.cookie = cookie or self.load_or_login_cookies()

        os.makedirs("./WBData", exist_ok=True)  
        os.makedirs("./WBData/Comments_level_1", exist_ok=True)  
        os.makedirs("./WBData/Comments_level_2", exist_ok=True)  
        self.main_body_filepath = "./WBData/demo.csv"  
        self.comments_level_1_filename = "./WBData/demo_comments_one.csv"  
        self.comments_level_2_filename = "./WBData/demo_comments_two.csv"  
        self.comments_level_1_dirpath = "./WBData/Comments_level_1/"  
        self.comments_level_2_dirpath = "./WBData/Comments_level_2/"  

    def load_or_login_cookies(self):
        # 检查是否存在 cookies.json 文件
        if os.path.exists("weibo_cookies.json"):
            with open("weibo_cookies.json", "r") as f:
                cookies = json.load(f)
            # 验证 cookies 是否有效
            if self.verify_cookies(cookies):
                print("加载已保存的有效 cookies。")
                return cookies
            else:
                print("保存的 cookies 已失效，重新登录。")

        # 如果没有 cookies 文件或 cookies 无效，则执行登录
        return self.login_and_get_cookies()

    def verify_cookies(self, cookies):
        # 使用 requests 库尝试使用 cookies 访问一个需要登录的页面
        import requests
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get("https://weibo.com", cookies=cookies, headers=headers)
        # 通过页面内容判断登录状态（这里只是示例，需要实际测试验证方法）
        return "登录" not in response.text  # 假设如果页面中没有"登录"字样则说明 cookies 有效

    def login_and_get_cookies(self):
        # 使用 Selenium 进行登录并保存 cookies
        options = Options()
        options.add_argument("--disable-infobars")
        options.add_argument("--start-maximized")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get("https://weibo.com/login")

        print("请在浏览器中手动登录微博...")

        try:
            # 使用新的选择器定位设置按钮
            settings_button_locator = (By.XPATH, "//button[@title='设置']")  # 根据按钮的 title 属性定位
            WebDriverWait(driver, 120).until(
                EC.presence_of_element_located(settings_button_locator)
            )
            print("登录成功，检测到设置按钮。")

            # 获取 cookies 并保存
            cookies = driver.get_cookies()
            driver.quit()

            # 将 cookies 转换为字典格式并保存到文件
            cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}
            with open("weibo_cookies.json", "w") as f:
                json.dump(cookies_dict, f)

            return cookies_dict
        except Exception as e:
            print("检测设置按钮失败:", e)
            driver.quit()

    def split_timescope_by_day(self, start_date, end_date): 
        """
        将时间段按天切分为多个子时间段
        :param start_date: 起始日期，格式为 "YYYY-MM-DD"
        :param end_date: 结束日期，格式为 "YYYY-MM-DD"
        :return: 生成器，每次返回一天的时间段
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        while start <= end:
            day_start = start.strftime("%Y-%m-%d")
            day_end = (start + timedelta(days=1) - timedelta(seconds=1)).strftime("%Y-%m-%d")
            yield day_start, day_end
            start += timedelta(days=1)

    def get_main_body_day_by_day(self, q, kind, start_date, end_date):
        """
        按天获取指定时间段内的微博数据，并在每一天完成后将数据追加到主CSV文件
        :param q: 查询话题
        :param kind: 类别（普通、热门等）
        :param start_date: 起始日期，格式 "YYYY-MM-DD"
        :param end_date: 结束日期，格式 "YYYY-MM-DD"
        """
        for start, end in self.split_timescope_by_day(start_date, end_date):
            sub_timescope = f"{start}:{end}"
            logging.info(f"当前处理时间段：{sub_timescope}")
            
            try:
                day_data = get_all_main_body(q, kind, self.cookie, sub_timescope)
                
                # 将每天的数据追加保存到主 CSV 文件
                if not day_data.empty:
                    # 追加模式 ('a')，不写入列名 (header=False) 除非是第一次创建文件
                    if not os.path.exists(self.main_body_filepath):
                        day_data.to_csv(self.main_body_filepath, encoding="utf_8_sig", index=False)
                        logging.info(f"已创建主文件并保存 {start} 的数据")
                    else:
                        day_data.to_csv(self.main_body_filepath, encoding="utf_8_sig", index=False, mode='a', header=False)
                        logging.info(f"已将 {start} 的数据追加到主文件 {self.main_body_filepath}")
                else:
                    logging.warning(f"{start} 没有数据，跳过保存。")

            except Exception as e:
                logging.error(f"爬取 {sub_timescope} 时出现错误: {e}")

            # 添加随机延迟
            delay = random.uniform(3, 6)
            logging.info(f"等待 {delay:.2f} 秒后继续下一天的数据爬取...")
            time.sleep(delay)
        
    def get_main_body(self, q, kind, timescope):  
        data = get_all_main_body(q, kind, self.cookie, timescope)

        data = data.reset_index(drop=True).astype(str).drop_duplicates()  
        data.to_csv(self.main_body_filepath, encoding="utf_8_sig")  


    def get_comments_level_one(self):  
        data_list = []  
        main_body = pd.read_csv(self.main_body_filepath, index_col=0)  

        logging.info(f"主体内容一共有{main_body.shape[0]:5d}个，现在开始解析...")  

        for ix in track(range(main_body.shape[0]), description=f"解析中..."):  
            uid = main_body.iloc[ix]["uid"]  
            mid = main_body.iloc[ix]["mid"]  
            final_file_path = f"{self.comments_level_1_dirpath}{uid}_{mid}.csv"  

            if os.path.exists(final_file_path):  
                length = pd.read_csv(final_file_path).shape[0]  
                if length > 0:  
                    continue  

            data = get_all_level_one(uid=uid, mid=mid, cookie=self.cookie)
            
            # 将列表或字典类型的列转换为字符串，确保 drop_duplicates 可正常工作
            data = data.applymap(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
            
            data.drop_duplicates(inplace=True)  
            data.to_csv(final_file_path, encoding="utf_8_sig")  
            data_list.append(data)  
            
        logging.info(f"主体内容一共有{main_body.shape[0]:5d}个，已经解析完毕！")  
        data = pd.concat(data_list).reset_index(drop=True).astype(str).drop_duplicates()  
        data.to_csv(self.comments_level_1_filename)
        
    def get_comments_level_two(self):  
        data_list = []  
        comments_level_1_data = pd.read_csv(self.comments_level_1_filename, index_col=0)  

        logging.info(  
            f"一级评论一共有{comments_level_1_data.shape[0]:5d}个，现在开始解析..."  
        )  

        for ix in track(  
            range(comments_level_1_data.shape[0]), description=f"解析中..."  
        ):  
            main_body_uid = comments_level_1_data.iloc[ix]["main_body_uid"]  
            mid = comments_level_1_data.iloc[ix]["mid"]  
            final_file_path = (  
                f"{self.comments_level_2_dirpath}{main_body_uid}_{mid}.csv"  
            )  

            if os.path.exists(final_file_path):  
                length = pd.read_csv(final_file_path).shape[0]  
                if length > 0:  
                    continue  

            data = get_all_level_two(uid=main_body_uid, mid=mid, cookie=self.cookie)  
            data.drop_duplicates(inplace=True)  
            data.to_csv(final_file_path, encoding="utf_8_sig")  
            data_list.append(data)  
        logging.info(  
            f"一级评论一共有{comments_level_1_data.shape[0]:5d}个，已经解析完毕！"  
        )  
        data = pd.concat(data_list).reset_index(drop=True).astype(str).drop_duplicates()  
        data.to_csv(self.comments_level_2_filename)  

if __name__ == "__main__":  
    q = "中国"  # 话题  
    kind = "热门"  # 综合，实时，热门，高级  
    # timescope = "2024-10-01:2024-10-02"
    start_date = "2020-01-01"  # 起始日期
    end_date = "2021-01-01"  # 结束日期
    cookie = None  # 如果有已保存的cookie，可以直接传入；否则会自动登录
    wbparser = WBParser(cookie)  
    wbparser.get_main_body_day_by_day(q, kind, start_date, end_date)# 获取主体内容(以天为维度)
    # wbparser.get_main_body(q, kind, timescope)  # 获取主体内容  
    # wbparser.get_comments_level_one()  # 获取一级评论
    # wbparser.get_comments_level_two()  # 获取二级评论
