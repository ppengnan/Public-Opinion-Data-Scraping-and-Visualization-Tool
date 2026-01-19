#weibo_crawler.py
import tkinter as tk
from tkinter import messagebox
from tkcalendar import Calendar
# from weibo.main import WBParser  # 确保从 weibo 文件夹导入
from main import WBParser  # 确保从 weibo 文件夹导入

def start_weibo_crawler():
    """微博爬虫输入界面"""

    def select_timescope():
        """弹出日期选择窗口并格式化选择的时间范围"""
        def apply_date():
            start_date = cal_start.get_date()
            end_date = cal_end.get_date()
            timescope_entry.delete(0, tk.END)
            timescope_entry.insert(0, f"{start_date}-0:{end_date}-16")
            date_window.destroy()

        date_window = tk.Toplevel(weibo_window)
        date_window.title("选择时间范围")
        
        tk.Label(date_window, text="开始日期：").grid(row=0, column=0, padx=10, pady=5)
        cal_start = Calendar(date_window, selectmode="day", date_pattern="yyyy-mm-dd")
        cal_start.grid(row=0, column=1, padx=10, pady=5)

        tk.Label(date_window, text="结束日期：").grid(row=1, column=0, padx=10, pady=5)
        cal_end = Calendar(date_window, selectmode="day", date_pattern="yyyy-mm-dd")
        cal_end.grid(row=1, column=1, padx=10, pady=5)

        tk.Button(date_window, text="应用", command=apply_date).grid(row=2, columnspan=2, pady=10)

    def on_kind_change(*args):
        """根据类别显示或隐藏时间范围选项"""
        if kind_var.get() == "高级":
            timescope_entry.grid(row=3, column=1, padx=10, pady=5)
            timescope_button.grid(row=3, column=2, padx=10, pady=5)
        else:
            timescope_entry.grid_remove()
            timescope_button.grid_remove()

    def run_crawler():
        # 获取用户输入的参数
        q = topic_entry.get()
        kind = kind_var.get()
        timescope = timescope_entry.get() if kind == "高级" else None
        
        # 验证输入内容
        if not q or not kind or (kind == "高级" and not timescope):
            messagebox.showerror("错误", "请填写所有必填字段！")
            return
        
        # 初始化 WBParser 并运行爬虫
        try:
            cookie = None  # 设置你的 cookie 或从其他地方加载
            wbparser = WBParser(cookie)
            wbparser.get_main_body(q, kind)  # 获取主体内容
            wbparser.get_comments_level_one()  # 获取一级评论
            wbparser.get_comments_level_two()  # 获取二级评论
            messagebox.showinfo("完成", "微博爬取成功！")
        except Exception as e:
            messagebox.showerror("错误", f"爬取失败: {str(e)}")

    # 创建微博爬虫的输入窗口
    weibo_window = tk.Toplevel()
    weibo_window.title("Weibo Crawler")

    tk.Label(weibo_window, text="请输入话题：").grid(row=0, column=0, padx=10, pady=5)
    topic_entry = tk.Entry(weibo_window, width=30)
    topic_entry.grid(row=0, column=1, padx=10, pady=5)

    tk.Label(weibo_window, text="请选择类别：").grid(row=1, column=0, padx=10, pady=5)
    kind_var = tk.StringVar(value="热门")
    kind_var.trace("w", on_kind_change)
    kind_menu = tk.OptionMenu(weibo_window, kind_var, "热门", "综合")
    kind_menu.grid(row=1, column=1, padx=10, pady=5)

    tk.Label(weibo_window, text="时间范围 (高级类别可选)：").grid(row=3, column=0, padx=10, pady=5)
    timescope_entry = tk.Entry(weibo_window, width=30)
    timescope_button = tk.Button(weibo_window, text="选择时间范围", command=select_timescope)

    # 初始隐藏时间范围输入框和按钮
    timescope_entry.grid_remove()
    timescope_button.grid_remove()

    tk.Button(weibo_window, text="开始爬取", command=run_crawler).grid(row=4, columnspan=2, pady=20)
