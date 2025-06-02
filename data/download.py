import os
import re
import shutil
from time import sleep
from requests import get
from datetime import datetime as dt, timedelta as td
import lhafile

# グローバル変数
INTERVAL = 2

# プロジェクトルートのパスを取得
project_root = os.path.dirname(os.path.dirname(__file__))

# ディレクトリパスの指定
lzh_dir_r = os.path.join(project_root, 'data', 'data_master', 'lzh_R')
txt_dir_r = os.path.join(project_root, 'data', 'data_master', 'txt_R')
lzh_dir_b = os.path.join(project_root, 'data', 'data_master', 'lzh_B')
txt_dir_b = os.path.join(project_root, 'data', 'data_master', 'txt_B')

def generate_date_list(start_date, end_date):
    days_num = (end_date - start_date).days + 1
    return [(start_date + td(days=i)).strftime("%Y%m%d") for i in range(days_num)]

def download_files(date_list, base_url, save_dir, prefix):
    shutil.rmtree(save_dir, ignore_errors=True)
    os.makedirs(save_dir, exist_ok=True)

    for date in date_list:
        yyyymm = date[:6]
        yymmdd = date[2:]
        file_name = f"{prefix}{yymmdd}.lzh"
        variable_url = f"{base_url}{yyyymm}/{file_name}"

        r = get(variable_url)
        if r.status_code == 200:
            with open(os.path.join(save_dir, file_name), 'wb') as f:
                f.write(r.content)
            print(f"{variable_url} をダウンロードしました")
        else:
            print(f"{variable_url} のダウンロードに失敗しました")

        sleep(INTERVAL)

def extract_lzh_files(lzh_dir, extract_dir):
    shutil.rmtree(extract_dir, ignore_errors=True)
    os.makedirs(extract_dir, exist_ok=True)

    for lzh_file_name in os.listdir(lzh_dir):
        if re.search(r".lzh$", lzh_file_name):
            file_path = os.path.join(lzh_dir, lzh_file_name)
            file = lhafile.Lhafile(file_path)
            name = file.infolist()[0].filename
            with open(os.path.join(extract_dir, name), "wb") as f:
                f.write(file.read(name))
            print(f"{file_path} を解凍しました")

def download_data(start_date_str, end_date_str):
    """
    指定された期間のデータをダウンロード＆解凍するメイン関数
    """
    start_date = dt.strptime(start_date_str, '%Y%m%d')
    end_date = dt.strptime(end_date_str, '%Y%m%d')
    date_list = generate_date_list(start_date, end_date)

    print("ダウンロード作業を開始します")
    download_files(date_list, "http://www1.mbrace.or.jp/od2/K/", lzh_dir_r, "k")
    download_files(date_list, "http://www1.mbrace.or.jp/od2/B/", lzh_dir_b, "b")

    print("解凍作業を開始します")
    extract_lzh_files(lzh_dir_r, txt_dir_r)
    extract_lzh_files(lzh_dir_b, txt_dir_b)

    print("作業を終了しました。")

