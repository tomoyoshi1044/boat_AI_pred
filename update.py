import sys
import os
from datetime import datetime, timedelta

# モジュール検索パスに追加
sys.path.append('data')
sys.path.append('utils')
sys.path.append('models_clf')

# 各種関数のインポート
from download import download_data
from read_text import make_result_csv
from make_personal import make_csv_personal
from rating import make_rating
from one_race import make_onerace
from make_model_clf import train_models, train_models3

def update(start: str, end: str):
    """
    月次更新処理を順番に実行
    """
    download_data(start, end)
    make_result_csv()
    make_csv_personal()
    make_rating()
    axis1_data = make_onerace()
    train_models(axis1_data)
    train_models3(axis1_data)

if __name__ == "__main__":
    # 今日の日付を取得
    today = datetime.today()
    
    # 1日前と7日前を算出
    end_date = (today - timedelta(days=1)).strftime('%Y%m%d')
    start_date = (today - timedelta(days=7)).strftime('%Y%m%d')
    
    update(start_date, end_date)
