import os
import re
import requests
import time
import pandas as pd
from datetime import datetime, timedelta

# プロジェクトルートを定義（このファイルの2階層上）
project_root = os.path.dirname(os.path.dirname(__file__))

# 各種ディレクトリ
TXT_FILE_DIR_B = os.path.join(project_root, 'data', 'data_master', 'txt_B')
TXT_FILE_DIR_R = os.path.join(project_root, 'data', 'data_master', 'txt_R')
RESULT_CSV_PATH = os.path.join(project_root, 'artifacts', 'result.csv')

# 保存先のディレクトリ作成
os.makedirs(TXT_FILE_DIR_B, exist_ok=True)
os.makedirs(TXT_FILE_DIR_R, exist_ok=True)
os.makedirs(os.path.dirname(RESULT_CSV_PATH), exist_ok=True)

def download_text_files(days=1):
    """指定日数分のレース結果テキスト（B/R）をダウンロードして保存"""
    today = datetime.today()
    for i in range(days):
        target_date = today - timedelta(days=i+1)
        ymd = target_date.strftime('%Y%m%d')

        for code in ['B', 'R']:
            url = f'https://www.boatrace.jp/owpc/pc/race/download?hd={ymd}&type={code}'
            try:
                res = requests.get(url)
                if res.status_code == 200 and len(res.content) > 1000:
                    filepath = os.path.join(TXT_FILE_DIR_B if code == 'B' else TXT_FILE_DIR_R, f'{ymd}_{code}.txt')
                    with open(filepath, 'wb') as f:
                        f.write(res.content)
                    print(f'{code}ファイル保存成功: {filepath}')
                else:
                    print(f'{code}ファイルなしまたはデータ不十分: {ymd}')
            except Exception as e:
                print(f'エラー({code}): {e}')
        time.sleep(1)  # サーバー負荷軽減のため

def convert_texts_to_csv():
    """保存されたテキストファイルからレース結果CSVを作成"""
    txt_file_list_B = sorted(os.listdir(TXT_FILE_DIR_B))
    txt_file_list_R = sorted(os.listdir(TXT_FILE_DIR_R))
    df_mix = []

    for r_file, b_file in zip(txt_file_list_R, txt_file_list_B):
        try:
            with open(os.path.join(TXT_FILE_DIR_R, r_file), encoding='shift-jis', errors='ignore') as f:
                dataR = f.read().split('KBGN')[1:]

            with open(os.path.join(TXT_FILE_DIR_B, b_file), encoding='shift-jis', errors='ignore') as f:
                dataB = f.read().split('BBGN')[1:]

            for r, b in zip(dataR[1:], dataB[1:]):
                try:
                    rrr = r.split('\n')

                    # 結果データ
                    results = [row.replace('\u3000','').replace('\n','') for row in rrr if re.match('^\s+[0]+[0-6]|\s+[A-Z]', row)]
                    pattern2 = '^(\s+[0][0-6]|\s+[A-Z][0-6]|\s+[A-Z])(\s+[0-6])(\s+\d{4})([^0-9]+\s)(\d+)(\s+\d+)'\
                               '(\s+\d{1}.\d{2}|\s+\D{1})(\s+[1-6]|\s+\D{1}|\s+)(\s+[0].[0-9]{2}|\s+\D{1})'
                    valueR = [re.match(re.compile(pattern2), result).groups() for result in results if re.match(pattern2, result)]

                    # 日付取得
                    date = r_file[:8]

                    for p in valueR:
                        df_mix.append([date] + list(p))

                except Exception as e:
                    print(f"サブデータ処理エラー: {e}")
        except Exception as e:
            print(f"ファイル読み込みエラー: {e}")

    # CSVとして保存（アーティファクトに）
    if df_mix:
        df_result = pd.DataFrame(df_mix)
        df_result.to_csv(RESULT_CSV_PATH, index=False)
        print(f'レース結果CSVを保存しました: {RESULT_CSV_PATH}')
    else:
        print('有効なレース結果データがありませんでした')

if __name__ == "__main__":
    download_text_files(days=1)  # 直近1日分を対象
    convert_texts_to_csv()
