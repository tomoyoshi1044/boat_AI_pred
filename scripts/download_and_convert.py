import os
import argparse
import datetime
import requests
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from bs4 import BeautifulSoup
from io import StringIO

# 保存先ディレクトリ
ROOT_DIR = Path(__file__).resolve().parents[1]
TXT_B_DIR = ROOT_DIR / "data" / "data_master" / "txt_B"
TXT_R_DIR = ROOT_DIR / "data" / "data_master" / "txt_R"
RESULT_DIR = ROOT_DIR / "data" / "data_master" / "csv_result"

for d in [TXT_B_DIR, TXT_R_DIR, RESULT_DIR]:
    d.mkdir(parents=True, exist_ok=True)


def download_file(url, filepath, code):
    try:
        res = requests.get(url, timeout=10)
        if res.status_code == 200:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(res.text)
            print(f'{code}ファイル保存成功: {filepath}')
        else:
            print(f'{code}ファイル保存失敗: ステータスコード {res.status_code} - {url}')
    except Exception as e:
        print(f'{code}ファイル保存失敗: {e} - {url}')


def parse_result(html):
    soup = BeautifulSoup(html, "html.parser")
    try:
        race_number = soup.select_one(".numberSet1_row").text.strip()
        table = soup.select_one(".table1")
        if table is None:
            return None
        df = pd.read_html(StringIO(str(table)))[0]
        df["レース"] = race_number
        return df
    except Exception:
        return None


def main(days):
    today = datetime.date.today()

    all_result_dfs = []

    for delta in range(days):
        target_date = today - datetime.timedelta(days=delta)
        date_str = target_date.strftime("%Y%m%d")

        for jcd in range(1, 25):  # 1〜24場
            jcd_str = f"{jcd:02}"
            for rno in range(1, 13):  # 各場12Rまで
                rno_str = f"{rno:02}"

                # ファイル保存パス
                b_filepath = TXT_B_DIR / f"{date_str}_B.txt"
                r_filepath = TXT_R_DIR / f"{date_str}_R.txt"

                # URL
                url_b = f"https://www.boatrace.jp/owpc/pc/race/beforeinfo?rno={rno_str}&jcd={jcd_str}&hd={date_str}"
                url_r = f"https://www.boatrace.jp/owpc/pc/race/raceresult?rno={rno_str}&jcd={jcd_str}&hd={date_str}"

                # ファイルがまだ存在しない場合のみダウンロード
                if not b_filepath.exists():
                    download_file(url_b, b_filepath, "B")

                if not r_filepath.exists():
                    download_file(url_r, r_filepath, "R")

                # 結果HTMLをパースして結果DataFrameに追加
                try:
                    res = requests.get(url_r, timeout=10)
                    if res.status_code == 200:
                        df = parse_result(res.text)
                        if df is not None:
                            df["場コード"] = jcd_str
                            df["日付"] = date_str
                            df["レース番号"] = rno
                            all_result_dfs.append(df)
                        else:
                            print(f"レース結果なし: {date_str} {jcd_str}-{rno_str}")
                    else:
                        print(f"結果取得失敗: ステータスコード {res.status_code} - {url_r}")
                except Exception as e:
                    print(f"結果取得エラー: {e} - {url_r}")

    # 結果を保存
    if all_result_dfs:
        df_result = pd.concat(all_result_dfs, ignore_index=True)
        result_path = RESULT_DIR / f"{today.strftime('%Y%m%d')}_result.csv"
        df_result.to_csv(result_path, index=False, encoding="utf-8-sig")
        print(f"レース結果CSV保存成功: {result_path}")
    else:
        print("有効なレース結果データがありませんでした")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=1, help="過去何日分を取得するか（デフォルト: 1日）")
    args = parser.parse_args()
    main(args.days)

