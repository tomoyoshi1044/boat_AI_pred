import os
import re
import shutil
import itertools
from time import sleep
from requests import get
from datetime import datetime as dt, timedelta as td
import lhafile
import pandas as pd

# グローバル変数
INTERVAL = 2

# プロジェクトルート（GitHub Actionsの実行ディレクトリ想定）
project_root = os.getcwd()

# ディレクトリパスの指定
lzh_dir_r = os.path.join(project_root, 'lzh_R')
lzh_dir_b = os.path.join(project_root, 'lzh_B')

RESULT_CSV_DIR = os.path.join(project_root, 'result_csv')
PAYOUT_CSV_DIR = os.path.join(project_root, 'payout_csv')

os.makedirs(lzh_dir_r, exist_ok=True)
os.makedirs(lzh_dir_b, exist_ok=True)
os.makedirs(RESULT_CSV_DIR, exist_ok=True)
os.makedirs(PAYOUT_CSV_DIR, exist_ok=True)


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


def extract_lzh_files(lzh_dir):
    """
    解凍して中のファイルのバイト列を返す辞書 {ファイル名: bytes}
    """
    extracted_files = {}
    for lzh_file_name in os.listdir(lzh_dir):
        if re.search(r".lzh$", lzh_file_name):
            file_path = os.path.join(lzh_dir, lzh_file_name)
            file = lhafile.Lhafile(file_path)
            name = file.infolist()[0].filename
            data = file.read(name)
            extracted_files[name] = data
            print(f"{file_path} を解凍しました -> {name}")
    return extracted_files


def parse_and_make_csv(data_r_dict, data_b_dict):
    """
    lzh解凍したバイトデータを解析しDataFrame作成→CSV保存
    """
    df_mix = []
    df_payout = []

    # 両方のファイル名をソートして合わせる
    keys_r = sorted(data_r_dict.keys())
    keys_b = sorted(data_b_dict.keys())

    for r_key, b_key in zip(keys_r, keys_b):
        try:
            text_r = data_r_dict[r_key].decode('shift_jis', errors='ignore')
            text_b = data_b_dict[b_key].decode('shift_jis', errors='ignore')

            dataR = text_r.split('KBGN')[1:]
            dataB = text_b.split('BBGN')[1:]

            for r, b in zip(dataR[1:], dataB[1:]):
                try:
                    rrr = r.split('\n')
                    bbb = b.split('\n')

                    pay = [row.replace('\u3000', '').replace('\n', '') for row in rrr if re.match('^\s+[0-9]+[R]+\s+[0-6]', row)]
                    pattern = r'(\s+\d*[R])(\s+[1-6]+[-]+[1-6]+[-]+[1-6])(\s+\d*)'
                    payout = [re.match(re.compile(pattern), p).groups() for p in pay]

                    results = [row.replace('\u3000', '').replace('\n', '') for row in rrr if re.match('^\s+[0]+[0-6]\s|\s+[A-Z]', row)]
                    pattern2 = r'^(\s+[0][0-6]|\s+[A-Z][0-6]|\s+[A-Z])(\s+[0-6])(\s+\d{4})([^0-9]+\s)(\d+)(\s+\d+)(\s+\d{1}.\d{2}|\s+\D{1})(\s+[1-6]|\s+\D{1}|\s+)(\s+[0].[0-9]{2}|\s+\D{1})'
                    valueR = [re.match(re.compile(pattern2), result).groups() for result in results]

                    place = [row.replace('\u3000', '').replace('\n', '') for row in rrr if re.match('\s+[第]\s+', row)]
                    pattern3 = r'\s+[第]\s[1-9][日]\s+(\d{4}[/]\s*\d+[/]\s*\d+)\s+([ボートレース][\S]+)'
                    values = [re.match(re.compile(pattern3), p).groups() for p in place]
                    date = list(values[0])

                    code = re.search(r'\d+[KEND]', r[-19:]).group()[0:2]

                    tes2 = [date + [code] + list(i) for i in payout]

                    valueR = [valueR[i:i + 6] for i in range(0, len(valueR), 6)]
                    df_R, df_p1 = [], []
                    for i, d in zip(valueR, tes2):
                        columns = ['着順', '艇番', '選手登番', '選手名', 'モーター', 'ボート', '展示', '進入', 'ST']
                        df_result = pd.DataFrame(i, columns=columns)
                        df_result['艇番'] = df_result['艇番'].astype(int)
                        sort_df = df_result.sort_values('艇番').reset_index(drop=True)
                        year = d[0].replace('/', '').replace(' ', '0')
                        raceNo = d[3].replace(' ', '').replace('R', '')
                        id_lis = [year + code + raceNo] * 6
                        sort_df['id'] = id_lis
                        df_R.append(sort_df)

                        race_id = [year + code + raceNo]
                        di = d + race_id
                        df_p = pd.DataFrame([di], columns=['日付', '場', '場コード', 'レースNo', '3連単目', '配当', 'id'])
                        df_p1.append(df_p)

                    racers = [row.replace('\u3000', '').replace('\n', '') for row in bbb if re.match('^[0-6]\s', row)]
                    pattern4 = r'^([1-6])\s(\d{4})([^0-9]+)(\d{2})([^0-9]+)(\d{2})([AB]\d{1})\s(\d{1}.\d{2})\s*(\d+.\d{2})\s*(\d+.\d{2})\s*(\d+.\d{2})\s+\d+\s*(\d+.\d{2})\s*\d+\s*(\d+.\d{2})'
                    valueB = [re.match(re.compile(pattern4), racer).groups() for racer in racers]
                    valueB = [valueB[i:i + 6] for i in range(0, len(valueB), 6)]
                    df_B = [pd.DataFrame(i, columns=['艇番', '選手登番', '選手名', '年齢', '支部', '体重', '級別', '全国勝率', '全国2連率', '当地勝率', '当地2連率', 'モーター2連率', 'ボート2連率']) for i in valueB]

                    if len(df_R) == len(df_B):
                        df_payout.append(df_p1)
                        for R, B in zip(df_R, df_B):
                            df_concat = pd.concat([R, B], axis=1)
                            df_concat = df_concat.loc[:, ~df_concat.columns.duplicated()]
                            df_mix.append(df_concat)

                except Exception as e:
                    print(f"Error processing a race pair in files {r_key}, {b_key}: {e}")

        except Exception as e:
            print(f"Error opening or processing files {r_key}, {b_key}: {e}")

    if not df_mix:
        print("No data to save.")
        return

    data = pd.concat(df_mix).reset_index(drop=True)
    for col in ['着順', '艇番', '展示', '進入', 'ST', '全国勝率', '全国2連率', '当地勝率', '当地2連率', 'モーター2連率', 'ボート2連率']:
        data[col] = pd.to_numeric(data[col], errors='coerce')

    data['ST順'] = data.groupby('id')['ST'].rank(method='min', ascending=True)
    data['展示タイム順'] = data.groupby('id')['展示'].rank(method='min', ascending=True)

    date = data['id'][0][:6]
    result_csv_path = os.path.join(RESULT_CSV_DIR, f"data_result{date}.csv")
    data.to_csv(result_csv_path, index=False)
    print(f"Result CSV saved: {result_csv_path}")

    data_payout = pd.concat(itertools.chain.from_iterable(df_payout)).reset_index(drop=True)
    data_payout['配当'] = pd.to_numeric(data_payout['配当'], errors='coerce')
    payout_csv_path = os.path.join(PAYOUT_CSV_DIR, f"data_payout{date}.csv")
    data_payout.to_csv(payout_csv_path, index=False)
    print(f"Payout CSV saved: {payout_csv_path}")


def main():
    start_date = dt.strptime("20220401", "%Y%m%d")
    end_date = dt.strptime("20220403", "%Y%m%d")

    base_url_r = "https://www.boatrace.jp/owpc/pc/race/raceresultinfo/getDownloadFile?fileName="
    base_url_b = "https://www.boatrace.jp/owpc/pc/race/raceresultinfo/getDownloadFile?fileName="

    date_list = generate_date_list(start_date, end_date)

    print("Rファイルのダウンロード開始")
    download_files(date_list, base_url_r, lzh_dir_r, 'r00')
    print("Bファイルのダウンロード開始")
    download_files(date_list, base_url_b, lzh_dir_b, 'b00')

    print("Rファイルの解凍開始")
    data_r_dict = extract_lzh_files(lzh_dir_r)
    print("Bファイルの解凍開始")
    data_b_dict = extract_lzh_files(lzh_dir_b)

    print("CSV変換処理開始")
    parse_and_make_csv(data_r_dict, data_b_dict)


if __name__ == "__main__":
    main()

