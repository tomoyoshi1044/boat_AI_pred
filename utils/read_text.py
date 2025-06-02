import os
import re
import itertools
import pandas as pd

# プロジェクトルートのパスを取得
project_root = os.path.dirname(os.path.dirname(__file__))

# テキストデータとCSVデータの保存先を定義
TXT_FILE_DIR_B = os.path.join(project_root, 'data', 'data_master', 'txt_B')
TXT_FILE_DIR_R = os.path.join(project_root, 'data', 'data_master', 'txt_R')
RESULT_CSV_DIR = os.path.join(project_root, 'data', 'data_master', 'result_csv')
PAYOUT_CSV_DIR = os.path.join(project_root, 'data', 'data_master', 'payout_csv')

# CSV保存先ディレクトリを作成
os.makedirs(RESULT_CSV_DIR, exist_ok=True)
os.makedirs(PAYOUT_CSV_DIR, exist_ok=True)

def make_result_csv():
    txt_file_list_B = sorted(os.listdir(TXT_FILE_DIR_B))
    txt_file_list_R = sorted(os.listdir(TXT_FILE_DIR_R))
    df_mix = []  
    df_payout = []
    for r, b in zip(txt_file_list_R, txt_file_list_B):
        try:
            with open(os.path.join(TXT_FILE_DIR_R, r), encoding='shift-jis', errors='ignore') as f:
                dataR = f.read().split('KBGN')[1:]

            with open(os.path.join(TXT_FILE_DIR_B, b), encoding='shift-jis', errors='ignore') as f:
                dataB = f.read().split('BBGN')[1:]

            for r,b in zip(dataR[1:],dataB[1:]):
                try:
                    rrr = r.split('\n')
                    bbb = b.split('\n')

                    pay = [row.replace('\u3000','').replace('\n','') for row in rrr if re.match('^\s+[0-9]+[R]+\s+[0-6]', row)]
                    pattern = '(\s+\d*[R])(\s+[1-6]+[-]+[1-6]+[-]+[1-6])(\s+\d*)'
                    payout = [re.match(re.compile(pattern), p).groups() for p in pay]

                    results = [row.replace('\u3000','').replace('\n','') for row in rrr if re.match('^\s+[0]+[0-6]\s|\s+[A-Z]', row)]
                    pattern2 = '^(\s+[0][0-6]|\s+[A-Z][0-6]|\s+[A-Z])(\s+[0-6])(\s+\d{4})([^0-9]+\s)(\d+)(\s+\d+)'\
                               '(\s+\d{1}.\d{2}|\s+\D{1})(\s+[1-6]|\s+\D{1}|\s+)(\s+[0].[0-9]{2}|\s+\D{1})'
                    valueR = [re.match(re.compile(pattern2), result).groups() for result in results]

                    place = [row.replace('\u3000','').replace('\n','') for row in rrr if re.match('\s+[第]\s+', row)]
                    pattern3 = '\s+[第]\s[1-9][日]\s+(\d{4}[/]\s*\d+[/]\s*\d+)\s+([ボートレース][\S]+)'
                    values = [re.match(re.compile(pattern3), p).groups() for p in place]
                    date = list(values[0])

                    code = re.search(r'\d+[KEND]', r[-19:]).group()[0:2]

                    tes2 = [date + [code] + list(i) for i in payout]

                    valueR = [valueR[i:i+6] for i in range(0, len(valueR), 6)]
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

                    racers = [row.replace('\u3000','').replace('\n','') for row in bbb if re.match('^[0-6]\s', row)]
                    pattern4 = '^([1-6])\s(\d{4})([^0-9]+)(\d{2})([^0-9]+)(\d{2})([AB]\d{1})\s(\d{1}.\d{2})\s*(\d+.\d{2})\s*(\d+.\d{2})\s*(\d+.\d{2})\s+\d+\s*(\d+.\d{2})\s*\d+\s*(\d+.\d{2})'
                    valueB = [re.match(re.compile(pattern4), racer).groups() for racer in racers]
                    valueB = [valueB[i:i+6] for i in range(0, len(valueB), 6)]
                    df_B = [pd.DataFrame(i, columns=['艇番', '選手登番', '選手名', '年齢', '支部', '体重', '級別', '全国勝率', '全国2連率', '当地勝率', '当地2連率', 'モーター2連率', 'ボート2連率']) for i in valueB]

                    if len(df_R) == len(df_B):
                        df_payout.append(df_p1)
                        for R, B in zip(df_R, df_B):
                            df_concat = pd.concat([R, B], axis=1)
                            df_concat = df_concat.loc[:, ~df_concat.columns.duplicated()]
                            df_mix.append(df_concat)
                except Exception as e:
                    print(f"Error processing a race pair in files {r}, {b}: {e}")

        except Exception as e:
            print(f"Error opening or processing file {r}, {b}: {e}")

    data = pd.concat(df_mix).reset_index(drop=True)
    for col in ['着順', '艇番', '展示', '進入', 'ST', '全国勝率', '全国2連率', '当地勝率', '当地2連率', 'モーター2連率', 'ボート2連率']:
        data[col] = pd.to_numeric(data[col], errors='coerce')

    data['ST順'] = data.groupby('id')['ST'].rank(method='min', ascending=True)
    data['展示タイム順'] = data.groupby('id')['展示'].rank(method='min', ascending=True)

    date = data['id'][0][:6]
    result_csv_path = os.path.join(RESULT_CSV_DIR, f"data_result{date}.csv")
    data.to_csv(result_csv_path, index=False)

    data_payout = pd.concat(itertools.chain.from_iterable(df_payout)).reset_index(drop=True)
    data_payout['配当'] = pd.to_numeric(data_payout['配当'], errors='coerce')
    payout_csv_path = os.path.join(PAYOUT_CSV_DIR, f"data_payout{date}.csv")
    data_payout.to_csv(payout_csv_path, index=False)

if __name__ == "__main__":
    make_result_csv()
