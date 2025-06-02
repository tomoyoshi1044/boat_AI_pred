import pandas as pd
import numpy as np
import os
import warnings

# RuntimeWarningを無視
warnings.filterwarnings("ignore", category=RuntimeWarning)


# プロジェクトルートのパスを取得
project_root = os.path.dirname(os.path.dirname(__file__))

# ディレクトリパスを定義
csv_result_dir = os.path.join(project_root, 'data', 'data_master', 'result_csv')
personal_csv_dir = os.path.join(project_root, 'data', 'data_master', 'personal_csv')

# CSV保存先ディレクトリを作成
os.makedirs(personal_csv_dir, exist_ok=True)

def make_csv_personal():
    # CSVファイルの取得とソート
    csv_result_files = sorted(os.listdir(csv_result_dir))
    if '.ipynb_checkpoints' in csv_result_files:
        csv_result_files.remove('.ipynb_checkpoints')
    
    # 最新6ファイルの読み込み
    df_r = [pd.read_csv(os.path.join(csv_result_dir, i)) for i in csv_result_files[-7:-1]]
    df_results = pd.concat(df_r).reset_index(drop=True)
    
    # 選手ごとの計算
    num = df_results['選手登番'].unique()
    num.sort()

    # 平均ST順位、枠ごとのST順位を計算
    st_data = calculate_st_and_ranking(df_results, num)
    
    # 連対率の計算
    st_data = calculate_win_rates(df_results, num, st_data)
    
    # データの保存（カラムの順番をオリジナルと同じにする）
    st_data = st_data[[
        '選手登番', '1枠1着率', '1枠2連対率', '1枠3連対率', '2枠1着率', '2枠2連対率', '2枠3連対率',
        '3枠1着率', '3枠2連対率', '3枠3連対率', '4枠1着率', '4枠2連対率', '4枠3連対率',
        '5枠1着率', '5枠2連対率', '5枠3連対率', '6枠1着率', '6枠2連対率', '6枠3連対率',
        '平均ST順位', '1枠ST順位', '2枠ST順位', '3枠ST順位', '4枠ST順位', '5枠ST順位', '6枠ST順位',
        '平均ST', '1枠ST', '2枠ST', '3枠ST', '4枠ST', '5枠ST', '6枠ST'
    ]]
    personal_data_path = os.path.join(personal_csv_dir, 'personal_data.csv')
    st_data.to_csv(personal_data_path, index=False)
    print('パーソナルデータ完了')

def calculate_st_and_ranking(df_results, num):
    """選手ごとの平均STとST順位を計算"""
    lis, lis1, lis2, lis3, lis4, lis5, lis6 = [], [], [], [], [], [], []
    avest_all, avest1, avest2, avest3, avest4, avest5, avest6 = [], [], [], [], [], [], []
    
    for i in num:
        st = df_results[df_results['選手登番'] == i]['ST'].values
        lis.append(np.nanmean(st) if st.size > 0 else np.nan)
        
        # 各枠ごとにSTとST順位を計算
        for j in range(1, 7):
            st_waku = df_results[(df_results['選手登番'] == i) & (df_results['艇番'] == j)]['ST'].values
            avest = df_results[(df_results['選手登番'] == i) & (df_results['艇番'] == j)]['ST順'].values
            
            # 空チェックを行い、空の場合はNaNを追加
            lis_value = np.nanmean(st_waku) if st_waku.size > 0 else np.nan
            avest_value = np.nanmean(avest) if avest.size > 0 else np.nan
            
            # リストに追加
            eval(f'lis{j}').append(lis_value)
            eval(f'avest{j}').append(avest_value)
        
        # 平均ST順位
        win = df_results[df_results['選手登番'] == i]['ST順'].values
        avest_all.append(np.nanmean(win) if win.size > 0 else np.nan)
    
    st_data = pd.DataFrame({
        '選手登番': num,
        '平均ST': lis, '1枠ST': lis1, '2枠ST': lis2, '3枠ST': lis3, '4枠ST': lis4, '5枠ST': lis5, '6枠ST': lis6,
        '平均ST順位': avest_all, '1枠ST順位': avest1, '2枠ST順位': avest2, '3枠ST順位': avest3,
        '4枠ST順位': avest4, '5枠ST順位': avest5, '6枠ST順位': avest6
    })
    return st_data

def calculate_win_rates(df_results, num, st_data):
    """選手ごとの1着率、2連対率、3連対率を枠ごとに計算"""
    df_results['1着'] = df_results['着順'].apply(lambda x: 0 if x == 1 else 1)
    df_results['2連対'] = df_results['着順'].apply(lambda x: 0 if x <= 2 else 1)
    df_results['3連対'] = df_results['着順'].apply(lambda x: 0 if x <= 3 else 1)
    
    for j in range(1, 7):
        for k, label in enumerate(['1着率', '2連対率', '3連対率'], start=1):
            win_rate = []
            for i in num:
                try:
                    total = len(df_results[(df_results['選手登番'] == i) & (df_results['艇番'] == j)])
                    if k == 1:
                        wins = len(df_results[(df_results['選手登番'] == i) & (df_results['艇番'] == j) & (df_results['1着'] == 0)])
                    else:
                        wins = len(df_results[(df_results['選手登番'] == i) & (df_results['艇番'] == j) & (df_results[f'{k}連対'] == 0)])
                    win_rate.append(wins / total if total > 0 else 0)
                except ZeroDivisionError:
                    win_rate.append(0)
            st_data[f'{j}枠{label}'] = win_rate
    return st_data
if __name__ == "__main__":
    make_csv_personal()
