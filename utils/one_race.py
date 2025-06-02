import pandas as pd
import os
import numpy as np

# プロジェクトルートを取得（GitHub Actionsなどでも動作）
try:
    project_root = os.path.dirname(os.path.dirname(__file__))
except NameError:
    project_root = os.getcwd()

def load_data():
    personal_data_path = os.path.join(project_root, 'data', 'data_master', 'personal_csv', 'personal_data.csv')
    rating_data_path = os.path.join(project_root, 'data', 'data_master', 'rating_csv', 'rating.csv')
    
    df_personal = pd.read_csv(personal_data_path)
    df_rating = pd.read_csv(rating_data_path)
    p_df = pd.merge(df_personal, df_rating, on='選手登番', how='left')
    return p_df

def load_results():
    csv_result_dir = os.path.join(project_root, 'data', 'data_master', 'result_csv')
    csv_result_files = [f for f in sorted(os.listdir(csv_result_dir)) if f != '.ipynb_checkpoints']
    df_r = [pd.read_csv(os.path.join(csv_result_dir, file)) for file in csv_result_files[-7:]]
    return pd.concat(df_r).reset_index(drop=True)

def process_results(df_results):
    df_results['1着'] = (df_results['着順'] == 1).astype(int)
    df_results['2連対'] = (df_results['着順'] <= 2).astype(int)
    df_results['3連対'] = (df_results['着順'] <= 3).astype(int)
    df_results['グループ'] = (df_results['着順'] == 1).astype(int)

    df_results['当地2連率'] = df_results['当地2連率'].apply(lambda x: df_results['当地2連率'].mean() if x <= 1 else x)
    df_results['当地勝率'] = df_results['当地勝率'].apply(lambda x: df_results['当地勝率'].mean() if x <= 1 else x)
    df_results['モーター2連率'] = df_results['モーター2連率'].apply(lambda x: df_results['モーター2連率'].mean() if x <= 1 or x >= 99 else x)

    return df_results

def prepare_race_data(mix_data):
    id_list = mix_data['id'].unique()
    one_race, jyogai = [], []
    for i in id_list:
        w = mix_data[mix_data['id'] == i]
        if len(w) == 6:
            one_race.append(w.sort_values('艇番'))
        else:
            jyogai.append(w)
    return one_race, jyogai

def create_teiban_columns(mix_data):
    colors = ['1号艇', '2号艇', '3号艇', '4号艇', '5号艇', '6号艇']
    return np.concatenate([mix_data.columns + color for color in colors])

def build_race_matrix(one_race, teiban):
    one_race_array = np.array([
        np.concatenate([race.iloc[i:i+1].values for i in range(6)]).flatten()
        for race in one_race
    ])
    axis1_data = pd.DataFrame(one_race_array, columns=teiban).reset_index(drop=True)
    return axis1_data

def make_onerace():
    p_df = load_data()
    df_results = load_results()
    df_results = process_results(df_results)
    mix_data = pd.merge(df_results, p_df, on='選手登番')
    one_race, jyogai = prepare_race_data(mix_data)
    teiban = create_teiban_columns(mix_data)
    axis1_data = build_race_matrix(one_race, teiban)
    return axis1_data

if __name__ == "__main__":
    data = make_onerace()
    print(data.head())  # デバッグ目的のみ（不要なら削除してOK）
