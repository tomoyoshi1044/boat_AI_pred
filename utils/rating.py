import pandas as pd
import os
import numpy as np

# プロジェクトルートを取得
project_root = os.path.dirname(os.path.dirname(__file__))

def load_recent_csv_files(directory, num_files=6):
    """
    指定されたディレクトリから最新の指定数のCSVファイルを読み込み、データフレームを連結して返す。
    """
    files = sorted([f for f in os.listdir(directory) if f.endswith('.csv')])[-num_files:]
    return pd.concat([pd.read_csv(os.path.join(directory, f)) for f in files], ignore_index=True)


def process_individual_results(df_results, id_col='id', rank_col='ST順'):
    """
    各レースIDごとに着順とSTポイントの計算を行い、データフレームを返す。
    """
    # 各レースIDごとに処理
    processed_dfs = []
    for race_id in df_results['id'].unique():
        race_df = df_results[df_results['id'] == race_id].copy()
        
        # ポイントリスト
        points = [100, 80, 60, 40, 20, 10]
        if (race_df['ST順'].isna().any()) or (race_df['着順'].isna().any()):
            pass  # NaNがある場合は処理をスキップ
        else:
            
            # ST順に基づきポイントを付与
            race_df['STポイント'] = race_df['ST順'].rank(method='dense').astype(int).apply(
                lambda x: points[x - 1] if x <= len(points) else 10
            )
        
            # 着順に基づきポイントを付与
            race_df['着順ポイント'] = race_df['着順'].rank(method='dense').astype(int).apply(
                lambda x: points[x - 1] if x <= len(points) else 10
            )
        
            # 勝率計算
            race_df['平均勝率'] = race_df['全国勝率'].mean()
            race_df['着順ポイント*平均勝率'] = race_df['平均勝率'] * race_df['着順ポイント']
            race_df['STポイント*平均勝率'] = race_df['平均勝率'] * race_df['STポイント']
            
            # 結果を格納
            processed_dfs.append(race_df.sort_values('艇番'))
    return pd.concat(processed_dfs)

def calculate_player_power(df_results, player_ids):
    """
    各選手ごとに枠ごとのパワー、トータルパワー、STパワーを計算しデータフレームとして返す。
    """
    power_data = {f'{i}枠パワー': [] for i in range(1, 7)}
    total_power, st_power = [], []

    for player_id in player_ids:
        player_df = df_results[df_results['選手登番'] == player_id]
        
        # 枠ごとのパワー計算
        for i in range(1, 7):
            frame_power = player_df[player_df['艇番'] == i]['着順ポイント*平均勝率'].sum() / max(len(player_df[player_df['艇番'] == i]), 1)
            power_data[f'{i}枠パワー'].append(frame_power)
        
        # トータルパワーとSTパワーの計算
        total_power.append(player_df['着順ポイント*平均勝率'].sum() / len(player_df))
        st_power.append(player_df['STポイント*平均勝率'].sum() / len(player_df))
    
    power_data.update({'選手登番': player_ids, 'トータルパワー': total_power, 'STパワー': st_power})
    
    # データフレーム作成
    power_df = pd.DataFrame(power_data)
    
    # カラム順序を指定して"選手登番"を先頭に配置
    column_order = ['選手登番'] + [f'{i}枠パワー' for i in range(1, 7)] + ['トータルパワー', 'STパワー']
    power_df = power_df[column_order]
    
    return power_df


def make_rating():
    # 動的にパスを生成
    csv_result_dir = os.path.join(project_root, 'data', 'data_master', 'result_csv')
    rating_output_path = os.path.join(project_root, 'data', 'data_master', 'rating_csv', 'rating.csv')
    
    # 最新のCSVファイルを読み込み
    df_results = load_recent_csv_files(csv_result_dir)
    
    # IDごとにSTと着順のランクとポイント計算を適用
    df_results = process_individual_results(df_results)
    
    # 選手ごとのパワー計算
    player_ids = df_results['選手登番'].unique()
    player_ids.sort()
    st_da = calculate_player_power(df_results, player_ids)
    
    # 結果の保存
    st_da.to_csv(rating_output_path, index=False)
    print("レーティングの作成が完了しました。")

if __name__ == "__main__":
    make_rating()
