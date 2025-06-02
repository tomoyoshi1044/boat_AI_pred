import os
import sys
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from joblib import dump


# プロジェクトルートを取得
project_root = os.path.dirname(os.path.dirname(__file__))

data_processing_dir = os.path.join(project_root, 'utils')
sys.path.append(data_processing_dir)
from one_race import make_onerace


def train_models(axis1_data):
    # データフレームのコピー
    df = axis1_data.copy()

    # ターゲットカラムのリスト
    t_list = [f'1着{i}号艇' for i in range(1, 7)]

    # 使用する特徴量のリスト
    feature_columns = [
        f'{col}{i}号艇' for i in range(1, 7)
        for col in ['全国勝率', f'{i}枠パワー', 'トータルパワー', 'STパワー']
    ]

    # 特徴量を数値型に変換（エラーを強制的にNaNに変換）
    for col in feature_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 共通パラメータ設定
    params = {
        'objective': 'binary:logistic',   # 2クラス分類の場合
        'eval_metric': 'logloss',         # 損失関数
        'learning_rate': 0.03,            # 学習率（徐々に収束）
        'max_depth': 6,                   # 木の深さ（適度な複雑さ）
        'min_child_weight': 7,            # 葉ノードの最小サンプル数（過学習防止）
        'lambda': 1.0,                    # L2正則化項（過学習抑制）
        'alpha': 0.5,                     # L1正則化項（過学習抑制）
        'n_estimators': 1000,             # 最大ラウンド数（早期停止で制御）
        'verbosity': 0                    # ログ出力を抑制
    }

    # モデルを保存するディレクトリ
    model_dir = os.path.join(project_root, 'models_clf')
    os.makedirs(model_dir, exist_ok=True)

    # 各ターゲットに対してモデルを作成
    for target in t_list:
        # 予測ターゲットと特徴量の設定
        X = df[feature_columns]
        y = df[target].astype(int)

        # データをtrain, eval, testに分割
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0)
        X_train, X_eval, y_train, y_eval = train_test_split(X_train, y_train, test_size=0.1, random_state=1, stratify=y_train)

        # XGBoost用DMatrixに変換（カテゴリカルデータ対応）
        dtrain = xgb.DMatrix(X_train, label=y_train, enable_categorical=False)
        deval = xgb.DMatrix(X_eval, label=y_eval, enable_categorical=False)

        # モデルの学習
        evals = [(dtrain, 'train'), (deval, 'eval')]
        model = xgb.train(params, dtrain, num_boost_round=1000, evals=evals, early_stopping_rounds=20, verbose_eval=False)

        # モデルの保存（joblib）
        model_path = os.path.join(model_dir, f'model_{target}.joblib')
        dump(model, model_path)
        print(f"モデルを保存しました: {model_path}")


def train_models3(axis1_data):
    # データフレームのコピー
    df = axis1_data.copy()

    # ターゲットカラムのリスト
    t_list = [f'3連対{i}号艇' for i in range(1, 7)]

    # 使用する特徴量のリスト
    feature_columns = [
        f'{col}{i}号艇' for i in range(1, 7)
        for col in ['全国勝率', f'{i}枠パワー', 'トータルパワー', 'STパワー']
    ]

    # 特徴量を数値型に変換（エラーを強制的にNaNに変換）
    for col in feature_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 共通パラメータ設定
    params = {
        'objective': 'binary:logistic',   # 2クラス分類の場合
        'eval_metric': 'logloss',         # 損失関数
        'learning_rate': 0.03,            # 学習率（徐々に収束）
        'max_depth': 6,                   # 木の深さ（適度な複雑さ）
        'min_child_weight': 7,            # 葉ノードの最小サンプル数（過学習防止）
        'lambda': 1.0,                    # L2正則化項（過学習抑制）
        'alpha': 0.5,                     # L1正則化項（過学習抑制）
        'n_estimators': 1000,             # 最大ラウンド数（早期停止で制御）
        'verbosity': 0                    # ログ出力を抑制
    }

    # モデルを保存するディレクトリ
    model_dir = os.path.join(project_root, 'models_clf')
    os.makedirs(model_dir, exist_ok=True)

    # 各ターゲットに対してモデルを作成
    for target in t_list:
        # 予測ターゲットと特徴量の設定
        X = df[feature_columns]
        y = df[target].astype(int)

        # データをtrain, eval, testに分割
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0)
        X_train, X_eval, y_train, y_eval = train_test_split(X_train, y_train, test_size=0.1, random_state=1, stratify=y_train)

        # XGBoost用DMatrixに変換（カテゴリカルデータ対応）
        dtrain = xgb.DMatrix(X_train, label=y_train, enable_categorical=False)
        deval = xgb.DMatrix(X_eval, label=y_eval, enable_categorical=False)

        # モデルの学習
        evals = [(dtrain, 'train'), (deval, 'eval')]
        model = xgb.train(params, dtrain, num_boost_round=1000, evals=evals, early_stopping_rounds=20, verbose_eval=False)

        # モデルの保存（joblib）
        model_path = os.path.join(model_dir, f'model_{target}.joblib')
        dump(model, model_path)
        print(f"モデルを保存しました: {model_path}")


if __name__ == "__main__":
    axis1_data = make_onerace()
    train_models(axis1_data)
    train_models3(axis1_data)
