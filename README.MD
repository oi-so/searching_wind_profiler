## ウィンドプロファイラ　グラフ作成プログラム

## セットアップ

ターミナル　(bash)で下記を実行

`pip install -r requirements.txt`

## 使い方

ターミナル　(bash)で下記を実行

`python generate_winprof.py`

実行後、YEAR, MONTH, DAY に取得したいグラフの年月日を入力。
(入力は半角で。ゼロ埋めはしなくてもいいです。)

作成終了後、グラフが自動的に表示されます。
グラフの画像は `final_work_dir/win_prof_figs/{year}_{month}_{day}_winprof.png`に保存されています。

