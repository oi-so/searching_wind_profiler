#!/usr/bin/env python3

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import copy
from convert_bufr_to_json import main as convert_BUFR
import requests
from bs4 import BeautifulSoup
import os
from glob import glob
import tarfile
import json
from datetime import datetime, timedelta
import shutil

print("[ ウィンドプロファイラ グラフ作成プログラム ]")
print("作成したい年月日を入力してください。\n")

year_to_import = int(input("YEAR:"))
month_to_import = int(input("MONTH:"))
day_to_import = int(input("DAY:"))
print("\n")

def download_and_convert_bufr():
    print("京都大学データベースからBUFRファイルをダウンロードしています...")
    kyotoac_database_base_url = f"https://database.rish.kyoto-u.ac.jp/arch/jmadata/data/jma-radar/wprof/original/{year_to_import}/{month_to_import:02}/{day_to_import:02}/"
    res_from_database = requests.get(kyotoac_database_base_url)
    if res_from_database.status_code != 200:
        raise RuntimeError(f"指定日のデータベースにアクセスできません: {kyotoac_database_base_url}")
    res_html = res_from_database.text
    html_parser = BeautifulSoup(res_html, "html.parser")
    links = html_parser.select("a")
    for i in links:
        if "IUPC00" in i.get("href", "") and i.get("href", "").endswith("tar.gz"):
            file_url = kyotoac_database_base_url + i.get("href", "")
            print(f"Downloading {file_url} ...")
            r = requests.get(file_url)
            if r.status_code != 200:
                print(f"⚠ ダウンロードに失敗しました: {file_url}")
                continue
            local_path = "final_work_dir/bufr_tar_files/"
            with open(local_path+i.get("href", ""), "wb") as f:
                f.write(r.content)
            print(f"Saved to {local_path+i.get('href', '')}")

    bufr_tar_files = glob("final_work_dir/bufr_tar_files/*.tar.gz")
    send_dir = f"final_work_dir/bufr_send_files/{year_to_import}_{month_to_import:02}_{day_to_import:02}_winprof/"
    os.makedirs(send_dir, exist_ok=True)
    print("ダウンロードは正常に完了しました。")
    print("圧縮BUFRファイルを展開しています...")

    for i in bufr_tar_files:
        print(f"Extracting {i} ...")
        with tarfile.open(i, "r:gz") as tar:
            IUPC44_tar_name = list(filter(lambda x: "IUPC44" in x, tar.getnames()))[0]
            tar.extract(IUPC44_tar_name, path=f"final_work_dir/bufr_send_files/{year_to_import}_{month_to_import:02}_{day_to_import:02}_winprof/")

    tar_dir = os.path.join("final_work_dir", "bufr_tar_files")
    if os.path.exists(tar_dir):
        shutil.rmtree(tar_dir)
    os.makedirs(tar_dir, exist_ok=True)
    print("展開は正常に完了しました。")


    print("BUFRファイルを解析してJSON形式に変換しています...")

    for i in glob(f"final_work_dir/bufr_send_files/{year_to_import}_{month_to_import:02}_{day_to_import:02}_winprof/IUPC44*"):
        print(f"Converting {i} ...")
        convert_BUFR(i, year_to_import, month_to_import, day_to_import)

    print("変換は正常に完了しました。")

if not os.path.exists(f"final_work_dir/converted_jsons/{year_to_import}_{month_to_import:02}_{day_to_import:02}_winprof/"):
    print("指定日のデータはダウンロードされていません。ダウンロードと解析を実行します...")
    download_and_convert_bufr()
else:
    print("指定日のデータは既にダウンロード・解析済みです。")

print("グラフ作成を開始します。")
# グラフ作成のために使われるソースのデータ
nested_data = []

paths_to_data_jsons = glob(f"final_work_dir/converted_jsons/{year_to_import}_{month_to_import:02}_{day_to_import:02}_winprof/*.json")
for i in paths_to_data_jsons:
    with open(i) as f:
        json_data = json.load(f)
        for j in json_data:
            nested_data.append(j)



nested_data = sorted(nested_data, key=lambda x : datetime(x[0][0], x[0][1], x[0][2], x[0][3], x[0][4]))

prev_datetime = datetime(year_to_import, month_to_import, day_to_import) + timedelta(days=-1)

times = pd.date_range(f"{prev_datetime.year}-{prev_datetime.month:02}-{prev_datetime.day:02} 23:10", f"{year_to_import}-{month_to_import:02}-{day_to_import:02} 23:00", freq="10min")
def get_time(rec):
    header = rec[0]
    try:
        return pd.Timestamp(year=int(header[0]), month=int(header[1]), day=int(header[2]),
                            hour=int(header[3]), minute=int(header[4]))
    except:
        return None

# --- 4. 各時刻にデータを割り当て、存在しない時刻に空データを入れる ---
new_nested_data = []
for t in times:
    # 既存データから該当時刻のデータを探す
    found = False
    for rec in nested_data:
        if get_time(rec) == t:
            new_nested_data.append(rec)
            found = True
            break
    if not found:
        # 空データ（例: ヘッダーのみ、標準的な値でOK）
        empty_header = [t.year, t.month, t.day, t.hour, t.minute, 0, 0, 0]
        # 空のデータリスト（高度などは未入力、または欠損値）
        new_nested_data.append([empty_header])


nested_data = list(new_nested_data)

output_filename = f"{year_to_import}_{month_to_import:02}_{day_to_import:02}_winprof.png"


sta_name = "Kawaguchiko (N=35.5, E=138.76)"
ymin, ymax = 0, 8000
MISSING_MARKERS = (114514, 999999)
INVALID_QUALITY = (255,)

def parse_nested_list(nested, missing_markers=MISSING_MARKERS, invalid_quality=INVALID_QUALITY):
    times = []
    records = []
    heights_set = set()
    for rec in nested:
        if not rec: continue
        header = rec[0]
        try:
            year, month, day, hour, minute = header[:5]
            dt = pd.Timestamp(year=int(year), month=int(month), day=int(day), hour=int(hour), minute=int(minute))
        except:
            dt = pd.Timestamp.now()
        times.append(dt)
        rows = []
        for entry in rec[1:]:
            if len(entry) < 6: continue
            try:
                h = float(entry[0])
            except:
                continue
            q, u_val, v_val, w_val, wd_val = entry[1:6]
            heights_set.add(h)
            rows.append((h, q, u_val, v_val, w_val, wd_val))
        records.append(rows)
    heights = sorted(heights_set)
    idx = pd.Index(times, name='time')
    u_df = pd.DataFrame(index=idx, columns=heights, dtype=float)
    v_df = pd.DataFrame(index=idx, columns=heights, dtype=float)
    w_df = pd.DataFrame(index=idx, columns=heights, dtype=float)
    qua_df = pd.DataFrame(index=idx, columns=heights, dtype=float)
    u_df.loc[:, :] = np.nan
    v_df.loc[:, :] = np.nan
    w_df.loc[:, :] = np.nan
    qua_df.loc[:, :] = np.nan
    for i, rows in enumerate(records):
        t = idx[i]
        for (h, q, u_val, v_val, w_val, wd_val) in rows:
            if (u_val in missing_markers) or (v_val in missing_markers) or (w_val in missing_markers) or (q in invalid_quality):
                qua_df.at[t, h] = np.nan
                continue
            try:
                u_f, v_f, w_f = float(u_val), float(v_val), float(w_val)
            except:
                qua_df.at[t, h] = np.nan
                continue
            u_df.at[t, h] = u_f
            v_df.at[t, h] = v_f
            w_df.at[t, h] = w_f
            qua_df.at[t, h] = q
    u_df = u_df.reindex(sorted(u_df.columns), axis=1)
    v_df = v_df.reindex(sorted(v_df.columns), axis=1)
    w_df = w_df.reindex(sorted(w_df.columns), axis=1)
    qua_df = qua_df.reindex(sorted(qua_df.columns), axis=1)
    return u_df, v_df, w_df, qua_df

# --- データ変換 ---
if nested_data is None:
    raise RuntimeError("nested_data にデータを代入してください")
u_df, v_df, w_df, qua_df = parse_nested_list(nested_data)

# --- 品質マスク ---
mask_quality = (~qua_df.isna()) & (~qua_df.isin(INVALID_QUALITY))
mask = mask_quality & u_df.notna() & v_df.notna() & w_df.notna()

u_clean = u_df.where(mask)
v_clean = v_df.where(mask)
w_clean  = w_df.where(mask)

# --- 配列化 ---
time_index = w_clean.index
height = np.array(w_clean.columns, dtype=float)
n_time = len(time_index)
n_height = len(height)

w_arr = np.array(w_clean).T  # shape (height, time)
u_arr = np.array(u_clean).T
v_arr = np.array(v_clean).T

def signed_log(x):
    # 2で除算
    return np.where(~np.isnan(x), x / 10, np.nan)

w_arr = signed_log(w_arr)
u_arr = signed_log(u_arr)
v_arr = signed_log(v_arr)
# --- メッシュ ---
X, Y = np.meshgrid(np.arange(n_time), height)

# --- 矢印間引き ---
step_t = max(1, n_time // 60)
step_h = max(1, n_height // 30)

fig, ax = plt.subplots(figsize=(80,5))

# --- scatterで鉛直速度を色分け ---
cmap = copy.copy(mpl.cm.get_cmap("coolwarm"))
cmap.set_over('r')
cmap.set_under('b')
vmin, vmax = -4, 4

for i in range(w_arr.shape[0]):
    for j in range(w_arr.shape[1]):
        if not np.isnan(w_arr[i, j]):
            ax.scatter(j, height[i], c=w_arr[i, j], cmap=cmap, vmin=vmin, vmax=vmax, s=100, marker='s')

# --- カラーバー ---
sc = mpl.cm.ScalarMappable(cmap=cmap)
sc.set_array(w_arr)
sc.set_clim(vmin, vmax)
cbar = plt.colorbar(sc, ax=ax, label="Vertical velocity (m/s)")

# --- 水平風矢印 ---
scale_factor = 1
u_plot = u_arr * scale_factor
v_plot = v_arr * scale_factor

# 矢印の出す場所だけmask
arrow_mask = (~np.isnan(u_plot)) & (~np.isnan(v_plot)) & (~np.isnan(w_arr))
for i in range(0, u_plot.shape[0], step_h):
    for j in range(0, u_plot.shape[1], step_t):
        if arrow_mask[i, j]:
            ax.quiver(j, height[i], u_plot[i, j], v_plot[i, j],
                      color='k', angles='uv', scale_units='xy', scale=1, width=0.00035, headwidth=5, headlength=6)

# --- 時間軸ラベル ---
ax.set_xlabel("Time")
ax.set_ylabel("Height (m)")
ax.set_ylim([ymin, ymax])
ax.set_title(f"{sta_name} Wind Profile", fontsize=18)
ax.set_xticks(np.arange(n_time))
ax.set_xticklabels([t.strftime('%H:%M') for t in time_index], rotation=45)


plt.subplots_adjust(hspace=0.8, bottom=0.2)

print("グラフの作成が完了しました。")
plt.savefig("final_work_dir/win_prof_figs/"+output_filename, dpi=300, bbox_inches='tight')
plt.show()