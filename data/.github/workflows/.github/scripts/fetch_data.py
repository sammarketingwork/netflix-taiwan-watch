"""
Netflix Taiwan Watch - Data Fetcher
每週自動抓取 Netflix Top 10 官方 CSV + IMDb 評分，產生 data/data.json
"""

import csv
import json
import os
import urllib.request
import urllib.error
import gzip
import io
import re
from datetime import datetime, timedelta
from collections import defaultdict

# ── 國家對照表 ──────────────────────────────────────────────────────────
COUNTRY_MAP = {
    'TW': ('🇹🇼', '台灣'), 'KR': ('🇰🇷', '韓國'), 'US': ('🇺🇸', '美國'),
    'GB': ('🇬🇧', '英國'), 'JP': ('🇯🇵', '日本'), 'IN': ('🇮🇳', '印度'),
    'ES': ('🇪🇸', '西班牙'), 'MX': ('🇲🇽', '墨西哥'), 'BR': ('🇧🇷', '巴西'),
    'FR': ('🇫🇷', '法國'), 'DE': ('🇩🇪', '德國'), 'IT': ('🇮🇹', '義大利'),
    'TH': ('🇹🇭', '泰國'), 'PH': ('🇵🇭', '菲律賓'), 'ID': ('🇮🇩', '印尼'),
    'AU': ('🇦🇺', '澳洲'), 'CA': ('🇨🇦', '加拿大'), 'TR': ('🇹🇷', '土耳其'),
    'AR': ('🇦🇷', '阿根廷'), 'CO': ('🇨🇴', '哥倫比亞'), 'NG': ('🇳🇬', '奈及利亞'),
    'PL': ('🇵🇱', '波蘭'), 'SE': ('🇸🇪', '瑞典'), 'DK': ('🇩🇰', '丹麥'),
    'NO': ('🇳🇴', '挪威'), 'NL': ('🇳🇱', '荷蘭'), 'BE': ('🇧🇪', '比利時'),
    'PT': ('🇵🇹', '葡萄牙'), 'CL': ('🇨🇱', '智利'), 'ZA': ('🇿🇦', '南非'),
    'HK': ('🇭🇰', '香港'), 'SG': ('🇸🇬', '新加坡'), 'MY': ('🇲🇾', '馬來西亞'),
}

def fetch_url(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    })
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()

# ── Netflix CSV ──────────────────────────────────────────────────────────
def fetch_netflix_csv():
    """抓 Netflix 官方 Top 10 CSV（全球影集、電影）"""
    base = 'https://www.netflix.com/tudum/top10/data'
    urls = {
        'series': f'{base}/all-weeks-global.csv',
        'movie':  f'{base}/all-weeks-global-films.csv',
    }
    results = {}
    for cat, url in urls.items():
        try:
            raw = fetch_url(url)
            text = raw.decode('utf-8-sig')
            reader = csv.DictReader(io.StringIO(text))
            results[cat] = list(reader)
            print(f'✓ Netflix {cat}: {len(results[cat])} rows')
        except Exception as e:
            print(f'✗ Netflix {cat} 失敗: {e}')
            results[cat] = []
    return results

def get_latest_week(rows):
    """取得最新的週次"""
    if not rows:
        return None
    weeks = sorted(set(r.get('week', '') for r in rows), reverse=True)
    return weeks[0] if weeks else None

def filter_by_week(rows, week):
    return [r for r in rows if r.get('week', '') == week]

# ── IMDb Dataset ─────────────────────────────────────────────────────────
def fetch_imdb_ratings():
    """從 IMDb 官方 dataset 抓評分（每日更新，完全免費合法）"""
    url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
    basics_url = 'https://datasets.imdbws.com/title.basics.tsv.gz'
    ratings = {}
    title_map = {}

    try:
        print('下載 IMDb ratings...')
        raw = fetch_url(url)
        with gzip.open(io.BytesIO(raw)) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'), delimiter='\t')
            for row in reader:
                ratings[row['tconst']] = row['averageRating']
        print(f'✓ IMDb ratings: {len(ratings)} 筆')
    except Exception as e:
        print(f'✗ IMDb ratings 失敗: {e}')
        return {}, {}

    try:
        print('下載 IMDb basics...')
        raw = fetch_url(basics_url)
        with gzip.open(io.BytesIO(raw)) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'), delimiter='\t')
            for row in reader:
                if row['titleType'] in ('tvSeries','tvMiniSeries','movie','tvMovie','tvSpecial'):
                    title_clean = row['primaryTitle'].lower().strip()
                    title_map[title_clean] = {
                        'tconst': row['tconst'],
                        'startYear': row['startYear'],
                        'titleType': row['titleType'],
                    }
        print(f'✓ IMDb basics: {len(title_map)} 筆')
    except Exception as e:
        print(f'✗ IMDb basics 失敗: {e}')

    return ratings, title_map

def lookup_imdb(title, ratings, title_map):
    """用片名查 IMDb 評分"""
    # 清理片名（去掉 Season/S2 等後綴）
    clean = re.sub(r'\s*(season\s*\d+|s\d+|part\s*\d+|:\s*season.*)$', '', title, flags=re.IGNORECASE).strip().lower()
    # 直接比對
    if clean in title_map:
        tconst = title_map[clean]['tconst']
        return ratings.get(tconst), title_map[clean].get('startYear')
    # 模糊比對（包含關係）
    for key, info in title_map.items():
        if clean in key or key in clean:
            tconst = info['tconst']
            rating = ratings.get(tconst)
            if rating and rating != '\\N':
                return rating, info.get('startYear')
    return None, None

# ── 主流程 ────────────────────────────────────────────────────────────────
def build_json():
    os.makedirs('data', exist_ok=True)

    # 讀取舊的 data.json（用來計算 streak 和歷史）
    old_data = {}
    if os.path.exists('data/data.json'):
        with open('data/data.json', 'r', encoding='utf-8') as f:
            old_data = json.load(f)

    old_top10_ranks = {item['title']: item for item in old_data.get('top10', [])}
    old_streak = {item['title']: item.get('streak_weeks', 1) for item in old_data.get('top10', [])}

    # 抓 Netflix CSV
    netflix = fetch_netflix_csv()

    # 取最新週次
    latest_series = get_latest_week(netflix.get('series', []))
    latest_movie  = get_latest_week(netflix.get('movie', []))
    latest_week   = latest_series or latest_movie or datetime.now().strftime('%Y-%m-%d')

    series_rows = filter_by_week(netflix.get('series', []), latest_series) if latest_series else []
    movie_rows  = filter_by_week(netflix.get('movie', []), latest_movie)   if latest_movie  else []

    print(f'最新週次 - 影集：{latest_series}，電影：{latest_movie}')
    print(f'影集筆數：{len(series_rows)}，電影筆數：{len(movie_rows)}')

    # 抓 IMDb
    ratings, title_map = fetch_imdb_ratings()

    # 組合 Top 10（影集 + 電影混合，依 weekly_rank 排）
    all_items = []

    def parse_row(row, cat):
        title = row.get('show_title', row.get('film_title', '')).strip()
        rank_str = row.get('weekly_rank', '99')
        try:
            rank = int(rank_str)
        except:
            rank = 99
        hours_str = row.get('weekly_hours_viewed', '0').replace(',', '')
        try:
            hours = round(int(hours_str) / 1_000_000, 1)
        except:
            hours = 0

        country_code = row.get('country_iso2', 'US').strip().upper()
        flag, country_name = COUNTRY_MAP.get(country_code, ('🌐', country_code))

        imdb_rating, start_year = lookup_imdb(title, ratings, title_map)
        if imdb_rating == '\\N':
            imdb_rating = None
        air_date = start_year if start_year and start_year != '\\N' else None

        prev_rank = old_top10_ranks.get(title, {}).get('rank')
        streak = old_streak.get(title, 0) + 1 if title in old_streak else 1

        return {
            'title': title,
            'rank': rank,
            'category': cat,
            'country_code': country_code,
            'country_flag': flag,
            'country': country_name,
            'hours_viewed': hours,
            'imdb_rating': imdb_rating,
            'air_date': air_date,
            'prev_rank': prev_rank,
            'streak_weeks': streak,
            'is_taiwan': country_code == 'TW',
        }

    for row in series_rows[:10]:
        all_items.append(parse_row(row, 'series'))
    for row in movie_rows[:10]:
        all_items.append(parse_row(row, 'movie'))

    # 若 Netflix CSV 完全抓不到，用 placeholder
    if not all_items:
        print('⚠ 無法取得 Netflix 資料，產生空白 JSON')
        all_items = []

    # 排序取 Top 10
    all_items.sort(key=lambda x: x['rank'])
    top10 = all_items[:10]

    # 統計指標
    tw_items = [x for x in top10 if x['is_taiwan']]
    total_hours = round(sum(x['hours_viewed'] for x in top10), 1)
    tw_hours = round(sum(x['hours_viewed'] for x in tw_items), 1)
    tw_pct = round(tw_hours / total_hours * 100, 1) if total_hours > 0 else 0
    tw_avg = round(tw_hours / len(tw_items), 1) if tw_items else 0
    global_avg = round(total_hours / len(top10), 1) if top10 else 0

    tw_series = sum(1 for x in tw_items if x['category']=='series')
    tw_movie  = sum(1 for x in tw_items if x['category']=='movie')
    tw_show   = sum(1 for x in tw_items if x['category']=='show')

    # 歷史紀錄
    history = old_data.get('history', [])
    history.append({
        'week': latest_week,
        'tw_hours': tw_hours,
        'total_hours': total_hours,
    })
    history = history[-8:]  # 最多保留 8 週

    # 類型分析
    series_hours = sum(x['hours_viewed'] for x in top10 if x['category']=='series')
    movie_hours  = sum(x['hours_viewed'] for x in top10 if x['category']=='movie')
    show_hours   = sum(x['hours_viewed'] for x in top10 if x['category']=='show')
    total_bd = series_hours + movie_hours + show_hours or 1

    country_counts = defaultdict(float)
    for x in top10:
        country_counts[(x['country_flag'], x['country'])] += x['hours_viewed']
    sorted_countries = sorted(country_counts.items(), key=lambda x: -x[1])
    total_ch = sum(v for _,v in sorted_countries) or 1
    countries_bd = [
        {'flag': k[0], 'name': k[1], 'pct': round(v/total_ch*100, 1)}
        for k,(k,v) in [(k,(k,v)) for k,v in sorted_countries[:5]]
    ]
    # 修正 countries_bd 寫法
    countries_bd = []
    for (flag, name), hrs in sorted_countries[:5]:
        countries_bd.append({'flag': flag, 'name': name, 'pct': round(hrs/total_ch*100, 1)})

    # by_category（台灣區分類榜）
    by_category = {
        'series': [x for x in all_items if x['category']=='series'][:10],
        'movie':  [x for x in all_items if x['category']=='movie'][:10],
        'show':   [x for x in all_items if x['category']=='show'][:10],
    }
    for cat_list in by_category.values():
        for i, item in enumerate(cat_list):
            item['rank'] = i + 1

    output = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'current_week': {
            'week': latest_week,
            'week_label': latest_week,
            'tw_count': len(tw_items),
            'tw_series': tw_series,
            'tw_movie': tw_movie,
            'tw_show': tw_show,
            'tw_pct': tw_pct,
            'tw_avg': tw_avg,
            'global_avg': global_avg,
            'total_hours': total_hours,
        },
        'prev_week': old_data.get('current_week'),
        'top10': top10,
        'by_category': by_category,
        'history': history,
        'breakdown': {
            'series_pct': round(series_hours/total_bd*100, 1),
            'movie_pct':  round(movie_hours/total_bd*100, 1),
            'show_pct':   round(show_hours/total_bd*100, 1),
            'countries':  countries_bd,
        }
    }

    with open('data/data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f'\n✅ data/data.json 已更新')
    print(f'   週次：{latest_week}')
    print(f'   Top 10 共 {len(top10)} 筆，台灣作品 {len(tw_items)} 筆')

if __name__ == '__main__':
    build_json()
