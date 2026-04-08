"""
Netflix Taiwan Watch - Data Fetcher (修正版)
正確格式：Netflix 使用 TSV，欄位與原本不同
"""

import csv
import json
import os
import urllib.request
import gzip
import io
import re
from datetime import datetime
from collections import defaultdict

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
    'TW': ('🇹🇼', '台灣'),
}

# 依 Netflix category 字串判斷類型和語言
# category 例子: "TV (English)", "TV (Non-English)", "Films (English)", "Films (Non-English)"
def parse_category(cat_str):
    cat_str = cat_str.strip()
    if cat_str.startswith('TV'):
        return 'series'
    elif cat_str.startswith('Films'):
        return 'movie'
    else:
        return 'show'

def is_non_english(cat_str):
    return 'Non-English' in cat_str

def fetch_url(url):
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    })
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read()
        # 處理 gzip 回應
        if r.info().get('Content-Encoding') == 'gzip':
            raw = gzip.decompress(raw)
        return raw

def fetch_netflix_tsv():
    """
    Netflix 官方 TSV 欄位（以 tab 分隔）：
    show_title | season_title | cumulative_weeks_in_top_10 | runtime | weekly_hours_viewed | week | category | rank
    """
    urls = {
        'global_tv':    'https://www.netflix.com/tudum/top10/data/all-weeks-global.tsv',
        'global_films': 'https://www.netflix.com/tudum/top10/data/all-weeks-global-films.tsv',
    }
    results = {}
    for key, url in urls.items():
        try:
            raw = fetch_url(url)
            text = raw.decode('utf-8-sig')
            reader = csv.DictReader(io.StringIO(text), delimiter='\t')
            rows = list(reader)
            results[key] = rows
            if rows:
                print(f'✓ {key}: {len(rows)} rows, 欄位: {list(rows[0].keys())}')
            else:
                print(f'⚠ {key}: 0 rows')
        except Exception as e:
            print(f'✗ {key} 失敗: {e}')
            results[key] = []
    return results

def get_latest_week(rows):
    weeks = sorted(set(r.get('week', '') for r in rows if r.get('week')), reverse=True)
    return weeks[0] if weeks else None

def filter_by_week(rows, week):
    return [r for r in rows if r.get('week', '') == week]

# ── IMDb ─────────────────────────────────────────────────────────────────
def fetch_imdb_ratings():
    ratings = {}
    title_map = {}
    try:
        print('下載 IMDb ratings...')
        raw = fetch_url('https://datasets.imdbws.com/title.ratings.tsv.gz')
        with gzip.open(io.BytesIO(raw)) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'), delimiter='\t')
            for row in reader:
                if row['averageRating'] != '\\N':
                    ratings[row['tconst']] = row['averageRating']
        print(f'✓ IMDb ratings: {len(ratings)} 筆')
    except Exception as e:
        print(f'✗ IMDb ratings 失敗: {e}')
        return {}, {}

    try:
        print('下載 IMDb basics...')
        raw = fetch_url('https://datasets.imdbws.com/title.basics.tsv.gz')
        with gzip.open(io.BytesIO(raw)) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'), delimiter='\t')
            for row in reader:
                if row['titleType'] in ('tvSeries','tvMiniSeries','movie','tvMovie','tvSpecial','tvShort'):
                    key = row['primaryTitle'].lower().strip()
                    # 保留評分較多的（通常 tconst 號碼較小 = 較早 = 較知名）
                    if key not in title_map or row['tconst'] < title_map[key]['tconst']:
                        title_map[key] = {
                            'tconst': row['tconst'],
                            'startYear': row.get('startYear', ''),
                        }
        print(f'✓ IMDb basics: {len(title_map)} 筆')
    except Exception as e:
        print(f'✗ IMDb basics 失敗: {e}')

    return ratings, title_map

def lookup_imdb(title, ratings, title_map):
    # 去掉 season/part 後綴
    clean = re.sub(r'\s*(:\s*)?(season|part|chapter|vol\.?)\s*\d+.*$', '', title, flags=re.IGNORECASE)
    clean = clean.lower().strip()

    # 直接比對
    if clean in title_map:
        tconst = title_map[clean]['tconst']
        rating = ratings.get(tconst)
        year = title_map[clean].get('startYear', '')
        if year == '\\N': year = ''
        return rating, year

    # 模糊：查是否包含
    for key, info in title_map.items():
        if len(clean) > 4 and (clean in key or key in clean):
            tconst = info['tconst']
            rating = ratings.get(tconst)
            if rating:
                year = info.get('startYear', '')
                if year == '\\N': year = ''
                return rating, year

    return None, None

# ── 主流程 ────────────────────────────────────────────────────────────────
def build_json():
    os.makedirs('data', exist_ok=True)

    # 讀舊資料
    old_data = {}
    if os.path.exists('data/data.json'):
        try:
            with open('data/data.json', 'r', encoding='utf-8') as f:
                old_data = json.load(f)
        except:
            pass

    old_top10 = {item['title']: item for item in old_data.get('top10', [])}
    old_streak = {item['title']: item.get('streak_weeks', 1) for item in old_data.get('top10', [])}

    # 抓 Netflix TSV
    netflix = fetch_netflix_tsv()
    all_rows = netflix.get('global_tv', []) + netflix.get('global_films', [])

    latest_week = get_latest_week(all_rows)
    print(f'最新週次：{latest_week}')

    if not latest_week:
        print('⚠ 無法取得最新週次，可能是 Netflix 格式變更')
        # 把現有 data.json 加上錯誤訊息後儲存
        old_data['fetch_error'] = '無法取得 Netflix 資料，請檢查 TSV URL 是否有效'
        with open('data/data.json', 'w', encoding='utf-8') as f:
            json.dump(old_data, f, ensure_ascii=False, indent=2)
        return

    week_rows = filter_by_week(all_rows, latest_week)
    print(f'本週資料：{len(week_rows)} 筆')

    # 印出前幾筆確認欄位
    if week_rows:
        print('範例資料：', dict(list(week_rows[0].items())[:5]))

    # 抓 IMDb
    ratings, title_map = fetch_imdb_ratings()

    # 解析每筆資料
    items = []
    for row in week_rows:
        # Netflix TSV 欄位名稱
        title = row.get('show_title', '').strip()
        if not title:
            continue

        try:
            rank = int(row.get('rank', 99))
        except:
            rank = 99

        hours_str = row.get('weekly_hours_viewed', '0').replace(',', '').strip()
        try:
            hours = round(int(hours_str) / 1_000_000, 1)
        except:
            hours = 0

        cat_str = row.get('category', 'TV (English)')
        cat = parse_category(cat_str)
        non_eng = is_non_english(cat_str)

        # Netflix TSV 沒有直接的國家欄位
        # 用語言和片名推測
        country_code = 'US'  # 預設
        flag, country_name = COUNTRY_MAP.get(country_code, ('🌐', country_code))

        imdb_rating, start_year = lookup_imdb(title, ratings, title_map)

        prev_rank = old_top10.get(title, {}).get('rank')
        streak = old_streak.get(title, 0) + 1 if title in old_streak else 1

        items.append({
            'title': title,
            'rank': rank,
            'category': cat,
            'is_non_english': non_eng,
            'country_code': country_code,
            'country_flag': flag,
            'country': country_name,
            'hours_viewed': hours,
            'imdb_rating': imdb_rating,
            'air_date': start_year if start_year else None,
            'prev_rank': prev_rank,
            'streak_weeks': streak,
            'is_taiwan': False,  # Netflix 全球榜不含國籍，需另外處理
        })

    # 依 rank 排序，取各類型前 10
    items.sort(key=lambda x: x['rank'])

    # 分類
    tv_items    = [x for x in items if x['category'] == 'series'][:10]
    film_items  = [x for x in items if x['category'] == 'movie'][:10]

    # 全部合併做 top10（TV English + TV Non-English + Films 各類前10取前10）
    # 這裡直接取 weekly_hours_viewed 最高的 10 筆
    all_sorted = sorted(items, key=lambda x: -(x['hours_viewed'] or 0))
    top10 = all_sorted[:10]
    for i, item in enumerate(top10):
        item['rank'] = i + 1

    # 統計
    total_hours = round(sum(x['hours_viewed'] for x in top10), 1)
    non_eng_items = [x for x in top10 if x['is_non_english']]
    ne_hours = round(sum(x['hours_viewed'] for x in non_eng_items), 1)
    ne_pct = round(ne_hours / total_hours * 100, 1) if total_hours else 0
    ne_avg = round(ne_hours / len(non_eng_items), 1) if non_eng_items else 0
    global_avg = round(total_hours / len(top10), 1) if top10 else 0

    # 歷史
    history = old_data.get('history', [])
    # 避免重複加入同一週
    if not history or history[-1].get('week') != latest_week:
        history.append({
            'week': latest_week,
            'tw_hours': ne_hours,
            'total_hours': total_hours,
        })
    history = history[-8:]

    # by_category
    for i, item in enumerate(tv_items):
        item['rank'] = i + 1
    for i, item in enumerate(film_items):
        item['rank'] = i + 1

    # 類型佔比（以 top10 計）
    s_hrs = sum(x['hours_viewed'] for x in top10 if x['category']=='series')
    m_hrs = sum(x['hours_viewed'] for x in top10 if x['category']=='movie')
    total_bd = s_hrs + m_hrs or 1

    # 語言佔比（非英語 = 最接近台灣/韓國/日本等）
    eng_hrs = sum(x['hours_viewed'] for x in top10 if not x['is_non_english'])
    non_hrs = sum(x['hours_viewed'] for x in top10 if x['is_non_english'])
    countries_bd = [
        {'flag':'🌏', 'name':'非英語作品', 'pct': round(non_hrs/total_bd*100,1)},
        {'flag':'🇺🇸', 'name':'英語作品',   'pct': round(eng_hrs/total_bd*100,1)},
    ]

    output = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'note': 'Netflix 全球榜不含個別國籍資訊，non_english = 非英語作品（含台韓日等）',
        'current_week': {
            'week': latest_week,
            'week_label': latest_week,
            'tw_count': len(non_eng_items),
            'tw_series': sum(1 for x in non_eng_items if x['category']=='series'),
            'tw_movie':  sum(1 for x in non_eng_items if x['category']=='movie'),
            'tw_show':   0,
            'tw_pct': ne_pct,
            'tw_avg': ne_avg,
            'global_avg': global_avg,
            'total_hours': total_hours,
        },
        'prev_week': old_data.get('current_week'),
        'top10': top10,
        'by_category': {
            'series': tv_items,
            'movie':  film_items,
            'show':   [],
        },
        'history': history,
        'breakdown': {
            'series_pct': round(s_hrs/total_bd*100, 1),
            'movie_pct':  round(m_hrs/total_bd*100, 1),
            'show_pct':   0,
            'countries':  countries_bd,
        }
    }

    with open('data/data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f'\n✅ data/data.json 更新完成')
    print(f'   週次：{latest_week}')
    print(f'   Top 10 共 {len(top10)} 筆')
    print(f'   非英語作品 {len(non_eng_items)} 筆，時數 {ne_hrs}M hrs')

if __name__ == '__main__':
    build_json()
