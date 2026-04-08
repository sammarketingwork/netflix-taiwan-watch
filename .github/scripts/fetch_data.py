"""
Netflix Taiwan Watch - Data Fetcher (精簡版)
跳過 IMDb 大型 dataset，先讓 Netflix 數據正常運作
IMDb 評分改用 OMDb API（免費，每天 1000 次）
"""

import csv
import json
import os
import urllib.request
import urllib.parse
import gzip
import io
import re
from datetime import datetime
from collections import defaultdict

# ── 設定 ─────────────────────────────────────────────────────────────────
# OMDb API key（免費申請：https://www.omdbapi.com/apikey.aspx）
# 留空則跳過 IMDb 評分
OMDB_API_KEY = os.environ.get('OMDB_API_KEY', '')

COUNTRY_MAP = {
    'US': ('🇺🇸', '美國'), 'KR': ('🇰🇷', '韓國'), 'TW': ('🇹🇼', '台灣'),
    'GB': ('🇬🇧', '英國'), 'JP': ('🇯🇵', '日本'), 'IN': ('🇮🇳', '印度'),
    'ES': ('🇪🇸', '西班牙'), 'MX': ('🇲🇽', '墨西哥'), 'BR': ('🇧🇷', '巴西'),
    'FR': ('🇫🇷', '法國'), 'DE': ('🇩🇪', '德國'), 'IT': ('🇮🇹', '義大利'),
    'TH': ('🇹🇭', '泰國'), 'ID': ('🇮🇩', '印尼'), 'AU': ('🇦🇺', '澳洲'),
    'CA': ('🇨🇦', '加拿大'), 'TR': ('🇹🇷', '土耳其'), 'PL': ('🇵🇱', '波蘭'),
    'SE': ('🇸🇪', '瑞典'), 'NO': ('🇳🇴', '挪威'), 'DK': ('🇩🇰', '丹麥'),
    'NL': ('🇳🇱', '荷蘭'), 'ZA': ('🇿🇦', '南非'), 'NG': ('🇳🇬', '奈及利亞'),
    'HK': ('🇭🇰', '香港'), 'SG': ('🇸🇬', '新加坡'), 'MY': ('🇲🇾', '馬來西亞'),
    'AR': ('🇦🇷', '阿根廷'), 'CO': ('🇨🇴', '哥倫比亞'), 'CL': ('🇨🇱', '智利'),
    'PT': ('🇵🇹', '葡萄牙'), 'BE': ('🇧🇪', '比利時'),
}

def fetch_url(url, timeout=30):
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        enc = r.headers.get('Content-Encoding', '')
        if enc == 'gzip':
            raw = gzip.decompress(raw)
        return raw

def parse_category(cat_str):
    cat_str = cat_str.strip()
    if 'TV' in cat_str:
        return 'series'
    elif 'Film' in cat_str or 'Movie' in cat_str:
        return 'movie'
    return 'show'

def is_non_english(cat_str):
    return 'Non-English' in cat_str

# ── Netflix TSV ───────────────────────────────────────────────────────────
def fetch_netflix_data():
    """
    Netflix Top 10 TSV 格式（Tab 分隔）
    欄位：show_title  season_title  cumulative_weeks_in_top_10  runtime  weekly_hours_viewed  week  category  rank
    """
    urls = {
        'tv':    'https://www.netflix.com/tudum/top10/data/all-weeks-global.tsv',
        'films': 'https://www.netflix.com/tudum/top10/data/all-weeks-global-films.tsv',
    }
    all_rows = []
    for key, url in urls.items():
        try:
            print(f'抓取 {key}: {url}')
            raw = fetch_url(url, timeout=30)
            text = raw.decode('utf-8-sig')
            reader = csv.DictReader(io.StringIO(text), delimiter='\t')
            rows = list(reader)
            print(f'  ✓ {len(rows)} rows')
            if rows:
                print(f'  欄位: {list(rows[0].keys())}')
            all_rows.extend(rows)
        except Exception as e:
            print(f'  ✗ 失敗: {e}')
    return all_rows

def get_latest_week(rows):
    weeks = sorted(
        set(r.get('week', '').strip() for r in rows if r.get('week', '').strip()),
        reverse=True
    )
    print(f'可用週次（最新5個）: {weeks[:5]}')
    return weeks[0] if weeks else None

# ── OMDb API（輕量 IMDb 評分）────────────────────────────────────────────
def get_omdb_rating(title, api_key, cache):
    if not api_key:
        return None, None
    if title in cache:
        return cache[title]
    try:
        clean = re.sub(r'\s*(:\s*)?(season|part|s\d+)\s*\d*.*$', '', title, flags=re.IGNORECASE).strip()
        encoded = urllib.parse.quote(clean)
        url = f'https://www.omdbapi.com/?t={encoded}&apikey={api_key}'
        raw = fetch_url(url, timeout=10)
        data = json.loads(raw)
        rating = data.get('imdbRating') if data.get('Response') == 'True' else None
        year = data.get('Year', '').split('–')[0] if data.get('Response') == 'True' else None
        if rating == 'N/A': rating = None
        if year == 'N/A': year = None
        cache[title] = (rating, year)
        return rating, year
    except Exception as e:
        print(f'  OMDb [{title}]: {e}')
        cache[title] = (None, None)
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
            print('✓ 讀取舊 data.json 成功')
        except Exception as e:
            print(f'⚠ 讀取舊 data.json 失敗: {e}')

    old_top10 = {item['title']: item for item in old_data.get('top10', [])}
    old_streak = {item['title']: item.get('streak_weeks', 1) for item in old_data.get('top10', [])}

    # 抓 Netflix
    all_rows = fetch_netflix_data()
    if not all_rows:
        print('✗ 完全沒有抓到資料，終止')
        raise SystemExit(1)

    latest_week = get_latest_week(all_rows)
    if not latest_week:
        print('✗ 無法取得最新週次，終止')
        raise SystemExit(1)

    week_rows = [r for r in all_rows if r.get('week', '').strip() == latest_week]
    print(f'本週 ({latest_week}) 資料: {len(week_rows)} 筆')
    if week_rows:
        print(f'範例: {dict(list(week_rows[0].items()))}')

    # OMDb cache（讀舊的省 API 次數）
    omdb_cache = {}
    if old_data.get('omdb_cache'):
        omdb_cache = old_data['omdb_cache']

    # 解析
    items = []
    for row in week_rows:
        title = row.get('show_title', '').strip()
        if not title:
            continue

        try:
            rank = int(str(row.get('rank', '99')).strip())
        except:
            rank = 99

        hours_str = str(row.get('weekly_hours_viewed', '0')).replace(',', '').strip()
        try:
            hours = round(int(hours_str) / 1_000_000, 1)
        except:
            hours = 0.0

        cat_str = row.get('category', 'TV (English)')
        cat = parse_category(cat_str)
        non_eng = is_non_english(cat_str)

        # IMDb
        imdb_rating, air_year = get_omdb_rating(title, OMDB_API_KEY, omdb_cache)

        # streak
        prev_rank = old_top10.get(title, {}).get('rank')
        streak = old_streak.get(title, 0) + 1 if title in old_streak else 1

        # 語系對應國旗
        if non_eng:
            flag, country = '🌏', '非英語'
        else:
            flag, country = '🇺🇸', '英語'

        items.append({
            'title': title,
            'rank': rank,
            'category': cat,
            'is_non_english': non_eng,
            'country_flag': flag,
            'country': country,
            'hours_viewed': hours,
            'imdb_rating': imdb_rating,
            'air_date': air_year,
            'prev_rank': prev_rank,
            'streak_weeks': streak,
        })

    if not items:
        print('✗ 解析後 0 筆，終止')
        raise SystemExit(1)

    # 依觀看時數排序取 Top 10
    items.sort(key=lambda x: -(x['hours_viewed'] or 0))
    top10 = items[:10]
    for i, item in enumerate(top10):
        item['rank'] = i + 1

    # 分類榜（TV / Films）
    tv_items   = sorted([x for x in items if x['category']=='series'], key=lambda x: -(x['hours_viewed'] or 0))
    film_items = sorted([x for x in items if x['category']=='movie'],  key=lambda x: -(x['hours_viewed'] or 0))
    for i, it in enumerate(tv_items[:10]):   it['rank'] = i + 1
    for i, it in enumerate(film_items[:10]): it['rank'] = i + 1

    # 統計
    total_hours = round(sum(x['hours_viewed'] for x in top10), 1)
    ne_items = [x for x in top10 if x['is_non_english']]
    ne_hours = round(sum(x['hours_viewed'] for x in ne_items), 1)
    ne_pct   = round(ne_hours / total_hours * 100, 1) if total_hours else 0
    ne_avg   = round(ne_hours / len(ne_items), 1) if ne_items else 0
    g_avg    = round(total_hours / len(top10), 1) if top10 else 0

    # 歷史（避免重複同週）
    history = old_data.get('history', [])
    if not history or history[-1].get('week') != latest_week:
        history.append({'week': latest_week, 'tw_hours': ne_hours, 'total_hours': total_hours})
    history = history[-8:]

    # 類型佔比
    s_hrs = sum(x['hours_viewed'] for x in top10 if x['category']=='series')
    m_hrs = sum(x['hours_viewed'] for x in top10 if x['category']=='movie')
    total_bd = s_hrs + m_hrs or 1

    output = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'current_week': {
            'week': latest_week,
            'week_label': latest_week,
            'tw_count':   len(ne_items),
            'tw_series':  sum(1 for x in ne_items if x['category']=='series'),
            'tw_movie':   sum(1 for x in ne_items if x['category']=='movie'),
            'tw_show':    0,
            'tw_pct':     ne_pct,
            'tw_avg':     ne_avg,
            'global_avg': g_avg,
            'total_hours': total_hours,
        },
        'prev_week': old_data.get('current_week'),
        'top10': top10,
        'by_category': {
            'series': tv_items[:10],
            'movie':  film_items[:10],
            'show':   [],
        },
        'history': history,
        'breakdown': {
            'series_pct': round(s_hrs/total_bd*100, 1),
            'movie_pct':  round(m_hrs/total_bd*100, 1),
            'show_pct':   0,
            'countries': [
                {'flag':'🌏', 'name':'非英語作品', 'pct': ne_pct},
                {'flag':'🇺🇸', 'name':'英語作品',   'pct': round(100-ne_pct, 1)},
            ],
        },
        'omdb_cache': omdb_cache,
    }

    with open('data/data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f'\n✅ 完成！週次：{latest_week}，Top10：{len(top10)} 筆，非英語：{len(ne_items)} 筆')

if __name__ == '__main__':
    build_json()
