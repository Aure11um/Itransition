import io
import json
import re
import base64
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import yaml
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

DATA_ROOT  = Path("data")
OUT_DIR    = Path("output")
DATASETS   = ["DATA1", "DATA2", "DATA3"]
EUR_TO_USD = 1.2

def parse_price(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return 0.0
    s = str(value).strip().lower()
    is_eur = "€" in s or "eur" in s
    nums = re.findall(r'\d+', s)
    if not nums:
        return 0.0
    if len(nums) >= 2:
        major = nums[0]
        minor = nums[1][:2].ljust(2, '0')
        s_clean = f"{major}.{minor}"
    else:
        s_clean = nums[0]
    try:
        price = float(s_clean)
        if is_eur:
            price *= EUR_TO_USD
        return round(price, 2)
    except:
        return 0.0

_TS_FORMATS = [
    "%H:%M:%S %Y-%m-%d", "%I:%M:%S %p %d-%B-%Y", "%d-%B-%Y %H:%M",
    "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%I:%M:%S %p %m/%d/%y",
    "%H:%M:%S %d-%b-%Y", "%m/%d/%y %I:%M:%S %p", "%d-%b-%Y %H:%M:%S %p",
    "%H:%M %d-%b-%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y",
]

def parse_timestamp(ts):
    if ts is None or (isinstance(ts, float) and np.isnan(ts)):
        return pd.NaT
    s = str(ts).strip().replace("A.M.", "AM").replace("P.M.", "PM").upper()
    s = re.sub(r"[;,]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for fmt in _TS_FORMATS:
        try:
            return pd.to_datetime(s, format=fmt)
        except:
            continue
    return pd.to_datetime(s, errors='coerce')

def load_users(path):
    df = pd.read_csv(path, dtype={"id": int}).drop_duplicates()
    df["phone_norm"] = df["phone"].astype(str).apply(lambda x: re.sub(r"\D", "", x))
    return df

def load_orders(path):
    df = pd.read_parquet(path).drop_duplicates()
    df["timestamp_processed"] = df["timestamp"].apply(parse_timestamp)
    df["unit_price_clean"] = df["unit_price"].apply(parse_price)
    df["quantity_clean"] = pd.to_numeric(df["quantity"], errors="coerce")
    
    df_clean = df.dropna(subset=["unit_price_clean", "quantity_clean", "timestamp_processed"]).copy()
    df_clean["paid_price"] = (df_clean["quantity_clean"] * df_clean["unit_price_clean"]).round(2)
    df_clean["date"] = pd.to_datetime(df_clean["timestamp_processed"]).dt.date
    
    df_clean["unit_price"] = df_clean["unit_price_clean"]
    df_clean["quantity"] = df_clean["quantity_clean"]
    return df_clean

def load_books(path):
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    df = pd.DataFrame([{k.lstrip(":"): v for k, v in b.items()} for b in raw]).drop_duplicates(subset=["id"])
    df["author"] = df["author"].astype(str).str.strip()
    df["author_set"] = df["author"].apply(lambda a: frozenset(x.strip() for x in a.split(",")))
    df["author_key"] = df["author_set"].apply(lambda s: ", ".join(sorted(s)))
    return df

_NAME_PREFIX_RE = re.compile(r'^(?:Amb|Msgr|Prof|Sen|Ms|Rev|Rep|Fr|Pres|Mrs|Dr|Gov|Mr|DVM|Miss)\.\s+', re.I)
_NAME_SUFFIX_RE = re.compile(r'\s+(?:LLD|DVM|Sr\.|Jr\.|PhD|MD|JD|II|III|IV)$', re.I)

def deduplicate_users(users):
    df = users.reset_index(drop=True)
    parent = list(range(len(df)))
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb: parent[ra] = rb

    email_idx, phone_idx, name_idx = {}, {}, {}
    for i, row in df.iterrows():
        b_name = _NAME_SUFFIX_RE.sub("", _NAME_PREFIX_RE.sub("", str(row["name"]).strip())).strip().lower()
        for val, idx in [(str(row["email"]).lower().strip(), email_idx), (str(row["phone_norm"]), phone_idx), (b_name, name_idx)]:
            if val and val != "nan" and val != "":
                if val in idx: union(i, idx[val])
                else: idx[val] = i

    df["group"] = [find(i) for i in range(len(df))]
    id_to_canonical = df.groupby("group")["id"].first().to_dict()
    id_mapping = dict(zip(df["id"], df["group"].map(id_to_canonical)))
    df["canonical_id"] = df["id"].map(id_mapping)
    return id_mapping, df

def analyze(data, name):
    users, orders, books = data["users"], data["orders"], data["books"]
    id_to_can, users_ext = deduplicate_users(users)
    orders["canonical_id"] = orders["user_id"].map(id_to_can)
    
    unique_buyers = int(orders["canonical_id"].nunique())
    daily_rev = orders.groupby("date")["paid_price"].sum().reset_index().rename(columns={"paid_price": "revenue"})
    ob = orders.merge(books[["id", "author_key"]].rename(columns={"id": "book_id"}), on="book_id", how="left")
    author_sales = ob.groupby("author_key")["quantity"].sum().sort_values(ascending=False)
    
    customer_spend = orders.groupby("canonical_id")["paid_price"].sum().sort_values(ascending=False)
    top_cid = customer_spend.index[0]
    
    return {
        "name": name,
        "top5_days": daily_rev.nlargest(5, "revenue").assign(date=lambda x: x['date'].astype(str)).to_dict(orient="records"),
        "unique_users": unique_buyers,
        "unique_author_sets": int(books["author_key"].nunique()),
        "top_author": author_sales.index[0],
        "top_author_sales": int(author_sales.iloc[0]),
        "top_customer_ids": sorted(users_ext[users_ext["canonical_id"] == top_cid]["id"].tolist()),
        "top_customer_spend": round(float(customer_spend.iloc[0]), 2),
        "daily_rev": daily_rev.sort_values("date"),
    }

def make_chart_b64(res):
    dr = res["daily_rev"].copy()
    dr["date"] = pd.to_datetime(dr["date"])
    fig, ax = plt.subplots(figsize=(11, 3.6))
    ax.plot(dr["date"], dr["revenue"], linewidth=1.8, color="#2563EB")
    ax.fill_between(dr["date"], dr["revenue"], alpha=0.10, color="#2563EB")
    ax.set_title(f"Daily Revenue — {res['name']}", fontsize=11, fontweight="bold", pad=10)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.grid(True, alpha=0.25)
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=140, bbox_inches="tight")
    plt.close()
    return base64.b64encode(buf.getvalue()).decode()

def build_dashboard(results, out_dir):
    tabs, panels = [], []
    for i, res in enumerate(results):
        tid, active = f"tab{i}", ("active" if i == 0 else "")
        chart = make_chart_b64(res)
        rows = "".join(f"<tr><td class='rank'>#{j+1}</td><td class='date'>{d['date']}</td><td class='money'>${d['revenue']:,.2f}</td></tr>" for j, d in enumerate(res["top5_days"]))
        
        panels.append(f"""
        <div id="{tid}" class="panel {'show' if i == 0 else ''}">
          <div class="kpi-row">
            <div class="kpi-card accent-blue"><div class="kpi-label">Unique Buyers (BI Match)</div><div class="kpi-value">{res['unique_users']:,}</div></div>
            <div class="kpi-card accent-violet"><div class="kpi-label">Unique Author Sets</div><div class="kpi-value">{res['unique_author_sets']:,}</div></div>
            <div class="kpi-card accent-green"><div class="kpi-label">Top Customer Spent</div><div class="kpi-value">${res['top_customer_spend']:,.2f}</div></div>
            <div class="kpi-card accent-amber"><div class="kpi-label">Top Author Sales</div><div class="kpi-value">{res['top_author_sales']:,}</div></div>
          </div>
          <div class="two-col">
            <div class="card"><div class="card-title">🏆 Top 5 Days</div><table><thead><tr><th>#</th><th>Date</th><th>Revenue</th></tr></thead><tbody>{rows}</tbody></table></div>
            <div class="card"><div class="card-title">📚 Popular Author</div><div class="author-pill"><div class="author-name">{res['top_author']}</div><div class="author-sub">{res['top_author_sales']:,} copies sold</div></div>
            <div class="card-title" style="margin-top:1.6rem">🥇 Best Buyer IDs</div><div class="ids-box">{json.dumps(res['top_customer_ids'])}</div></div>
          </div>
          <div class="card chart-card"><div class="card-title">📈 Daily Revenue</div><img src="data:image/png;base64,{chart}" class="chart-img"></div>
        </div>""")
        tabs.append(f'<button class="tab-btn {active}" onclick="showTab(\'{tid}\', this)">{res["name"]}</button>')

    css = "*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}body{font-family:'Segoe UI',sans-serif;background:#f1f5f9;color:#1e293b}header{background:linear-gradient(135deg,#0f172a,#1d4ed8);color:#fff;padding:1.4rem 2rem;display:flex;align-items:center;gap:1rem;box-shadow:0 2px 10px rgba(0,0,0,.25)}.logo{font-size:2rem}h1{font-size:1.45rem;font-weight:700}.tab-bar{display:flex;gap:.5rem;padding:1rem 2rem 0;background:#fff;border-bottom:2px solid #e2e8f0}.tab-btn{padding:.55rem 1.4rem;border:none;border-radius:6px 6px 0 0;background:#f1f5f9;color:#64748b;font-weight:600;cursor:pointer;border-bottom:3px solid transparent}.tab-btn.active{background:#fff;color:#1d4ed8;border-bottom:3px solid #1d4ed8}.panel{display:none;padding:1.5rem 2rem 2.5rem}.panel.show{display:block}.kpi-row{display:grid;grid-template-columns:repeat(4,1fr);gap:1rem;margin-bottom:1.25rem}.kpi-card{background:#fff;border-radius:10px;padding:1.1rem 1.4rem;box-shadow:0 1px 4px rgba(0,0,0,.07);border-top:4px solid transparent}.accent-blue{border-color:#2563eb}.accent-violet{border-color:#7c3aed}.accent-green{border-color:#16a34a}.accent-amber{border-color:#d97706}.kpi-label{font-size:.7rem;text-transform:uppercase;color:#64748b;margin-bottom:.35rem}.kpi-value{font-size:1.75rem;font-weight:800;color:#0f172a}.two-col{display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1.25rem}.card{background:#fff;border-radius:10px;padding:1.2rem 1.4rem;box-shadow:0 1px 4px rgba(0,0,0,.07)}.card-title{font-size:.72rem;text-transform:uppercase;color:#94a3b8;font-weight:700;margin-bottom:.9rem;padding-bottom:.55rem;border-bottom:1px solid #f1f5f9}table{width:100%;border-collapse:collapse;font-size:.88rem}th{font-size:.68rem;color:#94a3b8;padding:.4rem .6rem;text-align:left;background:#f8fafc}td{padding:.5rem .6rem}td.rank{color:#94a3b8;font-weight:700}td.money{font-weight:700;color:#1d4ed8}.author-pill{background:linear-gradient(135deg,#eff6ff,#dbeafe);border:1px solid #bfdbfe;border-radius:8px;padding:.9rem 1.1rem}.author-name{font-size:1rem;font-weight:700;color:#1d4ed8}.ids-box{background:#f8fafc;border:1px solid #e2e8f0;border-radius:7px;padding:.75rem 1rem;font-family:monospace;font-size:.82rem;word-break:break-all}.chart-img{width:100%;border-radius:5px}@media(max-width:900px){.kpi-row{grid-template-columns:repeat(2,1fr)}.two-col{grid-template-columns:1fr}}"
    
    html = f"<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'><title>Book Store BI</title><style>{css}</style></head><body><header><div class='logo'>📖</div><div><h1>Book Store BI Dashboard</h1><p>Revenue & Analytics</p></div></header><div class='tab-bar'>{''.join(tabs)}</div>{''.join(panels)}<script>function showTab(id,btn){{document.querySelectorAll('.panel').forEach(p=>p.classList.remove('show'));document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));document.getElementById(id).classList.add('show');btn.classList.add('active');}}</script></body></html>"
    (out_dir / "dashboard.html").write_text(html, encoding="utf-8")

if __name__ == "__main__":
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    all_results = []
    
    for ds in DATASETS:
        folder = DATA_ROOT / ds
        print(f"\n🚀 Waiting {ds}...")
        
        data = {
            "users":  load_users(folder / "users.csv"),
            "orders": load_orders(folder / "orders.parquet"),
            "books":  load_books(folder / "books.yaml")
        }
        
        result = analyze(data, ds)
        all_results.append(result)

    build_dashboard(all_results, OUT_DIR)
    
    print(f"\n✅ Ready!")
    print(f"📊 Dashboard: {OUT_DIR / 'dashboard.html'}")