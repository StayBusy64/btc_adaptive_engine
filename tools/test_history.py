import urllib.request, json

resp = urllib.request.urlopen("http://127.0.0.1:8000/cohorts/history")
data = json.loads(resp.read())
print(f"Count: {data['count']}")
for r in data["rows"][:10]:
    h = r.get("horizon", "?")
    ck = r.get("cohort_key", "?")
    qs = r.get("quality_score", 0)
    ds = r.get("decay_score", 0)
    ps = r.get("promotion_score", 0)
    n = r.get("sample_count", 0)
    print(f"  {h:7s} | {ck:40s} | quality={qs:+.4f} | promo={ps:.4f} | decay={ds:.4f} | n={n}")

# Test filter by horizon
resp2 = urllib.request.urlopen("http://127.0.0.1:8000/cohorts/history?horizon=medium")
data2 = json.loads(resp2.read())
print(f"\nFiltered (medium): {data2['count']} rows")

# Test filter by cohort_key
resp3 = urllib.request.urlopen("http://127.0.0.1:8000/cohorts/history?limit=2")
data3 = json.loads(resp3.read())
print(f"Limited to 2: {data3['count']} rows")
