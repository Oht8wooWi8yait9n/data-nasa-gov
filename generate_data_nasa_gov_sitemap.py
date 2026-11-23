#!/usr/bin/env python3
"""
NASA Open Data Portal (data.nasa.gov) Harvester, Static Mirror & Sitemap Pipeline
=================================================================================
Harvests 100% of NASA's public Open Data Portal (CKAN) dataset catalog via:
  - High-speed CKAN Solr field-list pagination:
    GET https://data.nasa.gov/api/3/action/package_search?rows=1000&sort=id+asc&fl=...
    (retrieves all ~36,500+ canonical CKAN dataset slugs, metadata, authoritative
    res_url/res_format distributions, and DCAT extras directly from CKAN Solr;
    intentionally bypasses /data.json because NASA's nightly data.json exporter
    periodically scrambles/drops distribution arrays across unrelated datasets)

Generates:
  - Clean, semantic, boilerplate-free static HTML pages at dataset/<slug>.html
    mirroring exact live NASA Open Data Portal URLs 1-to-1 for Onyx URL rewrite:
      https://oht8woowi8yait9n.github.io/data-nasa-gov/dataset/<slug>
        --> https://data.nasa.gov/dataset/<slug>
  - Flat <urlset> XML sitemaps compatible with Onyx Web Connector:
      * sitemap.xml & data_nasa_gov_sitemap.xml      (complete mirror catalog)
      * data_nasa_gov_direct_sitemap.xml             (live data.nasa.gov URLs)
      * sitemaps/data_nasa_gov_part{N}.xml           (staged 10,000-URL mirror chunks)
      * sitemaps/theme_<theme>.xml                   (domain/theme-filtered mirror sitemaps)
  - Plain-text URL indexes (data_nasa_gov_urls.txt & data_nasa_gov_direct_urls.txt)
  - Deterministic compressed JSONL catalog (data_nasa_gov_records.jsonl.gz, mtime=0)
  - Lightweight catalog overview dashboard (index.html) & shared stylesheet (assets/style.css)
"""

import ast
import csv
import gzip
import io
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from html import escape
from xml.sax.saxutils import escape as xml_escape

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(BASE_DIR, "dataset")
ASSETS_DIR = os.path.join(BASE_DIR, "assets")
SITEMAPS_DIR = os.path.join(BASE_DIR, "sitemaps")
CACHE_DIR = os.path.join(BASE_DIR, ".data_nasa_gov_cache")

SITEMAP_FILE = os.path.join(BASE_DIR, "sitemap.xml")
SITEMAP_ALIAS_FILE = os.path.join(BASE_DIR, "data_nasa_gov_sitemap.xml")
DIRECT_SITEMAP_FILE = os.path.join(BASE_DIR, "data_nasa_gov_direct_sitemap.xml")
URLS_FILE = os.path.join(BASE_DIR, "data_nasa_gov_urls.txt")
DIRECT_URLS_FILE = os.path.join(BASE_DIR, "data_nasa_gov_direct_urls.txt")
JSONL_GZ_FILE = os.path.join(BASE_DIR, "data_nasa_gov_records.jsonl.gz")
INDEX_HTML = os.path.join(BASE_DIR, "index.html")
STYLE_CSS = os.path.join(ASSETS_DIR, "style.css")

PORTAL_BASE = "https://data.nasa.gov"
CKAN_SEARCH_URL = f"{PORTAL_BASE}/api/3/action/package_search"
GH_PAGES_BASE = os.environ.get(
    "GH_PAGES_BASE", "https://oht8woowi8yait9n.github.io/data-nasa-gov"
).rstrip("/")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

SOLR_FIELDS = ",".join([
    "id",
    "name",
    "title",
    "notes",
    "tags",
    "maintainer",
    "maintainer_email",
    "metadata_created",
    "metadata_modified",
    "license",
    "res_url",
    "res_format",
    "extras_identifier",
    "extras_landingPage",
    "extras_publisher",
    "extras_theme",
    "extras_harvest_source_title",
    "extras_accessLevel",
    "extras_spatial",
    "extras_temporal",
    "extras_issued",
    "extras_modified",
    "extras_programCode",
    "extras_bureauCode",
    "extras_series-name",
    "extras_resource-type",
    "extras_references",
    "extras_accrualPeriodicity",
])

PAGE_SIZE = 1000
PAGE_STEP = 950  # 50-record overlap eliminates offset-boundary skips during live CKAN updates
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "4"))
CHUNK_SIZE = 10000
MIN_EXPECTED_DATASETS = 30000

MAJOR_THEME_MAP = {
    "earth science": ("earth-science", "Earth Science"),
    "planetary science": ("planetary-science", "Planetary Science"),
    "heliophysics": ("heliophysics", "Heliophysics"),
    "astrophysics": ("astrophysics", "Astrophysics"),
    "biological and physical sciences": ("biological-and-physical-sciences", "Biological and Physical Sciences"),
    "space science": ("space-science", "Space Science"),
    "aerospace": ("aerospace", "Aerospace"),
    "management/operations": ("management-operations", "Management/Operations"),
    "geospatial": ("geospatial", "Geospatial"),
}

SHARED_CSS = """/* NASA Open Data Portal Static Mirror Stylesheet */
:root {
  --nasa-blue: #0b3d91;
  --nasa-accent: #1a73e8;
  --bg: #f8fafc;
  --card: #ffffff;
  --border: #e2e8f0;
  --text: #1e293b;
  --muted: #475569;
  --tag-bg: #eef2ff;
  --tag-fg: #1e40af;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  background: var(--bg);
  color: var(--text);
  line-height: 1.6;
}
header.site-header {
  background: var(--nasa-blue);
  color: #fff;
  padding: 1rem 1.5rem;
}
header.site-header a { color: #dbeafe; text-decoration: none; }
header.site-header a:hover { text-decoration: underline; }
main {
  max-width: 980px;
  margin: 1.5rem auto;
  padding: 0 1rem 3rem;
}
article.dataset-card, section.panel {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 1.5rem 1.75rem;
  margin-bottom: 1.25rem;
}
h1 { margin: 0 0 0.75rem; font-size: 1.55rem; line-height: 1.3; color: #0f172a; }
h2 { margin: 1.5rem 0 0.6rem; font-size: 1.1rem; color: var(--nasa-blue); border-bottom: 1px solid var(--border); padding-bottom: 0.3rem; }
.meta-badges { display: flex; flex-wrap: wrap; gap: 0.45rem; margin: 0.5rem 0 1rem; }
.badge {
  display: inline-block;
  font-size: 0.78rem;
  font-weight: 600;
  padding: 0.2rem 0.55rem;
  border-radius: 999px;
  background: var(--tag-bg);
  color: var(--tag-fg);
}
.badge.source { background: #ecfdf5; color: #065f46; }
.badge.access { background: #fef3c7; color: #92400e; }
.description { white-space: pre-line; margin: 0.75rem 0 1.25rem; }
table.meta-table, table.res-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.92rem;
  margin: 0.5rem 0 1rem;
}
table.meta-table th, table.meta-table td,
table.res-table th, table.res-table td {
  border: 1px solid var(--border);
  padding: 0.45rem 0.65rem;
  text-align: left;
  vertical-align: top;
  word-break: break-word;
}
table.meta-table th { width: 220px; background: #f1f5f9; color: #334155; font-weight: 600; }
table.res-table th { background: #f1f5f9; color: #334155; font-weight: 600; }
.tags { display: flex; flex-wrap: wrap; gap: 0.35rem; margin-top: 0.5rem; }
.tag {
  font-size: 0.8rem;
  padding: 0.15rem 0.5rem;
  border-radius: 4px;
  background: #f1f5f9;
  color: #334155;
  border: 1px solid var(--border);
}
.official-link {
  display: inline-block;
  margin-top: 0.25rem;
  padding: 0.4rem 0.8rem;
  background: var(--nasa-accent);
  color: #fff !important;
  border-radius: 5px;
  text-decoration: none;
  font-size: 0.88rem;
  font-weight: 600;
}
.official-link:hover { background: var(--nasa-blue); }
.stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 0.85rem;
  margin: 1rem 0;
}
.stat-box {
  background: #f8fafc;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 0.85rem 1rem;
}
.stat-val { font-size: 1.5rem; font-weight: 700; color: var(--nasa-blue); }
.stat-lbl { font-size: 0.82rem; color: var(--muted); }
code { background: #f1f5f9; padding: 0.12rem 0.35rem; border-radius: 3px; font-size: 0.88em; }
"""


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
    })
    return session


def parse_pg_array(val) -> list[str]:
    """Parse PostgreSQL array strings such as '{Heliophysics}' or '{\"Planetary Science\",geospatial}'."""
    if not val:
        return []
    if isinstance(val, list):
        items = [str(v).strip() for v in val if v is not None]
    else:
        s = str(val).strip()
        if not s:
            return []
        if s.startswith("{") and s.endswith("}"):
            inner = s[1:-1].strip()
            if not inner:
                return []
            try:
                items = next(csv.reader(io.StringIO(inner), skipinitialspace=True))
            except Exception:
                items = [p.strip().strip('"') for p in inner.split(",")]
        elif s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s)
                items = [str(x).strip() for x in parsed] if isinstance(parsed, list) else [s]
            except Exception:
                try:
                    parsed = ast.literal_eval(s)
                    items = [str(x).strip() for x in parsed] if isinstance(parsed, list) else [s]
                except Exception:
                    items = [s]
        else:
            items = [s]
    out = []
    seen = set()
    for item in items:
        clean = item.strip()
        if not clean or clean in ("__", "None", "null"):
            continue
        key = clean.lower()
        if key not in seen:
            seen.add(key)
            out.append(clean)
    return out


def parse_date_ymd(raw_val) -> str:
    """Extract YYYY-MM-DD from ISO-8601 or date-like strings; return '' if invalid."""
    if not raw_val:
        return ""
    s = str(raw_val).strip()
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else ""


def clean_text(raw_val) -> str:
    """Normalize whitespace in text fields while preserving paragraph breaks."""
    if not raw_val:
        return ""
    s = str(raw_val).replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def format_spatial(val) -> str:
    """Render spatial coverage JSON/text concisely."""
    if not val:
        return ""
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False, sort_keys=True)
    s = str(val).strip()
    if s.startswith(("{", "[")):
        try:
            obj = json.loads(s)
            return json.dumps(obj, ensure_ascii=False, sort_keys=True)
        except Exception:
            pass
    return s


def fetch_ckan_page(session: requests.Session, start: int, rows: int = PAGE_SIZE, max_retries: int = 5):
    params = {
        "rows": rows,
        "start": start,
        "sort": "id asc",
        "fl": SOLR_FIELDS,
    }
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(CKAN_SEARCH_URL, params=params, timeout=60)
            if r.status_code == 200 and "json" in r.headers.get("Content-Type", "").lower():
                payload = r.json()
                if payload.get("success") and "result" in payload:
                    return payload["result"]
            last_err = f"HTTP {r.status_code} ({r.headers.get('Content-Type', '')})"
        except Exception as exc:
            last_err = str(exc)
        wait = min(2 ** attempt, 20)
        print(f"  [retry {attempt}/{max_retries}] start={start} failed ({last_err}); sleeping {wait}s...", flush=True)
        time.sleep(wait)
    raise RuntimeError(f"Failed to fetch CKAN page start={start} after {max_retries} attempts: {last_err}")


def harvest_ckan_catalog(force_refresh: bool = False) -> tuple[int, list[dict]]:
    cache_file = os.path.join(CACHE_DIR, "ckan_solr_records.json")
    if not force_refresh and os.path.isfile(cache_file):
        print(f"[cache] Loading CKAN Solr catalog from {cache_file}", flush=True)
        with open(cache_file, "r", encoding="utf-8") as f:
            cached = json.load(f)
        return cached["count"], cached["results"]

    t0 = time.time()
    session = create_session()
    first = fetch_ckan_page(session, 0, PAGE_SIZE)
    total_count = int(first["count"])
    all_results = list(first["results"])
    print(f"[ckan] Total reported datasets: {total_count:,} (page 0 fetched: {len(all_results)})", flush=True)

    starts = list(range(PAGE_STEP, total_count, PAGE_STEP))
    if not starts or starts[-1] + PAGE_SIZE < total_count + 50:
        starts.append(max(total_count - 100, 0))
    page_results = {}

    def _worker(st):
        s = create_session()
        res = fetch_ckan_page(s, st, PAGE_SIZE)
        return st, res["results"]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_worker, st): st for st in starts}
        completed = 1
        total_pages = len(starts) + 1
        for fut in as_completed(futures):
            st, rows = fut.result()
            page_results[st] = rows
            completed += 1
            if completed % 5 == 0 or completed == total_pages:
                print(f"[ckan] Progress: {completed}/{total_pages} pages ({time.time() - t0:.1f}s)", flush=True)

    for st in starts:
        all_results.extend(page_results.get(st, []))

    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump({"count": total_count, "results": all_results}, f)
    print(f"[ckan] Harvested {len(all_results):,} raw records in {time.time() - t0:.1f}s", flush=True)
    return total_count, all_results


def normalize_record(raw: dict) -> dict | None:
    slug = str(raw.get("name") or "").strip()
    if not slug or not re.match(r"^[A-Za-z0-9._-]+$", slug):
        return None

    title = clean_text(raw.get("title")) or slug
    identifier = str(raw.get("identifier") or "").strip()
    notes = clean_text(raw.get("notes"))
    publisher = clean_text(raw.get("publisher")) or "NASA"

    themes = sorted(parse_pg_array(raw.get("theme")), key=lambda s: s.lower())
    tags = sorted(parse_pg_array(raw.get("tags")), key=lambda s: s.lower())

    landing_page = str(raw.get("landingPage") or "").strip()
    license_url = str(raw.get("license") or "").strip()
    access_level = str(raw.get("accessLevel") or "public").strip()
    harvest_source = clean_text(raw.get("harvest_source_title"))
    program_codes = sorted(parse_pg_array(raw.get("programCode")), key=lambda s: s.lower())
    bureau_codes = sorted(parse_pg_array(raw.get("bureauCode")), key=lambda s: s.lower())

    # Prefer upstream DCAT modified date to avoid weekly harvest-touch churn on metadata_modified
    modified_date = (
        parse_date_ymd(raw.get("modified"))
        or parse_date_ymd(raw.get("metadata_modified"))
    )
    issued_date = parse_date_ymd(raw.get("issued"))

    spatial = format_spatial(raw.get("spatial"))
    temporal = clean_text(raw.get("temporal"))
    accrual = clean_text(raw.get("accrualPeriodicity"))
    series_name = clean_text(raw.get("series-name"))

    contact_name = clean_text(raw.get("maintainer"))
    contact_email = clean_text(raw.get("maintainer_email"))
    references = sorted(parse_pg_array(raw.get("references")), key=lambda s: s.lower())

    # Build authoritative, deterministically sorted resource list directly from CKAN Solr
    resources = []
    seen_res = set()
    raw_urls = raw.get("res_url") or []
    raw_fmts = raw.get("res_format") or []
    if isinstance(raw_urls, str):
        raw_urls = [raw_urls]
    if isinstance(raw_fmts, str):
        raw_fmts = [raw_fmts]
    for idx, rurl in enumerate(raw_urls):
        rurl = str(rurl or "").strip()
        if not rurl:
            continue
        rfmt = str(raw_fmts[idx]).strip() if idx < len(raw_fmts) else ""
        sig = (rurl, rfmt)
        if sig not in seen_res:
            seen_res.add(sig)
            resources.append({
                "url": rurl,
                "format": rfmt,
                "title": "",
                "description": "",
            })
    resources.sort(key=lambda r: (r["url"], r["format"]))

    return {
        "id": str(raw.get("id") or "").strip(),
        "slug": slug,
        "url": f"{PORTAL_BASE}/dataset/{slug}",
        "mirror_url": f"{GH_PAGES_BASE}/dataset/{slug}",
        "title": title,
        "description": notes,
        "publisher": publisher,
        "themes": themes,
        "tags": tags,
        "identifier": identifier,
        "landing_page": landing_page,
        "license": license_url,
        "access_level": access_level,
        "harvest_source": harvest_source,
        "modified": modified_date,
        "issued": issued_date,
        "temporal": temporal,
        "spatial": spatial,
        "accrual_periodicity": accrual,
        "series_name": series_name,
        "program_codes": program_codes,
        "bureau_codes": bureau_codes,
        "contact_name": contact_name,
        "contact_email": contact_email,
        "references": references,
        "resources": resources,
    }


def render_dataset_html(rec: dict) -> str:
    title_esc = escape(rec["title"])
    canonical_url = escape(rec["url"])
    desc_text = rec["description"] or "No description provided in NASA Open Data Portal metadata."

    badges = []
    for th in rec["themes"]:
        badges.append(f'<span class="badge">{escape(th)}</span>')
    if rec["harvest_source"]:
        badges.append(f'<span class="badge source">{escape(rec["harvest_source"])}</span>')
    if rec["access_level"]:
        badges.append(f'<span class="badge access">Access: {escape(rec["access_level"])}</span>')
    badges_html = f'<div class="meta-badges">{"".join(badges)}</div>' if badges else ""

    meta_rows = [
        ("Publisher", escape(rec["publisher"])),
        ("Official NASA Portal URL", f'<a href="{canonical_url}">{canonical_url}</a>'),
    ]
    if rec["identifier"]:
        ident = rec["identifier"]
        if ident.startswith(("http://", "https://")):
            meta_rows.append(("Identifier / DOI", f'<a href="{escape(ident)}">{escape(ident)}</a>'))
        else:
            meta_rows.append(("Identifier", escape(ident)))
    if rec["landing_page"] and rec["landing_page"] != rec["identifier"]:
        lp = rec["landing_page"]
        if lp.startswith(("http://", "https://")):
            meta_rows.append(("Landing Page", f'<a href="{escape(lp)}">{escape(lp)}</a>'))
        else:
            meta_rows.append(("Landing Page", escape(lp)))
    if rec["themes"]:
        meta_rows.append(("Themes", escape(", ".join(rec["themes"]))))
    if rec["modified"]:
        meta_rows.append(("Modified", escape(rec["modified"])))
    if rec["issued"]:
        meta_rows.append(("Issued", escape(rec["issued"])))
    if rec["temporal"]:
        meta_rows.append(("Temporal Coverage", escape(rec["temporal"])))
    if rec["spatial"]:
        meta_rows.append(("Spatial Coverage", f"<code>{escape(rec['spatial'][:600])}</code>"))
    if rec["contact_name"] or rec["contact_email"]:
        contact_str = " — ".join(p for p in [rec["contact_name"], rec["contact_email"]] if p)
        meta_rows.append(("Contact / Maintainer", escape(contact_str)))
    if rec["license"]:
        lic = rec["license"]
        if lic.startswith(("http://", "https://")):
            meta_rows.append(("License", f'<a href="{escape(lic)}">{escape(lic)}</a>'))
        else:
            meta_rows.append(("License", escape(lic)))
    if rec["series_name"]:
        meta_rows.append(("Series", escape(rec["series_name"])))
    if rec["accrual_periodicity"]:
        meta_rows.append(("Update Frequency", escape(rec["accrual_periodicity"])))
    if rec["program_codes"]:
        meta_rows.append(("Program Code", escape(", ".join(rec["program_codes"]))))
    if rec["bureau_codes"]:
        meta_rows.append(("Bureau Code", escape(", ".join(rec["bureau_codes"]))))

    meta_table_rows = "\n".join(f"<tr><th>{k}</th><td>{v}</td></tr>" for k, v in meta_rows)

    res_section = ""
    if rec["resources"]:
        res_rows = []
        for res in rec["resources"]:
            rurl = res["url"]
            rtitle = res["title"] or rurl
            rfmt = res["format"] or "Link"
            rdesc = res["description"]
            desc_cell = f"<br><small>{escape(rdesc)}</small>" if rdesc else ""
            res_rows.append(
                f'<tr><td>{escape(rfmt)}</td>'
                f'<td><a href="{escape(rurl)}">{escape(rtitle)}</a>{desc_cell}</td></tr>'
            )
        res_section = (
            f'<h2>Data Distributions &amp; Resources ({len(rec["resources"])})</h2>\n'
            f'<table class="res-table"><thead><tr><th>Format</th><th>Resource</th></tr></thead>\n'
            f'<tbody>\n{"".join(res_rows)}\n</tbody></table>\n'
        )

    refs_section = ""
    if rec["references"]:
        ref_items = []
        for ref in rec["references"]:
            if ref.startswith(("http://", "https://")):
                ref_items.append(f'<li><a href="{escape(ref)}">{escape(ref)}</a></li>')
            else:
                ref_items.append(f"<li>{escape(ref)}</li>")
        refs_section = f'<h2>References</h2>\n<ul>{"".join(ref_items)}</ul>\n'

    tags_section = ""
    if rec["tags"]:
        tag_spans = "".join(f'<span class="tag">{escape(t)}</span>' for t in rec["tags"])
        tags_section = f'<h2>Keywords &amp; Tags</h2>\n<div class="tags">{tag_spans}</div>\n'

    meta_desc = escape(desc_text[:280].replace("\n", " "))
    return (
        f'<!DOCTYPE html>\n'
        f'<html lang="en">\n'
        f'<head>\n'
        f'<meta charset="utf-8">\n'
        f'<meta name="viewport" content="width=device-width,initial-scale=1">\n'
        f'<title>{title_esc} — NASA Open Data Portal</title>\n'
        f'<meta name="description" content="{meta_desc}">\n'
        f'<link rel="canonical" href="{canonical_url}">\n'
        f'<link rel="stylesheet" href="../assets/style.css">\n'
        f'</head>\n'
        f'<body>\n'
        f'<header class="site-header"><a href="../">NASA Open Data Portal Mirror (data.nasa.gov)</a></header>\n'
        f'<main>\n'
        f'<article class="dataset-card">\n'
        f'<h1>{title_esc}</h1>\n'
        f'{badges_html}\n'
        f'<a class="official-link" href="{canonical_url}">View Official Record on data.nasa.gov</a>\n'
        f'<h2>Description</h2>\n'
        f'<div class="description">{escape(desc_text)}</div>\n'
        f'<h2>Dataset Metadata</h2>\n'
        f'<table class="meta-table"><tbody>\n{meta_table_rows}\n</tbody></table>\n'
        f'{res_section}'
        f'{refs_section}'
        f'{tags_section}'
        f'</article>\n'
        f'</main>\n'
        f'</body>\n'
        f'</html>\n'
    )


def write_if_changed(path: str, content: str) -> bool:
    data = content.encode("utf-8")
    if os.path.isfile(path):
        with open(path, "rb") as f:
            if f.read() == data:
                return False
    with open(path, "wb") as f:
        f.write(data)
    return True


def build_flat_sitemap_xml(entries: list[tuple[str, str]]) -> str:
    """Build a flat <urlset> XML sitemap with deterministic sorting and optional <lastmod>."""
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for url, lastmod in entries:
        loc = xml_escape(url)
        if lastmod:
            lines.append(f"  <url><loc>{loc}</loc><lastmod>{xml_escape(lastmod)}</lastmod></url>")
        else:
            lines.append(f"  <url><loc>{loc}</loc></url>")
    lines.append("</urlset>")
    lines.append("")
    return "\n".join(lines)


def classify_theme_buckets(rec: dict) -> list[tuple[str, str]]:
    """Assign dataset to one or more canonical theme sitemap buckets."""
    buckets = []
    seen = set()
    for th in rec["themes"]:
        key = th.strip().lower()
        if key in MAJOR_THEME_MAP:
            slug, label = MAJOR_THEME_MAP[key]
            if slug not in seen:
                seen.add(slug)
                buckets.append((slug, label))
    if not buckets:
        buckets.append(("other", "Other / Specialized Themes"))
    return buckets


def render_index_html(
    records: list[dict],
    theme_buckets: dict[str, tuple[str, list[tuple[str, str]]]],
    part_files: list[tuple[str, int]],
    source_counts: Counter,
    publisher_counts: Counter,
) -> str:
    total = len(records)
    total_resources = sum(len(r["resources"]) for r in records)
    public_count = sum(1 for r in records if r["access_level"].lower() == "public")

    theme_rows = []
    for slug, (label, entries) in sorted(theme_buckets.items(), key=lambda kv: (-len(kv[1][1]), kv[0])):
        sm_url = f"{GH_PAGES_BASE}/sitemaps/theme_{slug}.xml"
        theme_rows.append(
            f'<tr><td><strong>{escape(label)}</strong></td>'
            f'<td>{len(entries):,}</td>'
            f'<td><a href="sitemaps/theme_{escape(slug)}.xml"><code>{escape(sm_url)}</code></a></td></tr>'
        )

    part_rows = []
    for idx, (fname, cnt) in enumerate(part_files, start=1):
        sm_url = f"{GH_PAGES_BASE}/sitemaps/{fname}"
        part_rows.append(
            f'<tr><td>Part {idx}</td><td>{cnt:,}</td>'
            f'<td><a href="sitemaps/{escape(fname)}"><code>{escape(sm_url)}</code></a></td></tr>'
        )

    source_rows = "".join(
        f"<tr><td>{escape(src or 'Unspecified')}</td><td>{cnt:,}</td></tr>"
        for src, cnt in source_counts.most_common()
    )
    pub_rows = "".join(
        f"<tr><td>{escape(pub)}</td><td>{cnt:,}</td></tr>"
        for pub, cnt in publisher_counts.most_common(15)
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NASA Open Data Portal (data.nasa.gov) Static Mirror &amp; Sitemaps</title>
<link rel="stylesheet" href="assets/style.css">
</head>
<body>
<header class="site-header">
  <strong>NASA Open Data Portal (<code>data.nasa.gov</code>) Static HTML Mirror &amp; Onyx Sitemap Pipeline</strong>
</header>
<main>
  <section class="panel">
    <h1>NASA Open Data Portal Catalog Mirror</h1>
    <p>
      Pre-rendered static HTML mirror and flat XML sitemaps for the
      <a href="https://data.nasa.gov/">NASA Open Data Portal (data.nasa.gov)</a>,
      built for high-speed enterprise RAG ingestion (e.g., Onyx Web Connector) with 1-to-1 URL rewrite support.
    </p>
    <div class="stats-grid">
      <div class="stat-box"><div class="stat-val">{total:,}</div><div class="stat-lbl">Mirrored Datasets</div></div>
      <div class="stat-box"><div class="stat-val">{public_count:,}</div><div class="stat-lbl">Public Access Level</div></div>
      <div class="stat-box"><div class="stat-val">{total_resources:,}</div><div class="stat-lbl">Indexed Data Distributions</div></div>
      <div class="stat-box"><div class="stat-val">{len(theme_buckets)}</div><div class="stat-lbl">Theme Sitemaps</div></div>
    </div>
  </section>

  <section class="panel">
    <h2>Primary Flat XML Sitemaps (Onyx Ready)</h2>
    <table class="res-table">
      <thead><tr><th>Resource</th><th>URL</th></tr></thead>
      <tbody>
        <tr><td><strong>Complete Mirror Sitemap ({total:,} URLs)</strong></td><td><a href="sitemap.xml"><code>{escape(GH_PAGES_BASE)}/sitemap.xml</code></a></td></tr>
        <tr><td><strong>Alias Mirror Sitemap</strong></td><td><a href="data_nasa_gov_sitemap.xml"><code>{escape(GH_PAGES_BASE)}/data_nasa_gov_sitemap.xml</code></a></td></tr>
        <tr><td><strong>Direct Live NASA Sitemap (<code>data.nasa.gov</code>)</strong></td><td><a href="data_nasa_gov_direct_sitemap.xml"><code>{escape(GH_PAGES_BASE)}/data_nasa_gov_direct_sitemap.xml</code></a></td></tr>
        <tr><td><strong>Plain-Text Mirror URLs</strong></td><td><a href="data_nasa_gov_urls.txt"><code>data_nasa_gov_urls.txt</code></a></td></tr>
        <tr><td><strong>Compressed JSONL Catalog</strong></td><td><a href="data_nasa_gov_records.jsonl.gz"><code>data_nasa_gov_records.jsonl.gz</code></a></td></tr>
      </tbody>
    </table>
    <p>
      <strong>Onyx URL Rewrite Rule:</strong>
      Source Prefix <code>{escape(GH_PAGES_BASE)}/</code> &rarr;
      Target Prefix <code>https://data.nasa.gov/</code>
    </p>
  </section>

  <section class="panel">
    <h2>Theme-Filtered Sitemaps</h2>
    <table class="res-table">
      <thead><tr><th>NASA Theme</th><th>Datasets</th><th>Flat Sitemap URL</th></tr></thead>
      <tbody>
        {"".join(theme_rows)}
      </tbody>
    </table>
  </section>

  <section class="panel">
    <h2>Staged 10,000-URL Chunks</h2>
    <table class="res-table">
      <thead><tr><th>Chunk</th><th>Datasets</th><th>Flat Sitemap URL</th></tr></thead>
      <tbody>
        {"".join(part_rows)}
      </tbody>
    </table>
  </section>

  <section class="panel">
    <h2>Harvest Sources &amp; Top Publishers</h2>
    <table class="res-table">
      <thead><tr><th>CKAN Harvest Source</th><th>Datasets</th></tr></thead>
      <tbody>{source_rows}</tbody>
    </table>
    <table class="res-table">
      <thead><tr><th>Top 15 Publishers</th><th>Datasets</th></tr></thead>
      <tbody>{pub_rows}</tbody>
    </table>
  </section>
</main>
</body>
</html>
"""


def main():
    t_start = time.time()
    force_refresh = os.environ.get("FORCE_REFRESH", "0") == "1" or "--force" in sys.argv

    reported_count, raw_ckan = harvest_ckan_catalog(force_refresh=force_refresh)

    by_slug: dict[str, dict] = {}
    for raw in raw_ckan:
        rec = normalize_record(raw)
        if rec:
            by_slug[rec["slug"]] = rec

    records = [by_slug[s] for s in sorted(by_slug.keys())]
    total = len(records)
    print(f"[norm] Normalized {total:,} unique datasets (reported={reported_count:,})", flush=True)

    # Sanity guard: abort before mutating repo if API returned incomplete set
    min_floor = max(MIN_EXPECTED_DATASETS, int(reported_count * 0.95))
    if total < min_floor:
        raise RuntimeError(
            f"Sanity guard triggered: only harvested {total:,} datasets (minimum required: {min_floor:,}). "
            "Aborting without modifying existing mirror."
        )

    os.makedirs(DATASET_DIR, exist_ok=True)
    os.makedirs(ASSETS_DIR, exist_ok=True)
    os.makedirs(SITEMAPS_DIR, exist_ok=True)

    write_if_changed(STYLE_CSS, SHARED_CSS)

    # Write static HTML pages incrementally
    written = 0
    expected_files = set()
    for rec in records:
        fname = f"{rec['slug']}.html"
        expected_files.add(fname)
        fpath = os.path.join(DATASET_DIR, fname)
        if write_if_changed(fpath, render_dataset_html(rec)):
            written += 1

    # Remove stale dataset HTML pages that are no longer in CKAN
    removed = 0
    for existing in os.listdir(DATASET_DIR):
        if existing.endswith(".html") and existing not in expected_files:
            os.remove(os.path.join(DATASET_DIR, existing))
            removed += 1
    print(f"[html] Dataset mirror pages: total={total:,}, written/updated={written:,}, removed={removed:,}", flush=True)

    # Build flat sitemaps & plain-text URL lists
    mirror_entries = [(r["mirror_url"], r["modified"]) for r in records]
    direct_entries = [(r["url"], r["modified"]) for r in records]

    mirror_xml = build_flat_sitemap_xml(mirror_entries)
    direct_xml = build_flat_sitemap_xml(direct_entries)
    write_if_changed(SITEMAP_FILE, mirror_xml)
    write_if_changed(SITEMAP_ALIAS_FILE, mirror_xml)
    write_if_changed(DIRECT_SITEMAP_FILE, direct_xml)
    write_if_changed(URLS_FILE, "\n".join(u for u, _ in mirror_entries) + "\n")
    write_if_changed(DIRECT_URLS_FILE, "\n".join(u for u, _ in direct_entries) + "\n")

    # Build staged 10,000-URL mirror chunks
    expected_sitemaps = set()
    part_files: list[tuple[str, int]] = []
    for idx, start in enumerate(range(0, total, CHUNK_SIZE), start=1):
        chunk = mirror_entries[start : start + CHUNK_SIZE]
        fname = f"data_nasa_gov_part{idx}.xml"
        expected_sitemaps.add(fname)
        write_if_changed(os.path.join(SITEMAPS_DIR, fname), build_flat_sitemap_xml(chunk))
        part_files.append((fname, len(chunk)))

    # Build theme-specific flat sitemaps
    theme_buckets: dict[str, tuple[str, list[tuple[str, str]]]] = {}
    for rec in records:
        for slug, label in classify_theme_buckets(rec):
            if slug not in theme_buckets:
                theme_buckets[slug] = (label, [])
            theme_buckets[slug][1].append((rec["mirror_url"], rec["modified"]))

    for slug, (_label, entries) in theme_buckets.items():
        fname = f"theme_{slug}.xml"
        expected_sitemaps.add(fname)
        write_if_changed(os.path.join(SITEMAPS_DIR, fname), build_flat_sitemap_xml(entries))

    # Clean up stale sitemaps in sitemaps/
    for existing in os.listdir(SITEMAPS_DIR):
        if existing.endswith(".xml") and existing not in expected_sitemaps:
            os.remove(os.path.join(SITEMAPS_DIR, existing))

    # Deterministic gzipped JSONL dataset (mtime=0 for zero-churn diffs)
    jsonl_bytes = ("\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in records) + "\n").encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buf, mtime=0, compresslevel=6) as gz:
        gz.write(jsonl_bytes)
    gz_bytes = buf.getvalue()
    if not os.path.isfile(JSONL_GZ_FILE) or open(JSONL_GZ_FILE, "rb").read() != gz_bytes:
        with open(JSONL_GZ_FILE, "wb") as f:
            f.write(gz_bytes)

    # Render lightweight overview dashboard index.html
    source_counts = Counter(r["harvest_source"] for r in records)
    publisher_counts = Counter(r["publisher"] for r in records)
    write_if_changed(
        INDEX_HTML,
        render_index_html(records, theme_buckets, part_files, source_counts, publisher_counts),
    )

    print(
        f"[done] Complete in {time.time() - t_start:.1f}s | datasets={total:,} | "
        f"jsonl.gz={len(gz_bytes) / (1024 * 1024):.1f} MB | themes={len(theme_buckets)} | chunks={len(part_files)}",
        flush=True,
    )


if __name__ == "__main__":
    main()
