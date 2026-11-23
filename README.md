# NASA Open Data Portal (`data.nasa.gov`) Harvester, Mirror & Sitemap Pipeline

[![Update NASA Open Data Portal Catalog & Sitemap](https://github.com/Oht8wooWi8yait9n/data-nasa-gov/actions/workflows/update-sitemap.yml/badge.svg)](https://github.com/Oht8wooWi8yait9n/data-nasa-gov/actions/workflows/update-sitemap.yml)

Automated CKAN harvester, pre-rendered static HTML mirror, compressed JSONL catalog, and flat XML sitemaps for the **NASA Open Data Portal** ([https://data.nasa.gov](https://data.nasa.gov)) — NASA's central public catalog of **36,514 datasets** across Earth Science, Planetary Science, Heliophysics, Astrophysics, Biological & Physical Sciences, Space Science, Aerospace, and NASA Operations.

---

## 1. Why a Static Mirror + URL Rewrite Is Required

1. **No Native XML Sitemap (`404`)**:
   - `https://data.nasa.gov/sitemap.xml` returns `404 Not Found`, preventing standard sitemap crawlers (including Onyx Web Connector) from discovering datasets directly.
2. **Slow Live CKAN Pages & `Crawl-Delay: 10`**:
   - Uncached live dataset pages (`https://data.nasa.gov/dataset/<slug>`) take **6–9 seconds each** and `robots.txt` sets `Crawl-Delay: 10`. Live crawling all 36,514 dataset pages directly would take days and include ~50 KB of noisy CKAN navigation, sidebar, and view-counter boilerplate per page.
3. **Authoritative CKAN Solr Field-List Harvesting (~3.5 Minutes Total)**:
   - [`generate_data_nasa_gov_sitemap.py`](generate_data_nasa_gov_sitemap.py) harvests all **36,514** canonical dataset slugs, metadata, DCAT extras, and resource distributions directly from CKAN's Solr search API (`GET /api/3/action/package_search?rows=1000&sort=id+asc&fl=...`). It intentionally bypasses NASA's nightly `/data.json` bulk export because `/data.json` periodically scrambles or drops `distribution` entries across unrelated datasets, whereas CKAN Solr's `res_url`/`res_format` fields remain authoritative and zero-churn.
4. **1-to-1 Canonical URL Rewrite for Onyx**:
   - Every pre-rendered page on GitHub Pages mirrors the exact CKAN URL route on `https://data.nasa.gov/`:
     - `https://oht8woowi8yait9n.github.io/data-nasa-gov/dataset/<slug>` &rarr; `https://data.nasa.gov/dataset/<slug>`

---

## 2. Catalog Coverage by NASA Theme

| NASA Theme | Datasets | Flat XML Sitemap URL |
| :--- | ---: | :--- |
| **Earth Science** | **20,772** | `https://oht8woowi8yait9n.github.io/data-nasa-gov/sitemaps/theme_earth-science.xml` |
| **Planetary Science** | **6,214** | `https://oht8woowi8yait9n.github.io/data-nasa-gov/sitemaps/theme_planetary-science.xml` |
| **Heliophysics** | **3,305** | `https://oht8woowi8yait9n.github.io/data-nasa-gov/sitemaps/theme_heliophysics.xml` |
| **Geospatial** | **2,363** | `https://oht8woowi8yait9n.github.io/data-nasa-gov/sitemaps/theme_geospatial.xml` |
| **Astrophysics** | **1,459** | `https://oht8woowi8yait9n.github.io/data-nasa-gov/sitemaps/theme_astrophysics.xml` |
| **Other / Specialized Themes** | **1,012** | `https://oht8woowi8yait9n.github.io/data-nasa-gov/sitemaps/theme_other.xml` |
| **Space Science** | **440** | `https://oht8woowi8yait9n.github.io/data-nasa-gov/sitemaps/theme_space-science.xml` |
| **Management/Operations** | **379** | `https://oht8woowi8yait9n.github.io/data-nasa-gov/sitemaps/theme_management-operations.xml` |
| **Biological and Physical Sciences** | **346** | `https://oht8woowi8yait9n.github.io/data-nasa-gov/sitemaps/theme_biological-and-physical-sciences.xml` |
| **Aerospace** | **224** | `https://oht8woowi8yait9n.github.io/data-nasa-gov/sitemaps/theme_aerospace.xml` |
| **TOTAL UNIQUE DATASETS** | **36,514** | `https://oht8woowi8yait9n.github.io/data-nasa-gov/sitemap.xml` |

*(Some datasets carry multiple NASA themes, so theme totals exceed the unique catalog count of 36,514.)*

---

## 3. Live Endpoints & Flat XML Sitemaps

All sitemaps use flat `<urlset>` XML (no `<sitemapindex>` nesting) for direct compatibility with Onyx Web Connector.

| Resource | URL |
| :--- | :--- |
| **Catalog Overview Dashboard** | [https://oht8woowi8yait9n.github.io/data-nasa-gov/](https://oht8woowi8yait9n.github.io/data-nasa-gov/) |
| **Complete Mirror XML Sitemap (Recommended for Onyx — 36,514 URLs)** | `https://oht8woowi8yait9n.github.io/data-nasa-gov/sitemap.xml` |
| **Alias Mirror XML Sitemap** | `https://oht8woowi8yait9n.github.io/data-nasa-gov/data_nasa_gov_sitemap.xml` |
| **Direct Live NASA XML Sitemap (`data.nasa.gov`)** | `https://oht8woowi8yait9n.github.io/data-nasa-gov/data_nasa_gov_direct_sitemap.xml` |
| **Staged 10,000-URL Chunks (Zero Overlap)** | `sitemaps/data_nasa_gov_part1.xml` (10,000) &bull; `part2.xml` (10,000) &bull; `part3.xml` (10,000) &bull; `part4.xml` (6,514) |
| **Plain-Text URL Indexes** | [`data_nasa_gov_urls.txt`](data_nasa_gov_urls.txt) &bull; [`data_nasa_gov_direct_urls.txt`](data_nasa_gov_direct_urls.txt) |
| **Compressed Unified JSONL Dataset** | [`data_nasa_gov_records.jsonl.gz`](data_nasa_gov_records.jsonl.gz) |

---

## 4. Onyx Web Connector & URL Rewrite Configuration

1. **Onyx Web Connector**:
   - **Connector Name**: `NASA Open Data Portal (data.nasa.gov)`
   - **Scrape Type / Indexing Mode**: `Sitemap`
   - **Sitemap URL (Complete Catalog — 36,514 URLs)**:
     ```text
     https://oht8woowi8yait9n.github.io/data-nasa-gov/sitemap.xml
     ```
   - *(Optional: use the staged 10k chunks `sitemaps/data_nasa_gov_part1.xml`–`part4.xml` or any domain sitemap in Section 2 to ingest specific themes such as `theme_biological-and-physical-sciences.xml`.)*
2. **Onyx URL Rewrite Rule**:
   - **Source Prefix**:
     ```text
     https://oht8woowi8yait9n.github.io/data-nasa-gov/
     ```
   - **Target Prefix**:
     ```text
     https://data.nasa.gov/
     ```

---

## 5. Automation & Zero-Churn Maintenance

A GitHub Actions workflow ([`.github/workflows/update-sitemap.yml`](.github/workflows/update-sitemap.yml)) runs weekly on **Sundays at 06:00 UTC** (and on manual `workflow_dispatch`). It excludes volatile CKAN view counts (`tracking_summary`), prefers upstream DCAT `modified` timestamps, sorts all records deterministically, writes gzipped JSONL with `mtime=0`, and commits only when upstream dataset metadata actually changes (with a 45-day keepalive safeguard).
