# Web Scraper + Automated Report Generator

A Python-based job market scraper that collects tech job listings from multiple sources, stores them in SQLite, deduplicates entries, and generates automated weekly reports with skill demand analysis.

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![SQLite](https://img.shields.io/badge/SQLite-Database-green?logo=sqlite)

## Features

- Multi-source job scraping pipeline (Seek, Indeed, LinkedIn, Glassdoor)
- SQLite storage with deduplication (no duplicate job listings)
- Automated weekly report generation (JSON + CSV)
- Skill demand analysis across all listings
- Salary range tracking by job title
- Scraping logs for monitoring
- Melbourne-focused job market analysis

## How to Run

```bash
cd web-scraper-report
python scraper/job_scraper.py
```

## Output

- `reports/report_YYYYMMDD.json` - Full analysis report
- `reports/skills_demand_YYYYMMDD.csv` - Skills demand CSV
- `database/jobs.db` - SQLite database with all scraped data

## Skills Demonstrated

- Web scraping pipeline design (BeautifulSoup/Selenium pattern)
- Database design with deduplication
- Data analysis with SQL aggregations
- Automated report generation
- CSV/JSON export

## Author

**Muhammad Saboor** - [GitHub](https://github.com/saboor123-123)
