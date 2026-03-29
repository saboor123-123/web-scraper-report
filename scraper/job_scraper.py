"""
Job Market Web Scraper & Automated Report Generator
Scrapes tech job listings, stores in SQLite, and generates weekly reports.

Uses a simulated scraper that generates realistic data from multiple sources,
demonstrating the full scraping pipeline pattern.

Author: Muhammad Saboor
"""

import sqlite3
import random
import json
import os
import csv
from datetime import datetime, timedelta
from collections import Counter

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "jobs.db")
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
REPORT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports")


class JobScraper:
    """
    Job listing scraper that collects data from job boards.

    In production, this would use BeautifulSoup/Selenium to scrape real sites.
    This version simulates the scraping pattern with realistic data to
    demonstrate the full pipeline without violating any site's ToS.
    """

    SOURCES = ["Seek.com.au", "Indeed.com.au", "LinkedIn", "GlassDoor"]

    TITLES = [
        "Data Analyst", "Junior Data Analyst", "Senior Data Analyst",
        "Data Scientist", "Machine Learning Engineer", "Data Engineer",
        "Software Developer", "Junior Developer", "Full Stack Developer",
        "Frontend Developer", "Backend Developer", "DevOps Engineer",
        "IT Support Specialist", "System Administrator", "Cloud Engineer",
        "Cybersecurity Analyst", "QA Engineer", "Business Analyst",
        "Product Analyst", "Web Developer", "Python Developer",
    ]

    COMPANIES_MELBOURNE = [
        "ANZ Banking Group", "Telstra", "NAB", "BHP", "Woolworths",
        "REA Group", "Xero", "Canva", "Atlassian", "Culture Amp",
        "SEEK", "Carsales", "Suncorp", "Medibank", "Deloitte Melbourne",
        "PwC Melbourne", "Accenture AU", "IBM Australia", "Infosys Melbourne",
        "TechStartup Co", "DataFlow Analytics", "CloudNine Solutions",
    ]

    SKILLS_MAP = {
        "Data": ["Python", "SQL", "Excel", "Tableau", "Power BI", "R", "Pandas", "Statistics"],
        "Software": ["Python", "JavaScript", "React", "Node.js", "Java", "C#", "Docker", "Git"],
        "IT": ["Windows Server", "Linux", "Active Directory", "Networking", "Office 365"],
        "Cloud": ["AWS", "Azure", "GCP", "Docker", "Kubernetes", "Terraform"],
    }

    def __init__(self):
        self.init_database()

    def init_database(self):
        """Create the database tables."""
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS scraped_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                company TEXT NOT NULL,
                location TEXT NOT NULL,
                salary_min INTEGER,
                salary_max INTEGER,
                job_type TEXT,
                skills TEXT,
                source TEXT NOT NULL,
                url TEXT,
                date_scraped TEXT NOT NULL,
                date_posted TEXT,
                is_new BOOLEAN DEFAULT 1,
                UNIQUE(title, company, source)
            );

            CREATE TABLE IF NOT EXISTS scrape_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                jobs_found INTEGER NOT NULL,
                new_jobs INTEGER NOT NULL,
                timestamp TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_date ON scraped_jobs(date_scraped);
            CREATE INDEX IF NOT EXISTS idx_jobs_title ON scraped_jobs(title);
            CREATE INDEX IF NOT EXISTS idx_jobs_source ON scraped_jobs(source);
        """)

        conn.commit()
        conn.close()

    def scrape_source(self, source):
        """
        Simulate scraping a single job board source.

        In production, this method would:
        1. Send HTTP request to the job board
        2. Parse HTML with BeautifulSoup
        3. Extract job details from each listing
        4. Handle pagination
        5. Respect robots.txt and rate limits
        """
        print(f"  Scraping {source}...")
        jobs = []
        num_jobs = random.randint(15, 40)

        for _ in range(num_jobs):
            title = random.choice(self.TITLES)
            category = "Data" if "Data" in title or "ML" in title else \
                       "Cloud" if "Cloud" in title else \
                       "IT" if "IT" in title or "System" in title else "Software"

            salary_min = random.randint(45, 120) * 1000
            salary_max = salary_min + random.randint(15, 40) * 1000

            skills = random.sample(self.SKILLS_MAP[category], min(4, len(self.SKILLS_MAP[category])))
            days_ago = random.randint(0, 14)

            jobs.append({
                "title": title,
                "company": random.choice(self.COMPANIES_MELBOURNE),
                "location": random.choice(["Melbourne CBD", "Richmond", "South Melbourne",
                                            "Docklands", "Remote", "Hybrid - Melbourne"]),
                "salary_min": salary_min,
                "salary_max": salary_max,
                "job_type": random.choice(["Full-time", "Part-time", "Contract", "Casual"]),
                "skills": "|".join(skills),
                "source": source,
                "url": f"https://{source.lower().replace(' ', '')}/job/{random.randint(10000, 99999)}",
                "date_posted": (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d"),
            })

        return jobs

    def scrape_all(self):
        """Run the full scraping pipeline across all sources."""
        print(f"\n{'='*60}")
        print(f"JOB SCRAPING RUN - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        total_found = 0
        total_new = 0

        for source in self.SOURCES:
            jobs = self.scrape_source(source)
            new_count = 0

            for job in jobs:
                try:
                    cursor.execute("""
                        INSERT INTO scraped_jobs
                        (title, company, location, salary_min, salary_max, job_type,
                         skills, source, url, date_scraped, date_posted)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        job["title"], job["company"], job["location"],
                        job["salary_min"], job["salary_max"], job["job_type"],
                        job["skills"], job["source"], job["url"],
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        job["date_posted"],
                    ))
                    new_count += 1
                except sqlite3.IntegrityError:
                    pass  # Duplicate - already scraped

            # Log the scrape
            cursor.execute(
                "INSERT INTO scrape_logs (source, jobs_found, new_jobs, timestamp) VALUES (?, ?, ?, ?)",
                (source, len(jobs), new_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )

            total_found += len(jobs)
            total_new += new_count
            print(f"    Found: {len(jobs)} | New: {new_count}")

        conn.commit()
        conn.close()

        print(f"\n  TOTAL: {total_found} found, {total_new} new jobs added")
        return {"total_found": total_found, "new_jobs": total_new}

    def generate_report(self):
        """Generate a comprehensive weekly report."""
        print(f"\nGenerating weekly report...")

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Overall stats
        total_jobs = cursor.execute("SELECT COUNT(*) FROM scraped_jobs").fetchone()[0]
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        new_this_week = cursor.execute(
            "SELECT COUNT(*) FROM scraped_jobs WHERE date_scraped >= ?", (week_ago,)
        ).fetchone()[0]

        # Top job titles
        top_titles = cursor.execute("""
            SELECT title, COUNT(*) as count,
                   ROUND(AVG((salary_min + salary_max) / 2)) as avg_salary
            FROM scraped_jobs
            GROUP BY title ORDER BY count DESC LIMIT 10
        """).fetchall()

        # Top companies hiring
        top_companies = cursor.execute("""
            SELECT company, COUNT(*) as openings
            FROM scraped_jobs
            GROUP BY company ORDER BY openings DESC LIMIT 10
        """).fetchall()

        # Most in-demand skills
        all_skills = cursor.execute("SELECT skills FROM scraped_jobs").fetchall()
        skill_counter = Counter()
        for row in all_skills:
            if row["skills"]:
                for skill in row["skills"].split("|"):
                    skill_counter[skill.strip()] += 1
        top_skills = skill_counter.most_common(15)

        # Salary ranges by title
        salary_data = cursor.execute("""
            SELECT title,
                   ROUND(AVG(salary_min)) as avg_min,
                   ROUND(AVG(salary_max)) as avg_max,
                   ROUND(AVG((salary_min + salary_max) / 2)) as avg_mid
            FROM scraped_jobs
            GROUP BY title ORDER BY avg_mid DESC LIMIT 10
        """).fetchall()

        # Jobs by source
        by_source = cursor.execute("""
            SELECT source, COUNT(*) as count
            FROM scraped_jobs GROUP BY source ORDER BY count DESC
        """).fetchall()

        conn.close()

        # Build report
        report = {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "period": f"{week_ago} to {datetime.now().strftime('%Y-%m-%d')}",
            "summary": {
                "total_jobs_in_database": total_jobs,
                "new_this_week": new_this_week,
                "sources_tracked": len(self.SOURCES),
            },
            "top_titles": [{"title": t["title"], "count": t["count"], "avg_salary": t["avg_salary"]} for t in top_titles],
            "top_companies": [{"company": c["company"], "openings": c["openings"]} for c in top_companies],
            "top_skills": [{"skill": s[0], "demand": s[1]} for s in top_skills],
            "salary_ranges": [{"title": s["title"], "avg_min": s["avg_min"], "avg_max": s["avg_max"]} for s in salary_data],
            "by_source": [{"source": s["source"], "count": s["count"]} for s in by_source],
        }

        # Save JSON report
        os.makedirs(REPORT_DIR, exist_ok=True)
        report_file = os.path.join(REPORT_DIR, f"report_{datetime.now().strftime('%Y%m%d')}.json")
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2)

        # Save CSV summary
        csv_file = os.path.join(REPORT_DIR, f"skills_demand_{datetime.now().strftime('%Y%m%d')}.csv")
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Skill", "Demand (Job Count)"])
            for skill, count in top_skills:
                writer.writerow([skill, count])

        # Print report
        print(f"\n{'='*60}")
        print(f"WEEKLY JOB MARKET REPORT")
        print(f"Period: {report['period']}")
        print(f"{'='*60}")
        print(f"\nTotal Jobs in Database: {total_jobs}")
        print(f"New This Week: {new_this_week}")

        print(f"\nTop 10 Job Titles:")
        for t in top_titles:
            print(f"  {t['title']}: {t['count']} listings (${t['avg_salary']:,.0f} avg)")

        print(f"\nTop 10 In-Demand Skills:")
        for skill, count in top_skills[:10]:
            print(f"  {skill}: {count} listings")

        print(f"\nTop Hiring Companies:")
        for c in top_companies[:5]:
            print(f"  {c['company']}: {c['openings']} openings")

        print(f"\nReport saved to: {report_file}")
        print(f"Skills CSV saved to: {csv_file}")

        return report


def main():
    scraper = JobScraper()

    # Run scraping
    scraper.scrape_all()

    # Generate report
    scraper.generate_report()


if __name__ == "__main__":
    main()
