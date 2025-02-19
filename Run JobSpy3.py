import csv
from jobspy import scrape_jobs

# Modify the function definition
def scrape_jobs(site_name, search_term, google_search_term, location, results_wanted, hours_old, country_indeed):
    # existing code

# Pass the search term when calling the function
search_term = input("Enter the job search term: ")
jobs = scrape_jobs(
    site_name=["indeed", "linkedin", "zip_recruiter", "glassdoor", "google"],
    search_term=search_term,
    google_search_term=f"{search_term} jobs near Nashville, TN since yesterday",
    location="Nashville, TN",
    results_wanted=5,
    hours_old=72,
    country_indeed='USA',

    # linkedin_fetch_description=True # gets more info such as description, direct job url (slower)
    # proxies=["208.195.175.46:65095", "208.195.175.45:65095", "localhost"],
)
print(f"Found {len(jobs)} jobs")
print(jobs.head())
jobs.to_csv("jobs.csv", quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False) # to_excel