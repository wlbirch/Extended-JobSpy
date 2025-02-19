from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List

app = FastAPI()

class JobRequest(BaseModel):
    site_name: List[str]
    search_term: str
    google_search_term: str
    location: str
    results_wanted: int
    hours_old: int
    country_indeed: str

@app.post("/scrape-jobs")
def scrape_jobs_endpoint(request: JobRequest):
    jobs = scrape_jobs(
        site_name=request.site_name,
        search_term=request.search_term,
        google_search_term=request.google_search_term,
        location=request.location,
        results_wanted=request.results_wanted,
        hours_old=request.hours_old,
        country_indeed=request.country_indeed,
    )
    if not jobs:
        raise HTTPException(status_code=404, detail="No jobs found")
    return jobs

@app.get("/")
def read_root():
    return {"Hello": "World"}