import csv
import pandas as pd
from jobspy import scrape_jobs
from typing import List, Optional
from dataclasses import dataclass
from bs4 import BeautifulSoup
import requests
import re

@dataclass
class JobDetails:
    summary: Optional[str] = None
    essential_functions: Optional[List[str]] = None
    education: Optional[str] = None
    experience: Optional[str] = None

class EnhancedJobScraper:
    def __init__(
        self,
        sites: List[str] = ["indeed", "linkedin", "zip_recruiter"],
        location: str = "Nashville, TN",
        country: str = "USA",
        results_wanted: int = 5,
        hours_old: int = 72
    ):
        self.sites = sites
        self.location = location
        self.country = country
        self.results_wanted = results_wanted
        self.hours_old = hours_old

    def safe_extract(self, text: str, patterns: List[str]) -> Optional[str]:
        """Safely extract text using multiple patterns."""
        for pattern in patterns:
            try:
                match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if match:
                    # If the pattern has a capture group, use it; otherwise use the whole match
                    return (match.group(1) if match.groups() else match.group(0)).strip()
            except (IndexError, AttributeError):
                continue
        return None

    def extract_additional_details(self, job_description: str) -> JobDetails:
        """Extract additional job details from the description."""
        try:
            soup = BeautifulSoup(job_description, 'html.parser')
            text = soup.get_text()
            
            details = JobDetails()
            
            # Extract summary
            summary_patterns = [
                r'^([^.]+(?:[.]+[^.]+){0,2}\.)',
                r'^(.{50,200}(?:[.]|$))'
            ]
            details.summary = self.safe_extract(text, summary_patterns) or text[:200].strip()
            
            # Extract essential functions
            functions_text = None
            functions_patterns = [
                r'(?:Essential Functions|Responsibilities|Duties|Key Responsibilities|What You\'ll Do)[:]\s*((?:\s*[-•\*]\s*[^\n]+\n*)+)',
                r'(?:\n\s*[-•\*]\s*[^\n]+\n*){2,}'
            ]
            
            for pattern in functions_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    functions_text = matches[0]
                    break
            
            if functions_text:
                bullet_points = re.findall(r'[-•\*]\s*([^\n]+)', functions_text)
                details.essential_functions = [point.strip() for point in bullet_points if point.strip()]
            
            # Extract education
            education_patterns = [
                r'Education[^:]*:\s*([^\n\r•]*)',
                r'Qualifications[^:]*:\s*([^\n\r•]*)',
                r'(?:Bachelor|Master|PhD|degree)[^•\n.]+',
                r'Education(?:al)?\s+Requirements?:[^•\n.]+'
            ]
            details.education = self.safe_extract(text, education_patterns)
            
            # Extract experience
            experience_patterns = [
                r'Experience[^:]*:\s*([^\n\r•]*)',
                r'Requirements?[^:]*:\s*([^\n\r•]*)',
                r'\d+[+]?\s+years?(?:\s+of)?\s+experience[^•\n.]+',
                r'[Mm]inimum\s+(?:\d+|one|two|three|four|five)\s+years?[^•\n.]+'
            ]
            details.experience = self.safe_extract(text, experience_patterns)
            
            return details
            
        except Exception as e:
            print(f"Error extracting details: {str(e)}")
            return JobDetails()

    def scrape(self, search_term: str, output_file: str = "enhanced_jobs.csv"):
        """Scrape jobs with enhanced details."""
        print(f"Starting job search for term: {search_term}")
        
        try:
            # Initial job scraping using JobSpy
            jobs = scrape_jobs(
                site_name=self.sites,
                search_term=search_term,
                location=self.location,
                results_wanted=self.results_wanted,
                hours_old=self.hours_old,
                country_indeed=self.country,
                linkedin_fetch_description=True
            )
            
            print(f"Found {len(jobs)} initial jobs")
            
            # Enhanced dataframe with new columns
            jobs['summary'] = None
            jobs['essential_functions'] = None
            jobs['education'] = None
            jobs['experience'] = None
            
            # Process each job to extract additional details
            valid_jobs = []
            for index, job in jobs.iterrows():
                try:
                    if 'description' in job and job['description']:
                        print(f"\nProcessing job {index + 1}/{len(jobs)}: {job.get('title', 'No title')} at {job.get('company', 'Unknown company')}")
                        details = self.extract_additional_details(job['description'])
                        
                        # More lenient validation - keep job if we have at least a summary
                        if details.summary:
                            job['summary'] = details.summary
                            job['essential_functions'] = str(details.essential_functions) if details.essential_functions else None
                            job['education'] = details.education if details.education else "Not specified"
                            job['experience'] = details.experience if details.experience else "Not specified"
                            valid_jobs.append(job)
                            print("✓ Successfully extracted details")
                        else:
                            print("✗ No summary found")
                except Exception as e:
                    print(f"Error processing job {index + 1}: {str(e)}")
                    continue
            
            # Create new dataframe with valid jobs
            enhanced_jobs = pd.DataFrame(valid_jobs)
            
            # Save to CSV
            if not enhanced_jobs.empty:
                enhanced_jobs.to_csv(
                    output_file,
                    quoting=csv.QUOTE_NONNUMERIC,
                    escapechar="\\",
                    index=False
                )
                print(f"\nSuccessfully saved {len(enhanced_jobs)} jobs to {output_file}")
            else:
                print("\nNo jobs found matching criteria")
            
            return enhanced_jobs
            
        except Exception as e:
            print(f"Error during scraping: {str(e)}")
            return pd.DataFrame()

# Example usage
if __name__ == "__main__":
    scraper = EnhancedJobScraper(
        sites=["indeed", "linkedin", "zip_recruiter", "glassdoor"],
        location="Nashville, TN",
        results_wanted=10,
        hours_old=72
    )
    
    # Run the scraper with configurable search term
    jobs = scraper.scrape(search_term="Specialist")