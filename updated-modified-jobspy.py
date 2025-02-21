import csv
import pandas as pd
from jobspy import scrape_jobs
from typing import List, Optional
from dataclasses import dataclass
from bs4 import BeautifulSoup
import requests
import re
import time

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
        location: str = "United States",
        country: str = "USA",
        results_wanted: int = 100000,
        hours_old: int = 720
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

    def scrape_batch(self, search_term: str, offset: int = 0) -> pd.DataFrame:
        """Scrape a batch of jobs with offset."""
        try:
            return scrape_jobs(
                site_name=self.sites,
                search_term=search_term,
                location=self.location,
                results_wanted=25,  # Scrape in smaller batches
                offset=offset,
                hours_old=self.hours_old,
                country_indeed=self.country,
                linkedin_fetch_description=True
            )
        except Exception as e:
            print(f"Error scraping batch at offset {offset}: {str(e)}")
            return pd.DataFrame()

    def scrape(self, search_term: str, output_file: str = "enhanced_jobs.csv"):
        """Scrape jobs with enhanced details using pagination."""
        print(f"Starting nationwide job search for term: {search_term}")
        print(f"Location: {self.location}")
        print(f"Searching jobs posted in the last {self.hours_old} hours")
        
        try:
            all_jobs = pd.DataFrame()
            offset = 0
            total_results = 0
            batch_size = 25
            
            # Collect jobs in batches
            while total_results < self.results_wanted:
                print(f"\nFetching batch starting at offset {offset}...")
                batch = self.scrape_batch(search_term, offset)
                
                if batch.empty or len(batch) == 0:
                    print("No more results found")
                    break
                    
                all_jobs = pd.concat([all_jobs, batch], ignore_index=True)
                total_results = len(all_jobs)
                print(f"Total jobs collected: {total_results}")
                
                offset += batch_size
                time.sleep(2)  # Add delay between batches to avoid rate limiting
            
            print(f"\nFound {len(all_jobs)} total jobs")
            
            # Process jobs for additional details
            valid_jobs = []
            for index, job in all_jobs.iterrows():
                try:
                    if 'description' in job and job['description']:
                        print(f"\nProcessing job {index + 1}/{len(all_jobs)}: {job.get('title', 'No title')} at {job.get('company', 'Unknown company')}")
                        details = self.extract_additional_details(job['description'])
                        
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
                print(f"Jobs saved to: {output_file}")
            else:
                print("\nNo jobs found matching criteria")
            
            return enhanced_jobs
            
        except Exception as e:
            print(f"Error during scraping: {str(e)}")
            return pd.DataFrame()

# Example usage
if __name__ == "__main__":
    scraper = EnhancedJobScraper(
        sites=["indeed", "linkedin", "zip_recruiter"],
        location="United States",
        country="USA",
        results_wanted=100000,
        hours_old=720
    )
    
    # Run the scraper with configurable search term
    jobs = scraper.scrape(search_term="Specialist")