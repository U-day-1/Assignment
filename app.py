import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time

class WebScraper:
    def __init__(self):
        self.data = []

    def fetch_page(self, url, headers=None):
        """Fetch HTML content from a URL."""
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None

    def parse_data(self, html_content, platform):
        """Extract relevant fields based on the platform."""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        if platform == "books":
            # Parse data from "Books to Scrape"
            for book in soup.select(".product_pod"):
                name = book.h3.a.attrs['title']
                price = book.select_one(".price_color").get_text(strip=True)
                availability = book.select_one(".availability").get_text(strip=True)

                self.data.append({
                    "Name": name,
                    "Price": price,
                    "Availability": availability
                })

        elif platform == "quotes":
            # Parse data from "Quotes to Scrape"
            for quote in soup.select(".quote"):
                text = quote.select_one(".text").get_text(strip=True)
                author = quote.select_one(".author").get_text(strip=True)
                tags = ", ".join(tag.get_text(strip=True) for tag in quote.select(".tags .tag"))

                self.data.append({
                    "Text": text,
                    "Author": author,
                    "Tags": tags
                })

        elif platform == "imdb":
            # Parse data from "IMDB Top 250 Movies"
            for movie in soup.select(".lister-list tr"):
                rank = movie.select_one(".titleColumn").get_text(strip=True).split(".")[0]
                title = movie.select_one(".titleColumn a").get_text(strip=True)
                rating = movie.select_one(".imdbRating strong").get_text(strip=True)

                self.data.append({
                    "Rank": rank,
                    "Title": title,
                    "Rating": rating
                })

        elif platform == "global_news":
            # Parse data from "Global Giving"
            for project in soup.select(".project-card"):
                title = project.select_one(".project-title").get_text(strip=True)
                theme = project.select_one(".project-theme").get_text(strip=True)
                summary = project.select_one(".project-description").get_text(strip=True)

                self.data.append({
                    "Title": title,
                    "Theme": theme,
                    "Summary": summary
                })

    def clean_data(self):
        """Process the data to handle missing, duplicate, or inconsistent values."""
        df = pd.DataFrame(self.data)
        
        # Ensure all expected columns exist
        if "Name" in df.columns:
            expected_columns = ["Name", "Price", "Availability"]
        elif "Text" in df.columns:
            expected_columns = ["Text", "Author", "Tags"]
        elif "Rank" in df.columns:
            expected_columns = ["Rank", "Title", "Rating"]
        else:
            expected_columns = ["Title", "Theme", "Summary"]

        for col in expected_columns:
            if col not in df.columns:
                df[col] = "Unknown"
        
        # Drop duplicates
        df.drop_duplicates(inplace=True)
        
        # Handle missing values (fill with 'Unknown')
        df.fillna("Unknown", inplace=True)

        self.data = df

    def save_data(self, filename="scraped_data.csv"):
        """Save the processed data to a file."""
        self.data.to_csv(filename, index=False)
        print(f"Data saved to {filename}")

    def run(self, urls, platform, headers=None):
        """Main method to run the scraper."""
        for url in urls:
            print(f"Fetching data from {url}...")
            html_content = self.fetch_page(url, headers)
            if html_content:
                self.parse_data(html_content, platform)
            time.sleep(1)  # Be respectful of server load

        if not self.data:
            print("No data extracted. Skipping cleaning and saving.")
            return

        print("Cleaning data...")
        self.clean_data()
        self.save_data()

# Example usage
if __name__ == "__main__":
    # Books to Scrape
    books_urls = ["http://books.toscrape.com/catalogue/page-1.html", "http://books.toscrape.com/catalogue/page-2.html"]
    books_scraper = WebScraper()
    books_scraper.run(books_urls, platform="books")

