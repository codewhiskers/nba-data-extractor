import requests
from bs4 import BeautifulSoup
from datetime import timedelta
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
import json
import time
import os
from tqdm import tqdm
from random import uniform, choice, shuffle
import pdb
import re
from datetime import datetime
import sys
from pathlib import Path
# Add the parent directory to the system path
# parent_dir = Path(__file__).resolve().parent.parent
# sys.path.append(str(parent_dir))
from nba_com_main import NbaComMain


class NbaComDatesExtractor(NbaComMain):
    def __init__(self): 
        super().__init__()
        self.get_user_agent_list()
        # self.get_seasons()
        self.dates = []
        # Data Needed for this ingestion (Input Path)
        self.wikipedia_data_fp = self.data_directory / 'nba_com/stage_1_raw_wikipedia_html' 
        # Export Data Directory (Output Path)
        self.nba_com_date_data_fp = self.data_directory / 'nba_com/stage_2_raw_date_json'
       

    def extract_game_dates(self, start_year):
        wikipedia_pages = [x for x in os.listdir(self.wikipedia_data_fp) if '.html' in x]
        for page in wikipedia_pages:
            file = open(f'{self.wikipedia_data_fp}/{page}')
            # Get the first and last dates
            try:
                soup = BeautifulSoup(file, 'html.parser')
                # Find the 'Duration' row by navigating from its 'th' to its 'td' sibling
                duration_cell = soup.find('th', string='Duration').find_next_sibling('td')
                # Extract all dates
                text_content = duration_cell.get_text(strip=True)
                dates = re.findall(r'\w+\s\d{1,2}(?:–\d{1,2})?,\s\d{4}', text_content)
                first_date = datetime.strptime(dates[0], '%B %d, %Y').date()
                if first_date.year < start_year:
                    continue
                last_date = datetime.strptime(re.sub("\d{1,2}–", '', dates[-1]), '%B %d, %Y').date()
            except Exception as e:
                logging.info(f"Error processing file {file}: {e}")
            # Get dates in between the first and last date
            dates = [first_date + timedelta(days=x) for x in range((last_date-first_date).days + 1)]
            # Filter out dates that are in the future
            dates = [str(date) for date in dates if date < datetime.now().date()]
            self.dates += dates

    def filter_out_pulled_dates_from_date_links(self):
        '''
        This method filters out dates that have already been scraped from the 'stage_1_raw_date_json' directory.
        '''
        dates_pulled = [x for x in os.listdir(self.nba_com_date_data_fp) if 'nba_com' in x]
        dates_pulled = [x.split('nba_com_')[1].split('.')[0] for x in dates_pulled]
        self.dates = [str(x) for x in self.dates if x not in dates_pulled]
        shuffle(self.dates)

    def get_date_data(self):
        '''
        Pull the json file containing all the links for the games
        on the particular date requested. 
        URL Format: 'http://nba.com/games?date={}'
        '''
        for date in tqdm(self.dates):
            user_agent = choice(self.user_agent_list)
            headers = {'User-Agent': user_agent}
            link = 'http://nba.com/games?date={}'.format(date)
            try:
                response = requests.get(link, headers = headers)
            except Exception as e:
                print(e)
                exit(1)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                date_json_file = soup.find('script', {'id' : '__NEXT_DATA__'})
                date_json_file = date_json_file.getText()
                date_json_file = json.loads(date_json_file)

                file_name = f'nba_com_{date}.json'
                output_file_path = f'{self.nba_com_date_data_fp}/{file_name}'
                with open(output_file_path, 'w') as outfile:
                    json.dump(date_json_file, outfile)
                time.sleep(uniform(5, 10))
            else:
                error_string = f'Error for date {str(date)} with status code = {response.status_code}'
                print(error_string)
                time.sleep(60)

    def extract(self):
        """
        Run Stage 2: Generate and extract date links from nba.com.
        """
        logging.info('Beginning Stage 2')

        logging.info('Generating date links to pull from nba.com')
        self.extract_game_dates(start_year=2022)
        logging.info('Completed')

        logging.info('Filtering out Dates that have already been extracted')
        self.filter_out_pulled_dates_from_date_links()
        if len(self.dates) == 0:
            logging.info('All dates have been pulled')
        else:
            logging.info(f'Number of Dates to Pull: {len(self.dates)}')
            logging.info('Extracting Date pages from nba.com')
            self.get_date_data()
            logging.info('Completed')

        logging.info('Stage 2 Complete')
        logging.info('====================================')


if __name__ == "__main__":
    w = NbaComDatesExtractor()
    w.extract()





