import requests
from bs4 import BeautifulSoup
from datetime import timedelta
import json
import time
import os
from tqdm import tqdm
from random import uniform, choice, shuffle
from nba_data import NbaData
import pdb
import re
from datetime import datetime


class NbaComDatesExtractor(NbaData):
    def __init__(self): 
        super().__init__()
        self.get_user_agent_list()
        self.get_seasons()
        self.dates = []
        # Data Needed for this ingestion (Input Path)
        self.wikipedia_data_fp = self.data_directory / 'nba_com/stage_1_raw_wikipedia_html' 
        # Export Data Directory (Output Path)
        self.nba_com_date_data_fp = self.data_directory / 'nba_com/stage_2_raw_date_json'
       

    def extract_game_dates(self, start_year):
        wikipedia_pages = os.listdir(self.wikipedia_data_fp)
        for page in wikipedia_pages:
            file = open(f'{self.wikipedia_data_fp}/{page}')
            
            soup = BeautifulSoup(file, 'html.parser')
            # Find the 'Duration' row by navigating from its 'th' to its 'td' sibling
            duration_cell = soup.find('th', text='Duration').find_next_sibling('td')
            # Extract all dates
            text_content = duration_cell.get_text(strip=True)
            dates = re.findall(r'\w+\s\d{1,2}(?:–\d{1,2})?,\s\d{4}', text_content)
            
            # Get the first and last dates
            try:
                first_date = datetime.strptime(dates[0], '%B %d, %Y').date()
                if first_date.year < start_year:
                    continue
                last_date = datetime.strptime(re.sub("\d{1,2}–", '', dates[-1]), '%B %d, %Y').date()
            except:
                pdb.set_trace()
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

    def RunNbaDateExtractor(self):
        self.extract_game_dates(start_year = 2022)
        self.filter_out_pulled_dates_from_date_links()
        self.get_date_data()

if __name__ == "__main__":
    extractor = NbaComDatesExtractor()
    extractor.RunNbaDateExtractor()





