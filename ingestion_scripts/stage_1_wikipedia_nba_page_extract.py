
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import pdb
import time
from random import uniform, shuffle
from tqdm import tqdm
import os
from nba_data import NbaData

class WikipediaSeasonSummaryExtractor(NbaData):
    def __init__(self):
        super().__init__()
        self.start_year = 1970
        self.current_year = datetime.now().year
        self.wikipedia_data = self.data_directory / 'nba_com/stage_1_raw_wikipedia_html' 


    def generate_urls(self):
        self.urls = []
        for year in range(self.start_year, self.current_year + 1):
            year_part_1 = year
            year_part_2 = int(str(year)[2:]) + 1
            if year_part_2 < 10:
                year_part_2 = f'0{year_part_2}'
            self.urls.append(f'{year_part_1}-{year_part_2}_NBA_season')


    def filter_out_pulled_wikipedia_pages(self):
        '''
        This method filters out games that have already been scraped from the 'game_links' list, by
        comparing the list of games that have been scraped from the 'stage_2_raw_game_json' directory
        to the list of games that have been pulled.
        '''
        # check to make sure this data hasn't already been scraped
        wiki_pages_pulled = [x.split('.')[0] for x in os.listdir(self.wikipedia_data)]
        self.urls = [x for x in self.urls if x not in wiki_pages_pulled]
        shuffle(self.urls)

    def extract_wikipedia_pages(self):
        for url in tqdm(self.urls):
            url = f'https://en.wikipedia.org/wiki/{url}'
            response = requests.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                file_name = url.split('/')[-1]
                output_file_path = f'{self.wikipedia_data}/{file_name}.html'
                with open(output_file_path, "w", encoding='utf-8') as file:
                    file.write(str(soup))
                time.sleep(uniform(5, 10))
    

    def run_all(self):
        self.generate_urls()
        self.filter_out_pulled_wikipedia_pages()
        self.extract_wikipedia_pages()


if __name__ == '__main__':
    w = WikipediaSeasonSummaryExtractor()
    w.run_all()
