import requests
import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
from bs4 import BeautifulSoup
from datetime import date
import json
import time
import os
from tqdm import tqdm
from random import uniform, choice, shuffle
import pdb
import sys
from pathlib import Path
# Add the parent directory to the system path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from nba_data import NbaData


class NbaComGamesExtractor(NbaData):
    def __init__(self): 
        super().__init__()
        self.get_user_agent_list()
        self.game_links = []
        # Data Needed for this ingestion (Input Path)
        self.nba_com_date_data_fp = self.data_directory / 'nba_com/stage_2_raw_date_json'
        # Export Data Directory (Output Path)
        self.nba_com_game_data_fp = self.data_directory / 'nba_com/stage_3_raw_game_json'
    
    def extract_game_links(self):
        '''
        This method extracts game links from JSON files located in the 'stage_1_raw_date_json' directory,
        and appends them to the 'game_links' list.
        '''
        date_files = [x for x in os.listdir(self.nba_com_date_data_fp) if 'nba_com' in x]
        for date_file in date_files:
            date = date_file.split('nba_com_')[1].split('.')[0]
            file = '{}/{}'.format(self.nba_com_date_data_fp, date_file)
            f = open(file)
            date_json_file = json.load(f)
            if date_json_file['props']['pageProps']['gameCardFeed']['modules'] == []:
                continue
            game_cards = date_json_file['props']['pageProps']['gameCardFeed']['modules'][0]['cards']
            for game_card in game_cards:
                if game_card['cardData']['actions'] == []:
                    continue
                game_link = game_card['cardData']['actions'][2]['resourceLocator']['resourceUrl']
                game_link = game_link.split('game/')[1]
                game_link = '{}-{}'.format(date, game_link)
                self.game_links.append(game_link)

    def filter_out_pulled_games_from_game_links(self):
        '''
        This method filters out games that have already been scraped from the 'game_links' list, by
        comparing the list of games that have been scraped from the 'stage_2_raw_game_json' directory
        to the list of games that have been pulled.
        '''
        # check to make sure this data hasn't already been scraped
        games_links_pulled = [x.split('.')[0] for x in os.listdir(self.nba_com_game_data_fp)]
        self.game_links = [x for x in self.game_links if x not in games_links_pulled]
        shuffle(self.game_links)

    def get_game_data(self):
        '''
        Pull the json file containing all the links for the games
        on the particular date requested. 
        URL Format: 'http://nba.com/games?date={}'
        '''
        for game_link in tqdm(self.game_links):
            user_agent = choice(self.user_agent_list)
            headers = {'User-Agent': user_agent}
            game_url = '-'.join(game_link.split('-')[3:])
            game_url = 'http://nba.com/game/{}'.format(game_url)
            try:
                response = requests.get(game_url, headers = headers)
            except Exception as e:
                print(e)
                exit(1)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                game_json_file = soup.find('script', {'id' : '__NEXT_DATA__'})
                game_json_file = game_json_file.getText()
                game_json_file = json.loads(game_json_file)
                file_name = f'{game_link}.json'
                output_file_path = f'{self.nba_com_game_data_fp}/{file_name}'
                with open(output_file_path, 'w') as outfile:
                    json.dump(game_json_file, outfile)
            else:
                error_string = 'Error for date {} with status code = {}'.format(date, response.status_code)
                print(error_string)
                time.sleep(60)
            time.sleep(uniform(5, 10))

    def extract(self):
        """
        Run Stage 3: Generate and extract game links from nba.com.
        """
        logging.info('Beginning Stage 3')

        logging.info('Generating Game Links to pull from nba.com')
        self.extract_game_links()
        logging.info('Completed')

        logging.info('Filtering out Games that have already been extracted')
        self.filter_out_pulled_games_from_game_links()
        if len(self.game_links) == 0:
            logging.info('All games have been pulled')
        else:
            logging.info(f'Number of Games to Pull: {len(self.game_links)}')
            logging.info('Extracting Game pages from nba.com')
            self.get_game_data()
            logging.info('Completed')

        logging.info('Stage 3 Complete')
        logging.info('====================================')
        
if __name__ == "__main__":
    w = NbaComGamesExtractor()
    w.extract()




