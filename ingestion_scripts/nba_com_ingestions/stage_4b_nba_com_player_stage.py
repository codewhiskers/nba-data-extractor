

import pandas as pd
import os
import traceback
from tqdm import tqdm
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
import json
import pdb
import sys
from pathlib import Path
# Add the parent directory to the system path
# parent_dir = Path(__file__).resolve().parent.parent
# sys.path.append(str(parent_dir))
from nba_com_main import NbaComMain

class NbaComPlayersStage(NbaComMain):
    def __init__(self):
        super().__init__()
        # Data Needed for this ingestion (Input Path)
        # self.nba_com_game_data_fp = self.data_directory / 'nba_com/stage_3_raw_game_json'
        # self.error_directory = self.data_directory / 'nba_com/stage_4b_nba_com_player_stage_error_files'

    def extract_and_filter_out_pulled_players(self):
        self.files_to_transform = [x.split('.')[0] for x in os.listdir(self.stage_3_nba_com_game_data_fp) if 'json' in x]
        files_transformed = self.read_table_with_query('SELECT DISTINCT source_file FROM nba_com_stage.tbl_player')
        files_transformed = files_transformed['source_file'].tolist()
        self.files_to_transform = [x for x in self.files_to_transform if x not in files_transformed]

    def generate_player_set(self, game_object):
        box_score_object = game_object['players'] + game_object['inactives']
        player_set = [(player['personId'], player['firstName'], player['familyName']) for player in box_score_object]
        return player_set
    
    def extract_player_data(self, game_object):
        if game_object['homeTeam']['players'] == []:
            raise ValueError("No players object found in game_object")
        home_players = self.generate_player_set(game_object['homeTeam'])
        away_players = self.generate_player_set(game_object['awayTeam'])
        players = home_players + away_players 
        df_players = pd.DataFrame(players, columns=['player_id', 'first_name', 'last_name'])
        df_players['first_name'] = df_players['first_name'].str.upper()
        df_players['last_name'] = df_players['last_name'].str.upper()
        df_players['game_id'] = game_object.get('gameId')
        return df_players

    def extract_combine_data(self):
        # error_files = []
        for file in tqdm(self.files_to_transform):
            try:
                src_file = f'{self.stage_3_nba_com_game_data_fp}/{file}.json'
                with open(src_file) as f:
                    json_file = json.load(f)

                game_object = json_file['props']['pageProps'].get('game')
                if not game_object:
                    raise ValueError("Game object not found in JSON")

                df_players = self.extract_player_data(game_object)
                if df_players is None:
                    raise ValueError("Failed to extract player data")

                df_players['source_file'] = file
                df_players = self.filter_and_set_dtypes(df_players, 'nba_com_stage', 'tbl_player')
                self.load_data(df_players, 'nba_com_stage', 'tbl_player', progress_bar=False)

            except Exception as e:
                logging.info(f"Error processing file {file}: {e}")
                # traceback.print_exc()  # Provides a stack trace which can be very helpful for debugging
                # error_files.append(file)
                # self.move_error_file(src_file, self.error_directory, file)
        

    def stage(self):
        logging.info('Beginning Stage 4b: Staging NBA.com Player Data')
        logging.info('Extracting and Filtering Out Pulled Players from games')
        self.extract_and_filter_out_pulled_players()
        logging.info('Completed')

        files_to_transform = self.files_to_transform
        if len(files_to_transform) == 0:
            logging.info('No new game players to transform')
        else:
            logging.info(f'Number of new games with players to transform: {len(files_to_transform)}')
            logging.info('Extracting and Combining Data')
            self.extract_combine_data()
            logging.info('Completed')

        logging.info('Stage 4b Complete')
        logging.info('====================================')

if __name__ == '__main__':
    n = NbaComPlayersStage()
    n.stage()












