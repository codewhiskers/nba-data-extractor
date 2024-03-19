

import pandas as pd
import os
from tqdm import tqdm
import json
import pdb
from nba_data import NbaData

class NbaPlayersTransformLoad(NbaData):
    def __init__(self):
        super().__init__()
        # Data Needed for this ingestion (Input Path)
        self.nba_com_game_data_fp = self.data_directory / 'nba_com/stage_3_raw_game_json'
        self.create_table('nba_com', 'tbl_player')

    def extract_and_filter_out_pulled_players(self):
        # pdb.set_trace()
        self.files_to_transform = [x.split('.')[0] for x in os.listdir(self.nba_com_game_data_fp) if 'json' in x]
        files_transformed = self.read_table_with_query('SELECT DISTINCT source_file FROM nba_com.tbl_player')
        files_transformed = files_transformed['source_file'].tolist()
        self.files_to_transform = [x for x in self.files_to_transform if x not in files_transformed]

    def generate_player_set(self, game_object):
        box_score_object = game_object['players'] + game_object['inactives']
        player_set = [(player['personId'], player['firstName'], player['familyName']) for player in box_score_object]
        # player_set = [(player['personId'], f"{player['firstName']} {player['familyName']}") for player in box_score_object]
        return player_set
    
    def extract_player_data(self, game_object):
        home_players = self.generate_player_set(game_object['homeTeam'])
        away_players = self.generate_player_set(game_object['awayTeam'])
        players = home_players + away_players 
        df_players = pd.DataFrame(players, columns=['person_id', 'first_name', 'last_name'])
        df_players['game_id'] = game_object.get('gameId')
        return df_players

    def extract_combine_data(self):
        error_files = []
        for file in tqdm(self.files_to_transform):
            f = open(f'{self.nba_com_game_data_fp}/{file}.json')
            json_file = json.load(f)
            try:
                game_object = json_file['props']['pageProps']['game']
                df_players = self.extract_player_data(game_object)
                df_players['source_file'] = file   
                df_players = self.filter_and_set_dtypes(df_players, 'nba_com', 'tbl_player')
                self.load_data(df_players, 'nba_com', 'tbl_player', progress_bar=False)
                
            except Exception as e:
                print(file)
                print(e)
                error_files.append(file)
        
        pdb.set_trace()

    def transform_all(self):
        self.extract_and_filter_out_pulled_players()
        self.extract_combine_data()


if __name__ == '__main__':
    n = NbaPlayersTransformLoad()
    n.transform_all()












