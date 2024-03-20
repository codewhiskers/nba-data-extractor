

import pandas as pd
import os
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
from tqdm import tqdm
import json
import pdb
import sys
from pathlib import Path
# Add the parent directory to the system path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from nba_data import NbaData

class NbaComBoxStage(NbaData):
    def __init__(self):
        super().__init__()
        # Data Needed for this ingestion (Input Path)
        self.nba_com_game_data_fp = self.data_directory / 'nba_com/stage_3_raw_game_json'
        self.error_directory = self.data_directory / 'nba_com/stage_4c_nba_com_box_error_files'

    def extract_and_filter_out_pulled_box_scores(self):
        self.files_to_transform = [x.split('.')[0] for x in os.listdir(self.nba_com_game_data_fp) if 'json' in x]
        files_transformed = self.read_table_with_query('SELECT DISTINCT source_file FROM nba_com_stage.tbl_box')
        files_transformed = files_transformed['source_file'].tolist()
        self.files_to_transform = [x for x in self.files_to_transform if x not in files_transformed]

    def generate_box_score(self, box_score_object):
        active_players = box_score_object.get('players', None)
        inactive_players = box_score_object.get('inactives', None)
        box_score_object = active_players + inactive_players
        # player_set = {(player['personId'], f"{player['firstName']} {player['familyName']}") for player in box_score_object}

        box_score_table_rows = [
            {
                'player_id': player['personId'],
                'status': player.get('comment', 'inactive') if player.get('comment', 'inactive') != '' else 'active',
                **player.get('statistics', {})
            }
            for player in box_score_object
        ]
        df_box = pd.DataFrame(box_score_table_rows)
        # df_box.replace('', np.nan, inplace=True)
        # Fix minutes played
        seconds = df_box['minutes'].apply(self._convert_to_seconds)  

        columns_to_drop = [x for x in df_box.columns if 'Percentage' in x] + ['minutes']
        df_box.drop(columns=columns_to_drop, inplace=True)
        df_box.insert(2, 'seconds', seconds)
        # Convert inactive player columns to NaN
        df_box.loc[df_box['status'] != 'active', df_box.columns[2:]] = None
        return df_box#, player_set

    def extract_box_score_data(self, game_object):
        if game_object['homeTeam']['players'] == []:
            return None
        valid_box_score = game_object['homeTeam']['players'][0].get('statistics', None)
        if valid_box_score is None:
            return None
        box_score_home = self.generate_box_score(game_object['homeTeam'])
        box_score_away = self.generate_box_score(game_object['awayTeam'])
        df_box = pd.concat([box_score_home, box_score_away], axis=0)
        df_box.insert(0, 'game_id', game_object['gameId'])
        replacement_dictionary = {
            'fieldGoalsMade' : 'field_goals_made',
            'fieldGoalsAttempted' : 'field_goals_attempted',
            'threePointersMade' : 'three_pointers_made',
            'threePointersAttempted' : 'three_pointers_attempted',
            'freeThrowsMade' : 'free_throws_made',
            'freeThrowsAttempted' : 'free_throws_attempted',
            'reboundsOffensive' : 'rebounds_offensive',
            'reboundsDefensive' : 'rebounds_defensive',
            'reboundsTotal' : 'rebounds_total',
            'foulsPersonal' : 'fouls_personal',
            'plusMinusPoints' : 'plus_minus_points',
        }
        df_box.rename(columns=replacement_dictionary, inplace=True)
        return df_box#,player_set

    def extract_combine_data(self):
        for file in tqdm(self.files_to_transform):
            try:
                src_file = f'{self.nba_com_game_data_fp}/{file}.json'
                with open(src_file) as f:
                    json_file = json.load(f)

                game_object = json_file['props']['pageProps'].get('game')
                if not game_object:
                    raise ValueError("Game object not found in JSON")

                df_box = self.extract_box_score_data(game_object)
                if df_box is None:
                    raise ValueError("Failed to extract box score data")
                
                df_box['source_file'] = file   
                df_box = self.filter_and_set_dtypes(df_box, 'nba_com_stage', 'tbl_box')
                self.load_data(df_box, 'nba_com_stage', 'tbl_box', progress_bar=False)

            except Exception as e:
                print(f"Error processing file {file}: {e}")
                # traceback.print_exc()  # Provides a stack trace which can be very helpful for debugging
                self.move_error_file(src_file, self.error_directory, file)
        

    def stage(self):
        logging.info('Beginning Stage 4c: Staging NBA.com Box Score Data')
        logging.info('Extracting and Filtering Out Pulled Box Scores')
        self.extract_and_filter_out_pulled_box_scores()
        logging.info('Completed')

        files_to_transform = self.files_to_transform
        if len(files_to_transform) == 0:
            logging.info('No new box scores to transform')
        else:
            logging.info(f'Number of new games with box scores to transform: {len(files_to_transform)}')
            logging.info('Extracting and Combining Data')
            self.extract_combine_data()
            logging.info('Completed')

        logging.info('Stage 4c Complete')
        logging.info('====================================')

if __name__ == '__main__':
    n = NbaComBoxStage()
    n.stage()












