

import pandas as pd
import os
import logging
import re
logging.basicConfig(level=logging.INFO,  # Set minimum logging level to INFO
                    format='%(asctime)s - %(levelname)s - %(message)s',  # Include timestamp, log level, and message
                    datefmt='%Y-%m-%d %H:%M:%S',  # Timestamp format
                    handlers=[logging.FileHandler('test.log', mode='a'),  # Append to the log file if it exists
                              logging.StreamHandler()])  # Also log to standard output (console)
from tqdm import tqdm
import json
import pdb
import sys
from pathlib import Path
# Add the parent directory to the system path
# parent_dir = Path(__file__).resolve().parent.parent
# sys.path.append(str(parent_dir))
from nba_com_main import NbaComMain

class NbaComPbpStage(NbaComMain):
    def __init__(self):
        super().__init__()
        # Data Needed for this ingestion (Input Path)
        # self.nba_com_game_data_fp = self.data_directory / 'nba_com/stage_3_raw_game_json'
        # self.error_directory = self.data_directory / 'nba_com/stage_4d_nba_com_pbp_error_files'

    def extract_and_filter_out_pulled_pbp(self):
        self.files_to_transform = [x.split('.')[0] for x in os.listdir(self.stage_3_nba_com_game_data_fp) if 'json' in x]
        self.files_to_transform  = [
            x for x in self.files_to_transform 
            if int("".join(re.match(r'(\d{4})-(\d{2})', x).groups())) > 199607
        ]
        files_transformed = self.read_table_with_query('SELECT DISTINCT source_file FROM nba_com_stage.tbl_pbp')
        files_transformed = files_transformed['source_file'].tolist()
        self.files_to_transform = [x for x in self.files_to_transform if x not in files_transformed]
        self.files_to_transform.sort()

    def extract_pbp_data(self, action_object, game_id):
        
        df = pd.DataFrame(action_object)
        column_dictionary = {
                    'game_id': 'game_id',
                    'actionId': 'action_id',
                    'clock': 'clock',
                    'period': 'period',
                    'teamId': 'team_id',
                    'personId': 'person_id',
                    'xLegacy': 'x_legacy',
                    'yLegacy': 'y_legacy',
                    'shotDistance': 'shot_distance',
                    'shotResult': 'shot_result',
                    'isFieldGoal': 'is_field_goal',
                    'scoreHome': 'score_home',
                    'scoreAway': 'score_away',
                    'pointsTotal': 'points_total',
                    'location': 'location',
                    'description' : 'description',
                    'actionType': 'action_type',
                    'subType': 'sub_type',
                }
        columns_to_keep = [*column_dictionary.keys()]
        df.insert(0, 'game_id', game_id)
        df = df[columns_to_keep]
        df.rename(columns=column_dictionary, inplace=True)
        df.replace('', None, inplace=True)
        return df

    def extract_combine_data(self):
        for file in tqdm(self.files_to_transform):
            try:
                src_file = f'{self.stage_3_nba_com_game_data_fp}/{file}.json'
                with open(src_file) as f:
                    json_file = json.load(f)
                game_id =  json_file['props']['pageProps'].get('game').get('gameId') 
                action_object = json_file['props']['pageProps'].get('playByPlay').get('actions')
                if not action_object:
                    raise ValueError("Action object not found in JSON")

                df_action = self.extract_pbp_data(action_object, game_id)
                if df_action is None:
                    raise ValueError("Failed to extract box score data")
                
                df_action['source_file'] = file   
                df_action = self.filter_and_set_dtypes(df_action, 'nba_com_stage', 'tbl_pbp')
                self.load_data(df_action, 'nba_com_stage', 'tbl_pbp', progress_bar=False)

            except Exception as e:
                logging.info(f"Error processing file {file}: {e}")
                # traceback.print_exc()  # Provides a stack trace which can be very helpful for debugging
                # self.move_error_file(src_file, self.error_directory, file)
        

    def stage(self):
        logging.info('Beginning Stage 4c: Staging NBA.com Box Score Data')
        logging.info('Extracting and Filtering Out Pulled Box Scores')
        self.extract_and_filter_out_pulled_pbp()
        logging.info('Completed')

        files_to_transform = self.files_to_transform
        if len(files_to_transform) == 0:
            logging.info('No new pbp data to transform')
        else:
            logging.info(f'Number of new games with pbp to transform: {len(files_to_transform)}')
            logging.info('Extracting and Combining Data')
            self.extract_combine_data()
            logging.info('Completed')

        logging.info('Stage 4c Complete')
        logging.info('====================================')

if __name__ == '__main__':
    n = NbaComPbpStage()
    n.stage()












