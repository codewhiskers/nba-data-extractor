

import pandas as pd
import logging
import os
from tqdm import tqdm
import json
import shutil
import pdb
import sys
from pathlib import Path
# Add the parent directory to the system path
# parent_dir = Path(__file__).resolve().parent.parent
# sys.path.append(str(parent_dir))
from nba_com_main import NbaComMain


class NbaComGameInfoStage(NbaComMain):
    def __init__(self):
        super().__init__()
        # Data Needed for this ingestion (Input Path)
        # self.nba_com_game_data_fp = self.data_directory / 'nba_com/stage_3_raw_game_json'
        # self.error_directory = self.data_directory / 'nba_com/stage_4a_nba_com_game_info_stage_error_files'

    def extract_and_filter_out_pulled_game_details(self):
        self.files_to_transform = [x.split('.')[0] for x in os.listdir(self.stage_3_nba_com_game_data_fp) if 'json' in x]
        files_transformed = self.read_table_with_query('SELECT DISTINCT source_file FROM nba_com_stage.tbl_game_info')
        files_transformed = files_transformed['source_file'].tolist()
        self.files_to_transform = [x for x in self.files_to_transform if x not in files_transformed]

    def extract_game_data(self, game_object, analytics_object):
        # GameID
        game_id = game_object.get('gameId')
        # Date in US Eastern Timezone:
        game_date = game_object.get('gameEt')
        # Season Year
        season_year = analytics_object.get('season')
        # Regular Season or Playoffs
        season_type = analytics_object.get('seasonType')
        # Game Attendance
        attendance = game_object.get('attendance')
        # Game Sold Out
        sellout = game_object.get('sellout')
        # Game Duration
        duration = game_object.get('duration')
        duration = self.convert_to_minutes(duration)
        # Game Label
        game_label = game_object.get('gameLabel')
        # Game Sub-Label (What game of the series was it, game 1, 2... etc)
        game_sublabel = game_object.get('gameSubLabel')
        # Series Game Number--not sure how this is different from sublabel
        series_game_number = game_object.get('seriesGameNumber')
        # Series Text (Information about the series if it's a playoff game)
        series_text = game_object.get('seriesText')
        # The home/away team name, and corresponding NBA ID for the team:
        home_team_id = game_object['homeTeam'].get('teamId')
        home_team_city = game_object['homeTeam'].get('teamCity')
        home_team_name = game_object['homeTeam'].get('teamName')

        away_team_id = game_object['awayTeam'].get('teamId')
        away_team_city = game_object['awayTeam'].get('teamCity')
        away_team_name = game_object['awayTeam'].get('teamName')

        # Final Quarter
        final_quarter = analytics_object['gameQuarter']

        # Final Score 
        home_team_score = game_object['homeTeam'].get('score')
        away_team_score = game_object['awayTeam'].get('score')

        # National TV Broadcast
        national_broadcasters = game_object['broadcasters'].get('nationalBroadcasters', [])

        if national_broadcasters:
            national_tv_broadcast = national_broadcasters[0].get('broadcasterDisplay')
        else:
            national_tv_broadcast = None

        data = {
            'game_id' : game_id,
            'game_date' : game_date,
            'season_year' : season_year,
            'season_type' : season_type,
            'attendance' : attendance,
            'sellout' : sellout,
            'duration' : duration,
            'game_label' : game_label,
            'game_sublabel' : game_sublabel,
            'series_text' : series_text,
            'series_game_number' : series_game_number,
            'series_text' : series_text,
            'home_team_id' : home_team_id,
            'home_team_city' : home_team_city,
            'home_team_name' : home_team_name,
            'away_team_id' : away_team_id,
            'away_team_city' : away_team_city,
            'away_team_name' : away_team_name,
            'home_team_score' : home_team_score,
            'away_team_score' : away_team_score,
            'final_quarter' : final_quarter,
            'national_tv_broadcast' : national_tv_broadcast
        }
        return pd.DataFrame([data])

    def extract_combine_data(self):
        for file in tqdm(self.files_to_transform):
            src_file = f'{self.stage_3_nba_com_game_data_fp}/{file}.json'
            f = open(src_file)
            json_file = json.load(f)
            try:
                game_object = json_file['props']['pageProps'].get('game', None)
                if game_object is None:
                    print(f'The file: {file} could not be ingested because the game object was not found.')
                    # self.move_error_file(src_file, self.error_directory, file)
                    raise ValueError("Game object not found in JSON")
                analytics_object = json_file['props']['pageProps']['analyticsObject']
                df_game_details = self.extract_game_data(game_object, analytics_object)
                df_game_details['source_file'] = file
                df_game_details = self.filter_and_set_dtypes(df_game_details, 'nba_com_stage', 'tbl_game_info')
                self.load_data(df_game_details, 'nba_com_stage', 'tbl_game_info', progress_bar=False)

            except Exception as e:
                logging.info(f"Error processing file {file}: {e}")
        
    def stage(self):
        """
        Run Stage 4a: Staging NBA.com Game Info Data
        """
        logging.info('Beginning Stage 4a: Staging NBA.com Game Info Data')
        logging.info('Extracting and Filtering Out Pulled Game Details')
        self.extract_and_filter_out_pulled_game_details()
        logging.info('Completed')

        files_to_transform = self.files_to_transform
        if len(files_to_transform) == 0:
            logging.info('No new game details to transform')
        else:
            logging.info(f'Number of new game details to transform: {len(files_to_transform)}')
            logging.info('Extracting and Combining Data')
            self.extract_combine_data()
            logging.info('Completed')

        logging.info('Stage 4a Complete')
        logging.info('====================================')

if __name__ == '__main__':
    n = NbaComGameInfoStage()
    n.stage()












