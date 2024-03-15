

import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import json
import re
import pdb
from nba_data import NbaData
from stage_2_nba_com_dates_extract import NbaComDatesExtractor
from stage_3_nba_com_games_extract import NbaComGamesExtractor

class NbaBoxScoreTransform(NbaComDatesExtractor, NbaComGamesExtractor, NbaData):
    def __init__(self):
        super().__init__()
        # Data Needed for this ingestion (Input Path)
        self.nba_com_game_data_fp = self.data_directory / 'nba_com/stage_3_raw_game_json'
        # Export Data Directory for box scores(Output Path)
        self.nba_com_box_score_fp = self.data_directory / 'nba_com/stage_4_box_score_pickle'
        # Export Data Directory for player set(Output Path)
        self.nba_com_player_fp = self.data_directory / 'nba_com/stage_4_player_pickle'
        # Export Data Directory for game details(Output Path)
        self.nba_com_game_detail_fp = self.data_directory / 'nba_com/stage_4_game_detail_pickle'

    def _convert_to_seconds(self, time_str):
        try:
            if pd.isna(time_str):
                return np.nan
            minutes, seconds = map(int, time_str.split(':'))
            return minutes * 60 + seconds
        except ValueError:  # In case of any unexpected value that can't be split
            return np.nan

    def extract_and_filter_out_pulled_games(self):
        pdb.set_trace()
        self.files_to_transform = [x.split('.')[0] + '_box' for x in os.listdir(self.nba_com_game_data_fp) if 'json' in x]
        files_transformed = [x.split('.')[0] for x in os.listdir(self.nba_com_box_score_fp)]
        self.files_to_transform = [x for x in self.files_to_transform if x not in files_transformed]

    def generate_box_score(self, box_score_object):
        box_score_object = box_score_object['players'] + box_score_object['inactives']
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
        df_box.replace('', np.nan, inplace=True)
        # Fix minutes played
        seconds = df_box['minutes'].apply(self._convert_to_seconds)  

        columns_to_drop = [x for x in df_box.columns if 'Percentage' in x] + ['minutes']
        df_box.drop(columns=columns_to_drop, inplace=True)
        df_box.insert(2, 'seconds', seconds)
        # Convert inactive player columns to NaN
        df_box.loc[df_box['status'] != 'active', df_box.columns[2:]] = np.nan
        return df_box#, player_set



    def extract_box_score_data(self, game_object):
        # box_score_object = game_object['players'] + game_object['inactives']
        box_score_home = self.generate_box_score(game_object['homeTeam'])
        box_score_away = self.generate_box_score(game_object['awayTeam'])
        df_box = pd.concat([box_score_home, box_score_away], axis=0)
        df_box.insert(0, 'game_id', game_object['gameId'])
        return df_box#,player_set

    def generate_player_set(self, game_object):
        box_score_object = game_object['players'] + game_object['inactives']
        player_set = [(player['personId'], player['firstName'], player['familyName']) for player in box_score_object]
        # player_set = [(player['personId'], f"{player['firstName']} {player['familyName']}") for player in box_score_object]
        return player_set

    def extract_player_data(self, game_object):
        home_players = self.generate_player_set(game_object['homeTeam'])
        away_players = self.generate_player_set(game_object['awayTeam'])
        players = home_players + away_players 
        df_players = pd.DataFrame(players, columns=['personId', 'firstName', 'familyName'])
        return df_players
    
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
            'away_team_id' : home_team_id,
            'away_team_city' : away_team_city,
            'away_team_name' : away_team_name,
            'home_team_score' : home_team_score,
            'away_team_score' : away_team_score,
            'final_quarter' : final_quarter,
            'national_tv_broadcast' : national_tv_broadcast
        }
        return pd.DataFrame([data])

    def extract_combine_data(self):
        error_files = []
        for file in tqdm(self.files_to_transform):
            f = open(f'{self.nba_com_game_data_fp}/{file}.json')
            json_file = json.load(f)
            game_object = json_file['props']['pageProps']['game']
            analytics_object = json_file['props']['pageProps']['analyticsObject']
            try:
                df_players = self.extract_player_data(game_object)
                df_box = self.extract_box_score_data(game_object)
                df_game_details = self.extract_game_data(game_object, analytics_object)
                
                df_box.to_pickle(f'{self.nba_com_box_score_fp}/{file}_box.pkl')
                df_players.to_pickle(f'{self.nba_com_player_fp}/{file}_players.pkl')
                df_game_details.to_pickle(f'{self.nba_com_game_detail_fp}/{file}_game_details.pkl') 

            except Exception as e:
                print(file)
                print(e)
                error_files.append(file)
        
        pdb.set_trace()

    def transform_all(self):
        self.extract_and_filter_out_pulled_games()
        self.extract_combine_data()

if __name__ == '__main__':
    n = NbaBoxScoreTransform()
    n.transform_all()












