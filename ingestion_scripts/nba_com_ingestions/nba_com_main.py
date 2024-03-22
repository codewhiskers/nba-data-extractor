import sys
from pathlib import Path
# Add the parent directory to the system path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from nba_data import NbaData
import os


class NbaComMain(NbaData):
    def __init__(self):
        super().__init__()
        self.stage_1_wikipedia_data_fp = self.data_directory / 'nba_com/stage_1_raw_wikipedia_html' 
        self.stage_2_nba_com_date_data_fp = self.data_directory / 'nba_com/stage_2_raw_date_json'
        self.stage_3_nba_com_game_data_fp = self.data_directory / 'nba_com/stage_3_raw_game_json'
        os.makedirs(self.stage_1_wikipedia_data_fp, exist_ok=True)
        os.makedirs(self.stage_2_nba_com_date_data_fp, exist_ok=True)
        os.makedirs(self.stage_3_nba_com_game_data_fp, exist_ok=True)   

