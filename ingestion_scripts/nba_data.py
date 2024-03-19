
from sqlalchemy import create_engine
from datetime import date
from pathlib import Path
import pandas as pd
import numpy as np
import pdb
from nba_data_db_config import NbaData_DB_Config

class NbaData(NbaData_DB_Config):
    def __init__(self):
        super().__init__()
        self.base_directory = Path(__file__).parent.parent
        self.data_directory = self.base_directory / 'data'

    def convert_to_minutes(self, time_str):
        try:
            # Check if the value is missing or NaN and return np.nan if true
            if pd.isna(time_str):
                return np.nan
            # Split the string into hours and minutes
            hours, minutes = map(int, time_str.split(':'))
            # Convert hours to minutes and add to the minutes part
            return hours * 60 + minutes
        except ValueError:  # In case of any unexpected value that can't be split
            return np.nan


    def _convert_to_seconds(self, time_str):
        try:
            if pd.isna(time_str):
                return np.nan
            minutes, seconds = map(int, time_str.split(':'))
            return minutes * 60 + seconds
        except ValueError:  # In case of any unexpected value that can't be split
            return np.nan

    def get_user_agent_list(self):
        self.user_agent_list = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            ]
        
   
