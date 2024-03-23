
from sqlalchemy import create_engine
from datetime import date
from pathlib import Path
import pandas as pd
import numpy as np
import pdb
from nba_data_db_config import NbaData_DB_Config
import os
import shutil
import logging

# Set up logging
from pathlib import Path
logging_path = Path(__file__).parent.parent / 'ingestion_logs'
os.makedirs(logging_path, exist_ok=True)
date = date.today().strftime('%Y-%m-%d')
logging.basicConfig(level=logging.INFO,  # Set minimum logging level to INFO
                    format='%(asctime)s - %(levelname)s - %(message)s',  # Include timestamp, log level, and message
                    datefmt='%Y-%m-%d %H:%M:%S',  # Timestamp format
                    handlers=[logging.FileHandler(f'{logging_path}/{date}.log', mode='a'),  # Append to the log file if it exists
                              logging.StreamHandler()])  # Also log to standard output (console)

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

    def move_error_file(self, src_file, dest_dir, file):
        """
        Move an error file to the error directory.

        Args:
            src_file (str): The path of the source file.
            file (str): The name of the file.
        """
        # dest_dir = self.nba_com_game_data_errors_fp
        try:
            os.makedirs(dest_dir, exist_ok=True)
            dest_file = os.path.join(dest_dir, f"{file}.json")
            shutil.move(src_file, dest_file)
            print('File moved to stage_4_error_raw_game_json directory')
        except Exception as e:
            print(f"Error moving file {file}.json: {e}")

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
        
   
    # def get_seasons(self):
    #         seasons = {
    #             '1979' : [date(1979, 10, 12), date(1980, 5, 16)],
    #             '1980' : [date(1980, 10, 10), date(1981, 5, 14)],
    #             '1981' : [date(1981, 10, 30), date(1982, 6, 8)],
    #             '1982' : [date(1982, 10, 29), date(1983, 5, 31)],
    #             '1983' : [date(1983, 10, 28), date(1984, 6, 12)],
    #             '1984' : [date(1984, 10, 26), date(1985, 6, 9)],
    #             '1985' : [date(1985, 10, 25), date(1986, 6, 8)],
    #             '1986' : [date(1986, 10, 31), date(1987, 6, 14)],
    #             '1987' : [date(1987, 11, 6), date(1988, 6, 21)],
    #             '1988' : [date(1988, 11, 4), date(1989, 6, 13)],
    #             '1989' : [date(1989, 11, 3), date(1990, 6, 14)],
    #             '1990' : [date(1990, 11, 2), date(1991, 6, 12)],
    #             '1991' : [date(1991, 11, 1), date(1992, 6, 14)],
    #             '1992' : [date(1992, 11, 6), date(1993, 6, 20)],
    #             '1993' : [date(1993, 11, 11), date(2008, 11, 11)],
    #             '1994' : [date(1979, 10, 12), date(1980, 5, 16)],
    #             '1995' : [date(1980, 10, 10), date(1981, 5, 14)],
    #             '1996' : [date(1981, 10, 30), date(1982, 6, 8)],
    #             '1997' : [date(1982, 10, 29), date(1983, 5, 31)],
    #             '1998' : [date(1983, 10, 28), date(1984, 6, 12)],
    #             '1999' : [date(1984, 10, 26), date(1985, 6, 9)],
    #             '2000' : [date(1985, 10, 25), date(1986, 6, 8)],
    #             '2001' : [date(1986, 10, 31), date(1987, 6, 14)],
    #             '2002' : [date(1987, 11, 6), date(1988, 6, 21)],
    #             '2003' : [date(1988, 11, 4), date(1989, 6, 13)],
    #             '2004' : [date(1989, 11, 3), date(1990, 6, 14)],
    #             '2005' : [date(1990, 11, 2), date(1991, 6, 12)],
    #             '2006' : [date(1991, 11, 1), date(1992, 6, 14)],
    #             '2007' : [date(1992, 11, 6), date(1993, 6, 20)],
    #             '2008' : [date(1993, 11, 11), date(2008, 11, 11)],
    #             '2009' : [date(1982, 10, 29), date(1983, 5, 31)],
    #             '2010' : [date(1983, 10, 28), date(1984, 6, 12)],
    #             '2011' : [date(1984, 10, 26), date(1985, 6, 9)],
    #             '2012' : [date(1985, 10, 25), date(1986, 6, 8)],
    #             '2013' : [date(1986, 10, 31), date(1987, 6, 14)],
    #             '2014' : [date(1987, 11, 6), date(1988, 6, 21)],
    #             '2015' : [date(1988, 11, 4), date(1989, 6, 13)],
    #             '2016' : [date(1989, 11, 3), date(1990, 6, 14)],
    #             '2017' : [date(2017, 11, 2), date(1991, 6, 12)],
    #             '2018' : [date(2018, 10, 16), date(2019, 6, 13)],
    #             '2019' : [date(2019, 10, 22), date(2020, 10, 11)],
    #             '2020' : [date(2020, 12, 22), date(2021, 7, 20)],
    #             '2021' : [date(2021, 10, 19), date(2022, 6, 16)],
    #             '2022' : [date(2022, 10, 17), date(2023, 6, 12)],
    #             '2023' : [date(2023, 10, 23), date(2024, 3, 20)],
    #         }