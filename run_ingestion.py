from pathlib import Path
import sys
import pdb
directory = Path(__file__).resolve().parent / 'ingestion_scripts/nba_com_ingestions'
sys.path.append(str(directory))


from ingestion_scripts.nba_com_ingestions.stage_1_wikipedia_nba_page_extract import WikipediaSeasonSummaryExtractor
from ingestion_scripts.nba_com_ingestions.stage_2_nba_com_dates_extract import NbaComDatesExtractor
from ingestion_scripts.nba_com_ingestions.stage_3_nba_com_games_extract import NbaComGamesExtractor
from ingestion_scripts.nba_com_ingestions.stage_4a_nba_com_game_info_stage import NbaComGameInfoStage
from ingestion_scripts.nba_com_ingestions.stage_4b_nba_com_player_stage import NbaComPlayersStage
from ingestion_scripts.nba_com_ingestions.stage_4c_nba_com_box_stage import NbaComBoxStage
from ingestion_scripts.nba_com_ingestions.stage_4d_nba_com_pbp_stage import NbaComPbpStage


if __name__ == "__main__":
    WikipediaSeasonSummaryExtractor().extract()
    NbaComDatesExtractor().extract()
    NbaComGamesExtractor().extract()
    NbaComGameInfoStage().stage()   
    NbaComPlayersStage().stage()
    NbaComBoxStage().stage()
    NbaComPbpStage().stage()
    