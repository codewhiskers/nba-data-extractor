

from wikipedia_nba_season_data import WikipediaSeasonSummaryExtractor
from nba_com_stage_1_dates_extract import NbaComDatesExtractor
from nba_com_stage_2_games_extract import NbaComGamesExtractor



class NBAIngestion(WikipediaSeasonSummaryExtractor, NbaComDatesExtractor, NbaComGamesExtractor):

    def run_ingestion(self):
        WikipediaSeasonSummaryExtractor.run_all()
        NbaComDatesExtractor.run_all()
        NbaComGamesExtractor.run_all()


if __name__ == "__main__":
    ingestion = NBAIngestion()
    ingestion.run_ingestion()   