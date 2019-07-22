import logging
import requests
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from typing import Optional, List, Dict, Union, Tuple, cast
from Sheets import Sheets
from Spreadsheet import Spreadsheet


LEADERBOARD_ID: str = "1bA_P0JPWqEodCEEI_etqj4pSnYZxkr9fIDkiZhgUWAY"
PEAK_LEADERBOARD_ID: str = "1rZxJTshSK8wbsY7FOgJNJ8tUmvqX-VwMzJEdhWwbKFw"


def main() -> None:
    """Builds sheets and spreadsheet objects, creates and writes mmr peaks to spreadsheet.
    """

    # Creates sheet service and attaches them to the two spreadsheets.
    sheets_service: Optional[Sheets] = Sheets("creds.json")
    leaderboard_spreadsheet: Spreadsheet = Spreadsheet(LEADERBOARD_ID, sheets_service)
    peak_leaderboard_spreadsheet: Spreadsheet = Spreadsheet(PEAK_LEADERBOARD_ID, sheets_service)

    # Gets all players from the spreadsheet leaderboard. Keeping all players separated by their county.
    all_leaderboard_data: List[Dict] = leaderboard_spreadsheet.get_ranges_values(
        ["England", "Scotland", "Wales", "Northern Ireland", "Other"],
        value_render_option="FORMULA",
        major_dimension="COLUMNS")["valueRanges"]
    all_leaderboard_data: List[List[List[Union[int, str]]]] = [data["values"] for data in all_leaderboard_data]
    all_players = parse_leaderboard_data(all_leaderboard_data)

    # Collects each player's peak mmr, then writes this data to the new spreadsheet.
    # Casts to correct type (don't know why it infers 'str' instead of actual type.
    country_leaderboards = get_country_leaderboards(all_players)
    country_leaderboards = cast(List[Dict[str, List[Tuple[str, str, int]]]], country_leaderboards)
    write_all_leaderboards(peak_leaderboard_spreadsheet, country_leaderboards,
                           "England", "Scotland", "Wales", "Northern Ireland", "Other")


def parse_leaderboard_data(all_leaderboard_data: List[List[List[Union[str, int]]]])\
        -> List[Dict[str, List[Tuple[str, str]]]]:
    """Takes original spreadsheet data and parses player links, and re-formats data.

    Parameters
    ----------
    all_leaderboard_data
        Contains all data from the original leaderboard spreadsheet

    Returns
    -------
    List[Dict[str, List[Tuple[str, str]]]]
        List of dictionaries containing a county name, with a list of players
    """

    # Collects players from the spreadsheet data.
    all_players = []
    for country in all_leaderboard_data:
        country_players = {}
        for i in range(1, len(country), 4):
            player_sheet_data = list(map(lambda x: x.split('"'), country[i][1::]))
            # Changes link to point to the correct page to find peak mmr
            players = [(player_data[3], player_data[1][:45:] + "mmr/" + player_data[1][45::])
                       for player_data in player_sheet_data]
            country_players[country[i][0]] = players
        all_players.append(country_players)
    return all_players


def get_country_leaderboards(all_players: List[Dict[str, List[Tuple[str, str]]]])\
        -> List[Dict[str, List[Tuple[str, str, int]]]]:
    """Takes all the players and collects their peak mmr's

    Parameters
    ----------
    all_players
        List of dictionaries containing players for each county

    Returns
    -------
    List[Dict[str, List[Tuple[str, str, int]]]]
        List of dictionaries containing players for each county, with peak mmr's for each player.
    """

    all_country_leaderboards = []
    for country in all_players:
        # Collects players peak mmr up to 10 counties at a time within a country.
        with ThreadPoolExecutor(max_workers=10) as executor:
            country_leaderboard = dict(executor.map(get_county_mmrs, country.items()))
        all_country_leaderboards.append(country_leaderboard)
    for country in all_country_leaderboards:
        for county in country:
            # Removes any players that are None (TRN issue causes this), and sorts by peak mmr descending.
            country[county] = [player for player in country[county] if player is not None]
            country[county].sort(key=lambda x: x[2], reverse=True)
    return all_country_leaderboards


def get_county_mmrs(county: Tuple[str, List[Tuple[str, str]]]) -> Tuple[str, List[Tuple[str, str, int]]]:
    """Collects the peak mmr's of each player from an individual county

    Parameters
    ----------
    county
        List of players to collect peak mmr's from

    Returns
    -------
    Tuple[str, List[Tuple[str, str, int]]]
        County name, with a list of players with their peak mmr
    """

    with ThreadPoolExecutor(max_workers=10) as executor:
        # Collects players peak mmr's up to 10 players at a time within a county.
        county_leaderboard = list(executor.map(get_player_mmrs, county[1]))
    return county[0], county_leaderboard


def get_player_mmrs(player: Tuple[str, str]) -> Tuple[str, str, int]:
    """Collects the peak mmr of an individual player

    Parameters
    ----------
    player
        A link to the player's profile.

    Returns
    -------
    Tuple[str, str, int]
        A player containing name, profile link, peak mmr.
    """

    player_details = None
    try_count = 3
    while try_count > 0:
        try:
            # Attempts to find the players peak mmr inside a javascript variable, in one of many scripts
            # Data has to parsed as pure text so code is messy
            player_profile_html = requests.get(player[1]).text
            player_profile_tree = BeautifulSoup(player_profile_html, features="lxml")
            mmr_script = player_profile_tree.select(
                "div.container:nth-child(8) > div:nth-child(1) > script:nth-child(8)")[0].text
            data = mmr_script.split("data")
            for i in range(len(data)):
                if "Ranked Standard 3v3" in data[i]:
                    # Find and joins mmr and rank for each day together, so peak mmr achieved when unranked is ignored.
                    # Solves problem where finish mmr from previous season carries over till placement matches are
                    # completed, sometimes causing an inaccurate peak.
                    standard_mmr_data = data[i].split("rating:")[1]
                    standard_mmr_data = standard_mmr_data.split("tier: ")
                    standard_mmr_data_mmr = standard_mmr_data[0].replace("[", "").replace("]", "").strip()
                    standard_mmr_data_mmr = [int(x) for x in standard_mmr_data_mmr[:-1:].split(",")]
                    standard_mmr_data_rank = standard_mmr_data[1].replace("[", "").replace("]", "").strip()
                    standard_mmr_data_rank = standard_mmr_data_rank.split("\r")[0]
                    standard_mmr_data_rank = [int(x) for x in standard_mmr_data_rank.split(",")]
                    standard_mmr_rank_join = list(zip(standard_mmr_data_rank, standard_mmr_data_mmr))
                    standard_mmr_rank_join = list(filter(lambda x: x[0] > 0, standard_mmr_rank_join))
                    max_mmr = max(standard_mmr_rank_join, key=lambda x: x[1])[1]
                    player_details = (player[0], player[1], max_mmr)
        except Exception as ex:
            logging.exception(ex)
            try_count -= 1
            player = None
        else:
            try_count = 0
    return player_details


def write_all_leaderboards(spreadsheet: Spreadsheet, country_leaderboards: List[Dict[str, List[Tuple[str, str, int]]]],
                           *args: str) -> None:
    """Writes all country leaderboards to the selected spreadsheet

    Parameters
    ----------
    spreadsheet
        The spreadsheet to write the leaderboards to
    country_leaderboards
        The leaderboards to write to the spreadsheets
    args
        The names of the sheets that will have leaderboards written to them
    """

    def rowcol_to_a1(row: int, col: int) -> str:
        """Converts row col cell format into the A1 Google Sheets cell format.

        Parameters
        ----------
        row
            Integer representing the row of a cell.
        col
            Integer representing the column of a cell.

        Returns
        -------
        label
            The A1 representation of the row/col number representation of a cell.
        """

        row = int(row)
        col = int(col)
        div = col
        column_label = ''
        while div:
            (div, mod) = divmod(div, 26)
            if mod == 0:
                mod = 26
                div -= 1
            column_label = chr(mod + 64) + column_label
        label = '%s%s' % (column_label, row)
        return label

    all_value_ranges = []
    for i in range(len(args)):
        sheet_name = args[i]
        start_col = 1
        # Creates value range objects. Including formatting hyperlinks to player profiles.
        # Each county is 3 columns wide.
        for counties in sorted(country_leaderboards[i]):
            data = [["Rank", counties, "3's Peak MMR"]]
            rank = 1
            for player in country_leaderboards[i][counties]:
                data.append([rank, '=HYPERLINK("{}", "{}")'.format(player[1], player[0]), player[2]])
                rank += 1
            range_str = "{}!{}:{}".format(sheet_name, rowcol_to_a1(1, start_col), rowcol_to_a1(rank, start_col + 2))
            value_range = {
                "range": range_str,
                "values": data
            }
            all_value_ranges.append(value_range)
            start_col += 4

    # Casts args which is Tuple[str, ...] to expected List[str]
    # No runtime cost, just for ide type checking system
    args = cast(List[str], args)

    spreadsheet.clear_ranges_values(args)
    spreadsheet.update_ranges_values(all_value_ranges)
    spreadsheet.autosize_all_columns()


if __name__ == "__main__":
    logging.basicConfig(filename="logfile_peak.log", level=logging.WARNING,
                        format="%(asctime)s %(levelname)-8s %(name)-15s %(message)s")
    logging.warning("Program Started.")
    main()
    logging.warning("Program Finished.")
    logging.warning("-" * 64)