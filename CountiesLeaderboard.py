import logging
import requests
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup, SoupStrainer
from typing import List, Dict, Tuple, Union, Any
from Sheets import Sheets
from Spreadsheet import Spreadsheet


Player_T = List[Union[str, int]]


#Keys are the counties to be combined, values are new counties
COMBINE_COUNTIES = {
        "Tyne and Wear": "Northumberland",
        "Clwyd": "North Wales",
        "Gwynedd": "North Wales",
        "Dyfed": "Central Wales",
        "Powys": "Central Wales",
        "Ayrshire": "South-West Scotland",
        "Dumfries & Galloway": "South-West Scotland",
        "Herefordshire": "Herefordshire & Shropshire",
        "Shropshire": "Herefordshire & Shropshire",
        "Armagh": "Rest of Ulster",
        "Down": "Rest of Ulster",
        "Tyrone & Fermanagh": "Rest of Ulster",
        "Hampshire - *Including the Isle of Wight*": "Hampshire",
        "Northamptonshire - *Including Rutland*": "Northamptonshire",
        "Central Scotland - *Clackmannanshire, Falkirk and Stirling*": "Central Scotland",
        "Grampian - *Aberdeenshire and Moray*": "Grampian",
        "Highlands - *Highlands, Western Isles, Orkney & Shetland*": "Highlands",
        "Dunbartonshire, Argyll & Bute": "Highlands",
        "Renfrewshire - *Renfrewshire and Inverclyde*": "Renfrewshire",
        "Tayside - *Angus, Perth, Dundee and Kinross*": "Tayside"
}
TRN_ID = "1-_lyvXBx89qXE8B9hoQaaT0Q1N26A1B7cHMIy6nGdxU"
LEADERBOARD_ID = "1bA_P0JPWqEodCEEI_etqj4pSnYZxkr9fIDkiZhgUWAY"


def main() -> None:
    """Builds the sheet service, collects town data, and leaderboards. Then writes them to a new spreadsheet.
    """

    # Creates sheet service and attaches them to the two spreadsheets.
    sheets_service = Sheets("creds.json")
    trn_spreadsheet = Spreadsheet(TRN_ID, sheets_service)
    leaderboard_spreadsheet = Spreadsheet(LEADERBOARD_ID, sheets_service)

    # Gets all town links from the TRN spreadsheet and parses them to the correct format.
    all_data = trn_spreadsheet.get_ranges_values(
        ["England!B2:AY48", "Scotland!B2:AD13", "Wales!B2:Z9", "Northern Ireland!B2:AA6", "Other!B2:AY3"],
        value_render_option="FORMULA")["valueRanges"]
    all_data = [ranges["values"] for ranges in all_data]
    all_town_links = generate_town_links(all_data, 3, 6, 2, 3, 3)

    country_leaderboards = get_country_leaderboards(all_town_links)

    # Writes country leaderboards to BRL Counties Leaderboard spreadsheet with
    # the worksheets with the provided names.
    write_country_leaderboards(leaderboard_spreadsheet, country_leaderboards,
                               "England", "Scotland", "Wales", "Northern Ireland", "Other")


def generate_town_links(all_data: List[List[str]], *args: int) -> List[List[Tuple[str, List[str]]]]:
    """

    Parameters
    ----------
    all_data
        List of countries, with a list of string representing each row of data (county)
    args
        List of ints, representing the start column of the links for each row of data within a worksheet.

    Returns
    -------
    all_links
        For each country, a list of tuples containing a county name, and a list of links for that county.
    """

    all_links = []
    for i in range(len(args)):
        # Creates a list of tuples. Containing the county name, and the plain town links,
        # removing Google Sheets formatting of hyperlinks.
        country_town_links = [(county[0], list(map(lambda x: x.split('"')[1], county[args[i]::])))
                              for county in all_data[i]]
        all_links.append(country_town_links)
    return all_links


def get_country_leaderboards(all_town_links: List[List[Tuple[str, List[str]]]]) -> List[Dict[str, List[Player_T]]]:
    """

    Parameters
    ----------
    all_town_links
        For each country, a list of tuples containing a county name, and a list of town links.

    Returns
    -------
    all_country_leaderboards
        List of leaderboards containing dictionaries of each county with a list of players

    """

    all_country_leaderboards = []
    for country in all_town_links:
        with ThreadPoolExecutor(max_workers=10) as executor:
            country_players = dict(executor.map(get_county_players, country))
        all_country_leaderboards.append(country_players)
    for country_leaderboard in all_country_leaderboards:
        for country in COMBINE_COUNTIES:
            if country in country_leaderboard:
                if COMBINE_COUNTIES[country] in country_leaderboard:
                    country_leaderboard[COMBINE_COUNTIES[country]] += country_leaderboard[country]
                else:
                    country_leaderboard[COMBINE_COUNTIES[country]] = country_leaderboard[country]
                del country_leaderboard[country]
        for county in country_leaderboard:
            country_leaderboard[county].sort(key=lambda x: x[2], reverse=True)
    return all_country_leaderboards


def get_county_players(county: Tuple[str, List[str]]) -> Tuple[str, List[Player_T]]:
    """

    Parameters
    ----------
    county
        Tuple of the county name and a list of town links.

    Returns
    -------
    county_name, players
        A tuple of the county name along with a list of players from that county.
    """

    with ThreadPoolExecutor(max_workers=10) as executor:
        town_leaderboards = list(executor.map(get_town_players, county[1]))
        players = [player for town in town_leaderboards for player in town]
    return county[0], players


def get_town_players(town: str) -> List[Player_T]:
    """

    Parameters
    ----------
    town
        A link to the town leaderboard

    Returns
    -------
    town_players
        A list of players from the town.

    """

    town_players = []
    try_count = 3
    while try_count > 0:
        try:
            town_html = requests.get(town).text
            leaderboard_tree = BeautifulSoup(town_html, features="lxml", parse_only=SoupStrainer("table"))
            players = leaderboard_tree.findAll("tr")[2::]
            for player in players:
                try:
                    name = player.contents[3].contents[3].text.replace('"', "'")
                    link = "https://rocketleague.tracker.network" + player.contents[3].contents[3].attrs["href"]
                    mmr = int(player.contents[5].contents[1].text.splitlines()[2].replace(",", ""))
                    town_players.append([name, link, mmr])
                except IndexError:
                    continue
        except Exception as ex:
            logging.exception(ex)
            try_count -= 1
            town_players = []
        else:
            try_count = 0
    return town_players


def write_country_leaderboards(spreadsheet: Spreadsheet,
                               all_leaderboards: List[Dict[str, List[Player_T]]],
                               *args: str) -> None:
    """

    Parameters
    ----------
    spreadsheet
        The spreadsheet to write the leaderboards to.
    all_leaderboards
        The leaderboards to be writen to the spreadsheet.
    args
        The names of the worksheets each leaderboard will go to.
    """

    def rowcol_to_a1(row: int, col: int) -> str:
        """

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
        for counties in sorted(all_leaderboards[i]):
            value_range = {}
            data = [["Rank", counties, "3's MMR"]]
            rank = 1
            for player in all_leaderboards[i][counties]:
                data.append([rank, '=HYPERLINK("{}", "{}")'.format(player[1], player[0]), player[2]])
                rank += 1
            range_str = "{}!{}:{}".format(sheet_name, rowcol_to_a1(1, start_col), rowcol_to_a1(rank, start_col + 2))
            value_range["range"] = range_str
            value_range["values"] = data
            all_value_ranges.append(value_range)
            start_col += 4
    spreadsheet.clear_ranges_values(args)
    spreadsheet.update_ranges_values(all_value_ranges)
    spreadsheet.autosize_all_columns()


if __name__ == "__main__":
    logging.basicConfig(filename="logfile.log", level=logging.WARNING,
                        format="%(asctime)s %(levelname)-8s %(name)-15s %(message)s")
    logging.warning("Program Started.")
    main()
    logging.warning("Program Finished.")
    logging.warning("-" * 64)
