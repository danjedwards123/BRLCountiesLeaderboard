import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import UnknownApiNameOrVersion
from typing import List, Dict, Union, Any, Optional


Value_Range_T = List[Dict[str, Union[str, List[List[str]]]]]


class Sheets:
    """ A class that represents the low level Sheets service.

    The Sheets class is used to make the API calls to the Google Sheets service. Used in conjunction
    with the Spreadsheet class, a Spreadsheet object must be created with a Sheets object as a parameter.
    i.e. a sheets service is attached to a spreadsheet object.
    """

    def __init__(self, credentials_filename: str) -> None:
        """Assigns the properties of the sheet service, including building the service

        Parameters
        ----------
        credentials_filename
            The name of the file to construct credentials, and further on, the sheets service.
        """

        self._scope: List[str] = ["https://www.googleapis.com/auth/spreadsheets"]
        self._credentials: Optional[Any] = self.retrieve_credentials(credentials_filename)
        self._service: Optional[Any] = self.build_service()

    def retrieve_credentials(self, credentials_filename: str) -> Optional[Any]:
        """
        Parameters
        ----------
        credentials_filename
            The name of the file to attempt to construct the credentials from.

        Returns
        -------
        Credentials or None
            If the credentials file exists and is valid, then credentials object. Otherwise None.
        """

        credentials: Optional[Any] = None
        try:
            credentials = service_account.Credentials.from_service_account_file(credentials_filename,
                                                                                scopes=self._scope)
        except FileNotFoundError:
            logging.error("{} can not be found.".format(credentials_filename))
        except ValueError:
            logging.error("{} is not a valid service account file.".format(credentials_filename))
        except Exception as ex:
            logging.exception(ex)
        return credentials

    def build_service(self) -> Optional[Any]:
        """Takes the credentials object and attempts to build the Sheets service.

        Returns
        -------
        Sheet Service or None
            If the credentials is valid, and service is successfully built,
            then Sheets service is returned. Else None.
        """

        service: Optional[Any] = None
        if self._credentials is not None:
            try:
                service = build("sheets", "v4", credentials=self._credentials)
            except UnknownApiNameOrVersion:
                logging.error("API name or version not recognised.")
            except Exception as ex:
                logging.exception(ex)
        return service

    def get_ranges_values(self, spreadsheet_id: str, ranges: List[str], value_render_option: str,
                          major_dimension: str) -> Dict:
        """Gets the cell data stored at each range in the ranges list

        Parameters
        ----------
        spreadsheet_id
            The unique id of the spreadsheet the api call will be performed on.
        ranges
            The list of ranges to get cell data from.
        value_render_option
            The format the returned cell data is in.

        Returns
        -------
        Dict
            The results of the api call.

        """

        request: Any = self._service.spreadsheets().values().batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=ranges,
            valueRenderOption=value_render_option,
            majorDimension=major_dimension
        )
        return request.execute()

    def update_ranges_values(self, spreadsheet_id: str, value_ranges: Value_Range_T,
                             value_input_option: str) -> Dict:
        """Updates cell data within multiple range.

        .. warning::
            Each value range dictionary in the value_ranges list must only contain the keys 'range' and 'values'.
            i.e. List of dictionaries with format {'range': str, 'values': List[List[str]]}

        Parameters
        ----------
        spreadsheet_id
            The unique id of the spreadsheet the api call will be performed on.
        value_ranges
            List of value range types that list what call ranges will be updated with which values.
        value_input_option
            How the data is handled by the service. i.e. treat data as typed by a user

        Returns
        -------
        Dict
            The results of the api call.
        """

        request: Any = self._service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "data": value_ranges,
                "valueInputOption": value_input_option
            }
        )
        return request.execute()

    def get_worksheet_data(self, spreadsheet_id: str) -> Dict:
        """Retrieves all data about the spreadsheet with the given id.

        Parameters
        ----------
        spreadsheet_id
            The unique id of the spreadsheet the api call will be performed on.

        Returns
        -------
        Dict
            The results of the api call.
        """

        request: Any = self._service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            includeGridData=False
        )
        return request.execute()

    def autosize_worksheets(self, spreadsheet_id: str, worksheet_ids: List[str], dimension: str) -> Dict:
        """Autosizes each worksheet in worksheet_ids by specified dimension.

        Parameters
        ----------
        spreadsheet_id
            The unique id of the spreadsheet the api call will be performed on.
        worksheet_ids
            List of worksheet ids within the spreadsheet to perform auto sizing on.
        dimension
            Determines of rows or columns are auto sized to fit the data.

        Returns
        -------
        Dict
            The results of the api call.
        """

        # Creates the api request body with individual resize dimension requests for each required worksheet
        autosize_requests: List[Dict] = [{
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": dimension
                }
            }
        } for sheet_id in worksheet_ids]
        request: Any = self._service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": autosize_requests
            }
        )
        return request.execute()

    def clear_worksheet_values(self, spreadsheet_id: str, ranges: List[str]) -> Dict:
        """Clears all cell data from each range, preserving formatting

        Parameters
        ----------
        spreadsheet_id
            The unique id of the spreadsheet the api call will be performed on.
        ranges
            List of ranges to clear cell data from

        Returns
        -------
        Dict
            The results of the api call.
        """

        request: Any = self._service.spreadsheets().values().batchClear(
            spreadsheetId=spreadsheet_id,
            body={
                "ranges": ranges
            }
        )
        return request.execute()
