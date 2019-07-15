import logging
from Sheets import Sheets
from functools import wraps
from typing import List, Dict, Callable, Optional, Union, Any


# List of dictionaries with format {'range': str, 'values': List[List[str]]}
Value_Range_T = List[Dict[str, Union[str, List[List[str]]]]]


def sheet_service_function(spreadsheet_func: Callable) -> Callable:
    """Decorator for functions that make calls to the lower level Sheets object to provide common functionality.

    Wrapped function asserts the sheet service property is the right type and returns the result of executing
    the original function if so. Exceptions are caught and logged, with None returned if any exception occurs.
    i.e. if sheet service is not a Sheets object, no api calls are ever attempted due to AssertionError exception.

    Parameters
    ----------
    spreadsheet_func: Callable
        The spreadsheet function to execute

    Returns
    -------
    Callable
        A new function that executes the original function inside a try/except block.
    """

    # Asserts sheet service is valid, and executes required function if so.
    # AssertionErrors and APIErrors are caught and logged.
    # Done to provide all functions with same error handling cleanly.
    @wraps(spreadsheet_func)
    def _wrapper_sheet_service_function(self, *args: Any, **kwargs: Any) -> Optional[Any]:
        try:
            assert isinstance(self._sheet_service, Sheets)
            return spreadsheet_func(self, *args, **kwargs)
        except AssertionError as ex:
            logging.error("Spreadsheet service is invalid. "
                          "{} not executed".format(spreadsheet_func.__name__))
            logging.exception(ex)
            return None
        except Exception as ex:
            logging.error("Unknown error occurred. "
                          "{} not executed".format(spreadsheet_func.__name__))
            logging.exception(ex)
            return None
    return _wrapper_sheet_service_function


class Spreadsheet:
    """A class to represent an individual spreadsheet.
    """

    def __init__(self, sheet_id: str, sheet_service: Sheets) -> None:
        """Assigns spreadsheet properties.

        Parameters
        ----------
        sheet_id: str, optional
            Unique id of spreadsheet to associate with this object (default None)
        sheet_service: Sheets, optional
            The sheet service to be used when performing operations on this spreadsheet (default None)
        """

        self._sheet_id: str = sheet_id
        self._sheet_service: Sheets = sheet_service

    @sheet_service_function
    def get_ranges_values(self, ranges: List[str], value_render_option: str = "FORMATTED_VALUE") -> Optional[Dict]:
        """Gets data of all cells within all given ranges, in the render option provided.

        Parameters
        ----------
        ranges: List[str]
            List of ranges to get data from.
        value_render_option: str, optional
            What format the data returned should be in (default "FORMATTED_VALUE").

        Returns
        -------
        Dict or None
            The result of executing the sheet service batchGet api call.
        """

        return self._sheet_service.spreadsheet_get_ranges_values(self._sheet_id, ranges, value_render_option)

    @sheet_service_function
    def clear_ranges_values(self, ranges: List[str]) -> Optional[Dict]:
        """Removes all data from cells within all given ranges. Formatting is preserved

        Parameters
        ----------
        ranges: List[str]
            List of ranges to remove data from.

        Returns
        -------
        Dict or None
            The result of executing the sheet service batchClear api call.
        """

        return self._sheet_service.spreadsheet_clear_worksheet_values(self._sheet_id, ranges)

    @sheet_service_function
    def update_ranges_values(self, value_ranges: Value_Range_T,
                             value_input_option: str = "USER_ENTERED") -> Optional[Dict]:
        """Updates ranges with different data using list of value range dict structure, using the input format provided.

        .. warning::
            Each value range dictionary in the value_ranges list must only contain the keys 'range' and 'values'.
            i.e. List of dictionaries with format {'range': str, 'values': List[List[str]]}

        Parameters
        ----------
        value_ranges: List of 'value range' dictionaries
            List of value range dicts representing ranges and new data to go in those ranges.
        value_input_option: str, optional
            Decides how the api handles the new data to go in the ranges

        Returns
        -------
        Dict or None
            The result of executing the sheet service batchUpdate api call.
        """

        return self._sheet_service.spreadsheet_update_ranges_values(self._sheet_id, value_ranges, value_input_option)

    @sheet_service_function
    def autosize_all_columns(self) -> Optional[Dict]:
        """Autosizes all columns across all worksheets to fit data

        Returns
        -------
        Dict or None
            The result of executing the sheet service batchUpdate api call
        """

        all_worksheet_ids: List[str] = [str(worksheet["properties"]["sheetId"])
                                        for worksheet in self.get_worksheet_data()["sheets"]]
        return self._sheet_service.spreadsheet_autosize_worksheets(self._sheet_id, all_worksheet_ids, "COLUMNS")

    @sheet_service_function
    def get_worksheet_data(self) -> Optional[Dict]:
        """Returns all data relating to this spreadsheet.

        Returns
        -------
        Dict or None
            The result of executing the sheet service get api call.
        """

        return self._sheet_service.spreadsheet_get_worksheet_data(self._sheet_id)
