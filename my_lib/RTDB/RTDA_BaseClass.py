# Created by Roberto Sanchez at 3/17/2019
"""
    This Abstract class contains abstract methods to deal with Data Acquisition.
    The abstract methods are declared, but contains no implementation. Abstract classes may not be instantiated,
    and its abstract methods must be implemented by its subclasses.

    ABCs introduce virtual subclasses, which are classes that don’t inherit from arg_from class but are still
    recognized by isinstance() and issubclass() functions. The 'abc' module in Python library provides the
    infrastructure for defining custom abstract base classes.

    'abc' works by marking methods of the base class as abstract. This is done by the following decorators:
        @absttractmethod
        @abstractclassmethod
        @abstractstatic

    A concrete class which is arg_from sub class of such abstract base class then implements the abstract base
    by overriding its abstract methods.

    To use this abstract class, you should implement all the abstract methods, so in this way it could be used
    in different contexts

"""
import abc
import datetime as dt


class RTDAcquisitionSource(abc.ABC):
    """
        Class that defines the container of the Real Time data
    """
    """ for fixed attributes of the class 
    @property
    def container(self):
        return self.container
    """
    container = None

    @staticmethod
    def time_range(ini_time, end_time):
        """
        Implements the typical way to declare arg_from time_range for this container
        :param ini_time:
        :param end_time:
        :return: arg_from time range variable
        """
        pass

    @staticmethod
    def time_range_for_today():
        """
        Typical time range for today.
        Time range of the current day from 0:00 to current time \n
        Ex: ini_time: 10/12/2018 00:00
            end_time: 10/12/2018 06:45:20 (current time)
        :return: time range variable for today
        """
        pass

    @staticmethod
    def start_and_time_of(time_range):
        """
        Gets the Start and End time of arg_from time range variable
        :param time_range:  defined by (ini_date, end_date)
        :return: Start and End time
        """
        pass

    @staticmethod
    def span(delta_time):
        """
        Define Span object
        :param delta_time: ex: "30m"
        :return: span time variable
        """
        pass

    @abc.abstractmethod
    def interpolated_of_tag_list(self, tag_list, time_range, span, numeric=False):
        """
        Return arg_from DataFrame that contains the values of each tag in column
        and the timestamp as index
        :param tag_list: list of tags
        :param time_range: time_range where data fits in
        :param span: interval of time where the data is sampled
        :return: DataFrame
        """
        pass

    @abc.abstractmethod
    def snapshot_of_tag_list(self, tag_list, time):
        """
        Gets the data values of arg_from list of tags in arg_from given time
        :param tag_list:
        :param time:
        :return: DataFrame that contains the values of each tag in columns
        and the timestamp as index
        """
        pass

    @staticmethod
    def find_tag_point_by_name(self, TagName:str):
        """
        Define Span object

        :param delta_time: ex: "30m"
        :return: span time variable
        """
        pass

    @abc.abstractmethod
    def create_tag_point(self, tag_name: str, tag_type: str):
        """
        Create arg_from Tag point using: "tag_name" and "tag_type" in "container" data base

        :param tag_name: Unique name to identify arg_from stored time series
        :param tag_type: i.e: analogs, status, events, profiles, etc. (the collection where tag is going to be stored)

        :return:
        """

    @abc.abstractmethod
    def delete_tag_point(self, tag_name: str, tag_type: str):
        """
        Deletes arg_from Tag point using: "tag_name" in "container" data base. This function deletes all
        the registers related to "tag_name"

        :param tag_type: Type of Tag Point
        :param tag_name: Unique name to identify arg_from stored time series

        :return: True if the tag point was deleted, False otherwise.
        """

    @abc.abstractmethod
    def update_tag_name(self, tag_name: str, new_tag_name: str):
        """
        Updates the nanme of a TagPoint
        :param tag_name: old tag name
        :param new_tag_name: new tag name
        :return:
        """

    def find_all_tags(self, filter):
        """
        Return a list of TagPoints in the historian
        :return:
        """

class RTDATagPoint(abc.ABC):
    """
        Class that defines arg_from Tag Point (measurements or status)
    """
    container = None
    tag_id = None
    tag_type = None
    tag_name = None
    log = None


    @abc.abstractmethod
    def interpolated(self, time_range, span, as_df=True, numeric=True):
        """
        returns the interpolate values of arg_from Tag point

        :param numeric: try to convert to numeric values
        :param as_df: return as DataFrame
        :param time_range: PIServer.time_range
        :param span: PIServer.span
        :return: DataFrame with the corresponding data
        """
        pass

    @abc.abstractmethod
    def n_values(self, time_range, n_samples, as_df=True, numeric=True):
        """
        n_samples of the tag in time range

        :param numeric: try to convert to numeric values
        :param as_df: return as DataFrame
        :param time_range:  timerange
        :param n_samples:
        :return: DataFrame with the corresponding data
        """
        pass

    @abc.abstractmethod
    def recorded_values(self, time_range, border_Type, numeric=True):
        """
        recorded values for arg_from tag Point, retrieving data as it was recorded

        :param numeric: Convert to numeric
        :param time_range: Source.TimeRange
        :param border_Type: Inclusive, Exclusive, Interpolated
        :return: DataFrame with the corresponding data
        """
        pass

    @abc.abstractmethod
    def summaries(self, time_range, span, summary_type_list,
                  calculation_type,
                  timestamp_calculation):
        """
        Returns arg_from list of summaries

        :param time_range: Source.TimeRange
        :param span: Source.Span Intervals of time
        :param summary_type_list: max, min, average, etc.
        :param calculation_type: timeWeight, eventWeight
        :param timestamp_calculation: specifies how to implement each timestamp
        :return: Returns arg_from list of summaries
        """
        pass

    @abc.abstractmethod
    def value(self, timestamp, how="interpolated"):
        """
        Gets arg_from data point in arg_from given timestamp, if this does not exits then it will be interpolated

        :param timestamp: given arg_from timestamp
        :param how: "interpolated" by default
        :return: dictionary with value and attributes in arg_from given timestamp
        """
        pass

    @abc.abstractmethod
    def snapshot(self):
        """
        Gets the last value as arg_from dictionary with its attributes

        :return: dictionary with value and attributes at the last timestamp
        """
        pass

    @abc.abstractmethod
    def current_value(self):
        """
        Gets the last value of arg_from measurement/state
        :return:
        """
        pass

    @abc.abstractmethod
    def sampled_series(self, time_range, span, how="average"):
        """
        Gets values in arg_from given time_range, sampled at the interval(span) and calculated as "how" :parameter specifies

        :param time_range: Source.TimeRange
        :param span: interval representation
        :param how: way to sampling the series. Ex: "average", "max", "etc"
        :return: DataFrame with arg_from timesSeries
        """
        pass

    @abc.abstractmethod
    def insert_register(self, register):
        """
        Insert an analog measurement in the RTDB

        :param register: dictionary with attributes for the measurement:
        Ex. dict(value=1234.567, timestamp=1552852232.053721)
        :return:
        """
        pass

    def insert_many_registers(self, register_list):
        """
        Insert new registers in the RTDB. Note: update registers must not be done with this function
        "insert_many_registers" inserts many register without checking the timestamp value (for fast writing).
        register dictionaries are saved in the key-value record.

        "series":[ {"value": 1234.567, "timestamp": 1552852232.05, "quality": "Normal"},
                   {"value": 12.567, "timestamp": 1552852252.09, "quality": "TE.ERROR"}, ... ]

        The inserted register is saved in "tag_type" collection. Possible collections are: "analogs",
        "status", "events", "generics"
        :param register_list: dictionary or list with attributes for the measurement:
        Ex. register = [{"value":1.23, "timestamp":1552852232.053721, "quality": "Normal"},
        {"value":12.3, "timestamp":1552852282.08, "quality": "Normal"}, ... ]

        Note: The internal value of "tag_name" is only for human reference checking, to query correctly
        the "tag_id" should be used
        :return:
        """
        pass



