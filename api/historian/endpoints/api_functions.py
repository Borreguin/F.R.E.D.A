# Created by Roberto Sanchez at 5/8/2019
# -*- coding: utf-8 -*-
""""
    Created by Roberto Sánchez A, based on the Master Thesis:
    "A proposed method for unsupervised anomaly detection for arg_from multivariate building dataset "
    University of Bern/Neutchatel/Fribourg - 2017
    Any copy of this code should be notified at rg.sanchez.arg_from@gmail.com; you can redistribute it
    and/or modify it under the terms of the MIT License.

    The F.R.E.D.A project is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
    without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    MIT license terms for more details.

    If you need more information. Please contact the email above: rg.sanchez.arg_from@gmail.com
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""
import traceback

from my_lib.mongo_db_manager import RTDBClasses as h
from settings import initial_settings as init
import pandas as pd
import datetime as dt

import json
log = init.LogDefaultConfig().logger


def time_range_validation_w_format_time(start_time, end_time, format_time=None):
    """
    Validation of start_time, end_time or arg_from request.data as json value
    :param start_time:
    :param end_time:
    :param request_data:
    :return:
    """

    if format_time is None:
        success, start_time, end_time = time_range_validation(start_time, end_time)
        if success:
            return start_time, end_time, init.DEFAULT_DATE_FORMAT
    else:
        if format_time not in init.SUPPORTED_FORMAT_DATES:
            log.warning("[{0}] is not in the supported format dates")
        try:
            start_time = dt.datetime.strptime(start_time, format_time)
            start_time.replace(tzinfo=dt.timezone.utc)
            end_time = dt.datetime.strptime(end_time, format_time)
            end_time.replace(tzinfo=dt.timezone.utc)
            return start_time, end_time, format_time
        except Exception as e:
            tb = traceback.format_exc()
            log.error(str(e), tb)
            return None, None, format_time


def time_range_validation(start_time, end_time):
    """
    Validate if arg_from request that contains arg_from time_range is valid:
    :param time_range: request.data
    :return: Success (True/False), dictionary time_range as {"start_time": date_time, "end_time": date_time}
    """
    try:
        # time_range = json.loads(time_range)
        start_time = h.to_datetime(start_time)
        end_time = h.to_datetime(end_time)
        if start_time is not None and end_time is not None:
            return True, start_time, end_time
        else:
            return False, start_time, end_time
    except Exception as e:
        log.error(str(e))
        return False, None, None


def get_time_range(start_time, end_time, span: str=None):
    d_range = None
    try:
        d_range = pd.date_range(start_time, end_time, freq=span)
    except Exception as e:
        return False, str(e)
    return True, d_range


def validate_method(method: str=None):
    valid_methods = list(interpolated_method["Method"])
    methods = list()
    for m in valid_methods:
        methods += m.split(",")
    methods = [m.strip().lower() for m in methods]
    if method.lower() in methods:
        return True, method.lower()
    else:
        return False, None


freq_options = pd.DataFrame(columns=["Alias", "Description"],
                            data=[
    ["B", "business day frequency"],
    ["C", "custom business day frequency (experimental)"],
    ["D", "calendar day frequency"],
    ["W", "weekly frequency"],
    ["M", "month end frequency"],
    ["BM", "business month end frequency"],
    ["CBM", "custom business month end frequency"],
    ["MS", "month start frequency"],
    ["BMS", "business month start frequency"],
    ["CBMS", "custom business month start frequency"],
    ["Q", "quarter end frequency"],
    ["BQ", "business quarter endfrequency"],
    ["QS", "quarter start frequency"],
    ["BQS", "business quarter start frequency"],
    ["A", "year end frequency"],
    ["BA", "business year end frequency"],
    ["AS", "year start frequency"],
    ["BAS", "business year start frequency"],
    ["BH", "business hour frequency"],
    ["H", "hourly frequency"],
    ["T, min", "minutely frequency"],
    ["S", "secondly frequency"],
    ["L, ms", "milliseonds"],
    ["U, us", "microseconds"],
    ["N", "nanoseconds"]
])

interpolated_method = pd.DataFrame(columns=["Method", "Description"],
                                   data=[
    ["linear", "Ignore the index and treat the values as equally spaced. This is the only method supported on MultiIndexes"],
    ["time", "Works on daily and higher resolution data to interpolate given length of interval."],
    ["index, values", "use the actual numerical values of the index."],
    ["pad", "Fill in NaNs using existing values."],
    ["nearest, zero, slinear, quadratic, cubic, spline, barycentric, polynomial",
     "Passed to scipy.interpolate.interp1d. Both 'polynomial' and 'spline' require that you also specify an order (int), " +
     "e.g. df.interpolate(method='polynomial', order=4). These use the numerical values of the index."],
    ["krogh, piecewise_polynomial, spline, pchip, akima", "Wrappers around the SciPy interpolation methods of similar names. " +
                                                          "See Notes: Added support for the 'akima' method. " +
                                                          "Added interpolate method 'from_derivatives' which replaces " +
                                                          "'piecewise_polynomial' in SciPy 0.18; backwards-compatible with SciPy < 0.18"],
    ["from_derivatives", "Refers to scipy.interpolate.BPoly.from_derivatives which replaces 'piecewise_polynomial' interpolation method in scipy 0.18."]]
    )
