from flask_restplus import reqparse
from settings.initial_settings import SUPPORTED_FORMAT_DATES as fmt_time_list
from settings.initial_settings import DEFAULT_DATE_FORMAT as fmt_time_default
import datetime as dt
"""
    Configure the API HTML to show for each services the arguments that are needed 
    (Explain the arguments for each service)
"""

""" arguments for test_dict service """
test = reqparse.RequestParser()
test.add_argument('test_id', type=str, required=True, default="any_index", help='String index')

""" arguments for tag service """
tag = reqparse.RequestParser()
tag.add_argument('tag_name', type=str, required=True, default="New_Tag_Name",
                 help='Check whether a TagPoint exists or not in the historian')

tag.add_argument('tag_type', type=str, required=False, default="Generic",
                 help='Collection where the time series will be saved')


""" arguments for snapshoot """
format_time = reqparse.RequestParser()
format_time.add_argument("format_time", type=str, required=False, default="%Y-%m-%d %H:%M:%S",
                         help="Format: epoch or " + str(fmt_time_list))

""" arguments for recorded values """
start_time = dt.datetime.now() - dt.timedelta(days=1)
end_time = dt.datetime.now()
range_time = reqparse.RequestParser()
range_time.add_argument("start_time", type=str, required=False, default=start_time.strftime(fmt_time_default),
                        help="Supported formats: " + str(fmt_time_list))
range_time.add_argument("end_time", type=str, required=False, default=end_time.strftime(fmt_time_default),
                        help="Supported formats: " + str(fmt_time_list))
range_time.add_argument("format_time", type=str, required=False, default="%Y-%m-%d %H:%M:%S",
                        help="Format: " + str(fmt_time_list))


""" arguments for interpolated values """
range_time_with_span = range_time.copy()
range_time_with_span.add_argument("span", type=str, help="./help/span",
                                  required=False, default="15 min")

""" arguments for interpolated values with method """
range_time_with_span_and_method = range_time_with_span.copy()
range_time_with_span_and_method.add_argument("method", type=str, help="./help/interpolated/method",
                                             required=False, default="time")

""" arguments for registers service """
tag_list = reqparse.RequestParser()
tag_list.add_argument("tag_list", type=list, help="List of tag_names",
                      required=True, default=["dev1.tag1", "dev2.tag2", "dev3.tag3"])

"""arguments for tag_list with time format"""
tag_list_w_time_format = tag_list.copy()
tag_list_w_time_format.add_argument("format_time", type=str, required=False, default="%Y-%m-%d %H:%M:%S",
                        help="Format: " + str(fmt_time_list))

""" arguments for tag_list, time_range  """
tag_list_time_range = tag_list.copy()
tag_list_time_range.add_argument("start_time", type=str, required=True,
                        default=start_time.strftime(fmt_time_default),
                        help="Supported formats: " + str(fmt_time_list))
tag_list_time_range.add_argument("end_time", type=str, required=True,
                        default=end_time.strftime(fmt_time_default),
                        help="Supported formats: " + str(fmt_time_list))

""" arguments for tag_list, time_range and time_format """
tag_list_time_range_w_time_format = tag_list_time_range.copy()
tag_list_time_range_w_time_format.add_argument("format_time", type=str, required=False, default="%Y-%m-%d %H:%M:%S",
                        help="Format: " + str(fmt_time_list))

""" tag_list_time_range_span_and_time_format """
tag_list_time_range_span_w_time_format = tag_list_time_range.copy()
tag_list_time_range_span_w_time_format.add_argument("span", type=str, help="./help/span",
                                  required=False, default="15 min")
tag_list_time_range_span_w_time_format.add_argument("method", type=str, help="./help/interpolated/method",
                                             required=False, default="time")
tag_list_time_range_span_w_time_format.add_argument("format_time", type=str, required=False, default="%Y-%m-%d %H:%M:%S",
                        help="Format: " + str(fmt_time_list))
