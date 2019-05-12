from flask_restplus import fields
from api.restplus import api
from settings.initial_settings import SUPPORTED_FORMAT_DATES as time_formats

"""
    Configure the API HTML to show for each services the schemas that are needed 
    for posting and putting
    (Explain the arguments for each service)
"""

""" serializer for test_dict service """
test = api.model(
    'Value to post',
    {'value': fields.String(required=True, description='Any value to be stored using the test_id')})

test_out = api.inherit('Saved value', test, {
    'test_id': fields.String(description="String index"),
    'success': fields.Boolean(description="Successful operation")
})

""" serializers for tag service """
tag = api.model("Create a new TagPoint",
            {'tag_name': fields.String(description="Unique TagName for identifying a TagPoint",
                                       required=True, default='newTagName'),
             'tag_type': fields.String(description="Collection where the time series will be saved",
                                       default='generic')})

tag_delete = api.model("Delete a TagPoint",
            {'tag_name': fields.String(description="Unique TagName for identifying a TagPoint",
                                       required=True, default='tag_name_to_delete'),
             'tag_type': fields.String(description="Collection where the time series is saved",
                                       required=True, default='analogs, events, generic, etc.')})

tag_update = api.inherit("Update name of a TagPoint",{
    "new_tag_name": fields.String(description= "New tag name for a TagPoint",
                                  required=True, default="NewTagName")
})

""" serializers for tag snap shoot service """

fmt_time = api.model("Format time",
            {"format_time": fields.String(description="Time format options: " + str(time_formats) ,
                                          required=True, default="%Y-%m-%d %H:%M:%S"
                                          )})

register = api.model("Register", {
    "value": fields.Float(description="Value to save.", required=True),
    "timestamp": fields.String(descriptio="Timestamp",
                              required=True, default="yyyy-mm-dd H:M:S")
})


""" serializers for recorded_values service """

register_list = api.model("List of registers",{
    "registers": fields.List(fields.Nested(register))
})