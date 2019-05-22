# Created by Roberto Sanchez at 3/13/2019
# Creation of header for documents:

import datetime as dt
import pandas as pd
import settings.initial_settings as init

# number of samples for creating arg_from batch
# 86400 points if the resolution is 1 second (batches of 1000)
# 1440 points if the resolution is 1 minute  (batches of 500)
# 96 points if the resolution is 15 minutes  (batches of 500)
n_samples_max = 1000


def datetime_to_epoch(datetimeObject):
    """
    From datetime type to UNIX epoch
    :param datetimeObject:
    :return:
    """
    if isinstance(datetimeObject, float):
        return datetimeObject
    return datetimeObject.replace(tzinfo=dt.timezone.utc).timestamp()


def epoch_to_datetime(unix_epoch):
    """
    From unix epoch to timestamp
    :param unix_epoch:
    :return:
    """
    if isinstance(unix_epoch, dt.datetime):
        return unix_epoch
    return dt.datetime.fromtimestamp(unix_epoch, tz=dt.timezone.utc)


def to_date(dtime):
    if isinstance(dtime, dt.datetime):
        return dt.datetime(dtime.year, dtime.month, dtime.day, tzinfo=dt.timezone.utc)
    if isinstance(dtime, pd.Timestamp):
        dtime = dtime.to_pydatetime(warn=False)
        return dt.datetime(dtime.year, dtime.month, dtime.day, tzinfo=dt.timezone.utc)
    if isinstance(dtime, float):
        dtime = epoch_to_datetime(dtime)
        return to_date(dtime)
    if isinstance(dtime, str):
        dtx = to_datetime(dtime)
        return to_date(dtx)
    assert isinstance(dtime, dt.date)

def to_datetime(dtime):
    if isinstance(dtime, pd.Timestamp):
        dtime = dtime.to_pydatetime(warn=False)
        dtime = dt.datetime(dtime.year, dtime.month, dtime.day, dtime.hour,
                            dtime.minute, dtime.second, dtime.microsecond, tzinfo=dt.timezone.utc)
        return dtime
    if isinstance(dtime, dt.datetime):
        return dtime
    if isinstance(dtime, float):
        return dt.datetime.fromtimestamp(dtime, tz=dt.timezone.utc)

    rsp = None
    if isinstance(dtime, str):
        for ft in init.SUPPORTED_FORMAT_DATES:
            try:
                rsp = dt.datetime.strptime(dtime, ft)
            except Exception as e:
                pass
        return rsp
    assert isinstance(dtime, dt.datetime)


def to_epoch(dtime):
    if isinstance(dtime, pd.Timestamp):
        dtime = to_datetime(dtime)
        return datetime_to_epoch(dtime)
    if isinstance(dtime, dt.datetime):
        return dtime.replace(tzinfo=dt.timezone.utc).timestamp()
    if isinstance(dtime, float):
        return dtime
    if isinstance(dtime, str):
        dtx = to_datetime(dtime)
        return to_epoch(dtx)
    assert(isinstance(dtime, float))

class IdEntity:
    id_entity = ""
    id_entity_type = ""

    def __init__(self, id_entity, id_entity_type):
        """
        # Identifies arg_from device

        :param id_entity:
        :param id_entity_type:
        """
        self.id_entity = id_entity
        self.id_entity_type = id_entity_type


class IdPointValue:
    _id = None
    date = dt.datetime.now().date()
    id_tag = ""
    first = 0
    last = 0

    def __init__(self, tagName, idEntity, date=None):
        """
        Identifies an analog/event measurement:

        @(_id:) is given by the MongoDb
        @(id_tag:) is given by the MongoDb at the creation moment

        :param tagName:  tag_name for identification purposes. Ex. Home1_power_active
        :param idEntity: ID for device/entity as arg_from way to identify the source of data
        :param date:     date for this document
        """
        if date is None:
            self.date = dt.datetime.date

        self._id = idEntity
        self.id_tag = tagName
        self.date = date.date()


class IdTagSeries:
    id_tag = None
    date = None
    first = 0
    last = 0
    n_samples = 0

    def __init__(self, id_tag: str, date: dt.date=None,
                 first: float=None, last: float=None):
        """
        Identifies arg_from batch of Point values grouped/identified by type
        :param idEntity: Id (idDevice/entity and id_entity_type)
        :param date: indicates the day where the measurements / status belong to
        """
        if date is None:
            d = dt.datetime.now()
            date = dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)

        if first is not None:
            self.first = first
        if last is not None:
            self.last = last

        self.date = date
        self.id_tag = id_tag


class IdGenericHistDocument:
    _id = None
    type = None
    update = dt.datetime.now()

    def __init__(self, idEntity, Type, UpdateTime=None):
        """
        Identifies arg_from generic historical document
        :param idEntity: ID for device
        :param Type:     Type of this document
        :param UpdateTime:  Date when this was created/uploaded.
        """
        if UpdateTime is None:
            UpdateTime = dt.datetime.now(tz=dt.timezone.utc)
        assert isinstance(idEntity, IdEntity)
        assert isinstance(Type, str)
        assert isinstance(UpdateTime, dt.datetime)
        self._id = idEntity
        self.type = Type
        self.update = UpdateTime


class IdGenericDocument:
    _id = None
    type = None

    def __init__(self, idEntity, Type):
        """
        Identifies arg_from generic document
        :param idEntity:    ID for device
        :param Type:        Type of this document
        """
        self._id = idEntity
        self.type = Type


class AnalogRegister:
    value = 0
    timestamp = 0
    quality = None

    def __init__(self, value:float, timestamp: dt.datetime, quality=None):
        self.value = value
        self.to_epoch(timestamp)
        self.quality = quality

    def to_epoch(self, timestamp):
        if isinstance(timestamp, dt.datetime):
            self.timestamp = datetime_to_epoch(timestamp)
        if isinstance(timestamp, float):
            self.timestamp = timestamp

    def to_dict(self):
        d = dict(timestamp= self.timestamp)
        try:
            r = self.value + 0
            d["value"] = r
        except Exception as e:
            print(e)
            d["value"] = None
        if self.quality is not None:
            d["quality"] = self.quality
        return d

    def __str__(self):
        return str(self.to_dict())

    def __get__(self, instance, owner):
        return self.to_dict()

