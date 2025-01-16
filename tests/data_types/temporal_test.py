# pylint: disable=missing-class-docstring, redefined-outer-name

import json

import pytz
from neo4j.time import Date, DateTime, Duration, Time
from pydantic import BaseModel

from pyneo4j_ogm.data_types.temporal import (
    NativeDate,
    NativeDateTime,
    NativeDuration,
    NativeTime,
)
from pyneo4j_ogm.pydantic import (
    get_model_dump,
    get_model_dump_json,
    get_model_schema,
    parse_json,
    parse_obj,
)


class DateTimeModel(BaseModel):
    datetime: NativeDateTime


class DateModel(BaseModel):
    date: NativeDate


class TimeModel(BaseModel):
    time: NativeTime


class DurationModel(BaseModel):
    duration: Duration

    model_config = {"arbitrary_types_allowed": True}


class TestDateTime:
    def test_parsing_from_class(self):
        model = parse_obj(DateTimeModel, {"datetime": DateTime(2025, 1, 1, 1, 1, 1, 1, pytz.timezone("US/Eastern"))})

        assert isinstance(model.datetime, DateTime)
        assert model.datetime.year == 2025
        assert model.datetime.month == 1
        assert model.datetime.day == 1
        assert model.datetime.hour == 1
        assert model.datetime.minute == 1
        assert model.datetime.second == 1
        assert model.datetime.nanosecond == 1
        assert model.datetime.tzinfo is not None
        assert model.datetime.tzinfo.tzname(None) == pytz.timezone("US/Eastern").tzname(None)
        assert model.datetime.tzinfo.utcoffset(None) == pytz.timezone("US/Eastern").utcoffset(None)

    def test_parsing_from_str(self):
        model = parse_obj(
            DateTimeModel, {"datetime": DateTime(2025, 1, 1, 1, 1, 1, 1, pytz.timezone("US/Eastern")).iso_format()}
        )

        assert isinstance(model.datetime, DateTime)
        assert model.datetime.year == 2025
        assert model.datetime.month == 1
        assert model.datetime.day == 1
        assert model.datetime.hour == 1
        assert model.datetime.minute == 1
        assert model.datetime.second == 1
        assert model.datetime.nanosecond == 1
        assert model.datetime.tzinfo is not None

    def test_parsing_from_native_datetime(self):
        model = parse_obj(
            DateTimeModel, {"datetime": DateTime(2025, 1, 1, 1, 1, 1, 1, pytz.timezone("US/Eastern")).to_native()}
        )

        assert isinstance(model.datetime, DateTime)
        assert model.datetime.year == 2025
        assert model.datetime.month == 1
        assert model.datetime.day == 1
        assert model.datetime.hour == 1
        assert model.datetime.minute == 1
        assert model.datetime.second == 1
        assert model.datetime.nanosecond == 0
        assert model.datetime.tzinfo is not None
        assert model.datetime.tzinfo.tzname(None) == pytz.timezone("US/Eastern").tzname(None)
        assert model.datetime.tzinfo.utcoffset(None) == pytz.timezone("US/Eastern").utcoffset(None)

    def test_parsing_from_json(self):
        model = parse_json(
            DateTimeModel,
            json.dumps({"datetime": DateTime(2025, 1, 1, 1, 1, 1, 1, pytz.timezone("US/Eastern")).iso_format()}),
        )

        assert isinstance(model.datetime, DateTime)
        assert model.datetime.year == 2025
        assert model.datetime.month == 1
        assert model.datetime.day == 1
        assert model.datetime.hour == 1
        assert model.datetime.minute == 1
        assert model.datetime.second == 1
        assert model.datetime.nanosecond == 1
        assert model.datetime.tzinfo is not None

    def test_serializing(self):
        model = parse_obj(DateTimeModel, {"datetime": DateTime(2025, 1, 1, 1, 1, 1, 1, pytz.timezone("US/Eastern"))})
        serialized = get_model_dump(model)

        assert isinstance(serialized, dict)
        assert isinstance(serialized["datetime"], DateTime)
        assert model.datetime.year == serialized["datetime"].year
        assert model.datetime.month == serialized["datetime"].month
        assert model.datetime.day == serialized["datetime"].day
        assert model.datetime.hour == serialized["datetime"].hour
        assert model.datetime.minute == serialized["datetime"].minute
        assert model.datetime.second == serialized["datetime"].second
        assert model.datetime.nanosecond == serialized["datetime"].nanosecond
        assert model.datetime.tzinfo is not None
        assert serialized["datetime"].tzinfo is not None
        assert model.datetime.tzinfo.tzname(None) == serialized["datetime"].tzinfo.tzname(None)
        assert model.datetime.tzinfo.utcoffset(None) == serialized["datetime"].tzinfo.utcoffset(None)

    def test_serializing_json(self):
        model = parse_json(
            DateTimeModel,
            json.dumps({"datetime": DateTime(2025, 1, 1, 1, 1, 1, 1, pytz.timezone("US/Eastern")).iso_format()}),
        )
        serialized = get_model_dump_json(model)
        parsed_serialized = json.loads(serialized)

        assert isinstance(serialized, str)
        assert isinstance(parsed_serialized["datetime"], str)
        assert parsed_serialized["datetime"] == "2025-01-01T01:01:01.000000001-06:56"

    def test_json_schema(self):
        model = parse_json(
            DateTimeModel,
            json.dumps({"datetime": DateTime(2025, 1, 1, 1, 1, 1, 1, pytz.timezone("US/Eastern")).iso_format()}),
        )
        schema = get_model_schema(model)

        assert "datetime" in schema["properties"]
        assert schema["properties"]["datetime"]["type"] == "string"
        assert schema["properties"]["datetime"]["format"] == "date-time"


class TestDate:
    def test_parsing_from_class(self):
        model = parse_obj(DateModel, {"date": Date(2025, 1, 1)})

        assert isinstance(model.date, Date)
        assert model.date.year == 2025
        assert model.date.month == 1
        assert model.date.day == 1

    def test_parsing_from_str(self):
        model = parse_obj(DateModel, {"date": Date(2025, 1, 1).iso_format()})

        assert isinstance(model.date, Date)
        assert model.date.year == 2025
        assert model.date.month == 1
        assert model.date.day == 1

    def test_parsing_from_native_date(self):
        model = parse_obj(DateModel, {"date": Date(2025, 1, 1).to_native()})

        assert isinstance(model.date, Date)
        assert model.date.year == 2025
        assert model.date.month == 1
        assert model.date.day == 1

    def test_parsing_from_json(self):
        model = parse_json(DateModel, json.dumps({"date": Date(2025, 1, 1).iso_format()}))

        assert isinstance(model.date, Date)
        assert model.date.year == 2025
        assert model.date.month == 1
        assert model.date.day == 1

    def test_serializing(self):
        model = parse_obj(DateModel, {"date": Date(2025, 1, 1)})
        serialized = get_model_dump(model)

        assert isinstance(serialized, dict)
        assert isinstance(serialized["date"], Date)
        assert model.date.year == serialized["date"].year
        assert model.date.month == serialized["date"].month
        assert model.date.day == serialized["date"].day

    def test_serializing_json(self):
        model = parse_json(DateModel, json.dumps({"date": Date(2025, 1, 1).iso_format()}))
        serialized = get_model_dump_json(model)
        parsed_serialized = json.loads(serialized)

        assert isinstance(serialized, str)
        assert isinstance(parsed_serialized["date"], str)
        assert parsed_serialized["date"] == "2025-01-01"

    def test_json_schema(self):
        model = parse_json(DateModel, json.dumps({"date": Date(2025, 1, 1).iso_format()}))
        schema = get_model_schema(model)

        assert "date" in schema["properties"]
        assert schema["properties"]["date"]["type"] == "string"
        assert schema["properties"]["date"]["format"] == "date"


class TestTime:
    def test_parsing_from_class(self):
        model = parse_obj(
            TimeModel,
            {"time": Time(1, 1, 1, 1, pytz.timezone("US/Eastern"))},
        )

        assert isinstance(model.time, Time)
        assert model.time.hour == 1
        assert model.time.minute == 1
        assert model.time.second == 1
        assert model.time.nanosecond == 1
        assert model.time.tzinfo is not None
        assert model.time.tzinfo.tzname(None) == pytz.timezone("US/Eastern").tzname(None)
        assert model.time.tzinfo.utcoffset(None) == pytz.timezone("US/Eastern").utcoffset(None)

    def test_parsing_from_str(self):
        model = parse_obj(
            TimeModel,
            {"time": Time(1, 1, 1, 1, pytz.timezone("US/Eastern")).iso_format()},
        )

        assert isinstance(model.time, Time)
        assert model.time.hour == 1
        assert model.time.minute == 1
        assert model.time.second == 1
        assert model.time.nanosecond == 1
        assert model.time.tzinfo is None

    def test_parsing_from_native_time(self):
        model = parse_obj(
            TimeModel,
            {"time": Time(1, 1, 1, 1, pytz.timezone("US/Eastern")).to_native()},
        )

        assert isinstance(model.time, Time)
        assert model.time.hour == 1
        assert model.time.minute == 1
        assert model.time.second == 1
        assert model.time.nanosecond == 0
        assert model.time.tzinfo is not None
        assert model.time.tzinfo.tzname(None) == pytz.timezone("US/Eastern").tzname(None)
        assert model.time.tzinfo.utcoffset(None) == pytz.timezone("US/Eastern").utcoffset(None)

    def test_parsing_from_json(self):
        model = parse_json(
            TimeModel,
            json.dumps({"time": Time(1, 1, 1, 1, pytz.timezone("US/Eastern")).iso_format()}),
        )

        assert isinstance(model.time, Time)
        assert model.time.hour == 1
        assert model.time.minute == 1
        assert model.time.second == 1
        assert model.time.nanosecond == 1
        assert model.time.tzinfo is None

    def test_serializing(self):
        model = parse_obj(
            TimeModel,
            {"time": Time(1, 1, 1, 1, pytz.timezone("US/Eastern"))},
        )
        serialized = get_model_dump(model)

        assert isinstance(serialized, dict)
        assert isinstance(serialized["time"], Time)
        assert model.time.hour == serialized["time"].hour
        assert model.time.minute == serialized["time"].minute
        assert model.time.second == serialized["time"].second
        assert model.time.nanosecond == serialized["time"].nanosecond
        assert model.time.tzinfo is not None
        assert serialized["time"].tzinfo is not None
        assert model.time.tzinfo.tzname(None) == serialized["time"].tzinfo.tzname(None)
        assert model.time.tzinfo.utcoffset(None) == serialized["time"].tzinfo.utcoffset(None)

    def test_serializing_json(self):
        model = parse_json(
            TimeModel,
            json.dumps({"time": Time(1, 1, 1, 1, pytz.timezone("US/Eastern")).iso_format()}),
        )
        serialized = get_model_dump_json(model)
        parsed_serialized = json.loads(serialized)

        assert isinstance(serialized, str)
        assert isinstance(parsed_serialized["time"], str)
        assert parsed_serialized["time"] == "01:01:01.000000001"

    def test_json_schema(self):
        model = parse_json(
            TimeModel,
            json.dumps({"time": Time(1, 1, 1, 1, pytz.timezone("US/Eastern")).iso_format()}),
        )
        schema = get_model_schema(model)

        assert "time" in schema["properties"]
        assert schema["properties"]["time"]["type"] == "string"
        assert schema["properties"]["time"]["format"] == "time"


class TestDuration:
    def test_parsing_from_class(self):
        model = parse_obj(DurationModel, {"duration": Duration(1, 1, 1, 1, 1, 1, 1, 1, 1, 1)})

        assert isinstance(model.duration, Duration)
        assert model.duration.years_months_days == (1, 1, 8)
        assert model.duration.hours_minutes_seconds_nanoseconds == (1, 1, 1, 1001001)
        assert model.duration.months == 13
        assert model.duration.days == 8
        assert model.duration.seconds == 3661
        assert model.duration.nanoseconds == 1001001

    def test_parsing_from_str(self):
        model = parse_obj(DurationModel, {"duration": Duration(1, 1, 1, 1, 1, 1, 1, 1, 1, 1).iso_format()})

        assert isinstance(model.duration, Duration)
        assert model.duration.years_months_days == (1, 1, 8)
        assert model.duration.hours_minutes_seconds_nanoseconds == (1, 1, 1, 1001001)
        assert model.duration.months == 13
        assert model.duration.days == 8
        assert model.duration.seconds == 3661
        assert model.duration.nanoseconds == 1001001

    def test_parsing_from_json(self):
        model = parse_json(DurationModel, json.dumps({"duration": Duration(1, 1, 1, 1, 1, 1, 1, 1, 1, 1).iso_format()}))

        assert isinstance(model.duration, Duration)
        assert model.duration.years_months_days == (1, 1, 8)
        assert model.duration.hours_minutes_seconds_nanoseconds == (1, 1, 1, 1001001)
        assert model.duration.months == 13
        assert model.duration.days == 8
        assert model.duration.seconds == 3661
        assert model.duration.nanoseconds == 1001001

    def test_serializing(self):
        model = parse_obj(DurationModel, {"duration": Duration(1, 1, 1, 1, 1, 1, 1, 1, 1, 1)})
        serialized = get_model_dump(model)

        assert isinstance(serialized, dict)
        assert isinstance(serialized["duration"], Duration)
        assert model.duration.years_months_days == serialized["duration"].years_months_days
        assert (
            model.duration.hours_minutes_seconds_nanoseconds == serialized["duration"].hours_minutes_seconds_nanoseconds
        )
        assert model.duration.months == serialized["duration"].months
        assert model.duration.days == serialized["duration"].days
        assert model.duration.seconds == serialized["duration"].seconds
        assert model.duration.nanoseconds == serialized["duration"].nanoseconds

    def test_serializing_json(self):
        model = parse_json(DurationModel, json.dumps({"duration": Duration(1, 1, 1, 1, 1, 1, 1, 1, 1, 1).iso_format()}))
        serialized = get_model_dump_json(model)
        parsed_serialized = json.loads(serialized)

        assert isinstance(serialized, str)
        assert isinstance(parsed_serialized["duration"], str)
        assert parsed_serialized["duration"] == "P1Y1M8DT1H1M1.001001001S"

    def test_json_schema(self):
        model = parse_json(DurationModel, json.dumps({"duration": Duration(1, 1, 1, 1, 1, 1, 1, 1, 1, 1).iso_format()}))
        schema = get_model_schema(model)

        assert "duration" in schema["properties"]
        assert schema["properties"]["duration"]["type"] == "string"
        assert schema["properties"]["duration"]["format"] == "duration"
