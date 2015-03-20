"""
Tests for tasks that calculate course videos.

"""
import json
import StringIO
import math
from operator import itemgetter
from datetime import datetime

from mock import Mock, patch

from edx.analytics.tasks.course_video import (
    CourseVideoEventMixin,
    CourseVideoSummaryTask,
    CourseVideoSeekTimesTask,
    UserVideoSummaryTask,
    CourseVideoWorkflow,
    round_to_nearest,
    round_datetime_to_nearest_day,
    EVENT_ACTION_MAPPING,
    SEEK_INTERVAL_IN_SEC,
    DATE_FMT,
)

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.config import with_luigi_config, OPTION_REMOVED
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin


VIDEO_MAX_LEN_IN_SEC = 300


EVENT_TYPES = tuple(EVENT_ACTION_MAPPING.keys())


class CourseVideoTestSetupMixin(object):
    """ Base mixin class for testing course video tasks. """

    # Task constructor
    task = None

    # Which event_type to use for every log creation task.
    # If not set, uses default (play_video).
    log_event_type = None

    def setUp(self):
        self.username = "test_user"
        self.user_id = 24
        self.course_id = "HumanitiesSciences/HB135/Fall2014"
        self.org_id = "HumanitiesSciences"
        self.session_id = "195ng4w5yq834o854"
        self.video_id = "i4x-Engineering-CVX101-video-1938d1ec9c0b4eba97bc2f28dbbc9442"
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.timestamp_datetime = datetime.strptime(self.timestamp, "%Y-%m-%dT%H:%M:%S.%f")
        self.timestamp_datetime = datetime(2013, 12, 17, hour=15, minute=38, second=32, microsecond=805444)
        try:
            self.key = self.task.get_mapper_key(self._create_event_data_dict())
        except:
            self.key = None

    def create_event_log_line(self, **kwargs):
        """ Create an event log with test values, as a JSON string."""
        return json.dumps(self._create_event_data_dict(**kwargs))

    def _create_event_data_dict(self, **kwargs):
        """ Returns event data dict with test values."""
        default_event_type = self.log_event_type if self.log_event_type else EVENT_TYPES[0]
        event_type = kwargs.get("event_type", default_event_type)
        event_dict = {
            "username": self.username,
            "host": "test_host",
            "event_source": "server",
            "event_type": default_event_type,
            "context": {
                "course_id": kwargs.get('course_id', self.course_id),
                "org_id": kwargs.get('context_org_id', self.org_id),
                "path": "/event",
                "user_id": kwargs.get('context_user_id', self.user_id),
            },
            "time": "{0}+00:00".format(self.timestamp),
            "ip": "127.0.0.1",
            "event": self._generate_event_for_video_event_type(event_type),
            "agent": "blah, blah, blah",
            "session": self.session_id,
            "page": None
        }
        event_dict.update(**kwargs)
        return event_dict

    def _generate_event_for_video_event_type(self, event_type):
        """ Generates event data for event log depending on video player action"""
        if event_type == "seek_video":
            old_time = VIDEO_MAX_LEN_IN_SEC / 2
            new_time = VIDEO_MAX_LEN_IN_SEC
            return {
                "id": self.video_id,
                "old_time": old_time,
                "new_time": new_time,
                "type": "onSlideSeek",
                "code": "OzkqKknjuEQ",
            }
        else:
            return {
                "id": self.video_id,
                "currentTime": 77,
                "code": "LzRsgmV8c8c8",
            }

    def _get_reducer_output(self, lines):
        """ Gets dict of keys / values representing reducer output for a given array of lines """
        mapper_outputs = [tuple(self.task.mapper(line))[0] for line in lines]
        unique_keys = set(map(itemgetter(0), mapper_outputs))
        reducer_output = {}
        for _key in unique_keys:
            values = [output[1] for output in mapper_outputs if output[0] == _key]
            reducer_key, output = tuple(self.task.reducer(_key, values))[0]
            reducer_output[reducer_key] = output
        return reducer_output


class CommonCourseVideoTestsMixin(object):
    """ Common class that holds a series of tests useful
    for Course Video tasks. Implement as a mixin because
    these tests rely on implementations of `get_mapper_key`
    and `get_mapper_value` that are specific to the Task """

    def test_parse_log_line(self):
        """ Tests common parse log line for each task type.

        Each task must implement its own version of `get_mapper_key`
        and `get_mapper_value`, hence we put it here
        """
        line = self.create_event_log_line()
        key, value = self.task.parse_log_line(line)

        event_dict = self._create_event_data_dict()
        self.assertEquals(key, self.key)
        self.assertEquals(value, self.task.get_mapper_value(event_dict))

    def test_parse_log_line_bad_input(self):
        """ Tests bad input handling for parsing log lines
        for each task type.

        Each task must implement its own version of `get_mapper_key`
        and `get_mapper_value`, hence we put it here
        """
        line = self.create_event_log_line(time="invalid timestamp")
        self.assertIsNone(self.task.parse_log_line(line))

    def assert_no_output_for(self, line):
        """Assert that an input line generates no output."""
        self.assertEquals(tuple(self.task.mapper(line)), ())

    def test_malformed_log_entry(self):
        line = "this is garbage"
        self.assert_no_output_for(line)

    def test_missing_username(self):
        line = self.create_event_log_line(username=None)
        self.assert_no_output_for(line)

    def test_missing_event_type(self):
        event_dict = self._create_event_data_dict()
        event_dict["old_event_type"] = event_dict['event_type']
        del event_dict["event_type"]
        line = json.dumps(event_dict)
        self.assert_no_output_for(line)

    def test_bad_datetime(self):
        line = self.create_event_log_line(time="this is a bogus time")
        self.assert_no_output_for(line)

    def test_bad_event_data(self):
        line = self.create_event_log_line(event=["not an event"])
        self.assert_no_output_for(line)

    def test_missing_course_id(self):
        line = self.create_event_log_line(context={})
        self.assert_no_output_for(line)

    def test_illegal_course_id(self):
        line = self.create_event_log_line(course_id=";;;;bad/id/val")
        self.assert_no_output_for(line)

    def test_missing_video_id(self):
        temp = self.video_id
        self.video_id = None
        line = self.create_event_log_line()
        print line
        self.video_id = temp
        self.assert_no_output_for(line)

    def test_missing_context(self):
        line = self.create_event_log_line(context=None)
        self.assert_no_output_for(line)


class CourseVideoEventMixinTest(CourseVideoTestSetupMixin, unittest.TestCase):
    """ Tests the base mixin class's mapper / reducer
    functions. Tasks share these functions, so no need
    to rerun common tests. """

    def test_mapper(self):
        """ Tests a fake mapper """

        #--pylint: disable=W0612
        with patch('edx.analytics.tasks.course_video.CourseVideoEventMixin.get_mapper_key') as get_mapper_key, \
            patch('edx.analytics.tasks.course_video.CourseVideoEventMixin.get_mapper_value') as get_mapper_value:

            expected_key = (self.course_id, self.video_id,)
            expected_value = (self.timestamp,)

            self.task = CourseVideoEventMixin()

            get_mapper_key.return_value = expected_key
            get_mapper_value.return_value = expected_value

            line = self.create_event_log_line()
            mapper_output = tuple(self.task.mapper(line))[0]
            self.assertEquals(len(mapper_output), 2)
            self.assertEquals(mapper_output[0], expected_key)
            self.assertEquals(mapper_output[1], expected_value)


class CourseVideoSummaryTaskTest(CourseVideoTestSetupMixin, CommonCourseVideoTestsMixin, unittest.TestCase):
    """ Tests course video basic usage extraction from logs """

    task = CourseVideoSummaryTask(
        mapreduce_engine='local',
        src=None,
        dest=None,
        name=None,
        include=None)

    log_event_type = 'play_video'

    def test_reducer_single_event(self):
        """ Tests reducer output with single event """
        line = self.create_event_log_line()
        reducer_output = self._get_reducer_output([line])

        event_dict = self._create_event_data_dict()
        mapper_key = self.task.get_mapper_key(event_dict)
        value = reducer_output.get(mapper_key)
        self.assertEqual(self.key, mapper_key)
        self.assertIsNotNone(value)
        self.assertEqual(value[0], 1)
        self.assertEqual(value[1], 1)

    def test_reducer_multi_user(self):
        lines = [
            self.create_event_log_line(),
            self.create_event_log_line(username='johndoe111'),
        ]
        reducer_output = self._get_reducer_output(lines)

        event_dict = self._create_event_data_dict()
        mapper_key = self.task.get_mapper_key(event_dict)
        value = reducer_output.get(mapper_key)
        self.assertEqual(self.key, mapper_key)
        self.assertIsNotNone(value)
        self.assertEqual(value[0], 2)
        self.assertEqual(value[1], 2)

    def test_reducer_multi_events(self):
        lines = [
            self.create_event_log_line(),
            self.create_event_log_line(),
            self.create_event_log_line(),
        ]
        reducer_output = self._get_reducer_output(lines)

        event_dict = self._create_event_data_dict()
        mapper_key = self.task.get_mapper_key(event_dict)
        value = reducer_output.get(mapper_key)
        self.assertEqual(self.key, mapper_key)
        self.assertIsNotNone(value)
        self.assertEqual(value[0], 1)
        self.assertEqual(value[1], 3)

    def test_reducer_multi_course_events(self):
        lines = [
            self.create_event_log_line(),
            self.create_event_log_line(course_id="Engineering/CS-224/Spring2015"),
        ]
        reducer_output = self._get_reducer_output(lines)

        self.assertEquals(len(reducer_output), 2)

        event_dict = self._create_event_data_dict()
        mapper_key = self.task.get_mapper_key(event_dict)
        value = reducer_output.get(mapper_key)
        self.assertEqual(self.key, mapper_key)
        self.assertIsNotNone(value)
        self.assertEqual(value[0], 1)
        self.assertEqual(value[1], 1)

    def test_get_mapper_key(self):
        event_dict = self._create_event_data_dict()
        output_key = tuple(self.task.get_mapper_key(event_dict))
        self.assertEqual(len(output_key), 3)

        self.assertEqual(output_key[0], self.course_id)
        self.assertEqual(output_key[1], self.video_id)
        self.assertEqual(output_key[2], round_datetime_to_nearest_day(self.timestamp_datetime))

    def test_get_mapper_value(self):
        event_dict = self._create_event_data_dict()
        mapper_output = tuple(self.task.get_mapper_value(event_dict))
        self.assertEqual(len(mapper_output), 2)
        self.assertEqual(mapper_output[0], self.username)
        self.assertTrue(mapper_output[1] in EVENT_ACTION_MAPPING.values())


class CourseVideoSeekTimesTaskTest(CourseVideoTestSetupMixin, CommonCourseVideoTestsMixin, unittest.TestCase):
    """ Tests seek time extraction from logs """

    task = CourseVideoSeekTimesTask(
        mapreduce_engine='local',
        src=None,
        dest=None,
        name=None,
        include=None)

    log_event_type = 'seek_video'

    def create_seek_times_log(self, **kwargs):
        """ Create a series of seek events for our video """
        events = []
        start = VIDEO_MAX_LEN_IN_SEC / 4
        step_size = VIDEO_MAX_LEN_IN_SEC / 10
        for i in range(5):
            additional_attrs = {"event_type": "seek_video"}
            additional_attrs.update(kwargs)
            log_line = self._create_event_data_dict(**additional_attrs)
            log_line.get('event').update({
                'old_time': start + (i - 1) * step_size,
                'new_time': start + i * step_size
            })
            events.append(json.dumps(log_line))
        return events

    def test_reducer_single_event(self):
        """ Tests reducer output with single event """
        line = self.create_event_log_line()
        reducer_output = self._get_reducer_output([line])

        event_dict = self._create_event_data_dict()
        mapper_key = self.task.get_mapper_key(event_dict)
        value = reducer_output.get(mapper_key)
        self.assertEqual(self.key, mapper_key)
        self.assertIsNotNone(value)
        self.assertEqual(value[0], 1)
        self.assertEqual(value[1], 1)

    def test_reducer_multi_user(self):
        lines = [
            self.create_event_log_line(),
            self.create_event_log_line(username='johndoe111'),
        ]
        reducer_output = self._get_reducer_output(lines)

        event_dict = self._create_event_data_dict()
        mapper_key = self.task.get_mapper_key(event_dict)
        value = reducer_output.get(mapper_key)
        self.assertEqual(self.key, mapper_key)
        self.assertIsNotNone(value)
        self.assertEqual(value[0], 2)
        self.assertEqual(value[1], 2)

    def test_reducer_multi_events(self):
        lines = [
            self.create_event_log_line(event_type='seek_video', context_old_time=200, context_new_time=100),
            self.create_event_log_line(event_type='seek_video', context_old_time=300, context_new_time=500),
            self.create_event_log_line(event_type='seek_video', context_old_time=50, context_new_time=100),
        ]
        reducer_output = self._get_reducer_output(lines)

        event_dict = self._create_event_data_dict()
        mapper_key = self.task.get_mapper_key(event_dict)
        value = reducer_output.get(mapper_key)
        self.assertEqual(self.key, mapper_key)
        self.assertIsNotNone(value)
        self.assertEquals(len(value), 2),
        self.assertEquals(value[0], 3)
        self.assertEquals(value[1], 1)

    def test_reducer_multi_course_events(self):
        lines = [
            self.create_event_log_line(),
            self.create_event_log_line(course_id="Engineering/CS-224/Spring2015"),
        ]
        reducer_output = self._get_reducer_output(lines)

        self.assertEquals(len(reducer_output), 2)

        event_dict = self._create_event_data_dict()
        mapper_key = self.task.get_mapper_key(event_dict)
        value = reducer_output.get(mapper_key)
        self.assertEqual(self.key, mapper_key)
        self.assertIsNotNone(value)
        self.assertEqual(value[0], 1)
        self.assertEqual(value[1], 1)

    def test_get_mapper_key(self):
        event_dict = self._create_event_data_dict()
        output_key = self.task.get_mapper_key(event_dict)
        self.assertEqual(len(output_key), 4)

        self.assertEqual(output_key[0], self.course_id)
        self.assertEqual(output_key[1], self.video_id)
        self.assertEqual(output_key[2], round_datetime_to_nearest_day(self.timestamp_datetime))
        self.assertEqual(output_key[3], round_to_nearest(VIDEO_MAX_LEN_IN_SEC, SEEK_INTERVAL_IN_SEC))

    def test_get_mapper_value(self):
        event_dict = self._create_event_data_dict()
        mapper_output = self.task.get_mapper_value(event_dict)
        self.assertEqual(len(mapper_output), 2)
        self.assertEqual(mapper_output[0], self.username)
        self.assertEqual(mapper_output[1], self.timestamp_datetime)


class UserVideoSummaryTaskTest(CourseVideoTestSetupMixin, CommonCourseVideoTestsMixin, unittest.TestCase):
    """ Tests user video summary from logs """

    task = UserVideoSummaryTask(
        mapreduce_engine='local',
        src=None,
        dest=None,
        name=None,
        include=None)

    def test_reducer_single_event(self):
        """ Tests reducer output with single event """
        line = self.create_event_log_line()
        reducer_output = self._get_reducer_output([line])

        event_dict = self._create_event_data_dict()
        mapper_key = self.task.get_mapper_key(event_dict)
        value = reducer_output.get(mapper_key)
        self.assertEqual(self.key, mapper_key)
        self.assertIsNotNone(value)
        self.assertEqual(len(value), 3)

    def test_reducer_multi_user(self):
        lines = [
            self.create_event_log_line(),
            self.create_event_log_line(username='johndoe111'),
        ]
        reducer_output = self._get_reducer_output(lines)

        event_dict = self._create_event_data_dict()
        mapper_key = self.task.get_mapper_key(event_dict)
        value = reducer_output.get(mapper_key)
        self.assertEqual(self.key, mapper_key)
        self.assertIsNotNone(value)
        self.assertEqual(len(value), 3)

    def test_reducer_multi_events(self):
        lines = [
            self.create_event_log_line(event_type='play_video'),
            self.create_event_log_line(event_type='seek_video'),
            self.create_event_log_line(event_type='stop_video'),
        ]
        reducer_output = self._get_reducer_output(lines)

        event_dict = self._create_event_data_dict()
        mapper_key = self.task.get_mapper_key(event_dict)
        value = reducer_output.get(mapper_key)
        self.assertEqual(self.key, mapper_key)
        self.assertIsNotNone(value)
        self.assertEqual(len(value), 3)

    def test_reducer_multi_course_events(self):
        lines = [
            self.create_event_log_line(),
            self.create_event_log_line(course_id="Engineering/CS224/Spring2015"),
        ]
        reducer_output = self._get_reducer_output(lines)
        self.assertEquals(len(reducer_output), 2)

        event_dict = self._create_event_data_dict()
        mapper_key = self.task.get_mapper_key(event_dict)
        value = reducer_output.get(mapper_key)
        self.assertEqual(self.key, mapper_key)
        self.assertIsNotNone(value)
        self.assertEqual(len(value), 3)

    def test_get_mapper_key(self):
        event_dict = self._create_event_data_dict()
        output_key = self.task.get_mapper_key(event_dict)
        self.assertEqual(len(output_key), 3)

        self.assertEqual(output_key[0], self.course_id)
        self.assertEqual(output_key[1], self.username)
        self.assertEqual(output_key[2], round_datetime_to_nearest_day(self.timestamp_datetime))

    def test_get_mapper_value(self):
        event_dict = self._create_event_data_dict()
        result = self.task.get_mapper_value(event_dict)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0], self.video_id)
        self.assertTrue(result[1] in EVENT_ACTION_MAPPING.values())
        self.assertEqual(result[2], self.session_id)
        self.assertEqual(result[3], self.timestamp_datetime)


class TestHelperFunctions(unittest.TestCase):
    """
    Test cases for helper functions
    """

    def test_round_to_nearest(self):
        self.assertEqual(round_to_nearest(0, 60), 0)
        self.assertEqual(round_to_nearest(30, 60), 0)
        self.assertEqual(round_to_nearest(60, 60), 60)
        self.assertEqual(round_to_nearest(70, 60), 60)
        self.assertEqual(round_to_nearest(122, 60), 120)

    def test_round_to_nearest_bad_input(self):
        with self.assertRaises(ZeroDivisionError):
            round_to_nearest(0, 0)

        self.assertEquals(round_to_nearest(0, float("inf")), None)
        self.assertEquals(round_to_nearest(float("inf"), float('inf')), None)
        self.assertEquals(round_to_nearest(0, float("nan")), None)

        with self.assertRaises(ZeroDivisionError):
            round_to_nearest(float("nan"), 0)

    EXPECTED_DATETIMES_TO_NEAREST_DAYS = (
        (datetime(2015, 8, 3, hour=12, minute=12, second=30), datetime(2015, 8, 3)),
        (datetime(2015, 7, 1, hour=23, minute=59, second=59), datetime(2015, 7, 1)),
        (datetime(2015, 7, 2, hour=0,  minute=0,  second=1),  datetime(2015, 7, 2)),
        (datetime(2015, 3, 29, hour=23,  minute=30,  second=13),  datetime(2015, 3, 29)),
    )

    def test_datetime_to_nearest_day(self):
        for datetime_obj, datetime_rounded in self.EXPECTED_DATETIMES_TO_NEAREST_DAYS:
            self.assertEquals(round_datetime_to_nearest_day(datetime_obj),
                    datetime_rounded.strftime(DATE_FMT))

    def test_datetime_to_nearest_day_bad_input(self):
        self.assertIsNone(round_datetime_to_nearest_day(None))
        self.assertIsNone(round_datetime_to_nearest_day("bad input string"))
        self.assertIsNone(round_datetime_to_nearest_day(123))
        self.assertIsNone(round_datetime_to_nearest_day({}))
        self.assertIsNone(round_datetime_to_nearest_day([]))
