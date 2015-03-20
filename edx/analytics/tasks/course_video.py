"""
Luigi tasks for extracting course video information from tracking logs.
"""
import math
import html5lib
import json
from operator import itemgetter
from datetime import datetime

import luigi
import luigi.hdfs
import luigi.s3

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import PathSetTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.mysql_load import MysqlInsertTask, MysqlInsertTaskMixin
import edx.analytics.tasks.util.eventlog as eventlog
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util

import logging
log = logging.getLogger(__name__)

################################
# Task Map-Reduce definitions
################################

EVENT_ACTION_MAPPING = {
    'play_video': 'P',
    'seek_video': 'E',
    'stop_video': 'S',
    'load_video': 'L',
}

SEEK_INTERVAL_IN_SEC = 10

DATE_FMT = "%m-%d-%Y"

def round_to_nearest(number, roundto):
    """
    Rounds number to nearest multiple of roundto and returns the
    integer multiple of roundto.

    Returns None if an error occurs
    """
    result = round(number / roundto) * roundto
    try:
        return int(result)
    except:
        return None


def round_datetime_to_nearest_day(datetime_obj):
    """
    Rounds date down to nearest day,
    and reports the date in a common
    dd-mm-yyyy format.

    Returns None on error
    """
    if type(datetime_obj) != datetime:
        return None
    else:
        return datetime_obj.replace(hour=0, minute=0, second=0, microsecond=0).strftime(DATE_FMT)


class CourseVideoEventMixin(object):
    """
    Base class for extracting video play information from event.logs.
    The most basic implementation counts, for each video in each
    course, the number of unique users and number of times the
    video has been played.
    """

    def mapper(self, line):
        """ Docstring """
        parsed_tuple_or_none = self.parse_log_line(line)
        if parsed_tuple_or_none is not None:
            yield parsed_tuple_or_none

    def reducer(self, key, values):
        """
        Base classes should implement this method
        """
        raise NotImplementedError

    def parse_log_line(self, line):
        """ Docstring """
        try:
            event = json.loads(line)
        except ValueError:
            log.error("Error parsing log line %s" % line)
            return None

        key = self.get_mapper_key(event)
        value = self.get_mapper_value(event)
        if key is None or value is None:
            return None
        return key, value

    def get_mapper_key(self, event):
        """
        Outputs the key used by the mapper and reducer.
        Base classes should implement this method.
        """
        raise NotImplementedError

    def get_mapper_value(self, event):
        """
        Outputs the other tuple used by the mapper.
        Reducer is sent an iterable of these tuples.
        Base classes should implement this method.
        """
        raise NotImplementedError


class CourseVideoSummaryMixin(CourseVideoEventMixin):
    """ Docstring """

    def reducer(self, key, value_gen):
        """ Docstring """
        values = [x for x in value_gen]
        usernames = map(itemgetter(0), values)
        unique_users = len(set(usernames))
        total_activity = len(values)
        output = (unique_users, total_activity)
        yield key, output

    def get_mapper_key(self, event):
        """ Docstring """
        if type(event) != dict:
            return None

        video_data = eventlog.get_event_data(event)
        if video_data is None:
            return None

        context = event.get('context')
        if type(context) != dict:
            return None

        course_id = context.get('course_id')
        if course_id is None or not opaque_key_util.is_valid_course_id(course_id):
            return None

        video_id = video_data.get('id')
        if video_id is None:
            return None

        timestamp = eventlog.get_event_time(event)
        if timestamp is None:
            return None

        date = round_datetime_to_nearest_day(timestamp)
        return (course_id, video_id, date)

    def get_mapper_value(self, event):
        """ Docstring """
        if type(event) != dict:
            return None

        username = event.get('username')
        if not username:
            return None

        action = event.get('event_type')
        if action != "play_video":
            return None

        encoded_action = EVENT_ACTION_MAPPING.get(action)
        return (username, encoded_action)


class CourseVideoSeekTimesMixin(CourseVideoEventMixin):
    """
    For each video in a course, counts the number of seeks
    at each interval of the video.
    """

    def reducer(self, key, values):
        """ Docstring """
        num_seeks = 0
        users = set()
        for value in values:
            num_seeks += 1
            users.add(value[0])
        num_users = len(users)
        output = (num_seeks, num_users)
        yield key, output

    def get_mapper_key(self, event):
        """ Docstring """
        if type(event) != dict:
            return None

        action = event.get('event_type')

        if action != 'seek_video':
            return None

        video_data = eventlog.get_event_data(event)
        if video_data is None:
            return None

        timestamp = eventlog.get_event_time(event)
        if timestamp is None:
            return None

        context = event.get('context')
        if type(context) != dict:
            return None

        course_id = context.get('course_id')
        if course_id is None:
            return None

        video_id = video_data.get('id')
        if video_id is None:
            return None

        target = int(video_data.get('new_time'))
        if not (timestamp and course_id and video_id and target):
            return None

        if math.isnan(target) or not opaque_key_util.is_valid_course_id(course_id):
            return None

        date = round_datetime_to_nearest_day(timestamp)

        seek_interval = round_to_nearest(target, SEEK_INTERVAL_IN_SEC)
        if not date and not seek_interval:
            return None

        return (course_id, video_id, date, seek_interval)

    def get_mapper_value(self, event):
        """ Docstring """
        if type(event) != dict:
            return None

        username = event.get('username')
        if not username:
            return None

        timestamp = eventlog.get_event_time(event)
        if not timestamp:
            return None

        return (username, timestamp)


class UserVideoSummaryMixin(CourseVideoEventMixin):
    """ Docstring """

    def reducer(self, key, value_gen):
        """ Docstring """
        values = [value for value in value_gen]
        total_activity = len(values)
        video_ids = map(itemgetter(0), values)
        unique_videos_watched = len(set(video_ids))

        total_time_spent = self.get_total_time_spent(values)

        output = (total_activity, unique_videos_watched, total_time_spent)
        yield key, output

    def get_mapper_key(self, event):
        """ Docstring """
        if type(event) != dict:
            return None

        context = event.get('context')
        if type(context) != dict:
            return None

        course_id = context.get('course_id')
        username = event.get('username')
        timestamp = eventlog.get_event_time(event)
        date = round_datetime_to_nearest_day(timestamp)
        if not (course_id and username and timestamp and date):
            return None

        if not opaque_key_util.is_valid_course_id(course_id):
            return None

        return (course_id, username, date)

    def get_mapper_value(self, event):
        """ Docstring """
        if type(event) != dict:
            return None

        action = event.get('event_type')
        if action not in EVENT_ACTION_MAPPING:
            return None

        video_data = eventlog.get_event_data(event)
        if video_data is None:
            return None

        encoded_action = EVENT_ACTION_MAPPING.get(action)
        video_id = video_data.get("id")
        timestamp = eventlog.get_event_time(event)
        session_id = event.get('session')
        if not (video_id and timestamp and session_id):
            return None

        return (video_id, encoded_action, session_id, timestamp)

    def generate_video_session_attr_filter(self, video_id, session_id):
        """ Docstring """
        def filter_fn(value):
            """ Docstring """
            return value[0] == video_id and value[2] == session_id
        return filter_fn

    def get_video_session_attrs(self, value):
        """ Docstring """
        return (value[0], value[2])

    def get_timestamp_attr(self):
        """ Gets timestamp attribute from map value """
        return itemgetter(3)

    def get_total_time_spent(self, values):
        """ Docstring """
        total_time = 0
        video_session_pairs = set(map(self.get_video_session_attrs, values))

        # calculate total time spent on videos
        for video_id, session_id in video_session_pairs:
            session_events = filter(
                    self.generate_video_session_attr_filter(video_id, session_id),
                    values)

            timestamp_getter = self.get_timestamp_attr()
            if len(session_events) != 0:
                earliest_event = min(session_events, key=timestamp_getter)
                latest_event = max(session_events, key=timestamp_getter)
                started_watching = timestamp_getter(earliest_event)
                stopped_watching = timestamp_getter(latest_event)

                total_seconds = (stopped_watching - started_watching).total_seconds()
                total_time += max(total_seconds, 0)

        return int(total_time)

##################################
# Task requires/output definitions
##################################

class CourseVideoDownstreamMixin(object):
    """
    Base class for adding Luigi parameters to course video mapreduce tasks.

    Parameters:
        name: a unique identifier to distinguish one run from another.  It is used in
            the construction of output filenames, so each run will have distinct outputs.
        src:  a URL to the root location of input tracking log files.
        dest:  a URL to the root location to write output file(s).
        include:  a list of patterns to be used to match input files, relative to `src` URL.
            The default value is ['*'].
        manifest: a URL to a file location that can store the complete set of input files.
    """
    name = luigi.Parameter()
    src = luigi.Parameter(is_list=True)
    dest = luigi.Parameter()
    include = luigi.Parameter(is_list=True, default=('*',))
    # A manifest file is required by hadoop if there are too many
    # input paths. It hits an operating system limit on the number of
    # arguments passed to the mapper process on the task nodes.
    manifest = luigi.Parameter(default=None)


# pylint: disable=E1101
class BaseCourseVideoMapReduceTask(MapReduceJobTask):
    """Base class for course video mapreduce tasks."""
    def requires(self):
        return PathSetTask(self.src, self.include, self.manifest)

    def extra_modules(self):
        import six
        return [html5lib, six]


class CourseVideoSummaryTask(
    CourseVideoDownstreamMixin,
    CourseVideoSummaryMixin,
    BaseCourseVideoMapReduceTask
):
    """
    Basic mapreduce task. Calculates total activity and unique users for each
    video / user combo
    """
    def output(self):
        output_name = u'cv_video_summary_{name}/'.format(name=self.name)
        return get_target_from_url(url_path_join(self.dest, output_name))


class CourseVideoSeekTimesTask(
    CourseVideoDownstreamMixin,
    CourseVideoSeekTimesMixin,
    BaseCourseVideoMapReduceTask
):
    """
    Basic mapreduce task. Calculates total activity and unique users for each
    video / user combo
    """
    def output(self):
        output_name = u'cv_seek_times_{name}/'.format(name=self.name)
        return get_target_from_url(url_path_join(self.dest, output_name))


class UserVideoSummaryTask(
    CourseVideoDownstreamMixin,
    UserVideoSummaryMixin,
    BaseCourseVideoMapReduceTask
):
    """
    Basic mapreduce task. Calculates total activity and unique users for each
    video / user combo
    """
    def output(self):
        output_name = u'user_video_summary_{name}/'.format(name=self.name)
        return get_target_from_url(url_path_join(self.dest, output_name))


class InsertToMysqlDirectTableBase(MysqlInsertTask):
    """
    Base class for directly inserting into MySQL table results
    from a MapReduceJobTask. Subclasses should define their own
    property attributes `table`, `columns` and `indexes`
    """
    def rows(self):
        """
        Re-formats the output of AnswerDistributionPerCourse to something insert_rows can understand
        """
        with self.input()['insert_source'].open('r') as fobj:
            for line in fobj:
                row = line.split('\t')
                yield self.row_to_sql_tuple(row)

    def row_to_sql_tuple(self, row):
        """
        Subclasses should implement this method to convert a csv row into
        a tuple suitable for inserting into mysql
        """
        raise NotImplementedError


# pylint: disable=abstract-method
class InsertToMysqlCourseVideoSummaryTable(InsertToMysqlDirectTableBase):
    """
    Define course_video_summary table
    """
    @property
    def table(self):
        return "course_video_summary"

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('video_id', 'VARCHAR(255) NOT NULL'),
            ('date', 'DATETIME NOT NULL'),
            ('total_activity', 'INT(11) NOT NULL'),
            ('unique_users', 'INT(11) NOT NULL'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('video_id',),
            ('date',),
        ]

    def row_to_sql_tuple(self, row):
        return [
            row[0],
            row[1],
            datetime.strptime(row[2], DATE_FMT),
            int(row[3]),
            int(row[4]),
        ]


# pylint: disable=abstract-method
class InsertToMysqlCourseVideoSeekTimesTable(InsertToMysqlDirectTableBase):
    """
    Define course_video_seek_times table.
    """
    @property
    def table(self):
        return "course_video_seek_times"

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('video_id', 'VARCHAR(255) NOT NULL'),
            ('date', 'DATETIME NOT NULL'),
            ('seek_interval', 'INT(11) NOT NULL'),
            ('num_seeks', 'INT(11) NOT NULL'),
            ('num_users', 'INT(11) NOT NULL'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('video_id',),
            ('date',),
        ]

    def row_to_sql_tuple(self, row):
        return [
            row[0],
            row[1],
            datetime.strptime(row[2], DATE_FMT),
            int(row[3]),
            int(row[4]),
            int(row[5]),
        ]


class InsertToMysqlUserVideoSummaryTable(InsertToMysqlDirectTableBase):
    """
    Define answer_distribution table.
    """
    @property
    def table(self):
        return "user_video_summary"

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('username', 'VARCHAR(255) NOT NULL'),
            ('date', 'DATETIME NOT NULL'),
            ('total_activity', 'INT(11) NOT NULL'),
            ('unique_videos_watched', 'INT(11) NOT NULL'),
            ('total_time_spent', 'INT(11) NOT NULL'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('username',),
            ('date',),
        ]

    def row_to_sql_tuple(self, row):
        return [
            row[0],
            row[1],
            datetime.strptime(row[2], DATE_FMT),
            int(row[3]),
            int(row[4]),
            int(row[5]),
        ]


class CourseVideoSummaryToMySQLTaskWorkflow(
    InsertToMysqlCourseVideoSummaryTable,
    CourseVideoDownstreamMixin,
    MapReduceJobTaskMixin
):
    """
    Task to launch a pipeline that takes as input a raw event log file, runs
    the SQL summary task, and outputs to a MySQL database
    """
    @property
    def insert_source_task(self):
        """
        Write to answer_distribution table from AnswerDistributionTSVTask.
        """
        return CourseVideoSummaryTask(
            mapreduce_engine=self.mapreduce_engine,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            src=self.src,
            dest=self.dest,
            include=self.include,
            name=self.name,
            manifest=self.manifest,
        )


class CourseVideoSeekTimesToMySQLTaskWorkflow(
    InsertToMysqlCourseVideoSeekTimesTable,
    CourseVideoDownstreamMixin,
    MapReduceJobTaskMixin
):
    """
    Task to launch a pipeline that takes as input a raw event log file, runs
    the SQL summary task, and outputs to a MySQL database
    """
    @property
    def insert_source_task(self):
        """ Docstring """
        return CourseVideoSeekTimesTask(
            mapreduce_engine=self.mapreduce_engine,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            src=self.src,
            dest=self.dest,
            include=self.include,
            name=self.name,
            manifest=self.manifest,
        )


class UserVideoSummaryToMySQLTaskWorkflow(
    InsertToMysqlUserVideoSummaryTable,
    CourseVideoDownstreamMixin,
    MapReduceJobTaskMixin
):
    """
    Task to launch a pipeline that takes as input a raw event log file, runs
    the SQL summary task, and outputs to a MySQL database
    """
    @property
    def insert_source_task(self):
        """ Docstring """
        return UserVideoSummaryTask(
            mapreduce_engine=self.mapreduce_engine,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            src=self.src,
            dest=self.dest,
            include=self.include,
            name=self.name,
            manifest=self.manifest,
        )


class CourseVideoWorkflow(
    CourseVideoDownstreamMixin,
    MysqlInsertTaskMixin,
    MapReduceJobTaskMixin,
    luigi.WrapperTask
):
    """
    Calculates video summary, per-course, per-user, and seek-time
    information
    """
    def requires(self):
        kwargs = {
            'mapreduce_engine': self.mapreduce_engine,
            'lib_jar': self.lib_jar,
            'n_reduce_tasks': self.n_reduce_tasks,
            'name': self.name,
            'src': self.src,
            'dest': self.dest,
            'include': self.include,
            'manifest': self.manifest,
            'database': self.database,
            'credentials': self.credentials,
            'insert_chunk_size': self.insert_chunk_size,
        }

        yield (
            CourseVideoSummaryToMySQLTaskWorkflow(**kwargs),
            CourseVideoSeekTimesToMySQLTaskWorkflow(**kwargs),
            UserVideoSummaryToMySQLTaskWorkflow(**kwargs),
        )
