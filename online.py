#!/usr/bin/env python
import logging
import sys
import time
from argparse import ArgumentParser
from binascii import hexlify
from datetime import datetime, timedelta
from typing import List, NamedTuple

import pytz

from client import (Comment, EmptyDatasetError, NumberProperty,
                    ReinferSyncClient, StringProperty)

RawVerbatim = NamedTuple('RawVerbatim', (('raw_id', str),
                                         ('text', str),
                                         ('nps', int),
                                         ('timestamp', datetime),
                                         ('username', str)))

log = logging.getLogger(__name__)


class OnlineIntegration:
    """A sample polling, real-time data integration."""

    def __init__(self,
                 data_source: 'DataSource',
                 client: ReinferSyncClient,
                 dataset_name: str,
                 source_name: str):
        self._client = client
        self._data_source = data_source
        self._most_recent_timestamp = None
        self._dataset_name = dataset_name
        self._source_name = source_name
        self._page_index = 0

    def poll(self):
        """Performs one poll, sync-ing any new comments from the source."""
        if self._most_recent_timestamp is None:
            try:
                _, self._most_recent_timestamp = self._client.most_recent(
                    self._dataset_name,
                    self._source_name,
                )
            except EmptyDatasetError:
                self._most_recent_timestamp = datetime(1970, 1, 1,
                                                       tzinfo=pytz.UTC)

        limit = self._timestamp_limit(self._most_recent_timestamp)
        log.info('Sync-ing comments newer than %s, page %d...',
                 limit,
                 self._page_index)
        raw_verbatims = self._data_source.newer_than(
            limit, page_index=self._page_index)
        comments = list(map(_raw_to_comment, raw_verbatims))
        if not comments:
            log.info('No comments left to sync.')
            return

        self._client.sync(self._dataset_name, self._source_name, comments)
        if self._most_recent_timestamp != comments[-1].timestamp:
            self._most_recent_timestamp = comments[-1].timestamp
            self._page_index = 0
        else:
            self._page_index += 1
        log.info('Sync-d %d comments, most recent: %s',
                 len(comments),
                 self._most_recent_timestamp)

    def _timestamp_limit(self, most_recent: datetime) -> datetime:
        """Given the most recent timestamp, returns the limit comments should
        retrieved from the data source.

        This is `most_recent`, unless the current time is closed to
        `most_recent`, in which case new comments may still appear with out of
        order timestamps.
        """
        return min(most_recent, datetime.now(pytz.UTC) - timedelta(seconds=10))


def _raw_to_comment(raw: RawVerbatim) -> Comment:
    """Converts a `RawVerbatim` to a `Comment`."""
    return Comment(
        comment_id=hexlify(raw.raw_id.encode('utf-8')).decode('utf-8'),
        timestamp=raw.timestamp,
        verbatim=raw.text,
        user_properties=[NumberProperty('NPS', raw.nps),
                         StringProperty('Username', raw.username)],
    )


class FakeDataSource:
    """A fake data source with some fake data.

    We simulate a source which has its own type for the data, i.e.
    `RawVerbatim`. It also supports polling for comments newer than a
    timestamp, with a pagination API.
    """

    def __init__(self):
        # Set up the list of fake data.
        self._raw = [
            RawVerbatim(raw_id='this is an id {}'.format(i_comment),
                        text='Yay, I love this company {}!'.format(i_comment),
                        nps=i_comment % 11,
                        timestamp=datetime.now(pytz.UTC),
                        username='user{}'.format(i_comment))
            for i_comment in range(100)
        ]
        self._raw.extend(
            RawVerbatim(raw_id='this is an id {}'.format(i_comment),
                        text='Boo, I hate this company {}!'.format(i_comment),
                        nps=i_comment % 11,
                        timestamp=datetime.now(pytz.UTC),
                        username='user{}'.format(i_comment))
            for i_comment in range(100)
        )

    def newer_than(self,
                   timestamp: datetime,
                   page_size: int=40,
                   page_index: int=0) -> List[RawVerbatim]:
        """Paginate through verbatims, in order of timestamp."""
        num_skipped = page_index * page_size
        page = []
        for raw in self._raw:
            if raw.timestamp >= timestamp:
                if num_skipped > 0:
                    num_skipped -= 1
                else:
                    page.append(raw)
                    if len(page) == page_size:
                        break
        return page


def main():
    logging.basicConfig(format='[%(levelname)s] %(message)s',
                        level=logging.INFO)
    parser = ArgumentParser('Sample on-line integration for re:infer.')
    parser.add_argument('--auth-token',
                        type=str,
                        action='store',
                        required=True,
                        metavar='TOKEN',
                        help='The authentication token to use to upload the '
                        'comments')
    parser.add_argument('--source-name',
                        type=str,
                        action='store',
                        required=True,
                        metavar='NAME',
                        help='The source name to store the comments under eg. '
                        'Zendesk')
    parser.add_argument('--dataset-name',
                        type=str,
                        action='store',
                        required=True,
                        metavar='OWNER/NAME',
                        help='The dataset name to store the comments under, '
                        'prefixed with the owner eg. `company/chats`.')
    arguments = parser.parse_args()
    client = ReinferSyncClient(authentication_token=arguments.auth_token)
    integration = OnlineIntegration(data_source=FakeDataSource(),
                                    client=client,
                                    dataset_name=arguments.dataset_name,
                                    source_name=arguments.source_name)
    consecutive_failures = 0
    try:
        while True:
            try:
                integration.poll()
                consecutive_failures = 0
            except Exception:
                log.exception('Exception in poll loop.')
                consecutive_failures += 1
                if consecutive_failures == 5:
                    log.fatal('Too many consecutive failures, quitting.')
                    sys.exit(1)
            time.sleep(1.0)
    except KeyboardInterrupt:
        log.info('Ctrl-C pressed, done.')


if __name__ == '__main__':
    main()
