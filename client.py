from datetime import datetime
from http import HTTPStatus
from typing import (Any, Dict, Iterable, List, NamedTuple, Optional, Tuple,
                    Union)

import iso8601
from requests import ConnectionError as RequestsConnectionError
from requests import Response, Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util import Retry

Comment = NamedTuple('Comment', (('comment_id', str),
                                 ('timestamp', datetime),
                                 ('verbatim', str),
                                 ('user_properties',
                                  List[Union['NumberProperty',
                                             'StringProperty']])))

NumberProperty = NamedTuple('NumberProperty', (('name', str),
                                               ('value', Union[float, int])))

StringProperty = NamedTuple('StringProperty', (('name', str), ('value', str)))


class ReinferSyncClient:
    """A sync client for re:infer which performs the actual HTTP requests.

    On construction a `ReinferSyncClient` needs an `authentication_token`,
    which is then used to authenticate all requests peformed via the client.
    """

    def __init__(self, authentication_token: str, retry: Optional[Retry]=None):
        """Builds a new `ReinferSyncClient` with an authentication token."""
        self._session = Session()
        self._session.headers['X-Auth-Token'] = authentication_token
        self._session.headers['Content-Type'] = 'application/json'
        self._session.mount('https://',
                            HTTPAdapter(max_retries=retry or _DEFAULT_RETRY))

    def sync(self,
             dataset_name: str,
             source_name: str,
             comments: Iterable[Comment]):
        """Synchronises a batch of comments.

        The `dataset_name` refers to a `Dataset` which can be created on the
        re:infer platform. The name will look something like
        `organisation/emails`.

        The `source_name` is some identifier for the source e.g. 'Zendesk' or
        'Feefo'.

        The operation is idempotent. If any of the id-s in the batch were used
        before, the corresponding comments will be overwritten.

        A `Comment` represents a verbatim on the re:infer platform. It
        represents either a single piece of text or a list of messages in a
        conversation. This is best explained with an example.

            comment = Comment(comment_id='0123456789abcdef',
                              timestamp=datetime(2017, 1, 2, 13, 45, 21,
                                                 tzinfo=pytz.UTC),
                              verbatim='I love your company!',
                              user_properties=[
                                  NumberProperty('NPS', 4),
                                  NumberProperty('Order Value ($)', 430.2),
                                  StringProperty('Gender', 'Other'),
                                  StringProperty('Platform', 'iPhone'),
                                  StringProperty('Username',
                                                 'alex@example.com'),
                             ])

        Its fields are:
            `comment_id`
                A unique, hex, client-chosen identifier for the `Comment`. It
                should correspond to some identifier in the data source.
                This way re-uploading the same comment twice will be
                idempotent.

            `timestamp`
                A distinguished `datetime`, which is used on the platform to
                display timeseries and provide filters on. It should be the
                closest thing available to the date of collection.

            `verbatim`
                The free-form piece of feedback or survey response text for the
                `Comment`.

            `user_properties`
                Any client-specific string or numeric metadata associated with
                the verbatim. These are not used for predictions, but are
                displayed on the website and enable filtering, segmentation and
                statistics.

        :raises NoSuchDatasetError: If the source doesn't exist
        :raises ValidationError: If any of the comments are malformed.
        :raises RateLimitedError: If comments are being uploaded too fast.
        :raises ReinferBackendError: In the case of transient server errors.
        """
        try:
            response = self._session.post(
                'https://reinfer.io/api/voc/datasets/{}/sync'.format(
                    dataset_name,
                ),
                json={
                    'comments': [_comment_to_json(source_name, comment)
                                 for comment in comments]
                },
            )
        except RequestsConnectionError as error:
            raise ConnectionError(error)

        self._json(response)

    def most_recent(self,
                    dataset_name: str,
                    source_name: str) -> Tuple[str, datetime]:
        """Returns the id and timestamp of the most recent comment in a source.

        Most 'recent' meaning the comment with the highest timestamp, **not**
        the most recently uploaded.

        :raises NoSuchDatasetError: If the source doesn't exist
        :raises EmptyDatasetError: If the source is empty.
        :raises ReinferBackendError: In case of transient server errors.
        """
        try:
            response = self._session.post(
                'https://reinfer.io/api/voc/datasets/{}/recent'.format(
                    dataset_name,
                ),
                json={
                    'limit': 1,
                    'filter': {
                        'user_properties': {
                            'string:Source': {'one_of': [source_name]},
                        },
                    },
                },
            )
        except RequestsConnectionError as error:
            raise ConnectionError(error)

        results = self._json(response)['comments']
        if len(results) == 0:
            raise EmptyDatasetError(
                'Dataset `{}` is empty.'.format(dataset_name))
        comment_dict = results[0]
        return (
            comment_dict['id'],
            iso8601.parse_date(comment_dict['timestamp']),
        )

    def _json(self, response: Response) -> Dict[str, Any]:
        """Parses the JSON body and raises an exception based on status."""
        try:
            body = response.json()
        except ValueError as error:
            raise ReinferBackendError(error)

        if response.ok:
            return body

        message = body.get('message', '(no description available)')
        if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            raise RateLimitedError(message)
        elif response.status_code == HTTPStatus.BAD_REQUEST:
            raise ValidationError(message)
        elif response.status_code == HTTPStatus.NOT_FOUND:
            raise NoSuchDatasetError(message)
        else:
            raise ReinferBackendError(message)


class ReinferSyncError(Exception):
    """Base error type for `ReinferSyncClient`."""

    @classmethod
    def check(cls: type,
              condition: bool,
              template_string: str,
              *format_args,
              **format_kwargs):
        if not condition:
            raise cls(template_string.format(*format_args, **format_kwargs))


class ConnectionError(ReinferSyncError):
    """Raised if there is a problem with the HTTP connection."""


class ValidationError(ReinferSyncError):
    """Raised if a batch of comments is invalid."""


class NoSuchDatasetError(ReinferSyncError):
    """Raised if the source in a request doesn't exist."""


class EmptyDatasetError(ReinferSyncError):
    """Raised if the source in a request is empty."""


class RateLimitedError(ReinferSyncError):
    """Raised if the source in a request is empty."""


class ReinferBackendError(ReinferSyncError):
    """Raised in case of a transient backend error."""


_DEFAULT_RETRY = Retry(
    total=5,
    backoff_factor=0.1,
    raise_on_status=False,
    method_whitelist=frozenset(['POST']),
    status_forcelist=frozenset([
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.GATEWAY_TIMEOUT,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.REQUEST_TIMEOUT,
        HTTPStatus.SERVICE_UNAVAILABLE,
    ])
)


def _comment_to_json(source_name: str, comment: Comment) -> Dict[str, Any]:
    user_properties = dict(map(_user_property_to_json,
                               comment.user_properties))
    user_properties['string:Source'] = source_name
    return {
        'id': comment.comment_id,
        'timestamp': comment.timestamp.isoformat(),
        'original_text': comment.verbatim,
        'user_properties': user_properties,
    }


def _user_property_to_json(
    user_property: Union[StringProperty, NumberProperty],
) -> Tuple[str, Union[str, int, float]]:
    if user_property.name in ('conversation', 'title', 'Source'):
        raise ValidationError(
            'Reserved user property name {}'.format(user_property),
        )

    if isinstance(user_property, StringProperty):
        return ('string:' + user_property.name, user_property.value)
    if isinstance(user_property, NumberProperty):
        return ('number:' + user_property.name, user_property.value)
    raise ValidationError('Invalid user property {}'.fomat(user_property))
