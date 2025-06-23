"""REST client handling, including BigMarkerStream base class."""

import logging
from pathlib import Path
from pickle import NONE
from typing import Any, Callable, Dict, Generator, Iterable, Optional

import backoff
import requests
import requests_random_user_agent

from memoization import cached
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from urllib.parse import parse_qs, parse_qsl, urlparse


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
from singer_sdk.exceptions import RetriableAPIError

class BigMarkerStream(RESTStream):
    """BigMarker stream class."""

    # curr_page_token_jsonpath = "$.page"
    # totl_page_token_jsonpath = "$.total_pages"
    per_page = 10
    page_key = "page"
    has_pagination = True
    backoff_max_tries = 9999
    _LOG_REQUEST_METRIC_URLS = True
    tolerated_http_errors = [401]

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object."""
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="API-KEY",
            value=self.config.get("api_key"),
            location="header"
        )

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""

        # if self.curr_page_token_jsonpath:
        #     all_matches = extract_jsonpath(
        #         self.curr_page_token_jsonpath, response.json()
        #     )
        #     page = next(iter(all_matches), None)
        # if self.totl_page_token_jsonpath:
        #     all_matches = extract_jsonpath(
        #         self.curr_page_token_jsonpath, response.json()
        #     )
        #     pages = next(iter(all_matches), None)
        
        # if page < pages:
        #     next_page_token = page

        if not self.has_pagination:
            return None

        len_path = self.records_jsonpath.replace("[*]", "") + ".`len`"

        all_matches = extract_jsonpath(len_path, response.json())
        len = next(iter(all_matches), 0)

        if len > 0:
            return int(previous_token or "1") + 1

        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if self.rest_method == "GET":
            if next_page_token:
                params[self.page_key] = next_page_token
            if self.per_page:
                params["per_page"] = self.per_page

        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        if response.status_code in self.tolerated_http_errors:
            return []
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        Checks for error status codes and wether they are fatal or retriable.

        In case an error is deemed transient and can be safely retried, then this
        method should raise an :class:`singer_sdk.exceptions.RetriableAPIError`.
        By default this applies to 5xx error codes, along with values set in:
        :attr:`~singer_sdk.RESTStream.extra_retry_statuses`

        In case an error is unrecoverable raises a
        :class:`singer_sdk.exceptions.FatalAPIError`. By default, this applies to
        4xx errors, excluding values found in:
        :attr:`~singer_sdk.RESTStream.extra_retry_statuses`

        Tap developers are encouraged to override this method if their APIs use HTTP
        status codes in non-conventional ways, or if they communicate errors
        differently (e.g. in the response body).

        .. image:: ../images/200.png

        Args:
            response: A `requests.Response`_ object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        if response.status_code in self.tolerated_http_errors:
            full_path = urlparse(response.url).path
            query = urlparse(response.url).query
            msg = (
                f"{response.status_code} Tolerated Status Code "
                f"(Reason: {response.reason}) for path: {full_path}"
                f"Query: {query}"
            )
            self.logger.warn(msg)
            return
        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def backoff_wait_generator(self) -> Callable[..., Generator[int, Any, None]]:
        return backoff.constant(interval=10)  # type: ignore # ignore 'Returning Any'

    def request_decorator(self, func: Callable) -> Callable:
        decorator: Callable = backoff.on_exception(
            self.backoff_wait_generator,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
            ),
            max_tries=self.backoff_max_tries,
            on_backoff=self.backoff_handler,
        )(func)
        return decorator

    def backoff_handler(self, details: dict) -> None:
        if details["tries"] > 5:
            logging.info("resetting session")
            self._requests_session.close()
            self._requests_session = None
        return super().backoff_handler(details)