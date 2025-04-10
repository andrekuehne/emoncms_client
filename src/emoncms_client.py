# Requires: pandas, requests
import json
import logging
import math
import time # For potential timestamp conversions
from typing import Any, Dict, List, Optional, Union, cast
from urllib.parse import urljoin

import pandas as pd
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Custom Exception ---
class EmonCMSApiException(Exception):
    """Custom exception for EmonCMS API errors."""
    pass

class PermissionError(Exception):
    """Custom exception for attempting write operations in read-only mode."""
    pass

# --- EmonCMS Client Class ---
class EmonCMSClient:
    """
    A client for interacting with the EmonCMS Feed API.

    Provides methods for reading feed metadata, fetching data, and (if enabled)
    modifying feeds and data points. Handles API key authentication, JSON parsing,
    and batching for large data insertions/reads.

    Attributes:
        server_url (str): The base URL of the EmonCMS server (e.g., "http://emonpi").
        api_key (str): The EmonCMS API key (read or read/write).
        batch_size (int): The maximum number of data points to send/request in a single
                          API call for batch operations. Also used as a threshold
                          for triggering time-based batching in get_feed_data.
                          Defaults to 3600.
        read_write (bool): Indicates if the client is in read-write mode.
                           Write operations are blocked if False. Defaults to False.
    """

    DEFAULT_BATCH_SIZE = 3600
    # Some servers might use slightly different error messages for inserts
    TOO_MANY_POINTS_MESSAGES = [
        "Maximum number of datapoints", # Common message structure
        "too many points" # Potential simpler message
    ]

    def __init__(self, server_url: str, api_key: str, read_write: bool = False):
        """
        Initializes the EmonCMSClient.

        Args:
            server_url: The base URL of the EmonCMS server (e.g., "http://emonpi").
                        Should not end with a slash.
            api_key: The EmonCMS API key. Ensure it has write permissions if you
                     intend to use write operations.
            read_write: Set to True to enable write operations. Defaults to False (read-only).

        Raises:
            ValueError: If server_url or api_key is empty.
        """
        if not server_url:
            raise ValueError("Server URL cannot be empty.")
        if not api_key:
            raise ValueError("API key cannot be empty.")

        self._server_url = server_url.rstrip('/') # Ensure no trailing slash
        self._api_key = api_key
        self._read_write = read_write
        self._batch_size = self.DEFAULT_BATCH_SIZE
        self._session = requests.Session() # Use a session for potential connection pooling
        # Cache for feed intervals to avoid repeated lookups within a single get_feed_data call
        self._interval_cache: Dict[int, Optional[int]] = {}


        logger.info(f"EmonCMSClient initialized for server: {self._server_url}. Mode: {'Read/Write' if self._read_write else 'Read-Only'}")

    # --- Properties for controlled access ---

    @property
    def read_write(self) -> bool:
        """Gets the current read/write mode status."""
        return self._read_write

    @read_write.setter
    def read_write(self, value: bool) -> None:
        """
        Sets the read/write mode.

        Args:
            value: True to enable read/write, False for read-only.
        """
        if not isinstance(value, bool):
            raise TypeError("read_write mode must be a boolean value.")
        self._read_write = value
        mode = "Read/Write" if self._read_write else "Read-Only"
        logger.info(f"EmonCMSClient mode changed to: {mode}")

    @property
    def batch_size(self) -> int:
        """
        Gets the current batch size threshold.

        Used for limiting points in multi-point inserts and as a threshold
        to trigger time-based batching for data retrieval (`get_feed_data`).
        """
        return self._batch_size

    @batch_size.setter
    def batch_size(self, value: int) -> None:
        """
        Sets the batch size threshold.

        Args:
            value: The maximum number of points per batch request/threshold. Must be positive.

        Raises:
            ValueError: If the value is not a positive integer.
        """
        if not isinstance(value, int) or value <= 0:
            raise ValueError("Batch size must be a positive integer.")
        self._batch_size = value
        logger.info(f"EmonCMSClient batch size set to: {self._batch_size}")

    @property
    def server_url(self) -> str:
        """Gets the server URL."""
        return self._server_url

    # --- Internal Request Helper ---

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        is_write_operation: bool = False,
        expected_type: type = dict # Expected type of successful JSON response (dict, list, float, int, str, bool)
    ) -> Union[Dict[str, Any], List[Any], float, int, str, bool, None]: # Added None possibility
        """ Internal helper method to make API requests. (Handles None return) """
        if is_write_operation and not self._read_write:
            raise PermissionError("Write operation attempted but client is in read-only mode.")

        url = urljoin(self._server_url + '/', path.lstrip('/')) # Ensure correct joining
        request_params = params.copy() if params else {}
        request_params['apikey'] = self._api_key

        try:
            response = self._session.request(
                method,
                url,
                params=request_params,
                data=data,
                timeout=60 # Increased timeout slightly for potentially large data fetches
            )
            response.raise_for_status() # Raise HTTPError for bad status codes (4xx or 5xx)

            # Handle empty responses (e.g., some successful deletions might return 200 OK with no body)
            if not response.content:
                 logger.debug(f"Received empty response body for {method} {url}")
                 # Check if empty body is acceptable for the expected type
                 if expected_type is Any or expected_type is type(None):
                    return None
                 else:
                     # If expecting specific data and get nothing, it's likely an issue
                     raise ValueError(f"Received empty response, but expected type {expected_type}")


            content_type = response.headers.get('Content-Type', '')
            if 'application/json' in content_type:
                response_data = response.json()
            else:
                text_response = response.text.strip()
                if text_response.lower() == 'null':
                    response_data = None
                else:
                    try:
                        val = float(text_response)
                        if val.is_integer():
                            response_data = int(val)
                        else:
                            response_data = val
                    except ValueError:
                        response_data = text_response.strip('"')


            if isinstance(response_data, dict) and response_data.get("success") is False:
                message = response_data.get("message", "Unknown API error")
                logger.error(f"API Error on {method} {path}: {message}")
                raise EmonCMSApiException(f"API Error: {message}")

            # More flexible type checking, allowing None if expected
            if response_data is None and expected_type is not type(None) and expected_type is not Any:
                 logger.warning(f"Received None response for {method} {path}, but expected {expected_type}. Returning None.")
                 # Allow None to pass through if parsing resulted in it (e.g. "null" string)

            elif response_data is not None and not isinstance(response_data, expected_type):
                 # Allow int when float expected
                 if not (expected_type is float and isinstance(response_data, int)):
                     # Allow list when expected_type is Any (used for delete which might return nothing)
                     if not (expected_type is Any):
                         logger.warning(f"Unexpected response type for {method} {path}. Expected {expected_type}, got {type(response_data)}. Data: {str(response_data)[:100]}...")
                         try:
                            # Attempt casting only if reasonable (e.g. int to float)
                            if expected_type is float and isinstance(response_data, int):
                                return float(response_data)
                            # Don't try casting complex types arbitrarily
                            raise ValueError(f"Unexpected response type. Expected {expected_type}, got {type(response_data)}")
                         except (TypeError, ValueError):
                            raise ValueError(f"Unexpected response type. Expected {expected_type}, got {type(response_data)}")

            return response_data

        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON response from {url}. Response text: {response.text[:500]}...")
            raise ValueError(f"Invalid JSON response received from API: {e}") from e
        except requests.exceptions.RequestException as e:
            logger.error(f"Network or HTTP error during request to {url}: {e}")
            raise
        except EmonCMSApiException:
            raise
        except Exception as e:
            logger.exception(f"An unexpected error occurred during the API request to {url}: {e}")
            raise EmonCMSApiException(f"An unexpected error occurred: {e}") from e

    # --- Feed Metadata Helper ---
    def _get_feed_interval(self, feed_id: int) -> Optional[int]:
        """Internal helper to get the native interval of a feed, using cache."""
        if feed_id in self._interval_cache:
            return self._interval_cache[feed_id]

        try:
            meta = self.get_feed_metadata(feed_id)
            interval = meta.get('interval')
            if interval is not None:
                interval = int(interval) # Ensure it's an integer
                self._interval_cache[feed_id] = interval
                return interval
            else:
                logger.warning(f"Could not determine interval for feed {feed_id} from metadata: {meta}")
                self._interval_cache[feed_id] = None # Cache the failure
                return None
        except Exception as e:
            logger.error(f"Failed to fetch metadata to determine interval for feed {feed_id}: {e}")
            self._interval_cache[feed_id] = None # Cache the failure
            return None

    # --- Helper to convert time inputs ---
    def _to_timestamp(self, time_input: Union[int, str]) -> Optional[int]:
        """Attempts to convert various time inputs to Unix timestamp."""
        if isinstance(time_input, int):
            return time_input # Assume it's already a timestamp
        if isinstance(time_input, str):
            try:
                # Handle 'now' explicitly
                if time_input.lower() == 'now':
                    return int(time.time())
                # Try parsing with pandas (handles YYYY-MM-DD, ISO formats etc.)
                dt = pd.to_datetime(time_input, errors='coerce')
                if pd.notna(dt):
                    # Assume UTC if no timezone info, consistent with EmonCMS usual practice
                    if dt.tzinfo is None:
                        dt = dt.tz_localize('UTC')
                    else:
                        dt = dt.tz_convert('UTC')
                    return int(dt.timestamp())
                else:
                    # Pandas couldn't parse it (might be relative like "-1 week")
                    logger.warning(f"Could not parse date string '{time_input}' with pandas. Batching estimation may be skipped.")
                    return None
            except Exception as e:
                logger.warning(f"Error converting date string '{time_input}' to timestamp: {e}. Batching estimation may be skipped.")
                return None
        return None # Invalid input type

    # --- Feed Metadata and Information Methods (Unchanged from previous version) ---
    def list_feeds(self, include_meta: bool = True) -> pd.DataFrame:
        """ Lists all accessible feeds. (Docstring updated) """
        # ... (implementation remains the same as before) ...
        logger.info("Requesting feed list.")
        params = {}
        if include_meta:
            params['meta'] = 1

        response_data = self._request('GET', 'feed/list.json', params=params, expected_type=list)

        if not response_data:
            logger.info("No feeds found.")
            return pd.DataFrame()

        df = pd.DataFrame(response_data)
        # Ensure numeric columns are appropriate types
        for col in ['id', 'userid', 'public', 'size', 'engine', 'time', 'start_time', 'end_time', 'interval', 'npoints']:
            if col in df.columns:
                # Use errors='ignore' for start_time/end_time if meta=0 (they won't exist)
                df[col] = pd.to_numeric(df[col], errors='coerce' if col not in ['start_time', 'end_time'] or include_meta else 'ignore')
        if 'value' in df.columns:
             df['value'] = pd.to_numeric(df['value'], errors='coerce')

        logger.info(f"Successfully retrieved {len(df)} feeds.")
        return df

    def get_feed_field(self, feed_id: int, field_name: str) -> Union[str, int, float, None]:
        """ Gets a specific field value for a given feed. """
        # ... (implementation remains the same) ...
        logger.info(f"Requesting field '{field_name}' for feed ID {feed_id}.")
        params = {'id': feed_id, 'field': field_name}
        # Allow None explicitly in expected types
        response_data = self._request('GET', 'feed/get.json', params=params, expected_type=(str, int, float, type(None)))
        logger.info(f"Retrieved field '{field_name}' for feed ID {feed_id}. Value: {response_data}")
        return response_data


    def get_all_feed_fields(self, feed_id: int) -> Dict[str, Any]:
        """ Gets all basic fields for a feed. """
        # ... (implementation remains the same) ...
        logger.info(f"Requesting all fields for feed ID {feed_id}.")
        params = {'id': feed_id}
        response_data = self._request('GET', 'feed/aget.json', params=params, expected_type=dict)
        logger.info(f"Successfully retrieved all fields for feed ID {feed_id}.")
        return cast(Dict[str, Any], response_data)

    def get_feed_metadata(self, feed_id: int) -> Dict[str, Any]:
        """ Gets metadata (interval, start/end times, npoints) for a feed. """
        # ... (implementation remains the same) ...
        logger.info(f"Requesting metadata for feed ID {feed_id}.")
        params = {'id': feed_id}
        response_data = self._request('GET', 'feed/getmeta.json', params=params, expected_type=dict)
        logger.info(f"Successfully retrieved metadata for feed ID {feed_id}.")
        # Clear cache if metadata is explicitly fetched, as it might have changed
        if feed_id in self._interval_cache:
             del self._interval_cache[feed_id]
        return cast(Dict[str, Any], response_data)


    def get_feed_time_value(self, feed_id: int) -> Dict[str, Union[int, float, None]]:
        """ Gets the last updated time (unix timestamp) and value for a feed. (Allows None value)"""
        # ... (implementation updated slightly for None) ...
        logger.info(f"Requesting last time and value for feed ID {feed_id}.")
        params = {'id': feed_id}
        response_data = self._request('GET', 'feed/timevalue.json', params=params, expected_type=dict)
        result = cast(Dict[str, Any], response_data)
        # Handle potential missing keys or null values gracefully
        time_val = result.get('time')
        value_val = result.get('value')
        processed_result: Dict[str, Union[int, float, None]] = {
             'time': int(time_val) if time_val is not None else None,
             'value': float(value_val) if value_val is not None else None
        }
        logger.info(f"Successfully retrieved last time/value for feed ID {feed_id}: {processed_result}")
        return processed_result


    def get_feed_last_value(self, feed_id: int) -> Optional[float]:
        """ Gets the last recorded value for a specific feed. """
        # ... (implementation remains the same) ...
        logger.info(f"Requesting last value for feed ID {feed_id}.")
        params = {'id': feed_id}
        response_data = self._request('GET', 'feed/value.json', params=params, expected_type=(float, int, str, type(None)))

        if response_data is None or str(response_data).lower() == 'null':
             logger.info(f"Feed ID {feed_id} returned null value.")
             return None
        try:
            value = float(response_data)
            logger.info(f"Successfully retrieved last value for feed ID {feed_id}: {value}")
            return value
        except (ValueError, TypeError) as e:
             logger.error(f"Could not convert response for feed {feed_id} to float. Response: '{response_data}'")
             raise ValueError(f"Unexpected response format for feed value: {response_data}") from e


    def get_feed_value_at_time(self, feed_id: int, timestamp: int) -> Optional[float]:
        """ Gets the feed value at a specific unix timestamp. """
        # ... (implementation remains the same) ...
        logger.info(f"Requesting value for feed ID {feed_id} at time {timestamp}.")
        params = {'id': feed_id, 'time': timestamp}
        response_data = self._request('GET', 'feed/value.json', params=params, expected_type=(float, int, str, type(None)))

        if response_data is None or str(response_data).lower() == 'null':
             logger.info(f"Feed ID {feed_id} returned null value for time {timestamp}.")
             return None
        try:
            value = float(response_data)
            logger.info(f"Successfully retrieved value for feed ID {feed_id} at time {timestamp}: {value}")
            return value
        except (ValueError, TypeError) as e:
             logger.error(f"Could not convert response for feed {feed_id} at time {timestamp} to float. Response: '{response_data}'")
             raise ValueError(f"Unexpected response format for feed value at time: {response_data}") from e


    def get_multiple_feed_last_values(self, feed_ids: List[int]) -> List[Optional[float]]:
        """ Gets the last recorded values for multiple feeds simultaneously. """
        # ... (implementation remains the same) ...
        if not feed_ids:
            return []
        logger.info(f"Requesting last values for feed IDs: {feed_ids}.")
        params = {'ids': ','.join(map(str, feed_ids))}
        response_data = self._request('GET', 'feed/fetch.json', params=params, expected_type=list)

        results: List[Optional[float]] = []
        raw_values = cast(List[Any], response_data)

        # EmonCMS fetch.json might return fewer items than requested if some IDs are invalid
        # It's better to create a result map and fill it
        result_map = {feed_id: None for feed_id in feed_ids}
        # We don't know which value corresponds to which ID if some are missing,
        # so we rely on the order matching *if* the counts match.
        # If counts mismatch, we cannot reliably map, log warning.
        if len(raw_values) != len(feed_ids):
             logger.warning(f"Mismatch between requested feed count ({len(feed_ids)}) and received value count ({len(raw_values)}). Result order might be unreliable.")
             # Pad with None for missing ones, assuming they failed at the end
             raw_values.extend([None] * (len(feed_ids) - len(raw_values)))

        for i, value in enumerate(raw_values):
             current_feed_id = feed_ids[i] # Assume order matches if counts are equal
             if value is None or str(value).lower() == 'null':
                 result_map[current_feed_id] = None
             else:
                 try:
                     result_map[current_feed_id] = float(value)
                 except (ValueError, TypeError):
                     logger.warning(f"Could not convert value '{value}' to float for feed ID {current_feed_id} in multi-feed fetch. Inserting None.")
                     result_map[current_feed_id] = None

        # Return results in the original requested order
        ordered_results = [result_map[fid] for fid in feed_ids]
        logger.info(f"Successfully processed last values fetch for {len(feed_ids)} feeds.")
        return ordered_results


    # --- Feed Data Retrieval (MODIFIED FOR BATCHING) ---

    def get_feed_data(
        self,
        feed_id: int,
        start_time: Union[int, str],
        end_time: Union[int, str],
        interval: Optional[Union[int, str]] = None,
        average: bool = False,
        skip_missing: bool = False,
        limit_interval: bool = False,
        delta: bool = False,
    ) -> pd.DataFrame:
        """
        Fetches time series data for a feed, attempting time-based batching for large ranges.

        Corresponds to `feed/data.json`. If the estimated number of data points
        for the full requested period (based on feed interval or requested interval)
        exceeds `self.batch_size`, the time range will be split into chunks,
        and data for each chunk fetched sequentially.

        Note: Batching requires converting `start_time` and `end_time` to numeric
        timestamps client-side. If string inputs like '-1 week' or 'now' are used
        that cannot be reliably converted by this client, batching might be skipped,
        and a single large request will be attempted.

        Args:
            feed_id: The ID of the feed.
            start_time: Start time (unix timestamp or php date string e.g., 'now', '-1 day', 'YYYY-MM-DD').
            end_time: End time (unix timestamp or php date string e.g., 'now', 'YYYY-MM-DD').
            interval: Request data at a specific interval (seconds). Can also be
                      timezone aligned strings like 'daily', 'weekly'. If None, fetches
                      raw data at its native interval.
            average: If True and interval is set, averages data over the interval.
            skip_missing: If True, missing datapoints within the interval are skipped.
            limit_interval: If True, adjusts start/end times to align with interval boundaries.
            delta: If True, returns the change in value between points instead of absolute values.

        Returns:
            A pandas DataFrame with a DatetimeIndex ('time') and a 'value' column.
            Returns an empty DataFrame if no data is found in the range.

        Raises:
            EmonCMSApiException: For API-specific errors.
            requests.exceptions.RequestException: For network/HTTP errors.
            ValueError: If the response format is unexpected or time conversion fails badly.
        """
        logger.info(f"Requesting data for feed ID {feed_id} from '{start_time}' to '{end_time}'.")

        # --- Attempt to get numeric timestamps for estimation/batching ---
        start_ts = self._to_timestamp(start_time)
        end_ts = self._to_timestamp(end_time)

        perform_batching = False
        estimated_points = 0
        effective_interval = None

        if start_ts is not None and end_ts is not None:
            if end_ts <= start_ts:
                logger.warning("End time is not after start time. No data will be fetched.")
                return pd.DataFrame(columns=['value']).set_index(pd.Index([], dtype='datetime64[ns]', name='time'))

            duration_seconds = end_ts - start_ts

            # Determine the effective interval for estimation
            if isinstance(interval, int) and interval > 0:
                effective_interval = interval
            elif isinstance(interval, str):
                 # Map common string intervals to seconds for estimation
                 interval_map = {'daily': 86400, 'weekly': 604800, 'monthly': 2592000, 'annual': 31536000} # Approx
                 lower_interval = interval.lower()
                 if lower_interval in interval_map:
                      effective_interval = interval_map[lower_interval]
                 else:
                      logger.warning(f"Cannot estimate points for string interval '{interval}'. Batching may be skipped.")
                      # Fall through to use native interval if possible
            else: # interval is None or 0/invalid int, use native feed interval
                native_interval = self._get_feed_interval(feed_id)
                if native_interval is not None and native_interval > 0:
                    effective_interval = native_interval
                else:
                    logger.warning(f"Could not determine a valid native interval for feed {feed_id}. Cannot estimate points accurately. Batching will be skipped.")

            # Estimate points and decide on batching
            if effective_interval is not None and effective_interval > 0:
                estimated_points = math.ceil(duration_seconds / effective_interval)
                logger.debug(f"Estimated points: {estimated_points} (Duration: {duration_seconds}s, Interval: {effective_interval}s)")
                if estimated_points > self.batch_size:
                    perform_batching = True
                    logger.info(f"Estimated points ({estimated_points}) exceed batch size ({self.batch_size}). Performing time-based batching.")
                else:
                     logger.debug("Estimated points within batch size. Requesting all data at once.")
            else:
                 logger.warning("Effective interval is unknown or zero. Skipping point estimation and batching.")

        else:
            logger.warning("Could not reliably determine numeric start/end timestamps from input. Skipping point estimation and batching.")

        # --- Prepare base parameters for API calls ---
        base_params: Dict[str, Any] = {
            'id': feed_id,
            'timeformat': 'unix', # Force unix timestamps for easier parsing
            'average': int(average),
            'skipmissing': int(skip_missing),
            'limitinterval': int(limit_interval),
            'delta': int(delta),
        }
        if interval is not None:
            base_params['interval'] = interval

        all_data_points: List[List[Union[int, float, None]]] = [] # Allow None for potential null values

        # --- Execute requests (batched or single) ---
        try:
            if perform_batching and start_ts is not None and end_ts is not None and effective_interval is not None:
                 # Calculate batch chunks based on estimated points and batch size
                 num_batches = math.ceil(estimated_points / self.batch_size)
                 # Calculate chunk duration slightly differently: ensure total duration is covered
                 total_duration = end_ts - start_ts
                 chunk_duration = math.ceil(total_duration / num_batches) # Duration of each time slice

                 logger.info(f"Splitting request into {num_batches} time chunks of approx {chunk_duration}s.")

                 current_start_ts = start_ts
                 for i in range(num_batches):
                     current_end_ts = min(current_start_ts + chunk_duration, end_ts)
                     # Ensure the very last batch ends exactly at the original end_ts
                     if i == num_batches - 1:
                         current_end_ts = end_ts

                     # Prevent start == end for a chunk, unless it's the very last point
                     if current_start_ts >= current_end_ts and current_start_ts != start_ts:
                          logger.debug(f"Skipping empty time chunk {i+1}/{num_batches}.")
                          continue

                     logger.info(f"Fetching batch {i+1}/{num_batches} (Time: {current_start_ts} to {current_end_ts})")

                     chunk_params = base_params.copy()
                     chunk_params['start'] = current_start_ts
                     chunk_params['end'] = current_end_ts

                     # Make the request for this chunk
                     chunk_response = self._request('GET', 'feed/data.json', params=chunk_params, expected_type=list)
                     chunk_data = cast(List[List[Union[int, float, None]]], chunk_response) # Allow None from API

                     if chunk_data:
                         # Basic validation of inner list structure
                         if not isinstance(chunk_data[0], list) or not (1 <= len(chunk_data[0]) <= 2): # Allow [ts] or [ts, val] or [ts, null]
                            raise ValueError(f"Unexpected data format in chunk {i+1}. Expected list of [timestamp, value] pairs. Got: {str(chunk_data[0])[:100]}...")
                         all_data_points.extend(chunk_data)
                         logger.debug(f"Received {len(chunk_data)} points in batch {i+1}.")
                     else:
                          logger.debug(f"Received no points in batch {i+1}.")


                     # Prepare for next iteration
                     # Move start slightly past end to avoid overlap if end was calculated
                     current_start_ts = current_end_ts + 1 if interval is None or not isinstance(interval, int) else current_end_ts + effective_interval # step by interval if fixed
                     # However, simple approach is just setting start to end of last chunk
                     current_start_ts = current_end_ts # PHP API likely handles overlap internally better

                     # Add a small safety break if end doesn't advance
                     if current_start_ts >= end_ts:
                          break # Finished all chunks

            else: # Single request (no batching or batching failed pre-condition)
                logger.info("Fetching data in a single request.")
                single_params = base_params.copy()
                single_params['start'] = start_time # Use original inputs
                single_params['end'] = end_time

                response_data = self._request('GET', 'feed/data.json', params=single_params, expected_type=list)
                raw_data = cast(List[List[Union[int, float, None]]], response_data) # Allow None

                if raw_data:
                    if not isinstance(raw_data[0], list) or not (1 <= len(raw_data[0]) <= 2):
                        raise ValueError(f"Unexpected data format received. Expected list of [timestamp, value] pairs. Got: {str(raw_data[0])[:100]}...")
                    all_data_points.extend(raw_data)

        except (EmonCMSApiException, requests.exceptions.RequestException, ValueError) as e:
            logger.error(f"Error during feed data retrieval: {e}")
            raise # Re-raise the caught exception

        # --- Process combined data into DataFrame ---
        if not all_data_points:
            logger.info(f"No data points found for feed ID {feed_id} in the specified range/batches.")
            return pd.DataFrame(columns=['value']).set_index(pd.Index([], dtype='datetime64[ns]', name='time'))

        try:
            # Pad rows that only have a timestamp (API might return [ts] instead of [ts, null])
            processed_points = []
            for point in all_data_points:
                if len(point) == 1:
                    processed_points.append([point[0], None])
                elif len(point) == 2:
                    processed_points.append(point)
                else:
                    logger.warning(f"Skipping malformed data point: {point}")


            df = pd.DataFrame(processed_points, columns=['time', 'value'])

            # Convert time to datetime objects (expecting unix timestamps)
            df['time'] = pd.to_datetime(df['time'], unit='s', errors='coerce', utc=True) # Assume UTC

            # Convert value to numeric, coercing errors and handling None -> NaN
            df['value'] = pd.to_numeric(df['value'], errors='coerce')

            # Set index and remove rows where time conversion failed or original data was bad
            df = df.set_index('time')
            df = df[df.index.notna()]

            # Remove exact duplicates that might arise from chunk boundaries if server includes boundary points in both chunks
            df = df[~df.index.duplicated(keep='first')]

            # Sort by time index, as batch concatenation might mix order slightly
            df = df.sort_index()

            logger.info(f"Successfully retrieved and processed {len(df)} data points for feed ID {feed_id}.")
            return df
        except Exception as e:
            logger.exception(f"Error processing combined data into DataFrame for feed {feed_id}: {e}")
            raise ValueError(f"Failed to process data into DataFrame: {e}") from e


    # --- Write Operations (Unchanged from previous version) ---
    def insert_data_point(self, feed_id: int, timestamp: int, value: Union[int, float]) -> bool:
        """ Inserts or updates a single data point for a feed. """
        # ... (implementation remains the same) ...
        logger.debug(f"Attempting to insert single point ({timestamp}, {value}) into feed ID {feed_id}.")
        params = {'id': feed_id, 'time': timestamp, 'value': value}
        response_data = self._request(
            'POST',
            'feed/insert.json',
            params=params,
            is_write_operation=True,
            expected_type=dict
        )
        success = cast(Dict[str, Any], response_data).get('success', False)
        if success:
            logger.info(f"Successfully inserted single point into feed ID {feed_id}.")
        else:
             logger.warning(f"API indicated failure inserting single point into feed ID {feed_id}. Response: {response_data}")
        return success


    def insert_multiple_data_points(
        self,
        feed_id: int,
        data: Union[pd.DataFrame, List[List[Union[int, float]]]]
    ) -> bool:
        """ Inserts or updates multiple data points for a feed, handling batching. """
        # ... (implementation remains the same) ...
        # --- (Code from previous answer for this method) ---
        if isinstance(data, pd.DataFrame):
            # Check required columns
            required_cols = {'time', 'value'}
            time_col_name = 'time'
            value_col_name = 'value'

            # If using index as time:
            if isinstance(data.index, pd.DatetimeIndex) and 'time' not in data.columns:
                required_cols.remove('time')
                time_source = data.index
            elif 'time' in data.columns:
                 time_source = data['time']
            else:
                 raise ValueError("Input DataFrame must contain a 'time' column or have a DatetimeIndex.")

            if 'value' not in data.columns:
                 raise ValueError("Input DataFrame must contain a 'value' column.")

            # Ensure time is unix timestamp (integer)
            if pd.api.types.is_datetime64_any_dtype(time_source):
                 # Convert datetime index/column to Unix timestamp (integer seconds)
                 # Ensure UTC localization before conversion if naive
                 if time_source.tzinfo is None:
                     logger.debug("Localizing naive timestamp to UTC before converting to Unix epoch for insertion.")
                     time_source_utc = time_source.tz_localize('UTC')
                 else:
                      time_source_utc = time_source.tz_convert('UTC')
                 time_col_numeric = (time_source_utc.view('int64') // 10**9)
            else:
                # Assume it's already unix timestamps, ensure integer type
                try:
                    time_col_numeric = pd.to_numeric(time_source, errors='raise').astype(int)
                except (ValueError, TypeError) as e:
                     raise ValueError(f"Could not convert 'time' source to numeric Unix timestamps: {e}") from e

            # Ensure value is float
            try:
                 value_col_numeric = data[value_col_name].astype(float)
            except (ValueError, TypeError) as e:
                 raise ValueError(f"Could not convert 'value' column to numeric float: {e}") from e


            # Prepare data in list-of-lists format [[ts, val], ...]
            points_list = [[int(ts), float(val)] for ts, val in zip(time_col_numeric, value_col_numeric) if pd.notna(val)] # Drop rows with NaN values


        elif isinstance(data, list):
            if not data:
                 logger.info("No data provided for insertion.")
                 return True
            if not isinstance(data[0], (list, tuple)) or len(data[0]) != 2:
                 raise ValueError("Input list must contain lists/tuples of [timestamp, value].")
            try:
                # Ensure types are correct for JSON, filter out potential null/NaN values if necessary
                points_list = [[int(p[0]), float(p[1])] for p in data if p[1] is not None and not (isinstance(p[1], float) and math.isnan(p[1]))]

            except (ValueError, TypeError, IndexError) as e:
                 raise ValueError(f"Invalid data format or type in list: {e}") from e
        else:
            raise TypeError("Data must be a pandas DataFrame or a list of [timestamp, value] lists.")

        if not points_list:
            logger.info(f"No valid non-null data points to insert for feed ID {feed_id}.")
            return True

        num_points = len(points_list)
        logger.info(f"Attempting to insert {num_points} points into feed ID {feed_id}.")

        overall_success = True
        for i in range(0, num_points, self.batch_size):
            batch = points_list[i : i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = math.ceil(num_points / self.batch_size)
            logger.info(f"Sending batch {batch_num}/{total_batches} ({len(batch)} points) for feed ID {feed_id}.")

            data_payload = {'data': json.dumps(batch)}
            params = {'id': feed_id}

            try:
                response_data = self._request(
                    'POST',
                    'feed/insert.json',
                    params=params,
                    data=data_payload,
                    is_write_operation=True,
                    expected_type=dict
                )
                success = cast(Dict[str, Any], response_data).get('success', False)
                if not success:
                    message = cast(Dict[str, Any], response_data).get('message', 'Unknown error')
                    logger.error(f"Batch {batch_num} failed for feed ID {feed_id}: {message}")
                    if any(phrase.lower() in message.lower() for phrase in self.TOO_MANY_POINTS_MESSAGES):
                        logger.error(f"  -> Error suggests configured batch size ({self.batch_size}) might still be too large for this specific insert on the server.")
                    overall_success = False
                    return False # Stop on first failure
                else:
                    logger.info(f"Batch {batch_num} successfully inserted for feed ID {feed_id}.")

            except (EmonCMSApiException, requests.exceptions.RequestException, ValueError) as e: # Added ValueError
                 logger.error(f"Error inserting batch {batch_num} for feed ID {feed_id}: {e}")
                 overall_success = False
                 return False

        logger.info(f"Finished inserting data for feed ID {feed_id}. Overall success: {overall_success}")
        return overall_success


    def delete_data_point(self, feed_id: int, timestamp: int) -> bool:
        """ Deletes a single data point at a specific time for a feed. """
        # ... (implementation remains the same) ...
        logger.warning(f"Attempting to delete data point at time {timestamp} from feed ID {feed_id}.")
        params = {'id': feed_id, 'feedtime': timestamp}
        try:
            # Expect 'Any' as success response varies or might be empty
            self._request(
                'GET',
                'feed/deletedatapoint.json',
                params=params,
                is_write_operation=True,
                expected_type=Any # Be flexible
            )
            logger.info(f"Data point deletion request sent for feed ID {feed_id} at time {timestamp}. Assuming success based on lack of error.")
            return True
        except EmonCMSApiException as e:
             logger.error(f"API error during data point deletion for feed {feed_id}: {e}")
             # Check if it was explicitly success:false
             if "success\": false" in str(e).lower(): return False
             # Otherwise, it might be another API error, re-raise or log as appropriate
             return False # Assume failure on API error
        except requests.exceptions.RequestException as e:
             logger.error(f"Network/HTTP error during data point deletion for feed {feed_id}: {e}")
             raise # Re-raise network errors


    def create_feed(
        self,
        name: str,
        tag: str = "",
        engine: int = 5, # Default to PHPFINA
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[int]:
        """ Creates a new feed. """
        # ... (implementation remains the same) ...
        logger.info(f"Attempting to create feed with name '{name}'.")
        params: Dict[str, Any] = {
            'name': name,
            'tag': tag,
            'engine': engine,
        }
        if options is not None:
            # Ensure options is a valid dict before dumping
            if isinstance(options, dict):
                params['options'] = json.dumps(options)
            else:
                logger.warning("Invalid 'options' provided for feed creation, must be a dictionary. Ignoring options.")


        response_data = self._request(
            'POST',
            'feed/create.json',
            params=params,
            is_write_operation=True,
            expected_type=dict
        )

        result_dict = cast(Dict[str, Any], response_data)
        success = result_dict.get('success', False)
        feed_id_val = result_dict.get('feedid') # feedid might be string or int

        if success and feed_id_val is not None:
            try:
                feed_id = int(feed_id_val)
                logger.info(f"Successfully created feed '{name}' with ID: {feed_id}.")
                return feed_id
            except (ValueError, TypeError):
                 logger.error(f"Feed creation reported success, but feed ID '{feed_id_val}' is not a valid integer.")
                 raise EmonCMSApiException(f"Feed creation succeeded but received invalid feed ID: {feed_id_val}")

        else:
            message = result_dict.get("message", "Feed creation failed.")
            logger.error(f"Failed to create feed '{name}'. Reason: {message}")
            if not success:
                 raise EmonCMSApiException(f"Feed creation failed: {message}")
            return None


    def delete_feed(self, feed_id: int) -> bool:
        """ Deletes an existing feed. """
        # ... (implementation remains the same) ...
        logger.warning(f"Attempting to DELETE feed ID {feed_id}. This is permanent!")
        params = {'id': feed_id}
        response_data = self._request(
            'POST',
            'feed/delete.json',
            params=params,
            is_write_operation=True,
            expected_type=dict
        )
        success = cast(Dict[str, Any], response_data).get('success', False)
        message = cast(Dict[str, Any], response_data).get('message', '')
        if success:
            logger.info(f"Successfully deleted feed ID {feed_id}. Message: {message}")
        else:
             logger.error(f"Failed to delete feed ID {feed_id}. Message: {message}")
        return success


    def update_feed_properties(self, feed_id: int, fields: Dict[str, Any]) -> bool:
        """ Updates properties (fields) of an existing feed. """
        # ... (implementation remains the same) ...
        if not fields:
            raise ValueError("Fields dictionary cannot be empty for update.")

        logger.info(f"Attempting to update fields for feed ID {feed_id}: {fields}")
        params = {'id': feed_id}
        try:
             fields_json = json.dumps(fields)
        except TypeError as e:
             raise ValueError(f"Invalid data in fields dictionary, cannot serialize to JSON: {e}") from e

        data = {'fields': fields_json}

        response_data = self._request(
            'POST',
            'feed/set.json',
            params=params,
            data=data, # Use data payload for POST body according to docs example structure
            is_write_operation=True,
            expected_type=dict
        )
        success = cast(Dict[str, Any], response_data).get('success', False)
        message = cast(Dict[str, Any], response_data).get('message', '')
        if success:
            logger.info(f"Successfully updated fields for feed ID {feed_id}. Message: {message}")
        else:
             logger.error(f"Failed to update fields for feed ID {feed_id}. Message: {message}")
        return success


    # --- Server/System Information Methods (Unchanged) ---

    def update_feed_size(self) -> Optional[int]:
        """ Triggers a recalculation of feed sizes on the server. """
        # ... (implementation remains the same) ...
        logger.info("Requesting feed size update on the server.")
        try:
             response_data = self._request(
                 'GET',
                 'feed/updatesize.json',
                 is_write_operation=False, # Assumed safe, adjust if needed
                 expected_type=(int, float, type(None)) # Response might be float/int timestamp or null/empty
             )
             if isinstance(response_data, (int, float)):
                  logger.info(f"Feed size update request successful. Response: {response_data}")
                  return int(response_data) # Return as int
             elif response_data is None:
                  logger.info("Feed size update request returned null/empty.")
                  return None
             else:
                 logger.warning(f"Unexpected response type for feed size update: {type(response_data)}. Value: {response_data}")
                 return None
        except (EmonCMSApiException, requests.exceptions.RequestException) as e:
             logger.error(f"Error requesting feed size update: {e}")
             # Re-raise or return None based on desired behavior
             raise


    def get_buffer_size(self) -> Optional[int]:
        """ Gets the number of data points currently pending write in the server's buffer. """
        # ... (implementation remains the same) ...
        logger.info("Requesting server buffer size.")
        try:
            response_data = self._request(
                'GET',
                'feed/buffersize.json',
                expected_type=(int, float, str, type(None)) # Buffer might return non-int on error/disabled
            )
            if isinstance(response_data, (int, float)):
                 buffer_int = int(response_data)
                 logger.info(f"Server buffer size: {buffer_int} points.")
                 return buffer_int
            elif response_data is None:
                  logger.info("Buffer size endpoint returned null.")
                  return None # Treat null as 0 or unknown? Returning None seems safer.
            else:
                 # Check if it's a string indicating an issue
                 if isinstance(response_data, str) and "buffer module disabled" in response_data.lower():
                      logger.warning("Could not get buffer size: Buffer module appears disabled on server.")
                      return None
                 logger.warning(f"Unexpected response type or value for buffer size: {type(response_data)}. Value: {response_data}")
                 return None # Return None for unexpected responses
        except (EmonCMSApiException, requests.exceptions.RequestException) as e:
             if "invalid path" in str(e).lower() or "not found" in str(e).lower():
                  logger.warning("Could not get buffer size. Endpoint not found (buffer module might be disabled or API path incorrect).")
                  return None
             logger.error(f"Error getting buffer size: {e}")
             raise

    def get_feed_data_by_name(
        self,
        feed_name: str,
        start_time: Union[int, str],
        end_time: Union[int, str],
        tag: Optional[str] = None, # Optional tag for disambiguation
        interval: Optional[Union[int, str]] = None,
        average: bool = False,
        skip_missing: bool = False,
        limit_interval: bool = False,
        delta: bool = False,
    ) -> pd.DataFrame:
        """
        Fetches time series data for a feed identified by its name (and optionally tag).

        This is a convenience method that first looks up the feed ID based on the
        provided name and optional tag, then calls get_feed_data.

        Args:
            feed_name: The exact name of the feed. Case-sensitive matching is likely.
            start_time: Start time (unix timestamp or php date string).
            end_time: End time (unix timestamp or php date string).
            tag: (Optional) The tag associated with the feed to disambiguate if
                 multiple feeds share the same name. Case-sensitive matching likely.
            interval: Request data at a specific interval (seconds or 'daily', etc.).
                      If None, fetches raw data at its native interval.
            average: If True and interval is set, averages data over the interval.
            skip_missing: If True, missing datapoints within the interval are skipped.
            limit_interval: If True, adjusts start/end times to align with interval boundaries.
            delta: If True, returns the change in value between points.

        Returns:
            A pandas DataFrame with a DatetimeIndex ('time') and a 'value' column.
            Returns an empty DataFrame if the feed exists but has no data in the range.

        Raises:
            EmonCMSApiException: If multiple feeds match the criteria or for underlying API errors
                               from list_feeds or get_feed_data.
            ValueError: If no feed matching the criteria is found on the server or if
                        list_feeds returns an empty list.
            requests.exceptions.RequestException: For network/HTTP errors during feed list
                                                 or data fetch.
        """
        logger.info(f"Attempting to get data for feed name='{feed_name}'" + (f" tag='{tag}'" if tag else ""))

        try:
            # 1. List feeds to find the ID
            # Set include_meta=False as we only need id, name, tag for lookup
            all_feeds_df = self.list_feeds(include_meta=False)

            if all_feeds_df.empty:
                logger.error("list_feeds returned an empty list. Cannot find feed by name.")
                raise ValueError("No feeds found on the server via API key.")

            # 2. Filter by name (case-sensitive)
            matching_feeds = all_feeds_df[all_feeds_df['name'] == feed_name]

            # 3. Filter by tag if provided (case-sensitive)
            if tag is not None:
                # Ensure comparison handles potential missing/None tags correctly
                # Pandas comparison with None/NaN can be tricky, fillna is safer
                matching_feeds = matching_feeds[matching_feeds['tag'].fillna('') == tag]

            # 4. Check results and handle errors
            if matching_feeds.empty:
                err_msg = f"No feed found with name='{feed_name}'"
                if tag is not None: # Check if tag was actually provided, not just None
                    err_msg += f" and tag='{tag}'"
                # Provide context about available names/tags if helpful
                available_names = sorted(all_feeds_df['name'].unique())
                logger.debug(f"Available feed names: {available_names}")
                raise ValueError(err_msg)

            if len(matching_feeds) > 1:
                ids_found = matching_feeds['id'].tolist()
                err_msg = f"Multiple feeds found matching name='{feed_name}'"
                if tag is not None:
                   err_msg += f" and tag='{tag}'"
                err_msg += f". Matching IDs: {ids_found}. Please provide a unique tag or use get_feed_data() with a specific ID."
                # Log details of conflicting feeds
                logger.warning(f"Ambiguous feed specification. Found feeds:\n{matching_feeds[['id', 'name', 'tag']]}")
                # Using EmonCMSApiException for ambiguity seems appropriate
                raise EmonCMSApiException(err_msg)

            # 5. Extract the unique feed ID
            # Ensure 'id' column exists and value is convertible to int
            try:
                 feed_id = int(matching_feeds.iloc[0]['id'])
            except KeyError:
                 logger.error("Failed to find 'id' column in the filtered feed DataFrame.")
                 raise EmonCMSApiException("Internal error: Feed 'id' column missing after filtering.")
            except (ValueError, TypeError):
                 feed_id_val = matching_feeds.iloc[0]['id']
                 logger.error(f"Found feed ID '{feed_id_val}' is not a valid integer.")
                 raise EmonCMSApiException(f"Internal error: Invalid feed ID format '{feed_id_val}' found.")

            logger.info(f"Found unique feed ID {feed_id} for name='{feed_name}'" + (f" tag='{tag}'" if tag else ""))

            # 6. Call the original get_feed_data method with the found ID
            return self.get_feed_data(
                feed_id=feed_id,
                start_time=start_time,
                end_time=end_time,
                interval=interval,
                average=average,
                skip_missing=skip_missing,
                limit_interval=limit_interval,
                delta=delta,
            )

        # Catch exceptions from list_feeds or get_feed_data and re-raise for clarity
        except (EmonCMSApiException, ValueError, requests.exceptions.RequestException) as e:
            # Log the error occurred within this specific convenience method call
            logger.error(f"Error during get_feed_data_by_name(feed_name='{feed_name}'): {e}")
            raise # Re-raise the caught exception


# --- Example Usage (Illustrative - Unchanged) ---
if __name__ == "__main__":
    # --- Configuration ---
    EMONCMS_URL = "http://localhost/emoncms" # Replace
    READ_ONLY_API_KEY = "YOUR_READ_ONLY_API_KEY_HERE" # Replace
    READ_WRITE_API_KEY = "YOUR_READ_WRITE_API_KEY_HERE" # Replace (Use with caution!)

    # --- Initialize Client (Read-Only initially) ---
    try:
        client = EmonCMSClient(EMONCMS_URL, READ_ONLY_API_KEY)
        print(f"Client initialized for {client.server_url}. Mode: Read-Only")
        client.batch_size = 5000 # Example: Set custom batch size/threshold

        # --- Read Operations ---
        print("\n--- Listing Feeds ---")
        feeds_df = client.list_feeds(include_meta=True)
        if not feeds_df.empty:
            print(feeds_df.head())
            test_feed_id = int(feeds_df.iloc[0]['id']) if len(feeds_df) > 0 else None
            print(f"Using Feed ID: {test_feed_id} for subsequent tests.")
        else:
            print("No feeds found. Cannot run detailed tests.")
            test_feed_id = None

        if test_feed_id:
            # --- Test get_feed_data with potential batching ---
            print(f"\n--- Getting Feed Data (long range, potentially batched) for Feed {test_feed_id} ---")
            # Example: Fetch 2 months of data (might trigger batching depending on interval/batch_size)
            try:
                # Using standard date strings that _to_timestamp can parse
                data_df = client.get_feed_data(
                    feed_id=test_feed_id,
                    start_time="2023-09-01", # Example start date
                    end_time="2023-10-31",   # Example end date
                    interval=3600 # Request hourly average (reduces points, less likely to batch)
                    # interval=None # Use native interval (more likely to batch if native is frequent)
                )
                if not data_df.empty:
                    print(data_df.head())
                    print(f"Retrieved {len(data_df)} data points.")
                    print(data_df.info())
                else:
                    print("No data found for the specified range.")

                # Example with relative time (might skip batching estimation)
                # print(f"\n--- Getting Feed Data (relative time) for Feed {test_feed_id} ---")
                # data_df_relative = client.get_feed_data(
                #     feed_id=test_feed_id,
                #     start_time="-7 days",
                #     end_time="now",
                #     interval='daily'
                # )
                # print(f"Retrieved {len(data_df_relative)} daily points for last 7 days.")
                # print(data_df_relative.head())


            except (EmonCMSApiException, requests.exceptions.RequestException, ValueError) as e:
                 print(f"ERROR during get_feed_data test: {e}")


            # ... (other read examples from previous version can go here) ...


        # --- Write Operations Example (Requires R/W Key and Mode) ---
        # !! DANGER ZONE: Uncomment and modify carefully !!
        # print("\n--- Switching to Read/Write Mode ---")
        # try:
        #     rw_client = EmonCMSClient(EMONCMS_URL, READ_WRITE_API_KEY, read_write=True)
        #     rw_client.batch_size = 1000 # Set batch size for writes
        #     print("Client is now in Read/Write mode.")
        #     # ... (write operation examples from previous version) ...
        # except PermissionError as e: # ... etc ...

    # ... (rest of example usage exception handling) ...
    except ValueError as e:
        print(f"Initialization or Value Error: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Network Error: Could not connect to {EMONCMS_URL}. Please check the URL and network connection. Details: {e}")
    except EmonCMSApiException as e:
        print(f"API Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()