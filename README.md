```md
# EmonCMS Client

A Python wrapper for interacting with the EmonCMS API. This library provides methods for reading and writing data to EmonCMS feeds, as well as utilities for managing feeds and querying data.

## Features
- Fetch feed metadata and time-series data.
- Create, update, and delete feeds.
- Insert single or multiple data points into feeds.
- Batch processing for large data ranges.
- Support for read-only and read-write API keys.

---

## Installation

This project uses uv for project and dependency management. To setup the local venv and all dependencies run uv sync.

---

## API Overview

The main API is implemented in [`src/emoncms_client.py`](src/emoncms_client.py). Below is a summary of the key methods provided by the `EmonCMSClient` class:

### Initialization
```python
from emoncms_client import EmonCMSClient

client = EmonCMSClient(server_url="http://emonpi", api_key="YOUR_API_KEY", read_write=False)
```

### Methods
- **Feed Management**
  - `list_feeds(include_meta=True)`: List all accessible feeds.
  - `create_feed(name, tag="", engine=5, options=None)`: Create a new feed.
  - `delete_feed(feed_id)`: Delete an existing feed.
  - `update_feed_properties(feed_id, fields)`: Update feed properties.

- **Data Retrieval**
  - `get_feed_data(feed_id, start_time, end_time, interval=None, average=False, ...)`: Fetch time-series data for a feed.
  - `get_feed_data_by_name(feed_name, start_time, end_time, ...)`: Fetch data for a feed by its name and optional tag.

- **Data Insertion**
  - `insert_data_point(feed_id, timestamp, value)`: Insert a single data point.
  - `insert_multiple_data_points(feed_id, data)`: Insert multiple data points in batches.

- **Utilities**
  - `get_feed_metadata(feed_id)`: Retrieve metadata for a feed.
  - `get_feed_last_value(feed_id)`: Get the last recorded value for a feed.

Refer to the source code for detailed documentation of each method.

---

## Examples

### Test Scripts

The `tests` folder contains example scripts for interacting with the EmonCMS API. Below are some highlights:

#### 1. Reading Data from a Feed
File: [`tests/test_get_set_data.py`](tests/test_get_set_data.py)

```python
from emoncms_client import EmonCMSClient
from dotenv import load_dotenv
import os

# Load API keys from .env file
load_dotenv()
API_KEY_RO = os.getenv("API_KEY_RO")

client = EmonCMSClient(server_url="http://emonpi", api_key=API_KEY_RO)

data = client.get_feed_data_by_name(
    feed_name="electric_Power",
    tag="arotherm_plus_105",
    start_time="2024-02-06 00:00:00",
    end_time="2025-02-07 00:00:00",
    interval=60,
    average=True,
)

print(data)
```

#### 2. Writing Data to a New Feed
File: [`tests/test_get_set_data.py`](tests/test_get_set_data.py)

```python
from emoncms_client import EmonCMSClient
from dotenv import load_dotenv
import os

# Load API keys from .env file
load_dotenv()
API_KEY_RW = os.getenv("API_KEY_RW")

client = EmonCMSClient(server_url="http://emonpi", api_key=API_KEY_RW, read_write=True)

# Create a new feed
feed_id = client.create_feed(name="TEST_6", tag="TESTTAG", engine=5, options={"interval": 30})

# Insert data into the feed
data = [
    [timestamp, value],  # Replace with actual data points
]
client.insert_multiple_data_points(feed_id=feed_id, data=data)
```

---

## Configuration

### Environment Variables
Create a `.env` file in the root directory with the following structure:

```env
API_KEY_RO="your_readonly_api_key"
API_KEY_RW="your_rw_api_key"
INFLUX_TOKEN="your_influxdb_token"
INFLUX_ORG="your_influxdb_org"
```

### VSCode Settings
The project includes a `.vscode/settings.json` file to configure Python's analysis paths:

```json
{
    "python.analysis.extraPaths": ["./src"]
}
```

---

## License

This project is licensed under the MIT License.
```