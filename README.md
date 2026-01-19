# Python Dataflow Pipeline

Apache Beam pipeline that consumes messages from Google Cloud Pub/Sub, transforms them, and persists to BigQuery.

## Project Structure

```
python-dataflow/
├── main.py                          # Main pipeline implementation
├── transforms/
│   ├── __init__.py
│   └── message_transforms.py        # Transform functions (Parse, Enrich, Format)
├── utils/
│   ├── __init__.py
│   └── bigquery_schema.py          # BigQuery schema definition
├── test_pipeline.py                 # Unit tests using TestPipeline
├── pyproject.toml                   # Dependencies managed by UV
└── README.md
```

## Prerequisites

- Python 3.11+
- UV package manager
- Google Cloud Project with:
  - Pub/Sub API enabled
  - BigQuery API enabled
  - Dataflow API enabled (for production deployment)
  - Service account with appropriate permissions

## Installation

Install dependencies using UV:

```bash
uv sync
```

Or install individually:

```bash
uv pip install apache-beam[gcp] google-cloud-pubsub google-cloud-bigquery
```

## Pipeline Overview

The pipeline performs the following steps:

1. **Read from Pub/Sub**: Consumes messages from a Pub/Sub subscription
2. **Parse Messages**: Decodes and parses JSON messages
3. **Enrich Messages**: Adds processing timestamp and additional fields
4. **Format for BigQuery**: Maps fields to BigQuery schema
5. **Write to BigQuery**: Persists transformed data to BigQuery table

## Running the Pipeline

### Local Testing (DirectRunner)

```bash
uv run python main.py \
  --input_subscription=projects/YOUR_PROJECT/subscriptions/YOUR_SUBSCRIPTION \
  --output_table=YOUR_PROJECT:YOUR_DATASET.YOUR_TABLE \
  --project=YOUR_PROJECT \
  --runner=DirectRunner
```

### Production (DataflowRunner)

```bash
uv run python main.py \
  --input_subscription=projects/YOUR_PROJECT/subscriptions/YOUR_SUBSCRIPTION \
  --output_table=YOUR_PROJECT:YOUR_DATASET.YOUR_TABLE \
  --project=YOUR_PROJECT \
  --runner=DataflowRunner \
  --region=us-central1 \
  --temp_location=gs://YOUR_BUCKET/temp \
  --staging_location=gs://YOUR_BUCKET/staging
```

## Running Tests

Run unit tests with pytest:

```bash
uv run pytest test_pipeline.py -v
```

Or with unittest:

```bash
uv run python -m unittest test_pipeline.py
```

## BigQuery Schema

The pipeline writes to BigQuery with the following schema:

| Field | Type | Mode | Description |
|-------|------|------|-------------|
| message_id | STRING | REQUIRED | Unique message identifier |
| user_id | STRING | NULLABLE | User identifier |
| event_type | STRING | REQUIRED | Event type (purchase, signup, etc.) |
| amount | FLOAT | NULLABLE | Monetary amount |
| timestamp | TIMESTAMP | NULLABLE | Original event timestamp |
| processed_at | TIMESTAMP | REQUIRED | Processing timestamp |
| metadata | STRING | NULLABLE | Additional metadata (JSON) |

## Example Pub/Sub Message

```json
{
  "id": "msg-123",
  "user_id": "user-456",
  "event_type": "purchase",
  "amount": 99.99,
  "timestamp": "2026-01-19T10:00:00Z",
  "metadata": {
    "source": "mobile_app",
    "campaign": "winter2026"
  }
}
```

## Development

### Adding New Transforms

Add new transformation functions in [transforms/message_transforms.py](transforms/message_transforms.py):

```python
class MyCustomTransform(beam.DoFn):
    def process(self, element):
        # Your transformation logic
        yield transformed_element
```

### Testing with Mock Data

The [test_pipeline.py](test_pipeline.py) file uses Apache Beam's `TestPipeline` for unit testing without requiring actual GCP resources.

## Configuration

Key configuration parameters:
- `--input_subscription`: Full Pub/Sub subscription path
- `--output_table`: BigQuery table in format `project:dataset.table`
- `--project`: GCP project ID
- `--runner`: Pipeline runner (DirectRunner for local, DataflowRunner for production)

## Monitoring

For production deployments:
- Monitor pipeline in Google Cloud Console → Dataflow
- View BigQuery data in Cloud Console → BigQuery
- Check Pub/Sub metrics in Cloud Console → Pub/Sub

## License

MIT
