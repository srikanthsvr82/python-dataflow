"""BigQuery schema and table configuration."""

# BigQuery table schema definition
BIGQUERY_SCHEMA = {
    'fields': [
        {
            'name': 'message_id',
            'type': 'STRING',
            'mode': 'REQUIRED',
            'description': 'Unique identifier for the message'
        },
        {
            'name': 'user_id',
            'type': 'STRING',
            'mode': 'NULLABLE',
            'description': 'User identifier associated with the event'
        },
        {
            'name': 'event_type',
            'type': 'STRING',
            'mode': 'REQUIRED',
            'description': 'Type of event (e.g., purchase, signup, click)'
        },
        {
            'name': 'amount',
            'type': 'FLOAT',
            'mode': 'NULLABLE',
            'description': 'Monetary amount associated with the event'
        },
        {
            'name': 'timestamp',
            'type': 'TIMESTAMP',
            'mode': 'NULLABLE',
            'description': 'Original event timestamp'
        },
        {
            'name': 'processed_at',
            'type': 'TIMESTAMP',
            'mode': 'REQUIRED',
            'description': 'Timestamp when the message was processed by the pipeline'
        },
        {
            'name': 'metadata',
            'type': 'STRING',
            'mode': 'NULLABLE',
            'description': 'Additional metadata as JSON string'
        },
    ]
}


# Example schema for creating the table manually using bq CLI:
# bq mk --table \
#   your-project:your_dataset.events \
#   message_id:STRING,user_id:STRING,event_type:STRING,amount:FLOAT,timestamp:TIMESTAMP,processed_at:TIMESTAMP,metadata:STRING


# Example DDL for creating the table:
CREATE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS `your-project.your_dataset.events` (
    message_id STRING NOT NULL,
    user_id STRING,
    event_type STRING NOT NULL,
    amount FLOAT64,
    timestamp TIMESTAMP,
    processed_at TIMESTAMP NOT NULL,
    metadata STRING
)
PARTITION BY DATE(processed_at)
CLUSTER BY event_type, user_id;
"""
