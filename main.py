"""Apache Beam pipeline to read from Pub/Sub and write to BigQuery."""

import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

from transforms.message_transforms import (
    ParsePubSubMessage,
    EnrichMessage,
    FormatForBigQuery
)


def get_bigquery_schema():
    """Define BigQuery table schema."""
    return {
        'fields': [
            {'name': 'message_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'event_type', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'processed_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'metadata', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }


def run_pipeline(argv=None):
    """Main pipeline execution."""
    parser = argparse.ArgumentParser()
    
    # Pipeline arguments
    parser.add_argument(
        '--input_subscription',
        required=True,
        help='Pub/Sub subscription to read from (format: projects/<project>/subscriptions/<subscription>)'
    )
    parser.add_argument(
        '--output_table',
        required=True,
        help='BigQuery table to write to (format: project:dataset.table)'
    )
    parser.add_argument(
        '--project',
        required=True,
        help='GCP project ID'
    )
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    
    logging.info(f"Starting pipeline with subscription: {known_args.input_subscription}")
    logging.info(f"Output table: {known_args.output_table}")
    
    # Create and run pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read from Pub/Sub' >> ReadFromPubSub(subscription=known_args.input_subscription)
            | 'Parse JSON Messages' >> beam.ParDo(ParsePubSubMessage())
            | 'Enrich Messages' >> beam.ParDo(EnrichMessage())
            | 'Format for BigQuery' >> beam.ParDo(FormatForBigQuery())
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=known_args.output_table,
                schema=get_bigquery_schema(),
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                project=known_args.project
            )
        )


def main():
    """Entry point for the pipeline."""
    logging.basicConfig(level=logging.INFO)
    run_pipeline()


if __name__ == "__main__":
    main()

