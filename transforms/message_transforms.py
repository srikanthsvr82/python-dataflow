"""Transform functions for processing Pub/Sub messages."""

import json
import logging
from typing import Dict, Any
import apache_beam as beam


class ParsePubSubMessage(beam.DoFn):
    """Parse JSON messages from Pub/Sub."""
    
    def process(self, element: bytes, **kwargs):
        """
        Parse Pub/Sub message and extract data.
        
        Args:
            element: Raw message bytes from Pub/Sub
            
        Yields:
            Parsed message dictionary
        """
        try:
            # Decode bytes to string and parse JSON
            message_str = element.decode('utf-8')
            message_data = json.loads(message_str)
            
            logging.info(f"Successfully parsed message: {message_data}")
            yield message_data
            
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.error(f"Failed to parse message: {e}")
            # Optionally yield to a dead letter queue or skip


class EnrichMessage(beam.DoFn):
    """Enrich messages with additional fields."""
    
    def process(self, element: Dict[str, Any], **kwargs):
        """
        Add enrichment fields to the message.
        
        Args:
            element: Parsed message dictionary
            
        Yields:
            Enriched message dictionary
        """
        import datetime
        
        # Add processing timestamp
        element['processed_at'] = datetime.datetime.utcnow().isoformat()
        
        # Add any custom enrichment logic here
        if 'amount' in element:
            element['amount_doubled'] = element['amount'] * 2
            
        logging.info(f"Enriched message: {element}")
        yield element


class FormatForBigQuery(beam.DoFn):
    """Format messages for BigQuery ingestion."""
    
    def process(self, element: Dict[str, Any], **kwargs):
        """
        Format message to match BigQuery schema.
        
        Args:
            element: Enriched message dictionary
            
        Yields:
            Formatted row dictionary for BigQuery
        """
        # Map fields to BigQuery schema
        row = {
            'message_id': element.get('id', 'unknown'),
            'user_id': element.get('user_id'),
            'event_type': element.get('event_type', 'unknown'),
            'amount': float(element.get('amount', 0.0)),
            'timestamp': element.get('timestamp'),
            'processed_at': element.get('processed_at'),
            'metadata': json.dumps(element.get('metadata', {}))
        }
        
        logging.info(f"Formatted row for BigQuery: {row}")
        yield row
