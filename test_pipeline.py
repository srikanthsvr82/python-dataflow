"""Tests for the Pub/Sub to BigQuery pipeline using TestPipeline."""

import json
import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from transforms.message_transforms import (
    ParsePubSubMessage,
    EnrichMessage,
    FormatForBigQuery
)


class TestMessageTransforms(unittest.TestCase):
    """Test cases for message transformation functions."""
    
    def test_parse_pubsub_message(self):
        """Test parsing JSON messages from Pub/Sub."""
        # Mock Pub/Sub message
        test_message = json.dumps({
            'id': 'msg-123',
            'user_id': 'user-456',
            'event_type': 'purchase',
            'amount': 99.99,
            'timestamp': '2026-01-19T10:00:00Z',
            'metadata': {'source': 'mobile_app'}
        }).encode('utf-8')
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([test_message])
                | beam.ParDo(ParsePubSubMessage())
            )
            
            expected = [{
                'id': 'msg-123',
                'user_id': 'user-456',
                'event_type': 'purchase',
                'amount': 99.99,
                'timestamp': '2026-01-19T10:00:00Z',
                'metadata': {'source': 'mobile_app'}
            }]
            
            assert_that(output, equal_to(expected))
    
    def test_enrich_message(self):
        """Test message enrichment with additional fields."""
        test_data = [{
            'id': 'msg-123',
            'user_id': 'user-456',
            'event_type': 'purchase',
            'amount': 100.0
        }]
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(test_data)
                | beam.ParDo(EnrichMessage())
            )
            
            def check_enrichment(elements):
                assert len(elements) == 1
                element = elements[0]
                assert 'processed_at' in element
                assert element['amount_doubled'] == 200.0
                assert element['id'] == 'msg-123'
            
            assert_that(output, check_enrichment)
    
    def test_format_for_bigquery(self):
        """Test formatting messages for BigQuery schema."""
        test_data = [{
            'id': 'msg-123',
            'user_id': 'user-456',
            'event_type': 'purchase',
            'amount': 99.99,
            'timestamp': '2026-01-19T10:00:00Z',
            'processed_at': '2026-01-19T10:01:00Z',
            'metadata': {'source': 'mobile_app'}
        }]
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(test_data)
                | beam.ParDo(FormatForBigQuery())
            )
            
            def check_format(elements):
                assert len(elements) == 1
                row = elements[0]
                assert row['message_id'] == 'msg-123'
                assert row['user_id'] == 'user-456'
                assert row['event_type'] == 'purchase'
                assert row['amount'] == 99.99
                assert 'metadata' in row
                assert isinstance(row['metadata'], str)  # Should be JSON string
            
            assert_that(output, check_format)
    
    def test_full_pipeline(self):
        """Test the complete pipeline transformation."""
        test_message = json.dumps({
            'id': 'msg-789',
            'user_id': 'user-101',
            'event_type': 'signup',
            'amount': 0.0,
            'timestamp': '2026-01-19T12:00:00Z',
            'metadata': {'campaign': 'winter2026'}
        }).encode('utf-8')
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([test_message])
                | 'Parse' >> beam.ParDo(ParsePubSubMessage())
                | 'Enrich' >> beam.ParDo(EnrichMessage())
                | 'Format' >> beam.ParDo(FormatForBigQuery())
            )
            
            def check_pipeline(elements):
                assert len(elements) == 1
                row = elements[0]
                assert row['message_id'] == 'msg-789'
                assert row['event_type'] == 'signup'
                assert 'processed_at' in row
            
            assert_that(output, check_pipeline)


if __name__ == '__main__':
    unittest.main()
