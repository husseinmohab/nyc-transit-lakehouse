"""Real-Time NYC Transit Lakehouse

gtfs       decode and normalize mta gtfs realtime protobuf feeds
producer   kafka producer service and the synthetic load generator
streaming  spark structured streaming jobs writing iceberg bronze and silver
quality    great expectations suites and freshness sla definitions
backfill   partition scoped idempotent replay the airflow backfill dag uses
"""

__version__ = "0.3.0"
