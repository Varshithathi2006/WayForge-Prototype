"""
Kafka Configuration for Bangalore Transit Data Pipeline
"""

import os
from typing import Dict, List

# Kafka Broker Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_CLIENT_ID = 'bangalore_transit_pipeline'

# Topic Configuration
KAFKA_TOPICS = {
    'bmtc': {
        'gtfs_static': 'bmtc.gtfs_static',
        'fares': 'bmtc.fares',
        'vehicle_positions': 'bmtc.vehicle_positions'
    },
    'bmrcl': {
        'gtfs_static': 'bmrcl.gtfs_static',
        'fares': 'bmrcl.fares',
        'train_positions': 'bmrcl.train_positions'
    }
}

# Producer Configuration
PRODUCER_CONFIG = {
    'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
    'client_id': KAFKA_CLIENT_ID,
    'value_serializer': lambda x: x.encode('utf-8') if isinstance(x, str) else x,
    'key_serializer': lambda x: x.encode('utf-8') if isinstance(x, str) else x,
    'acks': 'all',  # Wait for all replicas to acknowledge
    'retries': 3,
    'batch_size': 16384,
    'linger_ms': 10,
    'buffer_memory': 33554432
}

# Consumer Configuration
CONSUMER_CONFIG = {
    'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
    'client_id': KAFKA_CLIENT_ID,
    'group_id': 'bangalore_transit_consumer_group',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'auto_commit_interval_ms': 1000,
    'value_deserializer': lambda x: x.decode('utf-8') if x else None,
    'key_deserializer': lambda x: x.decode('utf-8') if x else None
}

# Data File Paths
DATA_PATHS = {
    'static': {
        'bmtc': 'data/static/bmtc_static.json',
        'bmrcl': 'data/static/bmrcl_static.json'
    },
    'fares': {
        'bmtc': 'data/static/bmtc_fares.json',
        'bmrcl': 'data/static/bmrcl_fares.json'
    },
    'live': {
        'bmtc_vehicles': 'data/live/bmtc_vehicle_positions.json',
        'bmrcl_trains': 'data/live/bmrcl_train_positions.json'
    }
}

# Route Optimization Weights
ROUTE_WEIGHTS = {
    'fastest': {'time': 1.0, 'cost': 0.0, 'eco': 0.0},
    'cheapest': {'time': 0.3, 'cost': 1.0, 'eco': 0.0},
    'eco_friendly': {'time': 0.3, 'cost': 0.2, 'eco': 1.0},
    'balanced': {'time': 0.4, 'cost': 0.3, 'eco': 0.3}
}

# Logging Configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'file': {
            'level': 'DEBUG',
            'formatter': 'standard',
            'class': 'logging.FileHandler',
            'filename': 'logs/bangalore_transit.log',
            'mode': 'a',
        },
    },
    'loggers': {
        '': {
            'handlers': ['default', 'file'],
            'level': 'DEBUG',
            'propagate': False
        }
    }
}