"""
Common utilities for Bangalore Transit Data Pipeline
"""

import json
import logging
import logging.config
import os
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.kafka_config import LOGGING_CONFIG

def setup_logging(name: str) -> logging.Logger:
    """Setup logging configuration"""
    logging.config.dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger(name)
    return logger

def load_json_file(file_path: str) -> Optional[Dict[Any, Any]]:
    """
    Load JSON data from file with error handling
    
    Args:
        file_path: Path to JSON file
        
    Returns:
        Dictionary containing JSON data or None if error
    """
    try:
        # Convert relative path to absolute path
        if not os.path.isabs(file_path):
            file_path = os.path.join(project_root, file_path)
            
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error in {file_path}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        return None

def save_json_file(data: Dict[Any, Any], file_path: str) -> bool:
    """
    Save data to JSON file with error handling
    
    Args:
        data: Dictionary to save
        file_path: Path to save file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Convert relative path to absolute path
        if not os.path.isabs(file_path):
            file_path = os.path.join(project_root, file_path)
            
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=2, ensure_ascii=False)
            return True
    except Exception as e:
        logging.error(f"Error saving {file_path}: {e}")
        return False

def get_current_timestamp() -> int:
    """Get current timestamp in seconds"""
    return int(datetime.now().timestamp())

def format_message_key(agency: str, data_type: str, entity_id: str = None) -> str:
    """
    Format Kafka message key
    
    Args:
        agency: Agency name (bmtc, bmrcl)
        data_type: Type of data (static, fares, positions)
        entity_id: Optional entity identifier
        
    Returns:
        Formatted key string
    """
    if entity_id:
        return f"{agency}_{data_type}_{entity_id}"
    return f"{agency}_{data_type}"

def validate_kafka_message(message: Dict[Any, Any]) -> bool:
    """
    Validate Kafka message structure
    
    Args:
        message: Message dictionary to validate
        
    Returns:
        True if valid, False otherwise
    """
    required_fields = ['timestamp', 'agency', 'data_type']
    
    for field in required_fields:
        if field not in message:
            logging.warning(f"Missing required field: {field}")
            return False
    
    return True

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance between two coordinates using Haversine formula
    
    Args:
        lat1, lon1: First coordinate
        lat2, lon2: Second coordinate
        
    Returns:
        Distance in kilometers
    """
    import math
    
    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    # Radius of earth in kilometers
    r = 6371
    
    return c * r

def ensure_logs_directory():
    """Ensure logs directory exists"""
    logs_dir = os.path.join(project_root, 'logs')
    os.makedirs(logs_dir, exist_ok=True)

# Initialize logs directory when module is imported
ensure_logs_directory()