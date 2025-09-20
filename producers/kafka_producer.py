"""
Kafka Producer for Bangalore Transit Data Pipeline
Reads JSON files and publishes data to Kafka topics
"""

import json
import time
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.kafka_config import (
    PRODUCER_CONFIG, KAFKA_TOPICS, DATA_PATHS
)
from utils.common import (
    setup_logging, load_json_file, get_current_timestamp, 
    format_message_key, validate_kafka_message
)

class BangaloreTransitProducer:
    """Kafka producer for Bangalore transit data"""
    
    def __init__(self):
        self.logger = setup_logging(__name__)
        self.producer = None
        self.connect_to_kafka()
    
    def connect_to_kafka(self):
        """Initialize Kafka producer connection"""
        try:
            self.producer = KafkaProducer(**PRODUCER_CONFIG)
            self.logger.info("Successfully connected to Kafka")
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def publish_message(self, topic: str, key: str, message: Dict[Any, Any]) -> bool:
        """
        Publish message to Kafka topic
        
        Args:
            topic: Kafka topic name
            key: Message key
            message: Message payload
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Add metadata to message
            enriched_message = {
                **message,
                'pipeline_timestamp': get_current_timestamp(),
                'source': 'bangalore_transit_pipeline'
            }
            
            # Validate message
            if not validate_kafka_message(enriched_message):
                self.logger.warning(f"Invalid message structure for key: {key}")
                return False
            
            # Serialize message
            message_json = json.dumps(enriched_message, ensure_ascii=False)
            
            # Send message
            future = self.producer.send(
                topic=topic,
                key=key,
                value=message_json
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            self.logger.info(
                f"Message sent to topic: {record_metadata.topic}, "
                f"partition: {record_metadata.partition}, "
                f"offset: {record_metadata.offset}, "
                f"key: {key}"
            )
            
            return True
            
        except KafkaError as e:
            self.logger.error(f"Kafka error sending message: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            return False
    
    def publish_static_data(self, agency: str) -> bool:
        """
        Publish static GTFS data for an agency
        
        Args:
            agency: Agency name (bmtc or bmrcl)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Load static data
            static_file_path = DATA_PATHS['static'][agency]
            static_data = load_json_file(static_file_path)
            
            if not static_data:
                self.logger.error(f"Failed to load static data for {agency}")
                return False
            
            # Prepare message
            message = {
                'timestamp': get_current_timestamp(),
                'agency': agency,
                'data_type': 'gtfs_static',
                'data': static_data
            }
            
            # Get topic and publish
            topic = KAFKA_TOPICS[agency]['gtfs_static']
            key = format_message_key(agency, 'static')
            
            success = self.publish_message(topic, key, message)
            
            if success:
                self.logger.info(f"Published static data for {agency}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error publishing static data for {agency}: {e}")
            return False
    
    def publish_fare_data(self, agency: str) -> bool:
        """
        Publish fare data for an agency
        
        Args:
            agency: Agency name (bmtc or bmrcl)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Load fare data
            fare_file_path = DATA_PATHS['fares'][agency]
            fare_data = load_json_file(fare_file_path)
            
            if not fare_data:
                self.logger.error(f"Failed to load fare data for {agency}")
                return False
            
            # Prepare message
            message = {
                'timestamp': get_current_timestamp(),
                'agency': agency,
                'data_type': 'fares',
                'data': fare_data
            }
            
            # Get topic and publish
            topic = KAFKA_TOPICS[agency]['fares']
            key = format_message_key(agency, 'fares')
            
            success = self.publish_message(topic, key, message)
            
            if success:
                self.logger.info(f"Published fare data for {agency}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error publishing fare data for {agency}: {e}")
            return False
    
    def publish_vehicle_positions(self, agency: str) -> bool:
        """
        Publish real-time vehicle position data
        
        Args:
            agency: Agency name (bmtc or bmrcl)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Determine file path and topic based on agency
            if agency == 'bmtc':
                file_path = DATA_PATHS['live']['bmtc_vehicles']
                topic = KAFKA_TOPICS['bmtc']['vehicle_positions']
                position_type = 'vehicle_positions'
            elif agency == 'bmrcl':
                file_path = DATA_PATHS['live']['bmrcl_trains']
                topic = KAFKA_TOPICS['bmrcl']['train_positions']
                position_type = 'train_positions'
            else:
                self.logger.error(f"Unknown agency: {agency}")
                return False
            
            # Load position data
            position_data = load_json_file(file_path)
            
            if not position_data:
                self.logger.error(f"Failed to load position data for {agency}")
                return False
            
            # Update timestamp to current time
            position_data['header']['timestamp'] = get_current_timestamp()
            
            # Publish each vehicle/train position separately
            success_count = 0
            total_entities = len(position_data.get('entity', []))
            
            for entity in position_data.get('entity', []):
                # Update entity timestamp
                if 'vehicle' in entity and 'timestamp' in entity['vehicle']:
                    entity['vehicle']['timestamp'] = get_current_timestamp()
                
                # Prepare message
                message = {
                    'timestamp': get_current_timestamp(),
                    'agency': agency,
                    'data_type': position_type,
                    'entity': entity
                }
                
                # Create unique key for each vehicle/train
                entity_id = entity.get('id', 'unknown')
                key = format_message_key(agency, 'positions', entity_id)
                
                if self.publish_message(topic, key, message):
                    success_count += 1
            
            self.logger.info(
                f"Published {success_count}/{total_entities} position updates for {agency}"
            )
            
            return success_count == total_entities
            
        except Exception as e:
            self.logger.error(f"Error publishing position data for {agency}: {e}")
            return False
    
    def publish_all_static_data(self) -> bool:
        """Publish all static data for both agencies"""
        success = True
        
        for agency in ['bmtc', 'bmrcl']:
            if not self.publish_static_data(agency):
                success = False
            if not self.publish_fare_data(agency):
                success = False
        
        return success
    
    def start_live_data_simulation(self, interval_seconds: int = 30):
        """
        Start publishing live vehicle position data at regular intervals
        
        Args:
            interval_seconds: Interval between updates
        """
        self.logger.info(f"Starting live data simulation with {interval_seconds}s intervals")
        
        try:
            while True:
                # Publish vehicle positions for both agencies
                self.publish_vehicle_positions('bmtc')
                self.publish_vehicle_positions('bmrcl')
                
                self.logger.info(f"Waiting {interval_seconds} seconds for next update...")
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            self.logger.info("Live data simulation stopped by user")
        except Exception as e:
            self.logger.error(f"Error in live data simulation: {e}")
    
    def close(self):
        """Close Kafka producer connection"""
        if self.producer:
            self.producer.close()
            self.logger.info("Kafka producer connection closed")

def main():
    """Main function to run the producer"""
    producer = None
    
    try:
        producer = BangaloreTransitProducer()
        
        # Publish all static data first
        print("Publishing static data...")
        if producer.publish_all_static_data():
            print("✅ Static data published successfully")
        else:
            print("❌ Failed to publish some static data")
        
        # Ask user if they want to start live simulation
        response = input("\nStart live vehicle position simulation? (y/n): ").lower()
        
        if response == 'y':
            interval = input("Enter update interval in seconds (default 30): ")
            try:
                interval = int(interval) if interval else 30
            except ValueError:
                interval = 30
            
            print(f"\nStarting live simulation with {interval}s intervals...")
            print("Press Ctrl+C to stop")
            producer.start_live_data_simulation(interval)
        else:
            print("Skipping live simulation")
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    main()