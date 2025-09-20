"""
Pathway Engine Consumer for Bangalore Transit Data Pipeline
Consumes Kafka messages and processes them using Pathway for real-time route optimization
"""

import json
import sys
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.kafka_config import CONSUMER_CONFIG, KAFKA_TOPICS
from utils.common import setup_logging, get_current_timestamp, save_json_file
from consumers.route_optimizer import RouteOptimizer, RouteOption

class PathwayTransitConsumer:
    """Pathway-based consumer for real-time transit data processing"""
    
    def __init__(self):
        self.logger = setup_logging(__name__)
        self.consumer = None
        self.route_optimizer = RouteOptimizer()
        self.processed_routes = []
        self.connect_to_kafka()
    
    def connect_to_kafka(self):
        """Initialize Kafka consumer connection"""
        try:
            # Get all topic names
            all_topics = []
            for agency_topics in KAFKA_TOPICS.values():
                all_topics.extend(agency_topics.values())
            
            self.consumer = KafkaConsumer(
                *all_topics,
                **CONSUMER_CONFIG
            )
            
            self.logger.info(f"Successfully connected to Kafka, subscribed to topics: {all_topics}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def process_message(self, message) -> bool:
        """
        Process a single Kafka message
        
        Args:
            message: Kafka message object
            
        Returns:
            True if processed successfully, False otherwise
        """
        try:
            # Parse message
            message_data = json.loads(message.value)
            topic = message.topic
            key = message.key
            
            self.logger.debug(f"Processing message from topic: {topic}, key: {key}")
            
            # Determine message type and agency
            agency = message_data.get('agency')
            data_type = message_data.get('data_type')
            
            if not agency or not data_type:
                self.logger.warning(f"Missing agency or data_type in message: {key}")
                return False
            
            # Route message to appropriate handler
            if data_type == 'gtfs_static':
                return self._handle_static_data(agency, message_data)
            elif data_type == 'fares':
                return self._handle_fare_data(agency, message_data)
            elif data_type in ['vehicle_positions', 'train_positions']:
                return self._handle_position_data(agency, message_data)
            else:
                self.logger.warning(f"Unknown data type: {data_type}")
                return False
                
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            return False
    
    def _handle_static_data(self, agency: str, message_data: Dict[str, Any]) -> bool:
        """Handle static GTFS data"""
        try:
            data = message_data.get('data', {})
            self.route_optimizer.update_static_data(agency, data)
            
            self.logger.info(f"Processed static data for {agency}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error handling static data for {agency}: {e}")
            return False
    
    def _handle_fare_data(self, agency: str, message_data: Dict[str, Any]) -> bool:
        """Handle fare data"""
        try:
            data = message_data.get('data', {})
            self.route_optimizer.update_fare_data(agency, data)
            
            self.logger.info(f"Processed fare data for {agency}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error handling fare data for {agency}: {e}")
            return False
    
    def _handle_position_data(self, agency: str, message_data: Dict[str, Any]) -> bool:
        """Handle real-time position data"""
        try:
            entity = message_data.get('entity', {})
            self.route_optimizer.update_live_positions(agency, entity)
            
            # Trigger route optimization for demonstration
            self._trigger_route_optimization()
            
            self.logger.debug(f"Processed position data for {agency}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error handling position data for {agency}: {e}")
            return False
    
    def _trigger_route_optimization(self):
        """Trigger route optimization with sample coordinates"""
        try:
            # Sample route request (Koramangala to Whitefield)
            origin_lat, origin_lon = 12.9352, 77.6245  # Koramangala
            dest_lat, dest_lon = 12.9698, 77.7500     # Whitefield
            
            # Find routes
            routes = self.route_optimizer.find_routes(origin_lat, origin_lon, dest_lat, dest_lon)
            
            if routes:
                # Optimize for different criteria
                optimization_types = ['fastest', 'cheapest', 'eco_friendly', 'balanced']
                
                results = {}
                for opt_type in optimization_types:
                    optimized_routes = self.route_optimizer.optimize_routes(routes.copy(), opt_type)
                    results[opt_type] = self._format_route_results(optimized_routes[:3])  # Top 3 routes
                
                # Store results
                self._store_optimization_results(results)
                
                # Display results
                self._display_route_results(results)
                
        except Exception as e:
            self.logger.error(f"Error in route optimization: {e}")
    
    def _format_route_results(self, routes: List[RouteOption]) -> List[Dict[str, Any]]:
        """Format route results for storage/display"""
        formatted_routes = []
        
        for route in routes:
            formatted_route = {
                'route_id': route.route_id,
                'agency': route.agency,
                'total_time_minutes': round(route.total_time, 1),
                'total_cost_inr': round(route.total_cost, 2),
                'eco_score': round(route.eco_score, 1),
                'transfers': route.transfers,
                'distance_km': round(route.distance, 2),
                'composite_score': round(getattr(route, 'composite_score', 0), 3),
                'route_details': route.route_details,
                'timestamp': get_current_timestamp()
            }
            formatted_routes.append(formatted_route)
        
        return formatted_routes
    
    def _store_optimization_results(self, results: Dict[str, List[Dict[str, Any]]]):
        """Store optimization results to file"""
        try:
            timestamp = get_current_timestamp()
            filename = f"logs/route_optimization_results_{timestamp}.json"
            
            output_data = {
                'timestamp': timestamp,
                'origin': {'lat': 12.9352, 'lon': 77.6245, 'name': 'Koramangala'},
                'destination': {'lat': 12.9698, 'lon': 77.7500, 'name': 'Whitefield'},
                'optimization_results': results
            }
            
            if save_json_file(output_data, filename):
                self.logger.info(f"Saved optimization results to {filename}")
            
        except Exception as e:
            self.logger.error(f"Error storing optimization results: {e}")
    
    def _display_route_results(self, results: Dict[str, List[Dict[str, Any]]]):
        """Display route optimization results"""
        try:
            print("\n" + "="*80)
            print("🚌 BANGALORE TRANSIT ROUTE OPTIMIZATION RESULTS 🚇")
            print("="*80)
            print(f"📍 Route: Koramangala → Whitefield")
            print(f"⏰ Generated at: {datetime.fromtimestamp(get_current_timestamp()).strftime('%Y-%m-%d %H:%M:%S')}")
            print()
            
            for opt_type, routes in results.items():
                print(f"🎯 {opt_type.upper().replace('_', ' ')} ROUTES:")
                print("-" * 50)
                
                for i, route in enumerate(routes, 1):
                    print(f"  {i}. {route['route_id']} ({route['agency'].upper()})")
                    print(f"     ⏱️  Time: {route['total_time_minutes']} min")
                    print(f"     💰 Cost: ₹{route['total_cost_inr']}")
                    print(f"     🌱 Eco Score: {route['eco_score']}/10")
                    print(f"     🔄 Transfers: {route['transfers']}")
                    print(f"     📏 Distance: {route['distance_km']} km")
                    print(f"     ⭐ Score: {route['composite_score']}")
                    
                    # Show route details
                    print(f"     📋 Route Details:")
                    for detail in route['route_details']:
                        if detail['type'] == 'walk':
                            print(f"        🚶 Walk: {detail['from']} → {detail['to']} ({detail['distance']:.1f}km, {detail['time']:.0f}min)")
                        elif detail['type'] == 'transit':
                            print(f"        🚌 {detail['agency'].upper()}: {detail['route']} from {detail['from']} → {detail['to']} ({detail['time']:.0f}min, ₹{detail['cost']})")
                        elif detail['type'] == 'transfer':
                            print(f"        🔄 Transfer: {detail['from']} → {detail['to']} ({detail['time']}min)")
                    print()
                
                print()
            
            print("="*80)
            
        except Exception as e:
            self.logger.error(f"Error displaying results: {e}")
    
    def create_route_summary_dataframe(self, results: Dict[str, List[Dict[str, Any]]]) -> pd.DataFrame:
        """Create a pandas DataFrame summary of route results"""
        try:
            summary_data = []
            
            for opt_type, routes in results.items():
                for route in routes:
                    summary_data.append({
                        'optimization_type': opt_type,
                        'route_id': route['route_id'],
                        'agency': route['agency'],
                        'time_minutes': route['total_time_minutes'],
                        'cost_inr': route['total_cost_inr'],
                        'eco_score': route['eco_score'],
                        'transfers': route['transfers'],
                        'distance_km': route['distance_km'],
                        'composite_score': route['composite_score']
                    })
            
            df = pd.DataFrame(summary_data)
            
            # Save to CSV
            timestamp = get_current_timestamp()
            csv_filename = f"logs/route_summary_{timestamp}.csv"
            df.to_csv(csv_filename, index=False)
            
            self.logger.info(f"Saved route summary to {csv_filename}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error creating summary DataFrame: {e}")
            return pd.DataFrame()
    
    def start_consuming(self):
        """Start consuming messages from Kafka"""
        self.logger.info("Starting Kafka message consumption...")
        
        try:
            for message in self.consumer:
                success = self.process_message(message)
                
                if success:
                    self.logger.debug(f"Successfully processed message from {message.topic}")
                else:
                    self.logger.warning(f"Failed to process message from {message.topic}")
                
        except KeyboardInterrupt:
            self.logger.info("Consumer stopped by user")
        except Exception as e:
            self.logger.error(f"Error in message consumption: {e}")
        finally:
            self.close()
    
    def close(self):
        """Close Kafka consumer connection"""
        if self.consumer:
            self.consumer.close()
            self.logger.info("Kafka consumer connection closed")

def main():
    """Main function to run the consumer"""
    consumer = None
    
    try:
        print("🚀 Starting Bangalore Transit Pathway Consumer...")
        print("📡 Connecting to Kafka and waiting for messages...")
        print("🔄 Real-time route optimization will be triggered by vehicle position updates")
        print("⏹️  Press Ctrl+C to stop\n")
        
        consumer = PathwayTransitConsumer()
        consumer.start_consuming()
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        if consumer:
            consumer.close()

if __name__ == "__main__":
    main()