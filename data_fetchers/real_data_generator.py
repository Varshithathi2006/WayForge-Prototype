"""
Real Data Generator for Bangalore Transit Pipeline
Generates real-time data using BMTC and BMRCL fetchers with accurate pricing
"""

import os
import sys
import time
import logging
from datetime import datetime
from typing import Dict, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_fetchers.bmtc_fetcher import BMTCDataFetcher
from data_fetchers.bmrcl_fetcher import BMRCLDataFetcher
from utils.common import setup_logging, save_json_file, get_current_timestamp
from utils.error_handler import error_handler_decorator, performance_monitor

logger = setup_logging(__name__)

class RealDataGenerator:
    """Generates real transit data with accurate pricing for Bangalore"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.bmtc_fetcher = BMTCDataFetcher()
        self.bmrcl_fetcher = BMRCLDataFetcher()
        
        # Ensure directories exist
        os.makedirs(f"{data_dir}/static", exist_ok=True)
        os.makedirs(f"{data_dir}/live", exist_ok=True)
        
        logger.info("Real Data Generator initialized")
    
    @error_handler_decorator("real_data_generator")
    @performance_monitor("real_data_generator")
    def generate_all_data(self) -> Dict[str, Any]:
        """Generate all real data for both BMTC and BMRCL"""
        results = {
            "timestamp": get_current_timestamp(),
            "status": "success",
            "data_generated": []
        }
        
        try:
            logger.info("Starting real data generation...")
            
            # Generate BMTC data
            logger.info("Generating BMTC data...")
            bmtc_success = self._generate_bmtc_data()
            if bmtc_success:
                results["data_generated"].append("BMTC")
                logger.info("✓ BMTC data generated successfully")
            else:
                logger.error("✗ BMTC data generation failed")
            
            # Generate BMRCL data
            logger.info("Generating BMRCL data...")
            bmrcl_success = self._generate_bmrcl_data()
            if bmrcl_success:
                results["data_generated"].append("BMRCL")
                logger.info("✓ BMRCL data generated successfully")
            else:
                logger.error("✗ BMRCL data generation failed")
            
            # Generate comparison data
            self._generate_comparison_data()
            results["data_generated"].append("Comparison")
            
            logger.info(f"Real data generation completed. Generated: {results['data_generated']}")
            return results
            
        except Exception as e:
            logger.error(f"Error in data generation: {e}")
            results["status"] = "error"
            results["error"] = str(e)
            return results
    
    def _generate_bmtc_data(self) -> bool:
        """Generate BMTC real data"""
        try:
            # Routes data
            routes_data = self.bmtc_fetcher.fetch_routes()
            save_json_file(routes_data, f"{self.data_dir}/static/bmtc_routes_real.json")
            
            # Live positions
            positions_data = self.bmtc_fetcher.fetch_live_positions()
            save_json_file(positions_data, f"{self.data_dir}/live/bmtc_positions_real.json")
            
            # Fare structure (updated with real 2024 prices)
            fare_data = self.bmtc_fetcher.get_fare_structure()
            save_json_file(fare_data, f"{self.data_dir}/static/bmtc_fares_real.json")
            
            return True
            
        except Exception as e:
            logger.error(f"Error generating BMTC data: {e}")
            return False
    
    def _generate_bmrcl_data(self) -> bool:
        """Generate BMRCL real data"""
        try:
            # Routes/Lines data
            routes_data = self.bmrcl_fetcher.fetch_routes()
            save_json_file(routes_data, f"{self.data_dir}/static/bmrcl_routes_real.json")
            
            # Stations data
            stations_data = self.bmrcl_fetcher.fetch_stations()
            save_json_file(stations_data, f"{self.data_dir}/static/bmrcl_stations_real.json")
            
            # Live positions
            positions_data = self.bmrcl_fetcher.fetch_live_positions()
            save_json_file(positions_data, f"{self.data_dir}/live/bmrcl_positions_real.json")
            
            # Fare structure (updated with real distance-based prices)
            fare_data = self.bmrcl_fetcher.get_fare_structure()
            save_json_file(fare_data, f"{self.data_dir}/static/bmrcl_fares_real.json")
            
            return True
            
        except Exception as e:
            logger.error(f"Error generating BMRCL data: {e}")
            return False
    
    def _generate_comparison_data(self):
        """Generate fare comparison data between BMTC and BMRCL"""
        try:
            comparison_data = {
                "timestamp": get_current_timestamp(),
                "comparison_type": "fare_analysis",
                "currency": "INR",
                "routes": []
            }
            
            # Sample routes for comparison
            sample_distances = [2, 5, 8, 12, 15, 20, 25]
            
            for distance in sample_distances:
                route_comparison = {
                    "distance_km": distance,
                    "bmtc": {
                        "ordinary": self.bmtc_fetcher.calculate_fare(distance, "ordinary"),
                        "deluxe": self.bmtc_fetcher.calculate_fare(distance, "deluxe"),
                        "ac": self.bmtc_fetcher.calculate_fare(distance, "ac"),
                        "vajra": self.bmtc_fetcher.calculate_fare(distance, "vajra")
                    },
                    "bmrcl": {
                        "token": self.bmrcl_fetcher.calculate_fare(distance, "token"),
                        "smart_card": self.bmrcl_fetcher.calculate_fare(distance, "smart_card")
                    }
                }
                comparison_data["routes"].append(route_comparison)
            
            # Add summary insights
            comparison_data["insights"] = {
                "bmtc_range": "Rs 5-70+ (depending on distance and bus type)",
                "bmrcl_range": "Rs 10-60 (distance-based slabs)",
                "cost_effective_short": "BMTC ordinary buses for <3km",
                "cost_effective_long": "BMRCL metro for >10km",
                "notes": [
                    "BMTC fares increased in January 2024",
                    "BMRCL offers 5% smart card discount",
                    "Metro is generally faster for longer distances",
                    "Bus network has better coverage"
                ]
            }
            
            save_json_file(comparison_data, f"{self.data_dir}/static/fare_comparison_real.json")
            logger.info("Fare comparison data generated")
            
        except Exception as e:
            logger.error(f"Error generating comparison data: {e}")
    
    def generate_live_updates(self, interval_seconds: int = 30, duration_minutes: int = 5):
        """Generate live position updates for specified duration"""
        logger.info(f"Starting live updates for {duration_minutes} minutes...")
        
        end_time = time.time() + (duration_minutes * 60)
        update_count = 0
        
        while time.time() < end_time:
            try:
                # Generate BMTC live positions
                bmtc_positions = self.bmtc_fetcher.fetch_live_positions()
                save_json_file(bmtc_positions, f"{self.data_dir}/live/bmtc_positions_live.json")
                
                # Generate BMRCL live positions
                bmrcl_positions = self.bmrcl_fetcher.fetch_live_positions()
                save_json_file(bmrcl_positions, f"{self.data_dir}/live/bmrcl_positions_live.json")
                
                update_count += 1
                logger.info(f"Live update #{update_count} completed")
                
                time.sleep(interval_seconds)
                
            except KeyboardInterrupt:
                logger.info("Live updates stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in live update: {e}")
                time.sleep(interval_seconds)
        
        logger.info(f"Live updates completed. Total updates: {update_count}")
    
    def print_fare_examples(self):
        """Print fare examples for both BMTC and BMRCL"""
        print("\\n" + "="*60)
        print("BANGALORE TRANSIT FARE EXAMPLES (2024 Real Prices)")
        print("="*60)
        
        print("\\n🚌 BMTC Bus Fares:")
        print("-" * 30)
        distances = [3, 7, 12, 20]
        bus_types = ["ordinary", "deluxe", "ac", "vajra"]
        
        for distance in distances:
            print(f"\\n{distance}km journey:")
            for bus_type in bus_types:
                fare_info = self.bmtc_fetcher.calculate_fare(distance, bus_type)
                print(f"  {bus_type.capitalize():10}: Rs {fare_info['total_fare']}")
        
        print("\\n🚇 BMRCL Metro Fares:")
        print("-" * 30)
        for distance in distances:
            token_fare = self.bmrcl_fetcher.calculate_fare(distance, "token")
            card_fare = self.bmrcl_fetcher.calculate_fare(distance, "smart_card")
            print(f"{distance}km: Token Rs {token_fare['total_fare']}, Smart Card Rs {card_fare['total_fare']}")
        
        print("\\n💳 Passes & Discounts:")
        print("-" * 30)
        print("BMTC Daily Pass: Rs 80 (increased from Rs 70)")
        print("BMTC Monthly Pass: Rs 1,200 (increased from Rs 1,050)")
        print("BMRCL Day Pass: Rs 70")
        print("Smart Card Discount: 5% on metro")
        print("Student/Senior Discount: 50% on both")
        
        print("\\n📈 Recent Changes:")
        print("-" * 30)
        print("• BMTC fares increased January 15, 2024")
        print("• AC bus minimum fare: Rs 10 → Rs 15")
        print("• Reason: Diesel price increase since 2015")
        print("• BMRCL fares remain stable with distance-based pricing")

def main():
    """Main function to run real data generation"""
    print("🚀 Bangalore Transit Real Data Generator")
    print("=" * 50)
    
    generator = RealDataGenerator()
    
    # Generate all static and live data
    results = generator.generate_all_data()
    
    if results["status"] == "success":
        print(f"✅ Data generation successful!")
        print(f"📊 Generated data for: {', '.join(results['data_generated'])}")
        
        # Print fare examples
        generator.print_fare_examples()
        
        # Ask if user wants live updates
        response = input("\\n🔄 Generate live position updates? (y/n): ").lower()
        if response == 'y':
            try:
                duration = int(input("Duration in minutes (default 2): ") or "2")
                generator.generate_live_updates(duration_minutes=duration)
            except ValueError:
                generator.generate_live_updates(duration_minutes=2)
    else:
        print(f"❌ Data generation failed: {results.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main()