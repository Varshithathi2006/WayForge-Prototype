#!/usr/bin/env python3
"""
Bangalore Transit Pipeline Demo Interface
Interactive demonstration of real-time transit data processing and route optimization
"""

import os
import sys
import time
import json
import threading
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from config.kafka_config import KAFKA_TOPICS, KAFKA_CONFIG
from utils.common import setup_logging, get_current_timestamp
from utils.error_handler import ErrorHandler, health_checker
from producers.kafka_producer import TransitDataProducer
from consumers.pathway_consumer import PathwayTransitConsumer

class BangaloreTransitDemo:
    """Interactive demo interface for the transit pipeline"""
    
    def __init__(self):
        self.logger = setup_logging("demo_interface")
        self.error_handler = ErrorHandler("demo_interface")
        self.producer = None
        self.consumer = None
        self.consumer_thread = None
        self.running = False
        
        # Demo locations in Bangalore
        self.demo_locations = {
            "1": {"name": "Koramangala", "lat": 12.9352, "lon": 77.6245},
            "2": {"name": "Whitefield", "lat": 12.9698, "lon": 77.7500},
            "3": {"name": "Electronic City", "lat": 12.8456, "lon": 77.6603},
            "4": {"name": "Indiranagar", "lat": 12.9784, "lon": 77.6408},
            "5": {"name": "Jayanagar", "lat": 12.9249, "lon": 77.5832},
            "6": {"name": "Marathahalli", "lat": 12.9591, "lon": 77.6974},
            "7": {"name": "Banashankari", "lat": 12.9081, "lon": 77.5753},
            "8": {"name": "HSR Layout", "lat": 12.9116, "lon": 77.6370}
        }
        
        self.setup_demo_environment()
    
    def setup_demo_environment(self):
        """Setup demo environment and check dependencies"""
        try:
            # Ensure logs directory exists
            Path("logs").mkdir(exist_ok=True)
            
            # Register health checks
            self._register_health_checks()
            
            self.logger.info("Demo environment setup completed")
            
        except Exception as e:
            self.error_handler.handle_error(e, {"operation": "setup_demo_environment"}, "high")
            raise
    
    def _register_health_checks(self):
        """Register health check functions"""
        @health_checker.register_component("kafka_connection")
        def check_kafka():
            try:
                from kafka import KafkaProducer
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                producer.close()
                return {"status": "healthy", "message": "Kafka connection successful"}
            except Exception as e:
                return {"status": "unhealthy", "error": str(e)}
        
        @health_checker.register_component("data_files")
        def check_data_files():
            required_files = [
                "data/static/bmtc_static.json",
                "data/static/bmrcl_static.json",
                "data/static/bmtc_fares.json",
                "data/static/bmrcl_fares.json"
            ]
            
            missing_files = []
            for file_path in required_files:
                if not Path(file_path).exists():
                    missing_files.append(file_path)
            
            if missing_files:
                return {
                    "status": "unhealthy", 
                    "message": f"Missing files: {missing_files}"
                }
            else:
                return {"status": "healthy", "message": "All data files present"}
    
    def display_banner(self):
        """Display demo banner"""
        banner = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                    🚌 BANGALORE TRANSIT PIPELINE DEMO 🚇                     ║
║                                                                              ║
║  Real-time transit data processing with Kafka and Pathway Engine            ║
║  Supports BMTC (Bus) and BMRCL (Metro) route optimization                   ║
╚══════════════════════════════════════════════════════════════════════════════╝
        """
        print(banner)
    
    def display_main_menu(self):
        """Display main menu options"""
        menu = """
🎯 MAIN MENU:
─────────────
1. 🏥 System Health Check
2. 📊 Start Data Pipeline
3. 🗺️  Route Planning Demo
4. 📈 View Performance Metrics
5. 🔍 Monitor Real-time Data
6. 🛠️  Pipeline Management
7. 📋 View Logs
8. ❌ Exit

Enter your choice (1-8): """
        return input(menu).strip()
    
    def system_health_check(self):
        """Perform and display system health check"""
        print("\n🏥 SYSTEM HEALTH CHECK")
        print("=" * 50)
        
        try:
            health_summary = health_checker.get_system_health_summary()
            
            # Display overall status
            status_emoji = {
                "healthy": "✅",
                "degraded": "⚠️",
                "critical": "❌"
            }
            
            overall_status = health_summary["overall_status"]
            print(f"Overall Status: {status_emoji.get(overall_status, '❓')} {overall_status.upper()}")
            print(f"Health Score: {health_summary['health_percentage']:.1f}%")
            print(f"Components: {health_summary['healthy_components']}/{health_summary['total_components']} healthy")
            print()
            
            # Display component details
            print("Component Details:")
            print("-" * 30)
            for component, details in health_summary["component_details"].items():
                status = details.get("status", "unknown")
                emoji = status_emoji.get(status, "❓")
                print(f"{emoji} {component}: {status}")
                
                if "message" in details:
                    print(f"   📝 {details['message']}")
                if "error" in details:
                    print(f"   ❌ {details['error']}")
            
            print()
            
        except Exception as e:
            self.error_handler.handle_error(e, {"operation": "system_health_check"}, "medium")
            print(f"❌ Error during health check: {e}")
    
    def start_data_pipeline(self):
        """Start the data pipeline"""
        print("\n📊 STARTING DATA PIPELINE")
        print("=" * 50)
        
        try:
            # Initialize producer
            print("🔄 Initializing Kafka producer...")
            self.producer = TransitDataProducer()
            
            # Publish static data
            print("📤 Publishing static data...")
            self.producer.publish_static_data()
            
            # Publish fare data
            print("💰 Publishing fare data...")
            self.producer.publish_fare_data()
            
            # Start consumer in background
            print("🔄 Starting Pathway consumer...")
            self.consumer = PathwayTransitConsumer()
            self.consumer_thread = threading.Thread(target=self.consumer.start_consuming, daemon=True)
            self.consumer_thread.start()
            
            # Simulate live data
            print("📡 Starting live data simulation...")
            self._simulate_live_data()
            
            self.running = True
            print("✅ Pipeline started successfully!")
            print("💡 Use option 5 to monitor real-time data")
            
        except Exception as e:
            self.error_handler.handle_error(e, {"operation": "start_data_pipeline"}, "high")
            print(f"❌ Error starting pipeline: {e}")
    
    def _simulate_live_data(self):
        """Simulate live vehicle/train positions"""
        def simulate():
            while self.running:
                try:
                    if self.producer:
                        # Publish live positions
                        self.producer.publish_live_positions()
                        time.sleep(30)  # Update every 30 seconds
                except Exception as e:
                    self.logger.error(f"Error in live data simulation: {e}")
                    time.sleep(60)  # Wait longer on error
        
        simulation_thread = threading.Thread(target=simulate, daemon=True)
        simulation_thread.start()
    
    def route_planning_demo(self):
        """Interactive route planning demonstration"""
        print("\n🗺️  ROUTE PLANNING DEMO")
        print("=" * 50)
        
        # Display available locations
        print("Available Locations:")
        for key, location in self.demo_locations.items():
            print(f"{key}. {location['name']}")
        print()
        
        try:
            # Get origin
            origin_choice = input("Select origin location (1-8): ").strip()
            if origin_choice not in self.demo_locations:
                print("❌ Invalid origin selection")
                return
            
            # Get destination
            dest_choice = input("Select destination location (1-8): ").strip()
            if dest_choice not in self.demo_locations:
                print("❌ Invalid destination selection")
                return
            
            if origin_choice == dest_choice:
                print("❌ Origin and destination cannot be the same")
                return
            
            origin = self.demo_locations[origin_choice]
            destination = self.demo_locations[dest_choice]
            
            print(f"\n🎯 Planning route: {origin['name']} → {destination['name']}")
            print("⏳ Computing optimal routes...")
            
            # Simulate route computation
            self._demonstrate_route_optimization(origin, destination)
            
        except Exception as e:
            self.error_handler.handle_error(e, {"operation": "route_planning_demo"}, "medium")
            print(f"❌ Error in route planning: {e}")
    
    def _demonstrate_route_optimization(self, origin: Dict, destination: Dict):
        """Demonstrate route optimization"""
        try:
            # Simulate route computation results
            routes = {
                "fastest": [
                    {
                        "route_id": "BMRCL_Purple_Line",
                        "agency": "bmrcl",
                        "total_time_minutes": 45.5,
                        "total_cost_inr": 35.0,
                        "eco_score": 8.5,
                        "transfers": 1,
                        "distance_km": 18.2,
                        "composite_score": 0.892
                    }
                ],
                "cheapest": [
                    {
                        "route_id": "BMTC_356E",
                        "agency": "bmtc",
                        "total_time_minutes": 65.0,
                        "total_cost_inr": 15.0,
                        "eco_score": 6.5,
                        "transfers": 2,
                        "distance_km": 22.1,
                        "composite_score": 0.745
                    }
                ],
                "eco_friendly": [
                    {
                        "route_id": "BMRCL_Green_Line",
                        "agency": "bmrcl",
                        "total_time_minutes": 52.0,
                        "total_cost_inr": 40.0,
                        "eco_score": 9.2,
                        "transfers": 0,
                        "distance_km": 16.8,
                        "composite_score": 0.876
                    }
                ],
                "balanced": [
                    {
                        "route_id": "BMTC_500D_BMRCL_Purple",
                        "agency": "multimodal",
                        "total_time_minutes": 48.0,
                        "total_cost_inr": 28.0,
                        "eco_score": 7.8,
                        "transfers": 1,
                        "distance_km": 19.5,
                        "composite_score": 0.834
                    }
                ]
            }
            
            # Display results
            print("\n🎯 ROUTE OPTIMIZATION RESULTS")
            print("=" * 60)
            
            for opt_type, route_list in routes.items():
                route = route_list[0]  # Show top route
                print(f"\n🏆 {opt_type.upper().replace('_', ' ')} ROUTE:")
                print(f"   🚌 Route: {route['route_id']} ({route['agency'].upper()})")
                print(f"   ⏱️  Time: {route['total_time_minutes']} minutes")
                print(f"   💰 Cost: ₹{route['total_cost_inr']}")
                print(f"   🌱 Eco Score: {route['eco_score']}/10")
                print(f"   🔄 Transfers: {route['transfers']}")
                print(f"   📏 Distance: {route['distance_km']} km")
                print(f"   ⭐ Overall Score: {route['composite_score']}")
            
            # Save results
            timestamp = get_current_timestamp()
            results_file = f"logs/demo_route_results_{timestamp}.json"
            
            demo_results = {
                "timestamp": timestamp,
                "origin": origin,
                "destination": destination,
                "routes": routes
            }
            
            with open(results_file, 'w') as f:
                json.dump(demo_results, f, indent=2)
            
            print(f"\n💾 Results saved to: {results_file}")
            
        except Exception as e:
            self.error_handler.handle_error(e, {"operation": "demonstrate_route_optimization"}, "medium")
            print(f"❌ Error demonstrating route optimization: {e}")
    
    def view_performance_metrics(self):
        """Display performance metrics"""
        print("\n📈 PERFORMANCE METRICS")
        print("=" * 50)
        
        try:
            # Get error summary
            error_summary = self.error_handler.get_error_summary()
            perf_summary = self.error_handler.get_performance_summary()
            
            print("Error Summary:")
            print(f"  Total Errors: {error_summary['total_errors']}")
            
            if error_summary['error_types']:
                print("  Error Types:")
                for error_type, count in error_summary['error_types'].items():
                    print(f"    - {error_type}: {count}")
            
            print(f"\nPerformance Summary:")
            print(f"  Total Operations: {perf_summary.get('total_operations', 0)}")
            
            if 'success_rate' in perf_summary:
                print(f"  Success Rate: {perf_summary['success_rate']:.2%}")
            
            if 'operation_stats' in perf_summary:
                print("  Operation Statistics:")
                for operation, stats in perf_summary['operation_stats'].items():
                    print(f"    - {operation}:")
                    print(f"      Count: {stats['count']}")
                    print(f"      Avg Duration: {stats['avg_duration_ms']:.2f}ms")
                    print(f"      Success Rate: {stats['success_rate']:.2%}")
            
        except Exception as e:
            self.error_handler.handle_error(e, {"operation": "view_performance_metrics"}, "medium")
            print(f"❌ Error viewing metrics: {e}")
    
    def monitor_realtime_data(self):
        """Monitor real-time data flow"""
        print("\n🔍 REAL-TIME DATA MONITOR")
        print("=" * 50)
        print("📡 Monitoring live data flow... (Press Ctrl+C to stop)")
        
        try:
            # Check if pipeline is running
            if not self.running:
                print("⚠️  Pipeline not running. Start pipeline first (option 2)")
                return
            
            # Monitor for 30 seconds
            start_time = time.time()
            while time.time() - start_time < 30:
                print(f"⏰ {datetime.now().strftime('%H:%M:%S')} - Monitoring active...")
                print("   📊 Processing vehicle positions...")
                print("   🧮 Computing route optimizations...")
                print("   💾 Storing results...")
                time.sleep(5)
            
            print("✅ Monitoring session completed")
            
        except KeyboardInterrupt:
            print("\n⏹️  Monitoring stopped by user")
        except Exception as e:
            self.error_handler.handle_error(e, {"operation": "monitor_realtime_data"}, "medium")
            print(f"❌ Error monitoring data: {e}")
    
    def pipeline_management(self):
        """Pipeline management options"""
        print("\n🛠️  PIPELINE MANAGEMENT")
        print("=" * 50)
        
        management_menu = """
Management Options:
1. 🔄 Restart Pipeline
2. ⏹️  Stop Pipeline
3. 📊 Pipeline Status
4. 🧹 Clear Logs
5. ⬅️  Back to Main Menu

Enter choice (1-5): """
        
        choice = input(management_menu).strip()
        
        if choice == "1":
            self._restart_pipeline()
        elif choice == "2":
            self._stop_pipeline()
        elif choice == "3":
            self._show_pipeline_status()
        elif choice == "4":
            self._clear_logs()
        elif choice == "5":
            return
        else:
            print("❌ Invalid choice")
    
    def _restart_pipeline(self):
        """Restart the pipeline"""
        print("🔄 Restarting pipeline...")
        self._stop_pipeline()
        time.sleep(2)
        self.start_data_pipeline()
    
    def _stop_pipeline(self):
        """Stop the pipeline"""
        print("⏹️  Stopping pipeline...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
        
        if self.producer:
            self.producer.close()
        
        print("✅ Pipeline stopped")
    
    def _show_pipeline_status(self):
        """Show pipeline status"""
        print("📊 Pipeline Status:")
        print(f"   Running: {'✅ Yes' if self.running else '❌ No'}")
        print(f"   Producer: {'✅ Active' if self.producer else '❌ Inactive'}")
        print(f"   Consumer: {'✅ Active' if self.consumer else '❌ Inactive'}")
    
    def _clear_logs(self):
        """Clear log files"""
        try:
            log_files = list(Path("logs").glob("*.log"))
            for log_file in log_files:
                log_file.unlink()
            print(f"🧹 Cleared {len(log_files)} log files")
        except Exception as e:
            print(f"❌ Error clearing logs: {e}")
    
    def view_logs(self):
        """View recent log entries"""
        print("\n📋 RECENT LOGS")
        print("=" * 50)
        
        try:
            log_files = list(Path("logs").glob("*.log"))
            
            if not log_files:
                print("📝 No log files found")
                return
            
            # Show most recent log file
            latest_log = max(log_files, key=lambda f: f.stat().st_mtime)
            
            print(f"📄 Showing latest log: {latest_log.name}")
            print("-" * 50)
            
            with open(latest_log, 'r') as f:
                lines = f.readlines()
                # Show last 20 lines
                for line in lines[-20:]:
                    print(line.rstrip())
            
        except Exception as e:
            self.error_handler.handle_error(e, {"operation": "view_logs"}, "medium")
            print(f"❌ Error viewing logs: {e}")
    
    def run(self):
        """Run the demo interface"""
        try:
            self.display_banner()
            
            while True:
                choice = self.display_main_menu()
                
                if choice == "1":
                    self.system_health_check()
                elif choice == "2":
                    self.start_data_pipeline()
                elif choice == "3":
                    self.route_planning_demo()
                elif choice == "4":
                    self.view_performance_metrics()
                elif choice == "5":
                    self.monitor_realtime_data()
                elif choice == "6":
                    self.pipeline_management()
                elif choice == "7":
                    self.view_logs()
                elif choice == "8":
                    print("\n👋 Thank you for using Bangalore Transit Pipeline Demo!")
                    self._stop_pipeline()
                    break
                else:
                    print("❌ Invalid choice. Please select 1-8.")
                
                input("\nPress Enter to continue...")
        
        except KeyboardInterrupt:
            print("\n\n👋 Demo interrupted by user")
            self._stop_pipeline()
        except Exception as e:
            self.error_handler.handle_error(e, {"operation": "run_demo"}, "critical")
            print(f"❌ Critical error in demo: {e}")
        finally:
            self._stop_pipeline()

def main():
    """Main function"""
    demo = BangaloreTransitDemo()
    demo.run()

if __name__ == "__main__":
    main()