"""
Real-time Traffic Data Fetcher
Integrates with Google Maps API and other traffic data sources for live traffic information
"""

import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging
from dataclasses import dataclass
import os

from utils.common import setup_logging, save_json_file, get_current_timestamp
from utils.error_handler import error_handler_decorator, performance_monitor

logger = setup_logging(__name__)

@dataclass
class TrafficCondition:
    """Traffic condition data structure"""
    location: str
    latitude: float
    longitude: float
    traffic_level: str  # "light", "moderate", "heavy", "severe"
    speed_kmh: float
    delay_minutes: int
    timestamp: int
    source: str

class RealTimeTrafficFetcher:
    """Fetches real-time traffic data from multiple sources"""
    
    def __init__(self):
        self.google_api_key = os.getenv('GOOGLE_MAPS_API_KEY', '')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WayForge-Traffic-Fetcher/1.0',
            'Accept': 'application/json'
        })
        
        # Key traffic monitoring points in Bangalore
        self.traffic_points = [
            {"name": "Silk Board Junction", "lat": 12.9176, "lng": 77.6227},
            {"name": "Electronic City Flyover", "lat": 12.8446, "lng": 77.6606},
            {"name": "Hosur Road", "lat": 12.9279, "lng": 77.6271},
            {"name": "Outer Ring Road - Marathahalli", "lat": 12.9591, "lng": 77.6974},
            {"name": "Whitefield Main Road", "lat": 12.9698, "lng": 77.7499},
            {"name": "Bannerghatta Road", "lat": 12.9279, "lng": 77.5619},
            {"name": "Mysuru Road", "lat": 12.9540, "lng": 77.5707},
            {"name": "Tumkur Road", "lat": 13.0359, "lng": 77.5542},
            {"name": "Airport Road", "lat": 13.1986, "lng": 77.7066},
            {"name": "MG Road", "lat": 12.9759, "lng": 77.6046},
            {"name": "Brigade Road", "lat": 12.9716, "lng": 77.6100},
            {"name": "Richmond Road", "lat": 12.9591, "lng": 77.6100},
            {"name": "Koramangala", "lat": 12.9279, "lng": 77.6271},
            {"name": "Indiranagar", "lat": 12.9784, "lng": 77.6408},
            {"name": "Jayanagar", "lat": 12.9279, "lng": 77.5854}
        ]
    
    @error_handler_decorator("traffic_fetcher")
    @performance_monitor("traffic_fetcher")
    def fetch_real_time_traffic(self) -> Dict[str, Any]:
        """Fetch real-time traffic data from multiple sources"""
        try:
            traffic_data = {
                "timestamp": int(time.time()),
                "source": "multi_source",
                "traffic_conditions": []
            }
            
            # Try Google Maps API first
            google_data = self._fetch_google_traffic_data()
            if google_data:
                traffic_data["traffic_conditions"].extend(google_data)
                logger.info(f"Fetched {len(google_data)} traffic conditions from Google Maps API")
            
            # Fallback to enhanced simulation if no real data
            if not traffic_data["traffic_conditions"]:
                logger.info("Real traffic APIs unavailable, using enhanced traffic simulation")
                traffic_data["traffic_conditions"] = self._generate_enhanced_traffic_data()
            
            logger.info(f"Total traffic conditions: {len(traffic_data['traffic_conditions'])}")
            return traffic_data
            
        except Exception as e:
            logger.error(f"Error fetching traffic data: {e}")
            return {
                "timestamp": int(time.time()),
                "traffic_conditions": [],
                "error": str(e)
            }
    
    def _fetch_google_traffic_data(self) -> List[Dict[str, Any]]:
        """Fetch traffic data from Google Maps API"""
        try:
            if not self.google_api_key:
                logger.debug("Google Maps API key not configured")
                return []
            
            traffic_conditions = []
            
            # Use Google Maps Distance Matrix API to get travel times with traffic
            for point in self.traffic_points[:5]:  # Limit API calls
                try:
                    # Create a small route around the traffic point
                    origin = f"{point['lat']},{point['lng']}"
                    destination = f"{point['lat'] + 0.01},{point['lng'] + 0.01}"
                    
                    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
                    params = {
                        'origins': origin,
                        'destinations': destination,
                        'departure_time': 'now',
                        'traffic_model': 'best_guess',
                        'key': self.google_api_key
                    }
                    
                    response = self.session.get(url, params=params, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        
                        if data['status'] == 'OK' and data['rows']:
                            element = data['rows'][0]['elements'][0]
                            if element['status'] == 'OK':
                                duration = element.get('duration', {}).get('value', 0)
                                duration_in_traffic = element.get('duration_in_traffic', {}).get('value', duration)
                                
                                # Calculate traffic level based on delay
                                delay_ratio = duration_in_traffic / max(duration, 1)
                                traffic_level = self._classify_traffic_level(delay_ratio)
                                
                                condition = {
                                    "location": point['name'],
                                    "latitude": point['lat'],
                                    "longitude": point['lng'],
                                    "traffic_level": traffic_level,
                                    "speed_kmh": self._estimate_speed_from_delay(delay_ratio),
                                    "delay_minutes": max(0, (duration_in_traffic - duration) // 60),
                                    "timestamp": int(time.time()),
                                    "source": "google_maps"
                                }
                                traffic_conditions.append(condition)
                
                except Exception as e:
                    logger.debug(f"Error fetching traffic for {point['name']}: {e}")
                    continue
            
            return traffic_conditions
            
        except Exception as e:
            logger.debug(f"Google Maps API error: {e}")
            return []
    
    def _generate_enhanced_traffic_data(self) -> List[Dict[str, Any]]:
        """Generate enhanced realistic traffic data based on Bangalore traffic patterns"""
        import random
        from datetime import datetime
        
        traffic_conditions = []
        current_time = datetime.now()
        current_hour = current_time.hour
        current_day = current_time.weekday()  # 0=Monday, 6=Sunday
        
        # Traffic patterns based on time and day
        is_weekend = current_day >= 5
        is_peak_morning = 7 <= current_hour <= 10
        is_peak_evening = 17 <= current_hour <= 20
        is_office_hours = 9 <= current_hour <= 18
        
        for point in self.traffic_points:
            # Base traffic level calculation
            if is_weekend:
                base_traffic_factor = 0.6  # Less traffic on weekends
            elif is_peak_morning or is_peak_evening:
                base_traffic_factor = 1.5  # Heavy traffic during peaks
            elif is_office_hours:
                base_traffic_factor = 1.0  # Moderate traffic during office hours
            else:
                base_traffic_factor = 0.4  # Light traffic at other times
            
            # Location-specific adjustments
            location_factors = {
                "Silk Board Junction": 1.8,  # Always congested
                "Electronic City Flyover": 1.6,  # IT corridor
                "Whitefield Main Road": 1.7,  # IT corridor
                "Outer Ring Road - Marathahalli": 1.5,  # IT corridor
                "Airport Road": 1.2,  # Moderate traffic
                "MG Road": 1.4,  # Commercial area
                "Brigade Road": 1.3,  # Commercial area
                "Bannerghatta Road": 1.3,  # Residential + commercial
                "Mysuru Road": 1.1,  # Industrial area
                "Tumkur Road": 1.0,  # Moderate traffic
                "Richmond Road": 1.2,  # Residential area
                "Koramangala": 1.4,  # Commercial + residential
                "Indiranagar": 1.3,  # Commercial + residential
                "Jayanagar": 1.1   # Residential area
            }
            
            location_factor = location_factors.get(point['name'], 1.0)
            
            # Calculate final traffic intensity
            traffic_intensity = base_traffic_factor * location_factor
            
            # Add random variation
            traffic_intensity *= random.uniform(0.8, 1.2)
            
            # Classify traffic level
            if traffic_intensity < 0.5:
                traffic_level = "light"
                speed_kmh = random.uniform(40, 60)
                delay_minutes = random.randint(0, 2)
            elif traffic_intensity < 1.0:
                traffic_level = "moderate"
                speed_kmh = random.uniform(25, 40)
                delay_minutes = random.randint(2, 8)
            elif traffic_intensity < 1.5:
                traffic_level = "heavy"
                speed_kmh = random.uniform(15, 25)
                delay_minutes = random.randint(8, 20)
            else:
                traffic_level = "severe"
                speed_kmh = random.uniform(5, 15)
                delay_minutes = random.randint(20, 45)
            
            condition = {
                "location": point['name'],
                "latitude": point['lat'],
                "longitude": point['lng'],
                "traffic_level": traffic_level,
                "speed_kmh": round(speed_kmh, 1),
                "delay_minutes": delay_minutes,
                "timestamp": int(time.time()),
                "source": "enhanced_simulation",
                "traffic_intensity": round(traffic_intensity, 2)
            }
            traffic_conditions.append(condition)
        
        return traffic_conditions
    
    def _classify_traffic_level(self, delay_ratio: float) -> str:
        """Classify traffic level based on delay ratio"""
        if delay_ratio < 1.1:
            return "light"
        elif delay_ratio < 1.3:
            return "moderate"
        elif delay_ratio < 1.6:
            return "heavy"
        else:
            return "severe"
    
    def _estimate_speed_from_delay(self, delay_ratio: float) -> float:
        """Estimate average speed based on delay ratio"""
        base_speed = 50  # km/h
        if delay_ratio < 1.1:
            return base_speed * random.uniform(0.8, 1.0)
        elif delay_ratio < 1.3:
            return base_speed * random.uniform(0.6, 0.8)
        elif delay_ratio < 1.6:
            return base_speed * random.uniform(0.3, 0.6)
        else:
            return base_speed * random.uniform(0.1, 0.3)
    
    @error_handler_decorator("traffic_fetcher")
    def get_traffic_for_route(self, origin: Tuple[float, float], 
                            destination: Tuple[float, float]) -> Dict[str, Any]:
        """Get traffic information for a specific route"""
        try:
            if self.google_api_key:
                return self._get_google_route_traffic(origin, destination)
            else:
                return self._estimate_route_traffic(origin, destination)
                
        except Exception as e:
            logger.error(f"Error getting route traffic: {e}")
            return {
                "duration_normal": 0,
                "duration_traffic": 0,
                "traffic_level": "unknown",
                "error": str(e)
            }
    
    def _get_google_route_traffic(self, origin: Tuple[float, float], 
                                destination: Tuple[float, float]) -> Dict[str, Any]:
        """Get route traffic from Google Maps API"""
        try:
            origin_str = f"{origin[0]},{origin[1]}"
            destination_str = f"{destination[0]},{destination[1]}"
            
            url = "https://maps.googleapis.com/maps/api/distancematrix/json"
            params = {
                'origins': origin_str,
                'destinations': destination_str,
                'departure_time': 'now',
                'traffic_model': 'best_guess',
                'key': self.google_api_key
            }
            
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                if data['status'] == 'OK' and data['rows']:
                    element = data['rows'][0]['elements'][0]
                    if element['status'] == 'OK':
                        duration = element.get('duration', {}).get('value', 0)
                        duration_in_traffic = element.get('duration_in_traffic', {}).get('value', duration)
                        
                        delay_ratio = duration_in_traffic / max(duration, 1)
                        
                        return {
                            "duration_normal": duration,
                            "duration_traffic": duration_in_traffic,
                            "traffic_level": self._classify_traffic_level(delay_ratio),
                            "delay_minutes": (duration_in_traffic - duration) // 60,
                            "source": "google_maps"
                        }
            
            return {"error": "Google Maps API request failed"}
            
        except Exception as e:
            logger.error(f"Google route traffic error: {e}")
            return {"error": str(e)}
    
    def _estimate_route_traffic(self, origin: Tuple[float, float], 
                              destination: Tuple[float, float]) -> Dict[str, Any]:
        """Estimate route traffic based on nearby traffic conditions"""
        try:
            # Get current traffic data
            traffic_data = self.fetch_real_time_traffic()
            conditions = traffic_data.get('traffic_conditions', [])
            
            if not conditions:
                return {
                    "duration_normal": 1800,  # 30 minutes default
                    "duration_traffic": 2400,  # 40 minutes with traffic
                    "traffic_level": "moderate",
                    "source": "estimated"
                }
            
            # Find nearby traffic conditions
            import math
            
            def distance(lat1, lng1, lat2, lng2):
                return math.sqrt((lat1 - lat2)**2 + (lng1 - lng2)**2)
            
            # Weight traffic conditions by proximity
            weighted_delays = []
            for condition in conditions:
                dist_to_origin = distance(origin[0], origin[1], 
                                        condition['latitude'], condition['longitude'])
                dist_to_dest = distance(destination[0], destination[1], 
                                      condition['latitude'], condition['longitude'])
                
                # Consider conditions within 0.05 degrees (~5km)
                if min(dist_to_origin, dist_to_dest) < 0.05:
                    weight = 1 / (min(dist_to_origin, dist_to_dest) + 0.01)
                    weighted_delays.append(condition['delay_minutes'] * weight)
            
            if weighted_delays:
                avg_delay = sum(weighted_delays) / len(weighted_delays)
            else:
                avg_delay = 10  # Default 10 minutes delay
            
            # Estimate base travel time (rough calculation)
            route_distance = distance(origin[0], origin[1], destination[0], destination[1])
            base_time_minutes = route_distance * 111 * 2  # Rough conversion to minutes
            
            duration_normal = int(base_time_minutes * 60)  # Convert to seconds
            duration_traffic = int((base_time_minutes + avg_delay) * 60)
            
            delay_ratio = duration_traffic / max(duration_normal, 1)
            
            return {
                "duration_normal": duration_normal,
                "duration_traffic": duration_traffic,
                "traffic_level": self._classify_traffic_level(delay_ratio),
                "delay_minutes": int(avg_delay),
                "source": "estimated"
            }
            
        except Exception as e:
            logger.error(f"Route traffic estimation error: {e}")
            return {"error": str(e)}

if __name__ == "__main__":
    # Test the traffic fetcher
    fetcher = RealTimeTrafficFetcher()
    
    print("Fetching real-time traffic data...")
    traffic_data = fetcher.fetch_real_time_traffic()
    
    print(f"Found {len(traffic_data.get('traffic_conditions', []))} traffic conditions")
    
    # Show sample conditions
    for condition in traffic_data.get('traffic_conditions', [])[:5]:
        print(f"{condition['location']}: {condition['traffic_level']} traffic, "
              f"{condition['speed_kmh']} km/h, {condition['delay_minutes']} min delay")
    
    # Test route traffic
    print("\nTesting route traffic...")
    origin = (12.9716, 77.5946)  # MG Road
    destination = (12.9698, 77.7499)  # Whitefield
    
    route_traffic = fetcher.get_traffic_for_route(origin, destination)
    print(f"Route traffic: {route_traffic}")