"""
Route Optimization Engine for Bangalore Transit Data Pipeline
Processes transit data and computes optimal routes
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.kafka_config import ROUTE_WEIGHTS
from utils.common import setup_logging, calculate_distance

@dataclass
class RouteOption:
    """Data class for route options"""
    route_id: str
    agency: str
    total_time: float  # minutes
    total_cost: float  # INR
    eco_score: float   # 0-10 scale
    transfers: int
    distance: float    # km
    route_details: List[Dict[str, Any]]

@dataclass
class TransitStop:
    """Data class for transit stops"""
    stop_id: str
    name: str
    lat: float
    lon: float
    agency: str

class RouteOptimizer:
    """Route optimization engine"""
    
    def __init__(self):
        self.logger = setup_logging(__name__)
        self.static_data = {'bmtc': {}, 'bmrcl': {}}
        self.fare_data = {'bmtc': {}, 'bmrcl': {}}
        self.live_positions = {'bmtc': {}, 'bmrcl': {}}
        self.stops_index = {}  # For quick stop lookups
        
    def update_static_data(self, agency: str, data: Dict[str, Any]):
        """Update static GTFS data for an agency"""
        try:
            self.static_data[agency] = data
            self._build_stops_index(agency, data)
            self.logger.info(f"Updated static data for {agency}")
        except Exception as e:
            self.logger.error(f"Error updating static data for {agency}: {e}")
    
    def update_fare_data(self, agency: str, data: Dict[str, Any]):
        """Update fare data for an agency"""
        try:
            self.fare_data[agency] = data
            self.logger.info(f"Updated fare data for {agency}")
        except Exception as e:
            self.logger.error(f"Error updating fare data for {agency}: {e}")
    
    def update_live_positions(self, agency: str, entity: Dict[str, Any]):
        """Update live vehicle/train positions"""
        try:
            entity_id = entity.get('id', 'unknown')
            self.live_positions[agency][entity_id] = entity
            self.logger.debug(f"Updated position for {agency} entity {entity_id}")
        except Exception as e:
            self.logger.error(f"Error updating live positions for {agency}: {e}")
    
    def _build_stops_index(self, agency: str, data: Dict[str, Any]):
        """Build an index of stops for quick lookups"""
        try:
            stops = data.get('stops', [])
            for stop in stops:
                stop_obj = TransitStop(
                    stop_id=stop['stop_id'],
                    name=stop['stop_name'],
                    lat=stop['stop_lat'],
                    lon=stop['stop_lon'],
                    agency=agency
                )
                self.stops_index[stop['stop_id']] = stop_obj
        except Exception as e:
            self.logger.error(f"Error building stops index for {agency}: {e}")
    
    def find_nearest_stops(self, lat: float, lon: float, max_distance: float = 1.0) -> List[TransitStop]:
        """
        Find nearest stops within max_distance km
        
        Args:
            lat, lon: Coordinates to search from
            max_distance: Maximum distance in km
            
        Returns:
            List of nearby stops sorted by distance
        """
        nearby_stops = []
        
        for stop in self.stops_index.values():
            distance = calculate_distance(lat, lon, stop.lat, stop.lon)
            if distance <= max_distance:
                nearby_stops.append((stop, distance))
        
        # Sort by distance
        nearby_stops.sort(key=lambda x: x[1])
        return [stop for stop, _ in nearby_stops]
    
    def calculate_route_cost(self, agency: str, route_id: str, origin_zone: str, dest_zone: str) -> float:
        """Calculate cost for a route"""
        try:
            fare_data = self.fare_data.get(agency, {})
            
            if agency == 'bmtc':
                # Use zone-based fares for BMTC
                zone_fares = fare_data.get('zone_fares', {})
                if origin_zone == dest_zone:
                    return zone_fares.get('intra_zone', {}).get('regular', 8.0)
                else:
                    return zone_fares.get('inter_zone', {}).get('regular', 15.0)
            
            elif agency == 'bmrcl':
                # Use distance-based fares for BMRCL
                distance_fares = fare_data.get('distance_based_fares', {})
                # For simplicity, assume medium distance
                return distance_fares.get('5-10km', 20.0)
            
            return 10.0  # Default fare
            
        except Exception as e:
            self.logger.error(f"Error calculating cost for {agency} route {route_id}: {e}")
            return 10.0
    
    def calculate_route_time(self, agency: str, route_id: str, origin_stop: str, dest_stop: str) -> float:
        """Calculate estimated travel time for a route"""
        try:
            # Get static data for the agency
            static_data = self.static_data.get(agency, {})
            stop_times = static_data.get('stop_times', [])
            
            # Find stop times for this route
            route_stop_times = [st for st in stop_times if st.get('trip_id', '').startswith(route_id)]
            
            if not route_stop_times:
                # Default time based on agency type
                return 45.0 if agency == 'bmtc' else 25.0
            
            # Calculate time difference between stops
            origin_time = None
            dest_time = None
            
            for stop_time in route_stop_times:
                if stop_time['stop_id'] == origin_stop:
                    origin_time = stop_time['departure_time']
                elif stop_time['stop_id'] == dest_stop:
                    dest_time = stop_time['arrival_time']
            
            if origin_time and dest_time:
                # Parse time strings (HH:MM:SS)
                origin_minutes = self._time_to_minutes(origin_time)
                dest_minutes = self._time_to_minutes(dest_time)
                return max(dest_minutes - origin_minutes, 5.0)  # Minimum 5 minutes
            
            # Default time based on distance
            origin_stop_obj = self.stops_index.get(origin_stop)
            dest_stop_obj = self.stops_index.get(dest_stop)
            
            if origin_stop_obj and dest_stop_obj:
                distance = calculate_distance(
                    origin_stop_obj.lat, origin_stop_obj.lon,
                    dest_stop_obj.lat, dest_stop_obj.lon
                )
                # Estimate time based on average speed
                avg_speed = 25 if agency == 'bmrcl' else 15  # km/h
                return (distance / avg_speed) * 60  # Convert to minutes
            
            return 30.0  # Default time
            
        except Exception as e:
            self.logger.error(f"Error calculating time for {agency} route {route_id}: {e}")
            return 30.0
    
    def _time_to_minutes(self, time_str: str) -> float:
        """Convert HH:MM:SS to minutes"""
        try:
            parts = time_str.split(':')
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = int(parts[2]) if len(parts) > 2 else 0
            return hours * 60 + minutes + seconds / 60
        except:
            return 0.0
    
    def get_eco_score(self, agency: str, route_type: str = 'regular') -> float:
        """Get eco-friendliness score for a route"""
        try:
            fare_data = self.fare_data.get(agency, {})
            eco_scores = fare_data.get('eco_scores', {})
            
            if agency == 'bmrcl':
                return eco_scores.get('metro', 9.5)
            elif agency == 'bmtc':
                return eco_scores.get(route_type, 7.5)
            
            return 7.0  # Default score
            
        except Exception as e:
            self.logger.error(f"Error getting eco score for {agency}: {e}")
            return 7.0
    
    def find_routes(self, origin_lat: float, origin_lon: float, 
                   dest_lat: float, dest_lon: float) -> List[RouteOption]:
        """
        Find all possible routes between origin and destination
        
        Args:
            origin_lat, origin_lon: Origin coordinates
            dest_lat, dest_lon: Destination coordinates
            
        Returns:
            List of route options
        """
        routes = []
        
        try:
            # Find nearby stops for origin and destination
            origin_stops = self.find_nearest_stops(origin_lat, origin_lon, 1.0)
            dest_stops = self.find_nearest_stops(dest_lat, dest_lon, 1.0)
            
            if not origin_stops or not dest_stops:
                self.logger.warning("No nearby stops found")
                return routes
            
            # Check direct routes for each agency
            for agency in ['bmtc', 'bmrcl']:
                static_data = self.static_data.get(agency, {})
                agency_routes = static_data.get('routes', [])
                
                for route in agency_routes:
                    route_id = route['route_id']
                    
                    # Find if this route serves both origin and destination areas
                    for origin_stop in origin_stops[:3]:  # Check top 3 nearest
                        if origin_stop.agency != agency:
                            continue
                            
                        for dest_stop in dest_stops[:3]:
                            if dest_stop.agency != agency:
                                continue
                            
                            # Calculate route metrics
                            travel_time = self.calculate_route_time(
                                agency, route_id, origin_stop.stop_id, dest_stop.stop_id
                            )
                            
                            cost = self.calculate_route_cost(
                                agency, route_id, 'Central', 'South'  # Simplified zones
                            )
                            
                            eco_score = self.get_eco_score(agency)
                            
                            # Calculate total distance
                            walking_distance = (
                                calculate_distance(origin_lat, origin_lon, origin_stop.lat, origin_stop.lon) +
                                calculate_distance(dest_lat, dest_lon, dest_stop.lat, dest_stop.lon)
                            )
                            
                            transit_distance = calculate_distance(
                                origin_stop.lat, origin_stop.lon, dest_stop.lat, dest_stop.lon
                            )
                            
                            total_distance = walking_distance + transit_distance
                            
                            # Create route option
                            route_option = RouteOption(
                                route_id=route_id,
                                agency=agency,
                                total_time=travel_time + walking_distance * 12,  # 12 min/km walking
                                total_cost=cost,
                                eco_score=eco_score,
                                transfers=0,
                                distance=total_distance,
                                route_details=[
                                    {
                                        'type': 'walk',
                                        'from': 'origin',
                                        'to': origin_stop.name,
                                        'distance': calculate_distance(origin_lat, origin_lon, origin_stop.lat, origin_stop.lon),
                                        'time': calculate_distance(origin_lat, origin_lon, origin_stop.lat, origin_stop.lon) * 12
                                    },
                                    {
                                        'type': 'transit',
                                        'agency': agency,
                                        'route': route['route_short_name'],
                                        'from': origin_stop.name,
                                        'to': dest_stop.name,
                                        'time': travel_time,
                                        'cost': cost
                                    },
                                    {
                                        'type': 'walk',
                                        'from': dest_stop.name,
                                        'to': 'destination',
                                        'distance': calculate_distance(dest_lat, dest_lon, dest_stop.lat, dest_stop.lon),
                                        'time': calculate_distance(dest_lat, dest_lon, dest_stop.lat, dest_stop.lon) * 12
                                    }
                                ]
                            )
                            
                            routes.append(route_option)
            
            # Add some multi-modal routes (BMTC + BMRCL combinations)
            routes.extend(self._find_multimodal_routes(origin_lat, origin_lon, dest_lat, dest_lon))
            
            self.logger.info(f"Found {len(routes)} route options")
            return routes
            
        except Exception as e:
            self.logger.error(f"Error finding routes: {e}")
            return routes
    
    def _find_multimodal_routes(self, origin_lat: float, origin_lon: float, 
                               dest_lat: float, dest_lon: float) -> List[RouteOption]:
        """Find routes that combine BMTC and BMRCL"""
        multimodal_routes = []
        
        try:
            # Find interchange stations (simplified - using Majestic as main interchange)
            interchange_stops = [stop for stop in self.stops_index.values() 
                               if 'majestic' in stop.name.lower()]
            
            if not interchange_stops:
                return multimodal_routes
            
            interchange = interchange_stops[0]
            
            # Create a sample multimodal route: BMTC to Metro
            route_option = RouteOption(
                route_id="MULTIMODAL_001",
                agency="bmtc+bmrcl",
                total_time=65.0,  # Estimated total time
                total_cost=35.0,  # Combined cost
                eco_score=8.0,    # Average eco score
                transfers=1,
                distance=calculate_distance(origin_lat, origin_lon, dest_lat, dest_lon),
                route_details=[
                    {
                        'type': 'walk',
                        'from': 'origin',
                        'to': 'BMTC Stop',
                        'distance': 0.5,
                        'time': 6
                    },
                    {
                        'type': 'transit',
                        'agency': 'bmtc',
                        'route': 'Bus to Majestic',
                        'from': 'BMTC Stop',
                        'to': 'Majestic',
                        'time': 25,
                        'cost': 15
                    },
                    {
                        'type': 'transfer',
                        'from': 'Majestic Bus',
                        'to': 'Majestic Metro',
                        'time': 5
                    },
                    {
                        'type': 'transit',
                        'agency': 'bmrcl',
                        'route': 'Purple Line',
                        'from': 'Majestic',
                        'to': 'Destination Metro',
                        'time': 20,
                        'cost': 20
                    },
                    {
                        'type': 'walk',
                        'from': 'Destination Metro',
                        'to': 'destination',
                        'distance': 0.3,
                        'time': 4
                    }
                ]
            )
            
            multimodal_routes.append(route_option)
            
        except Exception as e:
            self.logger.error(f"Error finding multimodal routes: {e}")
        
        return multimodal_routes
    
    def optimize_routes(self, routes: List[RouteOption], optimization_type: str = 'balanced') -> List[RouteOption]:
        """
        Optimize and rank routes based on criteria
        
        Args:
            routes: List of route options
            optimization_type: Type of optimization (fastest, cheapest, eco_friendly, balanced)
            
        Returns:
            Sorted list of routes
        """
        if not routes:
            return routes
        
        try:
            weights = ROUTE_WEIGHTS.get(optimization_type, ROUTE_WEIGHTS['balanced'])
            
            # Calculate composite scores
            for route in routes:
                # Normalize values (simple min-max normalization)
                max_time = max(r.total_time for r in routes)
                max_cost = max(r.total_cost for r in routes)
                min_eco = min(r.eco_score for r in routes)
                max_eco = max(r.eco_score for r in routes)
                
                # Normalize (lower is better for time and cost, higher is better for eco)
                norm_time = 1 - (route.total_time / max_time) if max_time > 0 else 1
                norm_cost = 1 - (route.total_cost / max_cost) if max_cost > 0 else 1
                norm_eco = (route.eco_score - min_eco) / (max_eco - min_eco) if max_eco > min_eco else 1
                
                # Calculate weighted score
                route.composite_score = (
                    weights['time'] * norm_time +
                    weights['cost'] * norm_cost +
                    weights['eco'] * norm_eco
                )
            
            # Sort by composite score (higher is better)
            routes.sort(key=lambda r: r.composite_score, reverse=True)
            
            self.logger.info(f"Optimized {len(routes)} routes for {optimization_type}")
            return routes
            
        except Exception as e:
            self.logger.error(f"Error optimizing routes: {e}")
            return routes