#!/usr/bin/env python3
"""
Road-based Routing Service for Bangalore Transit
Provides real road distance calculation and route optimization
"""

import requests
import json
import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
import time
from functools import lru_cache

from .error_handler import error_handler_decorator, performance_monitor
from .common import setup_logging

logger = setup_logging("routing_service")

@dataclass
class RoutePoint:
    """Represents a point in a route"""
    latitude: float
    longitude: float
    name: Optional[str] = None

@dataclass
class RouteSegment:
    """Represents a segment of a route"""
    distance_km: float
    duration_minutes: float
    instructions: str
    geometry: List[Tuple[float, float]]

@dataclass
class Route:
    """Complete route information"""
    source: RoutePoint
    destination: RoutePoint
    total_distance_km: float
    total_duration_minutes: float
    segments: List[RouteSegment]
    geometry: List[Tuple[float, float]]
    transport_mode: str

class RoutingService:
    """Service for calculating real road-based routes and distances"""
    
    def __init__(self):
        self.logger = setup_logging("routing_service")
        
        # OpenRouteService API (free tier - 2000 requests/day)
        self.ors_api_key = "5b3ce3597851110001cf6248a1b2c8c8a4e04b7bb5c8b8e8f8e8e8e8"  # Demo key
        self.ors_base_url = "https://api.openrouteservice.org"
        
        # Fallback to local OSRM if available
        self.osrm_base_url = "http://router.project-osrm.org"
        
        # Cache for route calculations
        self.route_cache = {}
        
    @error_handler_decorator("routing_service")
    @performance_monitor("routing_service")
    def calculate_route(self, 
                       source: RoutePoint, 
                       destination: RoutePoint, 
                       transport_mode: str = "driving-car") -> Optional[Route]:
        """
        Calculate route between two points using real roads
        
        Args:
            source: Starting point
            destination: End point
            transport_mode: driving-car, cycling-regular, foot-walking
            
        Returns:
            Route object with distance, duration, and geometry
        """
        try:
            # Create cache key
            cache_key = f"{source.latitude},{source.longitude}_{destination.latitude},{destination.longitude}_{transport_mode}"
            
            if cache_key in self.route_cache:
                self.logger.info(f"Using cached route for {cache_key}")
                return self.route_cache[cache_key]
            
            # Try OpenRouteService first
            route = self._calculate_route_ors(source, destination, transport_mode)
            
            if not route:
                # Fallback to OSRM
                route = self._calculate_route_osrm(source, destination, transport_mode)
            
            if route:
                self.route_cache[cache_key] = route
                self.logger.info(f"Calculated route: {route.total_distance_km:.2f}km, {route.total_duration_minutes:.1f}min")
            
            return route
            
        except Exception as e:
            self.logger.error(f"Error calculating route: {str(e)}")
            return None
    
    def _calculate_route_ors(self, source: RoutePoint, destination: RoutePoint, transport_mode: str) -> Optional[Route]:
        """Calculate route using OpenRouteService"""
        try:
            url = f"{self.ors_base_url}/v2/directions/{transport_mode}"
            
            headers = {
                'Authorization': self.ors_api_key,
                'Content-Type': 'application/json'
            }
            
            data = {
                'coordinates': [
                    [source.longitude, source.latitude],
                    [destination.longitude, destination.latitude]
                ],
                'format': 'json',
                'instructions': True,
                'geometry': True
            }
            
            response = requests.post(url, json=data, headers=headers, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                return self._parse_ors_response(result, source, destination, transport_mode)
            else:
                self.logger.warning(f"ORS API error: {response.status_code}")
                return None
                
        except Exception as e:
            self.logger.error(f"ORS routing error: {str(e)}")
            return None
    
    def _calculate_route_osrm(self, source: RoutePoint, destination: RoutePoint, transport_mode: str) -> Optional[Route]:
        """Calculate route using OSRM (fallback)"""
        try:
            # OSRM only supports driving, so map other modes
            osrm_profile = "driving" if transport_mode.startswith("driving") else "driving"
            
            url = f"{self.osrm_base_url}/route/v1/{osrm_profile}/{source.longitude},{source.latitude};{destination.longitude},{destination.latitude}"
            
            params = {
                'overview': 'full',
                'geometries': 'geojson',
                'steps': 'true'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                return self._parse_osrm_response(result, source, destination, transport_mode)
            else:
                self.logger.warning(f"OSRM API error: {response.status_code}")
                return None
                
        except Exception as e:
            self.logger.error(f"OSRM routing error: {str(e)}")
            return None
    
    def _parse_ors_response(self, response: Dict, source: RoutePoint, destination: RoutePoint, transport_mode: str) -> Route:
        """Parse OpenRouteService response"""
        route_data = response['routes'][0]
        summary = route_data['summary']
        
        # Extract geometry
        geometry = []
        if 'geometry' in route_data:
            coords = route_data['geometry']['coordinates']
            geometry = [(lat, lon) for lon, lat in coords]
        
        # Extract segments
        segments = []
        if 'segments' in route_data:
            for segment in route_data['segments']:
                for step in segment.get('steps', []):
                    segments.append(RouteSegment(
                        distance_km=step['distance'] / 1000,
                        duration_minutes=step['duration'] / 60,
                        instructions=step['instruction'],
                        geometry=[(lat, lon) for lon, lat in step['geometry']['coordinates']]
                    ))
        
        return Route(
            source=source,
            destination=destination,
            total_distance_km=summary['distance'] / 1000,
            total_duration_minutes=summary['duration'] / 60,
            segments=segments,
            geometry=geometry,
            transport_mode=transport_mode
        )
    
    def _parse_osrm_response(self, response: Dict, source: RoutePoint, destination: RoutePoint, transport_mode: str) -> Route:
        """Parse OSRM response"""
        route_data = response['routes'][0]
        
        # Extract geometry
        geometry = []
        if 'geometry' in route_data:
            coords = route_data['geometry']['coordinates']
            geometry = [(lat, lon) for lon, lat in coords]
        
        # Extract segments from legs
        segments = []
        for leg in route_data.get('legs', []):
            for step in leg.get('steps', []):
                segments.append(RouteSegment(
                    distance_km=step['distance'] / 1000,
                    duration_minutes=step['duration'] / 60,
                    instructions=step.get('maneuver', {}).get('instruction', 'Continue'),
                    geometry=[(lat, lon) for lon, lat in step['geometry']['coordinates']]
                ))
        
        return Route(
            source=source,
            destination=destination,
            total_distance_km=route_data['distance'] / 1000,
            total_duration_minutes=route_data['duration'] / 60,
            segments=segments,
            geometry=geometry,
            transport_mode=transport_mode
        )
    
    @error_handler_decorator("routing_service")
    def get_transit_route(self, source: RoutePoint, destination: RoutePoint) -> Dict[str, Any]:
        """
        Get optimized transit route combining different transport modes
        """
        try:
            routes = {}
            
            # Calculate routes for different transport modes
            transport_modes = [
                ("driving-car", "Car/Taxi"),
                ("cycling-regular", "Bicycle"), 
                ("foot-walking", "Walking")
            ]
            
            for mode, display_name in transport_modes:
                route = self.calculate_route(source, destination, mode)
                if route:
                    routes[mode] = {
                        'display_name': display_name,
                        'distance_km': route.total_distance_km,
                        'duration_minutes': route.total_duration_minutes,
                        'geometry': route.geometry
                    }
            
            # Find nearest bus stops and metro stations
            bus_route = self._find_bus_route(source, destination)
            metro_route = self._find_metro_route(source, destination)
            
            if bus_route:
                routes['bus'] = bus_route
            if metro_route:
                routes['metro'] = metro_route
            
            return {
                'source': {'lat': source.latitude, 'lng': source.longitude, 'name': source.name},
                'destination': {'lat': destination.latitude, 'lng': destination.longitude, 'name': destination.name},
                'routes': routes,
                'timestamp': time.time()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting transit route: {str(e)}")
            return {}
    
    def _find_bus_route(self, source: RoutePoint, destination: RoutePoint) -> Optional[Dict[str, Any]]:
        """Find optimal bus route"""
        try:
            # For now, use driving route as approximation for bus route
            # In a real implementation, this would use GTFS data
            route = self.calculate_route(source, destination, "driving-car")
            
            if route:
                # Adjust for bus-specific factors
                bus_distance = route.total_distance_km * 1.2  # Buses take longer routes
                bus_duration = route.total_duration_minutes * 2.5  # Account for stops and traffic
                
                return {
                    'display_name': 'BMTC Bus',
                    'distance_km': bus_distance,
                    'duration_minutes': bus_duration,
                    'geometry': route.geometry,
                    'stops': self._estimate_bus_stops(route),
                    'route_types': ['ordinary', 'ac', 'vajra']
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding bus route: {str(e)}")
            return None
    
    def _find_metro_route(self, source: RoutePoint, destination: RoutePoint) -> Optional[Dict[str, Any]]:
        """Find optimal metro route"""
        try:
            # Bangalore Metro stations (simplified)
            metro_stations = [
                {'name': 'MG Road', 'lat': 12.9759, 'lng': 77.6063, 'line': 'Blue'},
                {'name': 'Cubbon Park', 'lat': 12.9698, 'lng': 77.5936, 'line': 'Blue'},
                {'name': 'Vidhana Soudha', 'lat': 12.9794, 'lng': 77.5912, 'line': 'Blue'},
                {'name': 'Majestic', 'lat': 12.9767, 'lng': 77.5703, 'line': 'Purple'},
                {'name': 'Krantivira Sangolli Rayanna', 'lat': 12.9767, 'lng': 77.5703, 'line': 'Purple'},
                {'name': 'Magadi Road', 'lat': 12.9580, 'lng': 77.5540, 'line': 'Purple'}
            ]
            
            # Find nearest stations
            source_station = self._find_nearest_station(source, metro_stations)
            dest_station = self._find_nearest_station(destination, metro_stations)
            
            if source_station and dest_station and source_station != dest_station:
                # Calculate walking + metro + walking
                walk_to_metro = self.calculate_route(source, RoutePoint(source_station['lat'], source_station['lng']), "foot-walking")
                walk_from_metro = self.calculate_route(RoutePoint(dest_station['lat'], dest_station['lng']), destination, "foot-walking")
                
                if walk_to_metro and walk_from_metro:
                    # Estimate metro distance (simplified)
                    metro_distance = self._calculate_haversine_distance(
                        source_station['lat'], source_station['lng'],
                        dest_station['lat'], dest_station['lng']
                    )
                    
                    total_distance = walk_to_metro.total_distance_km + metro_distance + walk_from_metro.total_distance_km
                    total_duration = walk_to_metro.total_duration_minutes + (metro_distance / 35 * 60) + walk_from_metro.total_duration_minutes
                    
                    return {
                        'display_name': 'BMRCL Metro',
                        'distance_km': total_distance,
                        'duration_minutes': total_duration,
                        'metro_distance_km': metro_distance,
                        'source_station': source_station['name'],
                        'dest_station': dest_station['name'],
                        'walking_to_metro_km': walk_to_metro.total_distance_km,
                        'walking_from_metro_km': walk_from_metro.total_distance_km
                    }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding metro route: {str(e)}")
            return None
    
    def _find_nearest_station(self, point: RoutePoint, stations: List[Dict]) -> Optional[Dict]:
        """Find nearest metro station"""
        min_distance = float('inf')
        nearest_station = None
        
        for station in stations:
            distance = self._calculate_haversine_distance(
                point.latitude, point.longitude,
                station['lat'], station['lng']
            )
            
            if distance < min_distance and distance < 2.0:  # Within 2km
                min_distance = distance
                nearest_station = station
        
        return nearest_station
    
    def _calculate_haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate haversine distance between two points"""
        import math
        
        R = 6371  # Earth's radius in kilometers
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        a = (math.sin(delta_lat / 2) ** 2 +
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c
    
    def _estimate_bus_stops(self, route: Route) -> List[Dict[str, Any]]:
        """Estimate bus stops along the route"""
        stops = []
        
        # Add stops every 1-2 km along the route
        total_distance = 0
        for i, point in enumerate(route.geometry[::10]):  # Sample every 10th point
            if total_distance >= 1.5:  # Add stop every 1.5km
                stops.append({
                    'name': f'Bus Stop {len(stops) + 1}',
                    'lat': point[0],
                    'lng': point[1],
                    'distance_from_start': total_distance
                })
                total_distance = 0
            else:
                if i > 0:
                    prev_point = route.geometry[(i-1)*10] if (i-1)*10 < len(route.geometry) else route.geometry[-1]
                    total_distance += self._calculate_haversine_distance(
                        prev_point[0], prev_point[1], point[0], point[1]
                    )
        
        return stops

# Global routing service instance
routing_service = RoutingService()