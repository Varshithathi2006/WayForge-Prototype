#!/usr/bin/env python3
"""
Fallback Routing Providers for Enhanced Reliability
Provides Google Directions API, Mapbox, and OSRM as fallback options when ORS fails
"""

import os
import requests
import json
import logging
import asyncio
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
import time

from .common import setup_logging
from .error_handler import error_handler_decorator, performance_monitor

logger = setup_logging("fallback_routing_providers")

@dataclass
class RoutePoint:
    """Represents a geographical point in a route"""
    latitude: float
    longitude: float
    name: Optional[str] = None

@dataclass
class FallbackRoute:
    """Route response from fallback providers"""
    distance_km: float
    duration_minutes: float
    geometry: List[Tuple[float, float]]
    instructions: List[str]
    provider: str
    success: bool = True
    error_message: Optional[str] = None

class GoogleDirectionsProvider:
    """Google Directions API fallback provider"""
    
    def __init__(self):
        self.api_key = os.getenv('GOOGLE_MAPS_API_KEY', '')
        self.base_url = "https://maps.googleapis.com/maps/api/directions/json"
        self.session = requests.Session()
        
    @error_handler_decorator("google_directions")
    @performance_monitor("google_directions")
    async def calculate_route(self, source: RoutePoint, destination: RoutePoint, 
                            transport_mode: str = "driving") -> Optional[FallbackRoute]:
        """Calculate route using Google Directions API"""
        if not self.api_key:
            logger.warning("Google Maps API key not configured")
            return None
        
        try:
            # Map transport modes
            mode_mapping = {
                "driving-car": "driving",
                "driving": "driving",
                "walking": "walking",
                "cycling": "bicycling",
                "transit": "transit"
            }
            
            google_mode = mode_mapping.get(transport_mode, "driving")
            
            params = {
                'origin': f"{source.latitude},{source.longitude}",
                'destination': f"{destination.latitude},{destination.longitude}",
                'mode': google_mode,
                'key': self.api_key,
                'alternatives': 'false',
                'units': 'metric'
            }
            
            # Add traffic model for driving
            if google_mode == "driving":
                params['departure_time'] = 'now'
                params['traffic_model'] = 'best_guess'
            
            response = self.session.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data['status'] != 'OK':
                logger.error(f"Google Directions API error: {data['status']}")
                return FallbackRoute(
                    distance_km=0, duration_minutes=0, geometry=[], 
                    instructions=[], provider="google", success=False,
                    error_message=data.get('error_message', data['status'])
                )
            
            route = data['routes'][0]
            leg = route['legs'][0]
            
            # Extract route information
            distance_km = leg['distance']['value'] / 1000.0
            duration_minutes = leg['duration']['value'] / 60.0
            
            # Add traffic duration if available
            if 'duration_in_traffic' in leg:
                duration_minutes = leg['duration_in_traffic']['value'] / 60.0
            
            # Extract geometry
            geometry = self._decode_polyline(route['overview_polyline']['points'])
            
            # Extract instructions
            instructions = []
            for step in leg['steps']:
                instructions.append(step['html_instructions'])
            
            logger.info(f"Google Directions route calculated: {distance_km:.2f}km, {duration_minutes:.1f}min")
            
            return FallbackRoute(
                distance_km=distance_km,
                duration_minutes=duration_minutes,
                geometry=geometry,
                instructions=instructions,
                provider="google",
                success=True
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Google Directions API request error: {e}")
            return FallbackRoute(
                distance_km=0, duration_minutes=0, geometry=[], 
                instructions=[], provider="google", success=False,
                error_message=str(e)
            )
        except Exception as e:
            logger.error(f"Google Directions API error: {e}")
            return FallbackRoute(
                distance_km=0, duration_minutes=0, geometry=[], 
                instructions=[], provider="google", success=False,
                error_message=str(e)
            )
    
    def _decode_polyline(self, polyline_str: str) -> List[Tuple[float, float]]:
        """Decode Google polyline to coordinates"""
        try:
            index = 0
            lat = 0
            lng = 0
            coordinates = []
            
            while index < len(polyline_str):
                # Decode latitude
                result = 1
                shift = 0
                while True:
                    b = ord(polyline_str[index]) - 63 - 1
                    index += 1
                    result += b << shift
                    shift += 5
                    if b < 0x1f:
                        break
                lat += (~result >> 1) if (result & 1) != 0 else (result >> 1)
                
                # Decode longitude
                result = 1
                shift = 0
                while True:
                    b = ord(polyline_str[index]) - 63 - 1
                    index += 1
                    result += b << shift
                    shift += 5
                    if b < 0x1f:
                        break
                lng += (~result >> 1) if (result & 1) != 0 else (result >> 1)
                
                coordinates.append((lat / 1e5, lng / 1e5))
            
            return coordinates
            
        except Exception as e:
            logger.error(f"Error decoding polyline: {e}")
            return []

class MapboxProvider:
    """Mapbox Directions API fallback provider"""
    
    def __init__(self):
        self.access_token = os.getenv('MAPBOX_ACCESS_TOKEN', '')
        self.base_url = "https://api.mapbox.com/directions/v5/mapbox"
        self.session = requests.Session()
        
    @error_handler_decorator("mapbox_directions")
    @performance_monitor("mapbox_directions")
    async def calculate_route(self, source: RoutePoint, destination: RoutePoint, 
                            transport_mode: str = "driving") -> Optional[FallbackRoute]:
        """Calculate route using Mapbox Directions API"""
        if not self.access_token:
            logger.warning("Mapbox access token not configured")
            return None
        
        try:
            # Map transport modes
            mode_mapping = {
                "driving-car": "driving-traffic",
                "driving": "driving-traffic",
                "walking": "walking",
                "cycling": "cycling"
            }
            
            mapbox_mode = mode_mapping.get(transport_mode, "driving-traffic")
            
            # Build URL
            coordinates = f"{source.longitude},{source.latitude};{destination.longitude},{destination.latitude}"
            url = f"{self.base_url}/{mapbox_mode}/{coordinates}"
            
            params = {
                'access_token': self.access_token,
                'geometries': 'geojson',
                'steps': 'true',
                'overview': 'full'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data['code'] != 'Ok':
                logger.error(f"Mapbox Directions API error: {data['code']}")
                return FallbackRoute(
                    distance_km=0, duration_minutes=0, geometry=[], 
                    instructions=[], provider="mapbox", success=False,
                    error_message=data.get('message', data['code'])
                )
            
            route = data['routes'][0]
            
            # Extract route information
            distance_km = route['distance'] / 1000.0
            duration_minutes = route['duration'] / 60.0
            
            # Extract geometry
            geometry = []
            if 'geometry' in route and 'coordinates' in route['geometry']:
                for coord in route['geometry']['coordinates']:
                    geometry.append((coord[1], coord[0]))  # Convert lng,lat to lat,lng
            
            # Extract instructions
            instructions = []
            for leg in route['legs']:
                for step in leg['steps']:
                    if 'maneuver' in step and 'instruction' in step['maneuver']:
                        instructions.append(step['maneuver']['instruction'])
            
            logger.info(f"Mapbox route calculated: {distance_km:.2f}km, {duration_minutes:.1f}min")
            
            return FallbackRoute(
                distance_km=distance_km,
                duration_minutes=duration_minutes,
                geometry=geometry,
                instructions=instructions,
                provider="mapbox",
                success=True
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Mapbox Directions API request error: {e}")
            return FallbackRoute(
                distance_km=0, duration_minutes=0, geometry=[], 
                instructions=[], provider="mapbox", success=False,
                error_message=str(e)
            )
        except Exception as e:
            logger.error(f"Mapbox Directions API error: {e}")
            return FallbackRoute(
                distance_km=0, duration_minutes=0, geometry=[], 
                instructions=[], provider="mapbox", success=False,
                error_message=str(e)
            )

class OSRMProvider:
    """OSRM (Open Source Routing Machine) fallback provider"""
    
    def __init__(self):
        self.base_url = "http://router.project-osrm.org/route/v1"
        self.session = requests.Session()
        
    @error_handler_decorator("osrm_directions")
    @performance_monitor("osrm_directions")
    async def calculate_route(self, source: RoutePoint, destination: RoutePoint, 
                            transport_mode: str = "driving") -> Optional[FallbackRoute]:
        """Calculate route using OSRM"""
        try:
            # OSRM primarily supports driving
            profile = "driving"
            
            # Build URL
            coordinates = f"{source.longitude},{source.latitude};{destination.longitude},{destination.latitude}"
            url = f"{self.base_url}/{profile}/{coordinates}"
            
            params = {
                'overview': 'full',
                'geometries': 'geojson',
                'steps': 'true'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data['code'] != 'Ok':
                logger.error(f"OSRM API error: {data['code']}")
                return FallbackRoute(
                    distance_km=0, duration_minutes=0, geometry=[], 
                    instructions=[], provider="osrm", success=False,
                    error_message=data.get('message', data['code'])
                )
            
            route = data['routes'][0]
            
            # Extract route information
            distance_km = route['distance'] / 1000.0
            duration_minutes = route['duration'] / 60.0
            
            # Extract geometry
            geometry = []
            if 'geometry' in route and 'coordinates' in route['geometry']:
                for coord in route['geometry']['coordinates']:
                    geometry.append((coord[1], coord[0]))  # Convert lng,lat to lat,lng
            
            # Extract instructions
            instructions = []
            for leg in route['legs']:
                for step in leg['steps']:
                    if 'maneuver' in step and 'instruction' in step['maneuver']:
                        instructions.append(step['maneuver']['instruction'])
            
            logger.info(f"OSRM route calculated: {distance_km:.2f}km, {duration_minutes:.1f}min")
            
            return FallbackRoute(
                distance_km=distance_km,
                duration_minutes=duration_minutes,
                geometry=geometry,
                instructions=instructions,
                provider="osrm",
                success=True
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"OSRM API request error: {e}")
            return FallbackRoute(
                distance_km=0, duration_minutes=0, geometry=[], 
                instructions=[], provider="osrm", success=False,
                error_message=str(e)
            )
        except Exception as e:
            logger.error(f"OSRM API error: {e}")
            return FallbackRoute(
                distance_km=0, duration_minutes=0, geometry=[], 
                instructions=[], provider="osrm", success=False,
                error_message=str(e)
            )

class LocalGTFSProvider:
    """Local GTFS-based routing fallback"""
    
    def __init__(self):
        self.gtfs_data_path = "data/static"
        
    @error_handler_decorator("local_gtfs")
    @performance_monitor("local_gtfs")
    async def calculate_route(self, source: RoutePoint, destination: RoutePoint, 
                            transport_mode: str = "transit") -> Optional[FallbackRoute]:
        """Calculate route using local GTFS data"""
        try:
            # This is a simplified implementation
            # In a real scenario, you'd use a GTFS routing library like OpenTripPlanner
            
            # For now, provide a basic estimation based on straight-line distance
            distance_km = self._calculate_haversine_distance(
                source.latitude, source.longitude,
                destination.latitude, destination.longitude
            )
            
            # Estimate duration based on average transit speed (15 km/h including stops)
            duration_minutes = (distance_km / 15.0) * 60
            
            # Create basic geometry (straight line)
            geometry = [
                (source.latitude, source.longitude),
                (destination.latitude, destination.longitude)
            ]
            
            instructions = [
                f"Board transit at nearest stop to {source.name or 'source'}",
                f"Travel approximately {distance_km:.1f} km",
                f"Alight at stop nearest to {destination.name or 'destination'}"
            ]
            
            logger.info(f"Local GTFS route estimated: {distance_km:.2f}km, {duration_minutes:.1f}min")
            
            return FallbackRoute(
                distance_km=distance_km,
                duration_minutes=duration_minutes,
                geometry=geometry,
                instructions=instructions,
                provider="local_gtfs",
                success=True
            )
            
        except Exception as e:
            logger.error(f"Local GTFS routing error: {e}")
            return FallbackRoute(
                distance_km=0, duration_minutes=0, geometry=[], 
                instructions=[], provider="local_gtfs", success=False,
                error_message=str(e)
            )
    
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

class FallbackRoutingManager:
    """Manages fallback routing providers with automatic switching"""
    
    def __init__(self):
        self.providers = {
            'google': GoogleDirectionsProvider(),
            'mapbox': MapboxProvider(),
            'osrm': OSRMProvider(),
            'local_gtfs': LocalGTFSProvider()
        }
        
        # Provider priority order
        self.provider_priority = ['google', 'mapbox', 'osrm', 'local_gtfs']
        self.current_provider = 'google'
        
        # Provider health tracking
        self.provider_health = {name: True for name in self.providers.keys()}
        self.provider_failures = {name: 0 for name in self.providers.keys()}
        
        logger.info("Fallback Routing Manager initialized")
    
    @error_handler_decorator("fallback_routing_manager")
    @performance_monitor("fallback_routing_manager")
    async def calculate_route_with_fallback(self, source: RoutePoint, destination: RoutePoint, 
                                          transport_mode: str = "driving") -> Optional[FallbackRoute]:
        """Calculate route with automatic fallback to other providers"""
        
        # Try providers in priority order
        for provider_name in self.provider_priority:
            if not self.provider_health[provider_name]:
                logger.debug(f"Skipping unhealthy provider: {provider_name}")
                continue
            
            try:
                logger.info(f"Attempting route calculation with {provider_name}")
                provider = self.providers[provider_name]
                
                route = await provider.calculate_route(source, destination, transport_mode)
                
                if route and route.success:
                    # Reset failure count on success
                    self.provider_failures[provider_name] = 0
                    self.provider_health[provider_name] = True
                    self.current_provider = provider_name
                    
                    logger.info(f"Route calculated successfully with {provider_name}")
                    return route
                else:
                    # Handle provider failure
                    await self._handle_provider_failure(provider_name, route.error_message if route else "Unknown error")
                    
            except Exception as e:
                logger.error(f"Error with provider {provider_name}: {e}")
                await self._handle_provider_failure(provider_name, str(e))
        
        # If all providers fail, return a basic fallback route
        logger.warning("All routing providers failed, returning basic fallback")
        return self._create_basic_fallback_route(source, destination)
    
    async def _handle_provider_failure(self, provider_name: str, error_message: str):
        """Handle provider failure and update health status"""
        self.provider_failures[provider_name] += 1
        
        # Mark provider as unhealthy after 3 consecutive failures
        if self.provider_failures[provider_name] >= 3:
            self.provider_health[provider_name] = False
            logger.warning(f"Marked provider {provider_name} as unhealthy after {self.provider_failures[provider_name]} failures")
        
        logger.error(f"Provider {provider_name} failed: {error_message}")
    
    def _create_basic_fallback_route(self, source: RoutePoint, destination: RoutePoint) -> FallbackRoute:
        """Create a basic fallback route when all providers fail"""
        # Calculate straight-line distance
        distance_km = self._calculate_haversine_distance(
            source.latitude, source.longitude,
            destination.latitude, destination.longitude
        )
        
        # Estimate duration (assuming 30 km/h average speed)
        duration_minutes = (distance_km / 30.0) * 60
        
        geometry = [
            (source.latitude, source.longitude),
            (destination.latitude, destination.longitude)
        ]
        
        instructions = [
            f"Navigate from {source.name or 'source'} to {destination.name or 'destination'}",
            f"Estimated distance: {distance_km:.1f} km",
            "Route calculated using fallback estimation"
        ]
        
        return FallbackRoute(
            distance_km=distance_km,
            duration_minutes=duration_minutes,
            geometry=geometry,
            instructions=instructions,
            provider="fallback_estimation",
            success=True
        )
    
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
    
    async def health_check_providers(self):
        """Perform health check on all providers"""
        logger.info("Performing health check on routing providers")
        
        # Test coordinates (Bangalore city center to Whitefield)
        test_source = RoutePoint(12.9716, 77.5946, "Test Source")
        test_dest = RoutePoint(12.9698, 77.7500, "Test Dest")
        
        for provider_name, provider in self.providers.items():
            try:
                route = await provider.calculate_route(test_source, test_dest)
                
                if route and route.success:
                    self.provider_health[provider_name] = True
                    self.provider_failures[provider_name] = 0
                    logger.info(f"Provider {provider_name} health check: PASSED")
                else:
                    self.provider_failures[provider_name] += 1
                    if self.provider_failures[provider_name] >= 3:
                        self.provider_health[provider_name] = False
                    logger.warning(f"Provider {provider_name} health check: FAILED")
                    
            except Exception as e:
                self.provider_failures[provider_name] += 1
                if self.provider_failures[provider_name] >= 3:
                    self.provider_health[provider_name] = False
                logger.error(f"Provider {provider_name} health check error: {e}")
    
    def get_provider_status(self) -> Dict[str, Any]:
        """Get status of all providers"""
        return {
            'current_provider': self.current_provider,
            'provider_health': self.provider_health,
            'provider_failures': self.provider_failures,
            'available_providers': [name for name, healthy in self.provider_health.items() if healthy]
        }
    
    def force_provider_switch(self, provider_name: str) -> bool:
        """Force switch to a specific provider"""
        if provider_name in self.providers:
            self.current_provider = provider_name
            # Reset health status
            self.provider_health[provider_name] = True
            self.provider_failures[provider_name] = 0
            logger.info(f"Forced switch to provider: {provider_name}")
            return True
        return False

# Global instance
fallback_routing_manager = FallbackRoutingManager()

async def test_fallback_providers():
    """Test all fallback providers"""
    logger.info("Testing fallback routing providers...")
    
    # Test coordinates
    source = RoutePoint(12.9716, 77.5946, "Bangalore City Center")
    destination = RoutePoint(12.9698, 77.7500, "Whitefield")
    
    route = await fallback_routing_manager.calculate_route_with_fallback(source, destination)
    
    if route:
        logger.info(f"Test route: {route.distance_km:.2f}km, {route.duration_minutes:.1f}min via {route.provider}")
    else:
        logger.error("Failed to calculate test route")
    
    # Print provider status
    status = fallback_routing_manager.get_provider_status()
    logger.info(f"Provider status: {status}")

if __name__ == "__main__":
    asyncio.run(test_fallback_providers())