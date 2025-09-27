#!/usr/bin/env python3
"""
Enhanced Routing Service with Real-time Data Integration
Handles route calculation, nearest stops, and real-time transit data
"""

import requests
import json
import logging
import os
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
import time
import asyncio
from functools import lru_cache
import math

from .error_handler import error_handler_decorator, performance_monitor
from .common import setup_logging
from .enhanced_distance_calculator import enhanced_distance_calculator, PathAnalysis

logger = setup_logging("routing_service")

@dataclass
class RoutePoint:
    """Represents a geographical point in a route"""
    latitude: float
    longitude: float
    name: Optional[str] = None
    is_exact_stop: bool = False
    nearest_stops: List[Dict[str, Any]] = None

@dataclass
class RouteSegment:
    """Represents a segment of a route"""
    distance_km: float
    duration_minutes: float
    instructions: str
    geometry: List[Tuple[float, float]]
    traffic_delay_minutes: int = 0
    real_time_data: Optional[Dict[str, Any]] = None

@dataclass
class Route:
    """Enhanced route with real-time data"""
    source: RoutePoint
    destination: RoutePoint
    total_distance_km: float
    total_duration_minutes: float
    segments: List[RouteSegment]
    geometry: List[Tuple[float, float]]
    transport_mode: str
    real_time_enhanced: bool = False
    nearest_stops_info: Optional[Dict[str, Any]] = None
    cost_info: Optional[Dict[str, Any]] = None
    distance_analysis: Optional[PathAnalysis] = None
    distance_validation: Optional[Dict[str, Any]] = None

class EnhancedRoutingService:
    """Enhanced routing service with real-time data integration"""
    
    def __init__(self):
        self.logger = logger
        
        # OpenRouteService API (free tier - 2000 requests/day)
        self.ors_api_key = os.getenv('ORS_API_KEY', None)  # Use environment variable
        self.ors_base_url = "https://api.openrouteservice.org"
        self.ors_enabled = self.ors_api_key is not None and self.ors_api_key != 'demo-key'
        
        # Fallback to local OSRM if available
        self.osrm_base_url = "http://router.project-osrm.org"
        
        # Cache for route calculations
        self.route_cache = {}
        
        # Real-time data integration
        self.pathway_streaming = None
        self._initialize_pathway_integration()
        
    def _initialize_pathway_integration(self):
        """Initialize integration with Pathway streaming service"""
        try:
            # Import pathway streaming service
            import sys
            import os
            sys.path.append(os.path.dirname(os.path.dirname(__file__)))
            from pathway_streaming import pathway_streaming
            self.pathway_streaming = pathway_streaming
            self.logger.info("Pathway streaming integration initialized")
        except ImportError:
            self.logger.warning("Pathway streaming not available, using fallback mode")

    @error_handler_decorator("routing_service")
    @performance_monitor("routing_service")
    async def calculate_enhanced_route(self, 
                                     source: RoutePoint, 
                                     destination: RoutePoint, 
                                     transport_mode: str = "driving-car",
                                     include_alternatives: bool = False,
                                     include_real_time: bool = True) -> Optional[Route]:
        """
        Calculate enhanced route with real-time data and nearest stops
        
        Args:
            source: Starting point
            destination: End point
            transport_mode: driving-car, cycling-regular, foot-walking, transit
            include_real_time: Whether to include real-time data
            
        Returns:
            Enhanced Route object with real-time data
        """
        try:
            # Check if source/destination are exact stops
            if not source.is_exact_stop:
                source.nearest_stops = await self.find_nearest_stops(source.latitude, source.longitude)
                
            if not destination.is_exact_stop:
                destination.nearest_stops = await self.find_nearest_stops(destination.latitude, destination.longitude)
            
            # Calculate base route
            route = await self._calculate_base_route(source, destination, transport_mode)
            
            if not route:
                return None
            
            # Enhance with real-time data if requested
            if include_real_time and self.pathway_streaming:
                route = await self._enhance_route_with_realtime_data(route)
            
            # Enhance with accurate distance calculation
            route = self._enhance_route_with_distance_analysis(route)
            
            # Add nearest stops information
            route.nearest_stops_info = {
                "source_stops": source.nearest_stops or [],
                "destination_stops": destination.nearest_stops or []
            }
            
            # Calculate real-time cost information
            route.cost_info = self._calculate_realtime_cost(route)
            
            return route
            
        except Exception as e:
            self.logger.error(f"Error calculating enhanced route: {str(e)}")
            return None

    async def _calculate_base_route(self, source: RoutePoint, destination: RoutePoint, transport_mode: str) -> Optional[Route]:
        """Calculate base route using traditional routing APIs"""
        try:
            # Create cache key
            cache_key = f"{source.latitude},{source.longitude}_{destination.latitude},{destination.longitude}_{transport_mode}"
            
            if cache_key in self.route_cache:
                self.logger.info(f"Using cached route for {cache_key}")
                return self.route_cache[cache_key]
            
            # Handle transit mode specially
            if transport_mode == "transit":
                return await self._calculate_transit_route(source, destination)
            
            route = None
            
            # Try OpenRouteService first (if enabled)
            if self.ors_enabled:
                route = await asyncio.to_thread(self._calculate_route_ors, source, destination, transport_mode)
            
            if not route:
                # Fallback to OSRM
                route = await asyncio.to_thread(self._calculate_route_osrm, source, destination, transport_mode)
            
            if not route:
                # Final fallback: generate a road-following route
                self.logger.warning("External routing APIs failed, generating fallback route")
                route = self._generate_fallback_route(source, destination, transport_mode)
            
            if route:
                self.route_cache[cache_key] = route
                self.logger.info(f"Calculated route: {route.total_distance_km:.2f}km, {route.total_duration_minutes:.1f}min")
            
            return route
            
        except Exception as e:
            self.logger.error(f"Error calculating base route: {str(e)}")
            # Generate fallback route even on error
            return self._generate_fallback_route(source, destination, transport_mode)

    async def _calculate_transit_route(self, source: RoutePoint, destination: RoutePoint) -> Optional[Route]:
        """Calculate transit route using bus and metro data"""
        try:
            # Get transit options
            transit_data = await self.get_enhanced_transit_route(source, destination)
            
            if not transit_data or not transit_data.get('routes'):
                return None
            
            # Use the best transit route
            best_route = transit_data['routes'][0]
            
            # Convert to Route object
            segments = []
            total_distance = 0
            total_duration = 0
            geometry = []
            
            for segment in best_route.get('segments', []):
                seg = RouteSegment(
                    distance_km=segment.get('distance_km', 0),
                    duration_minutes=segment.get('duration_minutes', 0),
                    instructions=segment.get('instructions', ''),
                    geometry=segment.get('geometry', [])
                )
                segments.append(seg)
                total_distance += seg.distance_km
                total_duration += seg.duration_minutes
                geometry.extend(seg.geometry)
            
            return Route(
                source=source,
                destination=destination,
                total_distance_km=total_distance,
                total_duration_minutes=total_duration,
                segments=segments,
                geometry=geometry,
                transport_mode="transit"
            )
            
        except Exception as e:
            self.logger.error(f"Error calculating transit route: {str(e)}")
            return None

    async def find_nearest_stops(self, lat: float, lng: float, max_distance_km: float = 1.0) -> List[Dict[str, Any]]:
        """Find nearest bus stops and metro stations"""
        try:
            if self.pathway_streaming:
                # Use Pathway streaming service for real-time nearest stops
                return self.pathway_streaming.find_nearest_stops(lat, lng, max_distance_km)
            else:
                # Fallback to static data
                return self._find_nearest_stops_static(lat, lng, max_distance_km)
                
        except Exception as e:
            self.logger.error(f"Error finding nearest stops: {str(e)}")
            return []

    def _find_nearest_stops_static(self, lat: float, lng: float, max_distance_km: float = 1.0) -> List[Dict[str, Any]]:
        """Find nearest stops using static data (fallback)"""
        try:
            nearest_stops = []
            
            # Load bus stops
            try:
                with open('data/static/bmtc_static.json', 'r') as f:
                    bmtc_data = json.load(f)
                    
                for stop in bmtc_data.get('stops', []):
                    distance = self._calculate_haversine_distance(lat, lng, stop['stop_lat'], stop['stop_lon'])
                    if distance <= max_distance_km:
                        walking_time = int(distance * 1000 / 80)  # 80 m/min walking speed
                        nearest_stops.append({
                            'stop_id': stop['stop_id'],
                            'stop_name': stop['stop_name'],
                            'stop_type': 'bus_stop',
                            'latitude': stop['stop_lat'],
                            'longitude': stop['stop_lon'],
                            'distance_meters': distance * 1000,
                            'walking_time_minutes': walking_time
                        })
            except FileNotFoundError:
                pass
            
            # Load metro stations
            try:
                with open('data/static/bmrcl_static.json', 'r') as f:
                    bmrcl_data = json.load(f)
                    
                for station in bmrcl_data.get('stops', []):
                    distance = self._calculate_haversine_distance(lat, lng, station['stop_lat'], station['stop_lon'])
                    if distance <= max_distance_km:
                        walking_time = int(distance * 1000 / 80)  # 80 m/min walking speed
                        nearest_stops.append({
                            'stop_id': station['stop_id'],
                            'stop_name': station['stop_name'],
                            'stop_type': 'metro_station',
                            'latitude': station['stop_lat'],
                            'longitude': station['stop_lon'],
                            'distance_meters': distance * 1000,
                            'walking_time_minutes': walking_time
                        })
            except FileNotFoundError:
                pass
            
            # Sort by distance
            nearest_stops.sort(key=lambda x: x['distance_meters'])
            return nearest_stops[:10]  # Return top 10 nearest stops
            
        except Exception as e:
            self.logger.error(f"Error finding nearest stops (static): {str(e)}")
            return []

    async def _enhance_route_with_realtime_data(self, route: Route) -> Route:
        """Enhance route with real-time traffic and transit data"""
        try:
            if not self.pathway_streaming:
                return route
            
            # Get real-time data for route waypoints
            enhanced_segments = []
            
            for segment in route.segments:
                if segment.geometry:
                    # Get midpoint of segment for real-time data
                    mid_idx = len(segment.geometry) // 2
                    lat, lng = segment.geometry[mid_idx]
                    
                    # Get real-time data
                    real_time_data = await self.pathway_streaming.get_comprehensive_transit_data(lat, lng)
                    
                    # Calculate traffic delay
                    traffic_delay = 0
                    if real_time_data.get('traffic'):
                        for traffic in real_time_data['traffic']:
                            if traffic.get('congestion_level') in ['high', 'severe']:
                                traffic_delay += traffic.get('estimated_delay_minutes', 0)
                    
                    # Create enhanced segment
                    enhanced_segment = RouteSegment(
                        distance_km=segment.distance_km,
                        duration_minutes=segment.duration_minutes + traffic_delay,
                        instructions=segment.instructions,
                        geometry=segment.geometry,
                        traffic_delay_minutes=traffic_delay,
                        real_time_data=real_time_data
                    )
                    enhanced_segments.append(enhanced_segment)
                else:
                    enhanced_segments.append(segment)
            
            # Update route with enhanced segments
            total_duration = sum(seg.duration_minutes for seg in enhanced_segments)
            
            enhanced_route = Route(
                source=route.source,
                destination=route.destination,
                total_distance_km=route.total_distance_km,
                total_duration_minutes=total_duration,
                segments=enhanced_segments,
                geometry=route.geometry,
                transport_mode=route.transport_mode,
                real_time_enhanced=True
            )
            
            return enhanced_route
            
        except Exception as e:
            self.logger.error(f"Error enhancing route with real-time data: {str(e)}")
            return route

    def _enhance_route_with_distance_analysis(self, route: Route) -> Route:
        """Enhance route with accurate distance calculation and analysis"""
        try:
            if not route.geometry or len(route.geometry) < 2:
                self.logger.warning("Route geometry insufficient for distance analysis")
                return route
            
            # Calculate enhanced distance analysis
            distance_analysis = enhanced_distance_calculator.calculate_path_distance(
                geometry=route.geometry,
                transport_mode=route.transport_mode,
                use_geodesic=True
            )
            
            # Validate the distance calculation
            distance_validation = enhanced_distance_calculator.validate_distance_calculation(
                calculated_distance=distance_analysis.total_distance_km,
                geometry=route.geometry,
                transport_mode=route.transport_mode,
                expected_duration_minutes=route.total_duration_minutes
            )
            
            # Update route with corrected distance if validation suggests improvement
            corrected_distance = distance_analysis.total_distance_km
            if distance_validation['is_valid'] and distance_validation['confidence_score'] > 0.8:
                # Use the enhanced calculation if it's more accurate
                if abs(corrected_distance - route.total_distance_km) / route.total_distance_km > 0.1:  # More than 10% difference
                    self.logger.info(f"Distance corrected from {route.total_distance_km:.2f}km to {corrected_distance:.2f}km")
                    route.total_distance_km = corrected_distance
            
            # Enhance segments with detailed distance information
            enhanced_segments = []
            cumulative_distance = 0.0
            
            for i, segment in enumerate(route.segments):
                if segment.geometry and len(segment.geometry) >= 2:
                    # Calculate accurate distance for this segment
                    segment_analysis = enhanced_distance_calculator.calculate_path_distance(
                        geometry=segment.geometry,
                        transport_mode=route.transport_mode,
                        use_geodesic=True
                    )
                    
                    # Update segment with accurate distance
                    enhanced_segment = RouteSegment(
                        distance_km=segment_analysis.total_distance_km,
                        duration_minutes=segment.duration_minutes,
                        instructions=segment.instructions,
                        geometry=segment.geometry,
                        traffic_delay_minutes=segment.traffic_delay_minutes,
                        real_time_data=segment.real_time_data
                    )
                    enhanced_segments.append(enhanced_segment)
                    cumulative_distance += segment_analysis.total_distance_km
                else:
                    enhanced_segments.append(segment)
                    cumulative_distance += segment.distance_km
            
            # Create enhanced route with distance analysis
            enhanced_route = Route(
                source=route.source,
                destination=route.destination,
                total_distance_km=corrected_distance,
                total_duration_minutes=route.total_duration_minutes,
                segments=enhanced_segments,
                geometry=route.geometry,
                transport_mode=route.transport_mode,
                real_time_enhanced=route.real_time_enhanced,
                nearest_stops_info=route.nearest_stops_info,
                cost_info=route.cost_info,
                distance_analysis=distance_analysis,
                distance_validation=distance_validation
            )
            
            return enhanced_route
            
        except Exception as e:
            self.logger.error(f"Error enhancing route with distance analysis: {str(e)}")
            return route

    async def calculate_road_distance(self, source: RoutePoint, destination: RoutePoint) -> Dict[str, Any]:
        """Calculate accurate road-based distance and duration"""
        try:
            route = await self.calculate_enhanced_route(source, destination, "driving-car", include_real_time=True)
            
            if not route:
                # Fallback to Haversine distance
                straight_distance = self._calculate_haversine_distance(
                    source.latitude, source.longitude,
                    destination.latitude, destination.longitude
                )
                return {
                    "distance_km": straight_distance,
                    "duration_minutes": straight_distance * 2,  # Rough estimate: 30 km/h average
                    "method": "haversine_fallback",
                    "real_time_enhanced": False
                }
            
            return {
                "distance_km": route.total_distance_km,
                "duration_minutes": route.total_duration_minutes,
                "method": "road_based",
                "real_time_enhanced": route.real_time_enhanced,
                "traffic_delays": sum(seg.traffic_delay_minutes for seg in route.segments),
                "geometry": route.geometry
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating road distance: {str(e)}")
            return {"error": str(e)}

    def _calculate_route_ors(self, source: RoutePoint, destination: RoutePoint, transport_mode: str) -> Optional[Route]:
        """Calculate route using OpenRouteService"""
        # Check if ORS is enabled with valid API key
        if not self.ors_enabled:
            self.logger.debug("ORS API disabled - no valid API key provided")
            return None
            
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

    def _calculate_realtime_cost(self, route: Route) -> Dict[str, Any]:
        """Calculate real-time cost for different transport modes"""
        try:
            # Import fare calculators
            import sys
            import os
            sys.path.append(os.path.dirname(os.path.dirname(__file__)))
            from data_fetchers.bmtc_fetcher import BMTCDataFetcher
            from data_fetchers.bmrcl_fetcher import BMRCLDataFetcher
            
            bmtc_fetcher = BMTCDataFetcher()
            bmrcl_fetcher = BMRCLDataFetcher()
            
            distance_km = route.total_distance_km
            cost_info = {
                "distance_km": distance_km,
                "transport_options": {},
                "recommendations": []
            }
            
            # Calculate BMTC bus fares
            cost_info["transport_options"]["bus"] = {
                "ordinary": bmtc_fetcher.calculate_fare(distance_km, "ordinary"),
                "deluxe": bmtc_fetcher.calculate_fare(distance_km, "deluxe"),
                "ac": bmtc_fetcher.calculate_fare(distance_km, "ac"),
                "vajra": bmtc_fetcher.calculate_fare(distance_km, "vajra")
            }
            
            # Calculate BMRCL metro fares
            cost_info["transport_options"]["metro"] = {
                "token": bmrcl_fetcher.calculate_fare(distance_km, "token"),
                "smart_card": bmrcl_fetcher.calculate_fare(distance_km, "smart_card")
            }
            
            # Calculate taxi/auto fare (estimated)
            taxi_base_fare = 25.0
            taxi_per_km = 15.0
            auto_base_fare = 20.0
            auto_per_km = 12.0
            
            cost_info["transport_options"]["taxi"] = {
                "base_fare": taxi_base_fare,
                "per_km_rate": taxi_per_km,
                "total_fare": round(taxi_base_fare + (distance_km * taxi_per_km)),
                "currency": "INR",
                "estimated": True
            }
            
            cost_info["transport_options"]["auto"] = {
                "base_fare": auto_base_fare,
                "per_km_rate": auto_per_km,
                "total_fare": round(auto_base_fare + (distance_km * auto_per_km)),
                "currency": "INR",
                "estimated": True
            }
            
            # Walking and cycling are free
            cost_info["transport_options"]["walking"] = {
                "total_fare": 0,
                "currency": "INR",
                "environmental_benefit": "Zero emissions"
            }
            
            cost_info["transport_options"]["cycling"] = {
                "total_fare": 0,
                "currency": "INR",
                "environmental_benefit": "Zero emissions",
                "health_benefit": "Exercise"
            }
            
            # Add cost-effective recommendations
            if distance_km < 3:
                cost_info["recommendations"].append("BMTC ordinary bus is most cost-effective for short distances")
            elif distance_km > 10:
                cost_info["recommendations"].append("BMRCL metro is faster and cost-effective for longer distances")
            
            if distance_km < 2:
                cost_info["recommendations"].append("Walking or cycling recommended for health and environment")
            
            return cost_info
            
        except Exception as e:
            self.logger.error(f"Error calculating real-time cost: {str(e)}")
            return {
                "error": "Cost calculation unavailable",
                "transport_options": {},
                "recommendations": []
            }

    def _generate_fallback_route(self, source: RoutePoint, destination: RoutePoint, transport_mode: str) -> Route:
        """Generate a fallback route with road-following geometry when external APIs fail"""
        try:
            import math
            
            # Calculate straight-line distance using haversine formula
            lat1, lon1 = math.radians(source.latitude), math.radians(source.longitude)
            lat2, lon2 = math.radians(destination.latitude), math.radians(destination.longitude)
            
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            straight_distance_km = 6371 * c  # Earth's radius in km
            
            # Apply road-following multiplier based on distance and location type
            # Urban areas typically have more winding roads, longer distances have more detours
            if straight_distance_km < 5:
                road_multiplier = 1.3  # 30% longer for short urban routes
            elif straight_distance_km < 15:
                road_multiplier = 1.5  # 50% longer for medium routes
            elif straight_distance_km < 30:
                road_multiplier = 1.6  # 60% longer for longer routes
            else:
                road_multiplier = 1.7  # 70% longer for very long routes
            
            # For airport routes, add extra multiplier due to highway access patterns
            if any(keyword in (destination.name or "").lower() for keyword in ["airport", "international", "kempegowda"]):
                road_multiplier *= 1.1  # Additional 10% for airport access routes
            
            distance_km = straight_distance_km * road_multiplier
            
            # Generate intermediate points to simulate road following
            num_points = max(8, int(distance_km * 3))  # More points for smoother curves
            geometry = []
            
            for i in range(num_points + 1):
                ratio = i / num_points
                
                # Add realistic curvature to simulate road following
                # Use multiple sine waves for more natural road patterns
                curve_factor = distance_km / 50  # Scale curve based on distance
                lat_offset = curve_factor * (
                    0.002 * math.sin(ratio * math.pi * 2) +
                    0.001 * math.sin(ratio * math.pi * 4) +
                    0.0005 * math.sin(ratio * math.pi * 6)
                )
                lon_offset = curve_factor * (
                    0.002 * math.cos(ratio * math.pi * 3) +
                    0.001 * math.cos(ratio * math.pi * 5)
                )
                
                lat = source.latitude + ratio * (destination.latitude - source.latitude) + lat_offset
                lon = source.longitude + ratio * (destination.longitude - source.longitude) + lon_offset
                geometry.append((lat, lon))
            
            # Estimate duration based on transport mode
            speed_kmh = {
                "driving-car": 35,
                "foot-walking": 5,
                "cycling-regular": 15,
                "transit": 25,
                "driving": 35,
                "walking": 5,
                "cycling": 15
            }.get(transport_mode, 35)
            
            duration_minutes = (distance_km / speed_kmh) * 60
            
            # Create route segments
            segments = [RouteSegment(
                distance_km=distance_km,
                duration_minutes=duration_minutes,
                instructions=f"Head towards {destination.name or 'destination'} via local roads",
                geometry=geometry
            )]
            
            return Route(
                source=source,
                destination=destination,
                total_distance_km=distance_km,
                total_duration_minutes=duration_minutes,
                segments=segments,
                geometry=geometry,
                transport_mode=transport_mode
            )
            
        except Exception as e:
            self.logger.error(f"Error generating fallback route: {str(e)}")
            # Return minimal straight-line route as last resort
            geometry = [
                (source.latitude, source.longitude),
                (destination.latitude, destination.longitude)
            ]
            return Route(
                source=source,
                destination=destination,
                total_distance_km=1.0,
                total_duration_minutes=5.0,
                segments=[RouteSegment(
                    distance_km=1.0,
                    duration_minutes=5.0,
                    instructions="Direct route",
                    geometry=geometry
                )],
                geometry=geometry,
                transport_mode=transport_mode
            )

    def calculate_route(self, source: RoutePoint, destination: RoutePoint, transport_mode: str = "driving-car") -> Optional[Dict[str, Any]]:
        """
        Synchronous wrapper for calculate_enhanced_route for backward compatibility
        """
        try:
            # Run the async method synchronously
            route = asyncio.run(self.calculate_enhanced_route(source, destination, transport_mode, include_alternatives=False, include_real_time=False))
            if route:
                return {
                    'source': asdict(route.source),
                    'destination': asdict(route.destination),
                    'distance_km': route.total_distance_km,
                    'duration_minutes': route.total_duration_minutes,
                    'geometry': route.geometry,
                    'transport_mode': route.transport_mode,
                    'segments': [asdict(segment) for segment in route.segments]
                }
            return None
        except Exception as e:
            self.logger.error(f"Error calculating route: {e}")
            return None

    @error_handler_decorator("routing_service")
    def calculate_multi_modal_distance(self, route_segments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate cumulative distance for multi-modal routes with enhanced accuracy
        
        Args:
            route_segments: List of route segments with different transport modes
            
        Returns:
            Dictionary with detailed distance breakdown by mode and total
        """
        try:
            # Use the enhanced distance calculator for multi-modal routes
            multi_modal_analysis = enhanced_distance_calculator.calculate_multi_modal_distance(route_segments)
            
            # Add additional routing service specific information
            enhanced_analysis = {
                **multi_modal_analysis,
                'route_efficiency_score': self._calculate_route_efficiency_score(multi_modal_analysis),
                'recommended_optimizations': self._suggest_route_optimizations(multi_modal_analysis),
                'fare_estimation': self._estimate_multi_modal_fare(multi_modal_analysis)
            }
            
            self.logger.info(f"Multi-modal distance calculated: {enhanced_analysis['total_distance_km']:.2f}km across {enhanced_analysis['number_of_modes']} transport modes")
            
            return enhanced_analysis
            
        except Exception as e:
            self.logger.error(f"Error calculating multi-modal distance: {str(e)}")
            return {
                'total_distance_km': 0.0,
                'distance_by_mode': {},
                'cumulative_tracking': [],
                'number_of_modes': 0,
                'primary_mode': 'unknown',
                'overall_accuracy': 0.0,
                'error': str(e)
            }

    def _calculate_route_efficiency_score(self, multi_modal_analysis: Dict[str, Any]) -> float:
        """Calculate efficiency score for multi-modal route"""
        try:
            total_distance = multi_modal_analysis.get('total_distance_km', 0)
            cumulative_tracking = multi_modal_analysis.get('cumulative_tracking', [])
            
            if not cumulative_tracking:
                return 0.5  # Neutral score
            
            # Calculate average path efficiency
            avg_efficiency = sum(seg.get('path_efficiency', 0.8) for seg in cumulative_tracking) / len(cumulative_tracking)
            
            # Penalize excessive mode changes
            mode_change_penalty = max(0, (multi_modal_analysis.get('number_of_modes', 1) - 2) * 0.1)
            
            # Reward shorter total distances (more efficient)
            distance_factor = max(0.1, 1.0 - (total_distance / 50.0))  # Normalize to 50km max
            
            efficiency_score = (avg_efficiency * 0.6 + distance_factor * 0.4) - mode_change_penalty
            
            return max(0.0, min(1.0, efficiency_score))
            
        except Exception:
            return 0.5

    def _suggest_route_optimizations(self, multi_modal_analysis: Dict[str, Any]) -> List[str]:
        """Suggest optimizations for multi-modal routes"""
        suggestions = []
        
        try:
            distance_by_mode = multi_modal_analysis.get('distance_by_mode', {})
            total_distance = multi_modal_analysis.get('total_distance_km', 0)
            
            # Suggest optimizations based on mode distribution
            if distance_by_mode.get('walking', 0) > 2.0:
                suggestions.append("Consider using cycling or public transport for long walking segments")
            
            if distance_by_mode.get('driving', 0) > 0 and distance_by_mode.get('transit', 0) > 0:
                if distance_by_mode['driving'] < 5.0:
                    suggestions.append("Short driving segment could be replaced with public transport")
            
            if multi_modal_analysis.get('number_of_modes', 0) > 3:
                suggestions.append("Route has many mode changes - consider simplifying")
            
            if total_distance > 30.0 and 'metro' not in distance_by_mode:
                suggestions.append("Consider using metro for long-distance travel")
            
            overall_accuracy = multi_modal_analysis.get('overall_accuracy', 1.0)
            if overall_accuracy < 0.8:
                suggestions.append("Route calculation has lower accuracy - verify waypoints")
                
        except Exception:
            pass
        
        return suggestions

    def _estimate_multi_modal_fare(self, multi_modal_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Estimate fare for multi-modal route"""
        try:
            distance_by_mode = multi_modal_analysis.get('distance_by_mode', {})
            
            # Basic fare estimation (would be enhanced with real fare data)
            fare_estimates = {
                'bus': distance_by_mode.get('bus', 0) * 2.0,  # ₹2 per km
                'metro': distance_by_mode.get('metro', 0) * 3.0,  # ₹3 per km
                'taxi': distance_by_mode.get('driving', 0) * 15.0,  # ₹15 per km
                'auto': distance_by_mode.get('driving', 0) * 12.0,  # ₹12 per km
                'walking': 0.0,
                'cycling': 0.0
            }
            
            total_estimated_fare = sum(fare_estimates.values())
            
            return {
                'fare_by_mode': fare_estimates,
                'total_estimated_fare': total_estimated_fare,
                'currency': 'INR',
                'estimation_accuracy': 'approximate'
            }
            
        except Exception:
            return {
                'fare_by_mode': {},
                'total_estimated_fare': 0.0,
                'currency': 'INR',
                'estimation_accuracy': 'unknown'
            }

# Global routing service instance
enhanced_routing_service = EnhancedRoutingService()

# Maintain backward compatibility
routing_service = enhanced_routing_service