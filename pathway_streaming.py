#!/usr/bin/env python3
"""
Pathway-based real-time transit data streaming service
Integrates traffic, taxi, and bus data for WayForge routing system
"""

import pathway as pw
import json
import time
import asyncio
import websockets
import requests
import aiohttp
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import logging
from datetime import datetime, timedelta
import math

from utils.common import setup_logging
from utils.error_handler import error_handler_decorator, performance_monitor
from utils.routing_service import routing_service, RoutePoint

logger = setup_logging("pathway_streaming")

@dataclass
class VehiclePosition:
    """Real-time vehicle position data"""
    vehicle_id: str
    route_id: str
    latitude: float
    longitude: float
    speed_kmh: float
    heading: int
    timestamp: datetime
    occupancy_status: str
    next_stop_id: Optional[str] = None
    delay_minutes: int = 0

@dataclass
class FareUpdate:
    """Dynamic fare pricing updates"""
    route_type: str  # bus, metro, taxi
    base_fare: float
    per_km_rate: float
    surge_multiplier: float
    effective_time: datetime
    zone: str

@dataclass
class TrafficData:
    """Real-time traffic information"""
    road_segment_id: str
    latitude: float
    longitude: float
    speed_kmh: float
    congestion_level: str  # low, medium, high, severe
    incident_reported: bool
    estimated_delay_minutes: int
    timestamp: datetime

@dataclass
class TaxiAvailability:
    """Real-time taxi availability and pricing"""
    service_provider: str  # uber, ola, rapido
    vehicle_type: str
    latitude: float
    longitude: float
    base_fare: float
    per_km_rate: float
    surge_multiplier: float
    eta_minutes: int
    available_count: int
    timestamp: datetime

@dataclass
class BusSchedule:
    """Real-time bus schedule information"""
    route_id: str
    bus_stop_id: str
    bus_stop_name: str
    next_arrival_time: datetime
    delay_minutes: int
    bus_number: str
    occupancy_level: str
    wheelchair_accessible: bool
    timestamp: datetime

@dataclass
class NearestStop:
    """Nearest stop/station information"""
    stop_id: str
    stop_name: str
    stop_type: str  # bus_stop, metro_station
    latitude: float
    longitude: float
    distance_meters: float
    walking_time_minutes: int

class PathwayTransitStreaming:
    """Main class for handling real-time transit data streaming with Pathway"""
    
    def __init__(self):
        """Initialize Pathway streaming service with API configurations"""
        # Initialize logger first
        self.logger = logger
        
        self.api_configs = {
            'bmtc_api': 'https://mybmtc.karnataka.gov.in/api',
            'bmrcl_api': 'https://english.bmrc.co.in/api',
            'traffic_api': 'https://api.traffic.bangalore.gov.in',
            'taxi_apis': {
                'uber': 'https://api.uber.com',
                'ola': 'https://api.olacabs.com',
                'rapido': 'https://api.rapido.bike'
            }
        }
        
        # Initialize table variables
        self.vehicle_table = None
        self.traffic_table = None
        self.taxi_table = None
        self.bus_schedule_table = None
        
        # Initialize real-time data fetchers
        self._initialize_data_fetchers()
        
        # Initialize Pathway tables for real-time data (commented out for now due to compatibility issues)
        # self.setup_pathway_tables()
        
        # WebSocket server for real-time updates
        self.websocket_clients = set()
        self.websocket_port = 8766
        
        # API configurations for external data sources
        self.api_config = {
            "traffic_api_key": "your_traffic_api_key",
            "taxi_apis": {
                "uber": "your_uber_api_key",
                "ola": "your_ola_api_key",
                "rapido": "your_rapido_api_key"
            },
            "bmtc_api_key": "your_bmtc_api_key"
        }
    
    def _initialize_data_fetchers(self):
        """Initialize real-time data fetchers"""
        try:
            from data_fetchers.bmtc_fetcher import BMTCDataFetcher
            from data_fetchers.bmrcl_fetcher import BMRCLDataFetcher
            from data_fetchers.traffic_fetcher import RealTimeTrafficFetcher
            
            self.bmtc_fetcher = BMTCDataFetcher()
            self.bmrcl_fetcher = BMRCLDataFetcher()
            self.traffic_fetcher = RealTimeTrafficFetcher()
            
            logger.info("Real-time data fetchers initialized successfully")
            
        except ImportError as e:
            logger.warning(f"Could not import data fetchers: {e}")
            self.bmtc_fetcher = None
            self.bmrcl_fetcher = None
            self.traffic_fetcher = None

    def setup_pathway_tables(self):
        """Setup Pathway tables for real-time data processing"""
        try:
            # Vehicle positions table
            self.vehicle_table = pw.Table.empty(
                vehicle_id=pw.column_definition(dtype=str),
                route_id=pw.column_definition(dtype=str),
                latitude=pw.column_definition(dtype=float),
                longitude=pw.column_definition(dtype=float),
                speed_kmh=pw.column_definition(dtype=float),
                timestamp=pw.column_definition(dtype=str),
                occupancy_status=pw.column_definition(dtype=str)
            )
            
            # Traffic data table
            self.traffic_table = pw.Table.empty(
                road_segment_id=pw.column_definition(dtype=str),
                latitude=pw.column_definition(dtype=float),
                longitude=pw.column_definition(dtype=float),
                speed_kmh=pw.column_definition(dtype=float),
                congestion_level=pw.column_definition(dtype=str),
                estimated_delay_minutes=pw.column_definition(dtype=int),
                timestamp=pw.column_definition(dtype=str)
            )
            
            # Taxi availability table
            self.taxi_table = pw.Table.empty(
                service_provider=pw.column_definition(dtype=str),
                vehicle_type=pw.column_definition(dtype=str),
                latitude=pw.column_definition(dtype=float),
                longitude=pw.column_definition(dtype=float),
                base_fare=pw.column_definition(dtype=float),
                surge_multiplier=pw.column_definition(dtype=float),
                eta_minutes=pw.column_definition(dtype=int),
                timestamp=pw.column_definition(dtype=str)
            )
            
            # Bus schedule table
            self.bus_schedule_table = pw.Table.empty(
                route_id=pw.column_definition(dtype=str),
                bus_stop_id=pw.column_definition(dtype=str),
                next_arrival_time=pw.column_definition(dtype=str),
                delay_minutes=pw.column_definition(dtype=int),
                occupancy_level=pw.column_definition(dtype=str),
                timestamp=pw.column_definition(dtype=str)
            )
            
            self.logger.info("Pathway tables initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error setting up Pathway tables: {e}")

    async def fetch_real_time_traffic(self, lat: float, lng: float, radius_km: float = 5) -> List[TrafficData]:
        """Fetch real-time traffic data from external APIs"""
        try:
            # Use the real-time traffic fetcher if available
            if hasattr(self, 'traffic_fetcher') and self.traffic_fetcher:
                traffic_data_response = await asyncio.to_thread(
                    self.traffic_fetcher.fetch_real_time_traffic
                )
                traffic_conditions = traffic_data_response.get('traffic_conditions', [])
                
                traffic_data = []
                for condition in traffic_conditions:
                    traffic_data.append(TrafficData(
                        road_segment_id=condition.get('segment_id', f"segment_{int(time.time())}"),
                        latitude=condition.get('latitude', lat),
                        longitude=condition.get('longitude', lng),
                        speed_kmh=condition.get('speed_kmh', 30),
                        congestion_level=condition.get('congestion_level', 'medium'),
                        incident_reported=condition.get('incident_reported', False),
                        estimated_delay_minutes=condition.get('estimated_delay_minutes', 0),
                        timestamp=datetime.now()
                    ))
                
                return traffic_data
            else:
                # Fallback to enhanced simulation
                traffic_data = []
                
                # Simulate traffic data for major roads in Bangalore
                major_roads = [
                    {"name": "Outer Ring Road", "lat": 12.9716, "lng": 77.5946, "congestion": "high"},
                    {"name": "Hosur Road", "lat": 12.9141, "lng": 77.6101, "congestion": "medium"},
                    {"name": "Bannerghatta Road", "lat": 12.9343, "lng": 77.6094, "congestion": "low"},
                    {"name": "Whitefield Road", "lat": 12.9698, "lng": 77.7500, "congestion": "medium"},
                    {"name": "Electronic City", "lat": 12.8456, "lng": 77.6603, "congestion": "high"}
                ]
                
                for road in major_roads:
                    distance = self._calculate_distance(lat, lng, road["lat"], road["lng"])
                    if distance <= radius_km:
                        congestion_delays = {"low": 2, "medium": 8, "high": 15, "severe": 25}
                        traffic_data.append(TrafficData(
                            road_segment_id=f"road_{road['name'].replace(' ', '_').lower()}",
                            latitude=road["lat"],
                            longitude=road["lng"],
                            speed_kmh=60 - (congestion_delays[road["congestion"]] * 2),
                            congestion_level=road["congestion"],
                            incident_reported=road["congestion"] in ["high", "severe"],
                            estimated_delay_minutes=congestion_delays[road["congestion"]],
                            timestamp=datetime.now()
                        ))
                
                return traffic_data
            
        except Exception as e:
            self.logger.error(f"Error fetching traffic data: {e}")
            return []

    async def fetch_taxi_availability(self, lat: float, lng: float) -> List[TaxiAvailability]:
        """Fetch real-time taxi availability and pricing"""
        try:
            taxi_data = []
            
            # Simulate taxi availability data
            providers = ["uber", "ola", "rapido"]
            vehicle_types = ["mini", "sedan", "suv", "auto", "bike"]
            
            for provider in providers:
                for vehicle_type in vehicle_types:
                    if provider == "rapido" and vehicle_type not in ["auto", "bike"]:
                        continue
                    
                    # Simulate availability around the location
                    for i in range(3):  # 3 vehicles per type per provider
                        surge = 1.0 + (0.5 * (hash(f"{provider}{vehicle_type}{i}") % 3))
                        base_fares = {"mini": 25, "sedan": 35, "suv": 50, "auto": 15, "bike": 10}
                        per_km_rates = {"mini": 12, "sedan": 15, "suv": 18, "auto": 8, "bike": 5}
                        
                        taxi_data.append(TaxiAvailability(
                            service_provider=provider,
                            vehicle_type=vehicle_type,
                            latitude=lat + (0.01 * (i - 1)),
                            longitude=lng + (0.01 * (i - 1)),
                            base_fare=base_fares[vehicle_type],
                            per_km_rate=per_km_rates[vehicle_type],
                            surge_multiplier=surge,
                            eta_minutes=2 + i,
                            available_count=5 - i,
                            timestamp=datetime.now()
                        ))
            
            return taxi_data
            
        except Exception as e:
            self.logger.error(f"Error fetching taxi availability: {e}")
            return []

    async def fetch_bus_schedules(self, lat: float, lng: float, radius_km: float = 2) -> List[BusSchedule]:
        """Fetch real-time bus schedules and availability"""
        try:
            bus_schedules = []
            
            # Use the real-time BMTC fetcher if available
            if hasattr(self, 'bmtc_fetcher') and self.bmtc_fetcher:
                # Get live bus positions from BMTC fetcher
                live_buses = await asyncio.to_thread(self.bmtc_fetcher.fetch_live_positions)
                
                # Convert live positions to bus schedules
                for bus in live_buses:
                    # Calculate distance from user location
                    distance = self._calculate_distance(lat, lng, bus['latitude'], bus['longitude'])
                    if distance <= radius_km:
                        # Estimate arrival time based on distance and speed
                        if bus['speed_kmh'] > 0:
                            eta_minutes = int((distance / bus['speed_kmh']) * 60)
                        else:
                            eta_minutes = 5 + (hash(bus['vehicle_id']) % 15)
                        
                        arrival_time = datetime.now() + timedelta(minutes=eta_minutes)
                        
                        bus_schedules.append(BusSchedule(
                            route_id=bus['route_id'],
                            bus_stop_id=f"BS_{bus['vehicle_id'][-3:]}",
                            bus_stop_name=f"Stop near {bus['vehicle_id']}",
                            next_arrival_time=arrival_time,
                            delay_minutes=bus.get('delay_minutes', 0),
                            bus_number=bus['vehicle_id'],
                            occupancy_level=bus.get('occupancy_status', 'medium'),
                            wheelchair_accessible=(hash(bus['vehicle_id']) % 2 == 0),
                            timestamp=datetime.now()
                        ))
            else:
                # Fallback to static data simulation
                try:
                    with open('data/static/bmtc_static.json', 'r') as f:
                        bmtc_data = json.load(f)
                        
                    # Find nearby bus stops
                    nearby_stops = []
                    for route in bmtc_data.get('routes', []):
                        for stop in route.get('stops', []):
                            distance = self._calculate_distance(lat, lng, stop['lat'], stop['lng'])
                            if distance <= radius_km:
                                nearby_stops.append({
                                    'route_id': route['route_id'],
                                    'stop_id': stop['stop_id'],
                                    'stop_name': stop['stop_name'],
                                    'lat': stop['lat'],
                                    'lng': stop['lng']
                                })
                    
                    # Generate schedule data for nearby stops
                    for stop in nearby_stops[:10]:  # Limit to 10 nearest stops
                        for i in range(3):  # 3 upcoming buses per stop
                            arrival_time = datetime.now() + timedelta(minutes=5 + (i * 10))
                            delay = (hash(f"{stop['stop_id']}{i}") % 5)
                            
                            bus_schedules.append(BusSchedule(
                                route_id=stop['route_id'],
                                bus_stop_id=stop['stop_id'],
                                bus_stop_name=stop['stop_name'],
                                next_arrival_time=arrival_time,
                                delay_minutes=delay,
                                bus_number=f"KA-01-{1000 + i}",
                                occupancy_level=["low", "medium", "high"][i % 3],
                                wheelchair_accessible=i % 2 == 0,
                                timestamp=datetime.now()
                            ))
                            
                except FileNotFoundError:
                    self.logger.warning("BMTC static data file not found")
            
            return bus_schedules
            
        except Exception as e:
            self.logger.error(f"Error fetching bus schedules: {e}")
            return []

    def find_nearest_stops(self, lat: float, lng: float, max_distance_km: float = 1.0) -> List[NearestStop]:
        """Find nearest bus stops and metro stations"""
        try:
            nearest_stops = []
            
            # Load bus stops
            try:
                with open('data/static/bmtc_static.json', 'r') as f:
                    bmtc_data = json.load(f)
                    
                for route in bmtc_data.get('routes', []):
                    for stop in route.get('stops', []):
                        distance = self._calculate_distance(lat, lng, stop['lat'], stop['lng'])
                        if distance <= max_distance_km:
                            walking_time = int(distance * 1000 / 80)  # 80 m/min walking speed
                            nearest_stops.append(NearestStop(
                                stop_id=stop['stop_id'],
                                stop_name=stop['stop_name'],
                                stop_type="bus_stop",
                                latitude=stop['lat'],
                                longitude=stop['lng'],
                                distance_meters=distance * 1000,
                                walking_time_minutes=walking_time
                            ))
            except FileNotFoundError:
                pass
            
            # Load metro stations
            try:
                with open('data/static/bmrcl_static.json', 'r') as f:
                    bmrcl_data = json.load(f)
                    
                for station in bmrcl_data.get('stations', []):
                    distance = self._calculate_distance(lat, lng, station['lat'], station['lng'])
                    if distance <= max_distance_km:
                        walking_time = int(distance * 1000 / 80)  # 80 m/min walking speed
                        nearest_stops.append(NearestStop(
                            stop_id=station['station_id'],
                            stop_name=station['station_name'],
                            stop_type="metro_station",
                            latitude=station['lat'],
                            longitude=station['lng'],
                            distance_meters=distance * 1000,
                            walking_time_minutes=walking_time
                        ))
            except FileNotFoundError:
                pass
            
            # Sort by distance
            nearest_stops.sort(key=lambda x: x.distance_meters)
            return nearest_stops[:10]  # Return top 10 nearest stops
            
        except Exception as e:
            self.logger.error(f"Error finding nearest stops: {e}")
            return []

    def _calculate_distance(self, lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate distance between two points using Haversine formula"""
        R = 6371  # Earth's radius in kilometers
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lng = math.radians(lng2 - lng1)
        
        a = (math.sin(delta_lat / 2) ** 2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lng / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c

    async def get_comprehensive_transit_data(self, lat: float, lng: float) -> Dict[str, Any]:
        """Get comprehensive real-time transit data for a location"""
        try:
            # Fetch all data concurrently
            traffic_data, taxi_data, bus_schedules, nearest_stops = await asyncio.gather(
                self.fetch_real_time_traffic(lat, lng),
                self.fetch_taxi_availability(lat, lng),
                self.fetch_bus_schedules(lat, lng),
                asyncio.create_task(asyncio.to_thread(self.find_nearest_stops, lat, lng))
            )
            
            return {
                "traffic": [asdict(t) for t in traffic_data],
                "taxis": [asdict(t) for t in taxi_data],
                "bus_schedules": [asdict(b) for b in bus_schedules],
                "nearest_stops": [asdict(n) for n in nearest_stops],
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting comprehensive transit data: {e}")
            return {}

    # Wrapper methods for web server compatibility
    def get_traffic_data(self, lat: float, lng: float, radius_km: float = 5) -> List[Dict[str, Any]]:
        """Synchronous wrapper for traffic data"""
        try:
            # Check if there's already an event loop running
            try:
                loop = asyncio.get_running_loop()
                # If we're in an async context, create a task
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, self.fetch_real_time_traffic(lat, lng, radius_km))
                    traffic_data = future.result(timeout=10)
            except RuntimeError:
                # No event loop running, safe to use asyncio.run
                traffic_data = asyncio.run(self.fetch_real_time_traffic(lat, lng, radius_km))
            return [asdict(t) for t in traffic_data]
        except Exception as e:
            self.logger.error(f"Error getting traffic data: {e}")
            return []

    def get_bus_schedules(self, lat: float, lng: float, radius_km: float = 2) -> List[Dict[str, Any]]:
        """Synchronous wrapper for bus schedules"""
        try:
            try:
                loop = asyncio.get_running_loop()
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, self.fetch_bus_schedules(lat, lng, radius_km))
                    bus_schedules = future.result(timeout=10)
            except RuntimeError:
                bus_schedules = asyncio.run(self.fetch_bus_schedules(lat, lng, radius_km))
            return [asdict(b) for b in bus_schedules]
        except Exception as e:
            self.logger.error(f"Error getting bus schedules: {e}")
            return []

    def get_taxi_availability(self, lat: float, lng: float, radius_km: float = 2) -> List[Dict[str, Any]]:
        """Synchronous wrapper for taxi availability"""
        try:
            try:
                loop = asyncio.get_running_loop()
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, self.fetch_taxi_availability(lat, lng))
                    taxi_data = future.result(timeout=10)
            except RuntimeError:
                taxi_data = asyncio.run(self.fetch_taxi_availability(lat, lng))
            return [asdict(t) for t in taxi_data]
        except Exception as e:
            self.logger.error(f"Error getting taxi availability: {e}")
            return []

    def get_current_traffic(self) -> List[Dict[str, Any]]:
        """Get current traffic conditions for chatbot"""
        try:
            # Use Bangalore city center as default location
            return self.get_traffic_data(12.9716, 77.5946, 10)
        except Exception as e:
            self.logger.error(f"Error getting current traffic: {e}")
            return []

    def get_current_fares(self) -> Dict[str, Any]:
        """Get current fare information for chatbot"""
        try:
            return {
                'bus_base': 5,
                'metro_min': 10,
                'taxi_base': 50,
                'last_updated': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error getting current fares: {e}")
            return {}

    def get_vehicle_positions(self) -> List[Dict[str, Any]]:
        """Get current vehicle positions for chatbot"""
        try:
            vehicle_positions = []
            
            # Get real-time bus positions
            if hasattr(self, 'bmtc_fetcher') and self.bmtc_fetcher:
                try:
                    bus_data = self.bmtc_fetcher.fetch_live_positions()
                    if bus_data and 'entity' in bus_data and isinstance(bus_data['entity'], list):
                        bus_positions = bus_data['entity']
                        for bus in bus_positions[:5]:  # Limit to 5 buses for performance
                            if isinstance(bus, dict) and 'vehicle' in bus:
                                vehicle_info = bus['vehicle']
                                position = vehicle_info.get('position', {})
                                vehicle_positions.append({
                                    'type': 'bus',
                                    'route': vehicle_info.get('trip', {}).get('route_id', 'Unknown'),
                                    'vehicle_id': vehicle_info.get('vehicle', {}).get('id', 'Unknown'),
                                    'lat': position.get('latitude', 12.9716),
                                    'lng': position.get('longitude', 77.5946),
                                    'speed': position.get('speed', 0),
                                    'occupancy': vehicle_info.get('occupancy_status', 'medium')
                                })
                except Exception as e:
                    self.logger.error(f"Error fetching BMTC positions: {e}")
            
            # Get real-time metro positions
            if hasattr(self, 'bmrcl_fetcher') and self.bmrcl_fetcher:
                try:
                    metro_data = self.bmrcl_fetcher.fetch_live_positions()
                    if metro_data and 'entity' in metro_data and isinstance(metro_data['entity'], list):
                        metro_positions = metro_data['entity']
                        for metro in metro_positions[:3]:  # Limit to 3 metros for performance
                            if isinstance(metro, dict) and 'vehicle' in metro:
                                vehicle_info = metro['vehicle']
                                position = vehicle_info.get('position', {})
                                vehicle_positions.append({
                                    'type': 'metro',
                                    'line': vehicle_info.get('trip', {}).get('route_id', 'Unknown'),
                                    'vehicle_id': vehicle_info.get('vehicle', {}).get('id', 'Unknown'),
                                    'lat': position.get('latitude', 12.9716),
                                    'lng': position.get('longitude', 77.5946),
                                    'speed': position.get('speed', 0),
                                    'occupancy': vehicle_info.get('occupancy_status', 'medium'),
                                    'current_station': vehicle_info.get('current_station', 'Unknown'),
                                    'next_station': vehicle_info.get('next_station', 'Unknown')
                                })
                except Exception as e:
                    self.logger.error(f"Error fetching BMRCL positions: {e}")
            
            # If no real data available, return mock data
            if not vehicle_positions:
                vehicle_positions = [
                    {'type': 'bus', 'route': 'V-500', 'lat': 12.9716, 'lng': 77.5946},
                    {'type': 'metro', 'line': 'Purple', 'lat': 12.9716, 'lng': 77.5946},
                    {'type': 'taxi', 'provider': 'ola', 'lat': 12.9716, 'lng': 77.5946}
                ]
            
            return vehicle_positions
            
        except Exception as e:
            self.logger.error(f"Error getting vehicle positions: {e}")
            return []

    def get_nearest_stops(self, lat: float, lng: float, max_distance_km: float = 1.0) -> List[Dict[str, Any]]:
        """Wrapper method for web server compatibility - get nearest stops"""
        try:
            stops = self.find_nearest_stops(lat, lng, max_distance_km)
            return [asdict(stop) for stop in stops]
        except Exception as e:
            self.logger.error(f"Error getting nearest stops: {e}")
            return []

    async def get_realtime_updates(self, source_lat: float, source_lng: float,
                                 dest_lat: float, dest_lng: float) -> Dict[str, Any]:
        """Get comprehensive real-time updates for transport options"""
        try:
            # Get real-time data from various sources
            traffic_data = await self.fetch_real_time_traffic(source_lat, source_lng, 5)
            taxi_data = await self.fetch_taxi_availability(source_lat, source_lng)
            bus_schedules = await self.fetch_bus_schedules(source_lat, source_lng)
            
            # Simulate real-time bus and metro updates
            bus_updates = await self._get_realtime_bus_updates(source_lat, source_lng, dest_lat, dest_lng)
            metro_updates = await self._get_realtime_metro_updates(source_lat, source_lng, dest_lat, dest_lng)
            service_alerts = await self._get_service_alerts(source_lat, source_lng, dest_lat, dest_lng)
            
            updates = {
                'buses': bus_updates,
                'metros': metro_updates,
                'traffic': [asdict(t) for t in traffic_data],
                'taxi_availability': [asdict(t) for t in taxi_data],
                'bus_schedules': [asdict(b) for b in bus_schedules],
                'alerts': service_alerts,
                'timestamp': datetime.now().isoformat()
            }
            
            return updates
            
        except Exception as e:
            self.logger.error(f"Error getting real-time updates: {e}")
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}

    async def _get_realtime_bus_updates(self, source_lat: float, source_lng: float,
                                      dest_lat: float, dest_lng: float) -> List[Dict[str, Any]]:
        """Get real-time bus position and schedule updates"""
        try:
            bus_updates = []
            
            # Use real-time BMTC fetcher if available
            if hasattr(self, 'bmtc_fetcher') and self.bmtc_fetcher:
                live_buses = await asyncio.to_thread(self.bmtc_fetcher.fetch_live_positions)
                
                for bus in live_buses:
                    # Calculate distance from source
                    distance_from_source = self._calculate_distance(
                        source_lat, source_lng, bus['latitude'], bus['longitude']
                    )
                    
                    # Only include buses within 5km of the route
                    if distance_from_source <= 5.0:
                        # Estimate ETA based on distance and speed
                        if bus['speed_kmh'] > 0:
                            eta_minutes = int((distance_from_source / bus['speed_kmh']) * 60)
                        else:
                            eta_minutes = 5 + (hash(bus['vehicle_id']) % 15)
                        
                        bus_updates.append({
                            'route_id': bus['route_id'],
                            'vehicle_id': bus['vehicle_id'],
                            'current_lat': bus['latitude'],
                            'current_lng': bus['longitude'],
                            'delay_minutes': bus.get('delay_minutes', 0),
                            'next_stop': f"Stop near {bus['vehicle_id'][-3:]}",
                            'eta_minutes': eta_minutes,
                            'occupancy': bus.get('occupancy_status', 'MEDIUM').upper(),
                            'speed_kmh': bus['speed_kmh']
                        })
                
                return bus_updates[:5]  # Limit to 5 buses
            else:
                # Fallback to simulated data
                bus_updates = [
                    {
                        'route_id': 'BMTC_356E',
                        'vehicle_id': 'KA01F1234',
                        'current_lat': source_lat + 0.001,
                        'current_lng': source_lng + 0.001,
                        'delay_minutes': 3,
                        'next_stop': 'Silk Board',
                        'eta_minutes': 8,
                        'occupancy': 'MEDIUM'
                    },
                    {
                        'route_id': 'BMTC_500D',
                        'vehicle_id': 'KA01F5678',
                        'current_lat': source_lat + 0.002,
                        'current_lng': source_lng - 0.001,
                        'delay_minutes': -2,
                        'next_stop': 'Electronic City',
                        'eta_minutes': 12,
                        'occupancy': 'LOW'
                    }
                ]
                
                return bus_updates
            
        except Exception as e:
            self.logger.error(f"Error getting real-time bus updates: {e}")
            return []

    async def _get_realtime_metro_updates(self, source_lat: float, source_lng: float,
                                        dest_lat: float, dest_lng: float) -> List[Dict[str, Any]]:
        """Get real-time metro position and schedule updates"""
        try:
            metro_updates = []
            
            # Use real-time BMRCL fetcher if available
            if hasattr(self, 'bmrcl_fetcher') and self.bmrcl_fetcher:
                live_metros = await asyncio.to_thread(self.bmrcl_fetcher.fetch_live_positions)
                
                for metro in live_metros:
                    # Calculate distance from source
                    distance_from_source = self._calculate_distance(
                        source_lat, source_lng, metro['latitude'], metro['longitude']
                    )
                    
                    # Only include metros within 10km of the route
                    if distance_from_source <= 10.0:
                        # Estimate ETA based on distance and speed
                        if metro['speed_kmh'] > 0:
                            eta_minutes = int((distance_from_source / metro['speed_kmh']) * 60)
                        else:
                            eta_minutes = 3 + (hash(metro['vehicle_id']) % 10)
                        
                        metro_updates.append({
                            'line': metro['route_id'],
                            'train_id': metro['vehicle_id'],
                            'current_station': metro.get('current_station', 'Unknown'),
                            'next_station': metro.get('next_station', 'Unknown'),
                            'delay_minutes': metro.get('delay_minutes', 0),
                            'eta_minutes': eta_minutes,
                            'direction': metro.get('direction', 'Unknown'),
                            'occupancy': metro.get('occupancy_status', 'MEDIUM').upper(),
                            'current_lat': metro['latitude'],
                            'current_lng': metro['longitude'],
                            'speed_kmh': metro['speed_kmh']
                        })
                
                return metro_updates[:3]  # Limit to 3 metros
            else:
                # Fallback to simulated data
                metro_updates = [
                    {
                        'line': 'Purple Line',
                        'train_id': 'PL_001',
                        'current_station': 'Baiyappanahalli',
                        'next_station': 'Swami Vivekananda Road',
                        'delay_minutes': 1,
                        'eta_minutes': 4,
                        'direction': 'Mysore Road',
                        'occupancy': 'HIGH'
                    },
                    {
                        'line': 'Green Line',
                        'train_id': 'GL_003',
                        'current_station': 'Majestic',
                        'next_station': 'City Railway Station',
                        'delay_minutes': 0,
                        'eta_minutes': 6,
                        'direction': 'Nagasandra',
                        'occupancy': 'MEDIUM'
                    }
                ]
                
                return metro_updates
            
        except Exception as e:
            self.logger.error(f"Error getting real-time metro updates: {e}")
            return []

    async def _get_service_alerts(self, source_lat: float, source_lng: float,
                                dest_lat: float, dest_lng: float) -> List[Dict[str, Any]]:
        """Get service alerts and disruptions"""
        try:
            # Simulate service alerts
            alerts = [
                {
                    'type': 'TRAFFIC',
                    'severity': 'MEDIUM',
                    'message': 'Heavy traffic on Hosur Road due to construction',
                    'affected_routes': ['BMTC_356E', 'BMTC_500D'],
                    'start_time': (datetime.now() - timedelta(hours=2)).isoformat(),
                    'estimated_end': (datetime.now() + timedelta(hours=1)).isoformat()
                },
                {
                    'type': 'METRO',
                    'severity': 'LOW',
                    'message': 'Purple Line experiencing minor delays',
                    'affected_routes': ['Purple Line'],
                    'start_time': (datetime.now() - timedelta(minutes=30)).isoformat(),
                    'estimated_end': (datetime.now() + timedelta(minutes=15)).isoformat()
                }
            ]
            
            return alerts
            
        except Exception as e:
            self.logger.error(f"Error getting service alerts: {e}")
            return []

    @error_handler_decorator
    async def generate_mock_vehicle_stream(self):
        """Generate mock vehicle position data for testing"""
        while True:
            try:
                # Simulate vehicle positions for Bangalore routes
                mock_vehicles = [
                    VehiclePosition(
                        vehicle_id=f"BUS_{i:03d}",
                        route_id=f"ROUTE_{i % 10}",
                        latitude=12.9716 + (0.01 * (i % 10)),
                        longitude=77.5946 + (0.01 * (i % 10)),
                        speed_kmh=25 + (i % 20),
                        heading=i % 360,
                        timestamp=datetime.now(),
                        occupancy_status=["low", "medium", "high"][i % 3],
                        next_stop_id=f"STOP_{i % 50}",
                        delay_minutes=i % 10
                    ) for i in range(20)
                ]
                
                # Convert to Pathway format and update table
                for vehicle in mock_vehicles:
                    vehicle_data = asdict(vehicle)
                    vehicle_data['timestamp'] = vehicle_data['timestamp'].isoformat()
                    
                    # In a real implementation, you would update the Pathway table here
                    # self.vehicle_table = self.vehicle_table.update_rows(...)
                
                await asyncio.sleep(30)  # Update every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in mock vehicle stream: {e}")
                await asyncio.sleep(5)

    @performance_monitor
    async def process_vehicle_stream(self, vehicle_data: Dict[str, Any]):
        """Process incoming vehicle position data"""
        try:
            vehicle = VehiclePosition(**vehicle_data)
            
            # Update Pathway table
            if self.vehicle_table is not None:
                # Process with Pathway
                pass
            
            # Broadcast to WebSocket clients
            await self.broadcast_update({
                "type": "vehicle_update",
                "data": asdict(vehicle)
            })
            
        except Exception as e:
            self.logger.error(f"Error processing vehicle stream: {e}")

    async def broadcast_update(self, data: Dict[str, Any]):
        """Broadcast updates to all connected WebSocket clients"""
        if self.websocket_clients:
            message = json.dumps(data, default=str)
            disconnected_clients = set()
            
            for client in self.websocket_clients:
                try:
                    await client.send(message)
                except websockets.exceptions.ConnectionClosed:
                    disconnected_clients.add(client)
                except Exception as e:
                    self.logger.error(f"Error broadcasting to client: {e}")
                    disconnected_clients.add(client)
            
            # Remove disconnected clients
            self.websocket_clients -= disconnected_clients

    async def handle_websocket_client(self, websocket, path):
        """Handle WebSocket client connections"""
        self.websocket_clients.add(websocket)
        self.logger.info(f"New WebSocket client connected. Total clients: {len(self.websocket_clients)}")
        
        try:
            await websocket.wait_closed()
        except Exception as e:
            self.logger.error(f"WebSocket client error: {e}")
        finally:
            self.websocket_clients.discard(websocket)
            self.logger.info(f"WebSocket client disconnected. Total clients: {len(self.websocket_clients)}")

    @error_handler_decorator
    async def enhance_route_with_realtime_data(self, route_data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance route data with real-time information"""
        try:
            enhanced_route = route_data.copy()
            
            # Get real-time data for route points
            if 'waypoints' in route_data:
                for waypoint in route_data['waypoints']:
                    lat, lng = waypoint['lat'], waypoint['lng']
                    
                    # Get comprehensive transit data
                    transit_data = await self.get_comprehensive_transit_data(lat, lng)
                    waypoint['real_time_data'] = transit_data
            
            # Add traffic delays to route segments
            if 'segments' in route_data:
                for segment in route_data['segments']:
                    # Calculate traffic impact on segment
                    traffic_delay = await self._calculate_traffic_delay(segment)
                    segment['traffic_delay_minutes'] = traffic_delay
                    segment['adjusted_duration'] = segment.get('duration', 0) + traffic_delay
            
            enhanced_route['last_updated'] = datetime.now().isoformat()
            return enhanced_route
            
        except Exception as e:
            self.logger.error(f"Error enhancing route with real-time data: {e}")
            return route_data

    async def _calculate_traffic_delay(self, segment: Dict[str, Any]) -> int:
        """Calculate traffic delay for a route segment"""
        try:
            # Get traffic data for segment
            start_lat, start_lng = segment.get('start_lat', 0), segment.get('start_lng', 0)
            traffic_data = await self.fetch_real_time_traffic(start_lat, start_lng, radius_km=1)
            
            if traffic_data:
                # Calculate average delay
                total_delay = sum(t.estimated_delay_minutes for t in traffic_data)
                return total_delay // len(traffic_data)
            
            return 0
            
        except Exception as e:
            self.logger.error(f"Error calculating traffic delay: {e}")
            return 0

    async def start_streaming_service(self):
        """Start the real-time streaming service"""
        try:
            self.setup_pathway_tables()
            
            # Start background tasks
            tasks = [
                asyncio.create_task(self.generate_mock_vehicle_stream()),
                websockets.serve(self.handle_websocket_client, "localhost", 8765)
            ]
            
            self.logger.info("Pathway streaming service started")
            await asyncio.gather(*tasks)
            
        except Exception as e:
            self.logger.error(f"Error starting streaming service: {e}")

# Global streaming instance
pathway_streaming = PathwayTransitStreaming()

if __name__ == "__main__":
    asyncio.run(pathway_streaming.start_streaming_service())