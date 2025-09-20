#!/usr/bin/env python3
"""
Pathway Streaming Pipeline for Real-time Transit Data
Integrates live vehicle positions, fare updates, and route changes
"""

import pathway as pw
import json
import time
import asyncio
import websockets
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import logging
from datetime import datetime, timedelta

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
    """Dynamic fare update"""
    route_type: str  # bus, metro, taxi
    base_fare: float
    per_km_rate: float
    surge_multiplier: float
    effective_time: datetime
    zone: str



class PathwayTransitStreaming:
    """Pathway-based real-time transit data streaming"""
    
    def __init__(self):
        self.logger = setup_logging("pathway_streaming")
        self.websocket_clients = set()
        self.vehicle_positions = {}
        self.fare_updates = {}
        
        # Pathway tables for streaming data
        self.setup_pathway_tables()
        
    def setup_pathway_tables(self):
        """Setup Pathway streaming tables"""
        try:
            # Vehicle positions stream
            self.vehicle_stream = pw.io.kafka.read(
                rdkafka_settings={
                    "bootstrap.servers": "localhost:9092",
                    "group.id": "transit_vehicles",
                    "auto.offset.reset": "latest"
                },
                topic="vehicle_positions",
                format="json",
                schema=pw.schema_from_types(
                    vehicle_id=str,
                    route_id=str,
                    latitude=float,
                    longitude=float,
                    speed_kmh=float,
                    heading=int,
                    timestamp=str,
                    occupancy_status=str
                )
            )
            
            # Fare updates stream
            self.fare_stream = pw.io.kafka.read(
                rdkafka_settings={
                    "bootstrap.servers": "localhost:9092",
                    "group.id": "transit_fares",
                    "auto.offset.reset": "latest"
                },
                topic="fare_updates",
                format="json",
                schema=pw.schema_from_types(
                    route_type=str,
                    base_fare=float,
                    per_km_rate=float,
                    surge_multiplier=float,
                    effective_time=str,
                    zone=str
                )
            )
            
            # Route updates stream
            self.route_stream = pw.io.kafka.read(
                rdkafka_settings={
                    "bootstrap.servers": "localhost:9092",
                    "group.id": "transit_routes",
                    "auto.offset.reset": "latest"
                },
                topic="route_updates",
                format="json",
                schema=pw.schema_from_types(
                    route_id=str,
                    route_type=str,
                    status=str,
                    alternative_routes=str,
                    message=str,
                    effective_time=str
                )
            )
            
            self.logger.info("Pathway streaming tables initialized")
            
        except Exception as e:
            self.logger.error(f"Error setting up Pathway tables: {str(e)}")
            # Fallback to mock data generation
            self.setup_mock_streams()
    
    def setup_mock_streams(self):
        """Setup mock data streams for development"""
        self.logger.info("Setting up mock data streams")
        
        # Generate mock vehicle positions
        asyncio.create_task(self.generate_mock_vehicles())
        asyncio.create_task(self.generate_mock_fares())
    
    async def generate_mock_vehicles(self):
        """Generate mock vehicle position data"""
        try:
            # Bangalore bus routes (simplified)
            routes = [
                {"id": "335E", "name": "Kempegowda Bus Station to Electronic City", "type": "bus"},
                {"id": "500K", "name": "Kempegowda Bus Station to Kengeri", "type": "bus"},
                {"id": "201", "name": "Shivajinagar to Banashankari", "type": "bus"},
                {"id": "BLUE", "name": "Nagasandra to Baiyappanahalli", "type": "metro"},
                {"id": "PURPLE", "name": "Challaghatta to Whitefield", "type": "metro"}
            ]
            
            # Generate vehicles for each route
            vehicles = []
            for route in routes:
                for i in range(3 if route["type"] == "bus" else 2):
                    vehicle_id = f"{route['id']}_V{i+1}"
                    vehicles.append({
                        "vehicle_id": vehicle_id,
                        "route_id": route["id"],
                        "route_type": route["type"],
                        "base_lat": 12.9716 + (i * 0.01),
                        "base_lng": 77.5946 + (i * 0.01),
                        "direction": 1 if i % 2 == 0 else -1
                    })
            
            while True:
                current_time = datetime.now()
                
                for vehicle in vehicles:
                    # Simulate vehicle movement
                    time_factor = (current_time.timestamp() % 3600) / 3600  # Hour cycle
                    
                    # Move vehicle along a simplified route
                    lat_offset = vehicle["direction"] * time_factor * 0.05
                    lng_offset = vehicle["direction"] * time_factor * 0.03
                    
                    position = VehiclePosition(
                        vehicle_id=vehicle["vehicle_id"],
                        route_id=vehicle["route_id"],
                        latitude=vehicle["base_lat"] + lat_offset,
                        longitude=vehicle["base_lng"] + lng_offset,
                        speed_kmh=25 + (time_factor * 15),  # 25-40 kmh
                        heading=90 if vehicle["direction"] > 0 else 270,
                        timestamp=current_time,
                        occupancy_status=["EMPTY", "MANY_SEATS_AVAILABLE", "FEW_SEATS_AVAILABLE", "STANDING_ROOM_ONLY"][int(time_factor * 4)],
                        delay_minutes=int(time_factor * 10) - 5  # -5 to +5 minutes
                    )
                    
                    self.vehicle_positions[vehicle["vehicle_id"]] = position
                
                # Broadcast to WebSocket clients
                await self.broadcast_vehicle_updates()
                
                await asyncio.sleep(5)  # Update every 5 seconds
                
        except Exception as e:
            self.logger.error(f"Error generating mock vehicles: {str(e)}")
    
    async def generate_mock_fares(self):
        """Generate mock fare updates"""
        try:
            base_fares = {
                "bus_ordinary": {"base": 8, "per_km": 1.5},
                "bus_ac": {"base": 15, "per_km": 2.0},
                "bus_vajra": {"base": 25, "per_km": 3.0},
                "metro": {"base": 10, "per_km": 2.5},
                "taxi": {"base": 25, "per_km": 12.0}
            }
            
            while True:
                current_time = datetime.now()
                
                for route_type, fare_info in base_fares.items():
                    # Simulate surge pricing during peak hours
                    hour = current_time.hour
                    surge_multiplier = 1.0
                    
                    if route_type == "taxi":
                        if 8 <= hour <= 10 or 17 <= hour <= 20:  # Peak hours
                            surge_multiplier = 1.5
                        elif 22 <= hour or hour <= 6:  # Night hours
                            surge_multiplier = 1.8
                    
                    fare_update = FareUpdate(
                        route_type=route_type,
                        base_fare=fare_info["base"] * surge_multiplier,
                        per_km_rate=fare_info["per_km"] * surge_multiplier,
                        surge_multiplier=surge_multiplier,
                        effective_time=current_time,
                        zone="bangalore_central"
                    )
                    
                    self.fare_updates[route_type] = fare_update
                
                # Broadcast fare updates
                await self.broadcast_fare_updates()
                
                await asyncio.sleep(60)  # Update every minute
                
        except Exception as e:
            self.logger.error(f"Error generating mock fares: {str(e)}")
    

    
    @error_handler_decorator("pathway_streaming")
    @performance_monitor("pathway_streaming")
    async def process_vehicle_stream(self):
        """Process vehicle position stream with Pathway"""
        try:
            # Transform vehicle data
            enriched_vehicles = self.vehicle_stream.select(
                vehicle_id=pw.this.vehicle_id,
                route_id=pw.this.route_id,
                latitude=pw.this.latitude,
                longitude=pw.this.longitude,
                speed_kmh=pw.this.speed_kmh,
                heading=pw.this.heading,
                timestamp=pw.this.timestamp,
                occupancy_status=pw.this.occupancy_status,
                # Add computed fields
                is_delayed=pw.this.speed_kmh < 10,
                location_zone=pw.apply(self._determine_zone, pw.this.latitude, pw.this.longitude)
            )
            
            # Output to WebSocket
            pw.io.null.write(enriched_vehicles)
            
            self.logger.info("Vehicle stream processing started")
            
        except Exception as e:
            self.logger.error(f"Error processing vehicle stream: {str(e)}")
    
    def _determine_zone(self, lat: float, lng: float) -> str:
        """Determine zone based on coordinates"""
        # Simplified zone determination for Bangalore
        if 12.95 <= lat <= 13.05 and 77.55 <= lng <= 77.65:
            return "central"
        elif 12.85 <= lat <= 12.95 and 77.45 <= lng <= 77.55:
            return "south"
        elif 13.05 <= lat <= 13.15 and 77.55 <= lng <= 77.65:
            return "north"
        elif 12.95 <= lat <= 13.05 and 77.65 <= lng <= 77.75:
            return "east"
        else:
            return "outer"
    
    async def broadcast_vehicle_updates(self):
        """Broadcast vehicle position updates to WebSocket clients"""
        if not self.websocket_clients:
            return
        
        try:
            vehicle_data = {
                "type": "vehicle_update",
                "timestamp": datetime.now().isoformat(),
                "vehicles": [asdict(pos) for pos in self.vehicle_positions.values()]
            }
            
            # Convert datetime objects to strings
            for vehicle in vehicle_data["vehicles"]:
                vehicle["timestamp"] = vehicle["timestamp"].isoformat()
            
            message = json.dumps(vehicle_data)
            
            # Send to all connected clients
            disconnected_clients = set()
            for client in self.websocket_clients:
                try:
                    await client.send(message)
                except websockets.exceptions.ConnectionClosed:
                    disconnected_clients.add(client)
            
            # Remove disconnected clients
            self.websocket_clients -= disconnected_clients
            
        except Exception as e:
            self.logger.error(f"Error broadcasting vehicle updates: {str(e)}")
    
    async def broadcast_fare_updates(self):
        """Broadcast fare updates to WebSocket clients"""
        if not self.websocket_clients:
            return
        
        try:
            fare_data = {
                "type": "fare_update",
                "timestamp": datetime.now().isoformat(),
                "fares": {}
            }
            
            for route_type, fare in self.fare_updates.items():
                fare_dict = asdict(fare)
                fare_dict["effective_time"] = fare_dict["effective_time"].isoformat()
                fare_data["fares"][route_type] = fare_dict
            
            message = json.dumps(fare_data)
            
            # Send to all connected clients
            disconnected_clients = set()
            for client in self.websocket_clients:
                try:
                    await client.send(message)
                except websockets.exceptions.ConnectionClosed:
                    disconnected_clients.add(client)
            
            self.websocket_clients -= disconnected_clients
            
        except Exception as e:
            self.logger.error(f"Error broadcasting fare updates: {str(e)}")
    

    
    async def handle_websocket_client(self, websocket, path):
        """Handle WebSocket client connections"""
        try:
            self.websocket_clients.add(websocket)
            self.logger.info(f"WebSocket client connected. Total clients: {len(self.websocket_clients)}")
            
            # Send initial data
            await self.send_initial_data(websocket)
            
            # Keep connection alive
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self.handle_client_message(websocket, data)
                except json.JSONDecodeError:
                    await websocket.send(json.dumps({"error": "Invalid JSON"}))
                    
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self.websocket_clients.discard(websocket)
            self.logger.info(f"WebSocket client disconnected. Total clients: {len(self.websocket_clients)}")
    
    async def send_initial_data(self, websocket):
        """Send initial data to newly connected client"""
        try:
            # Send current vehicle positions
            if self.vehicle_positions:
                await self.broadcast_vehicle_updates()
            
            # Send current fare information
            if self.fare_updates:
                await self.broadcast_fare_updates()
            
            # Send current route status
            if self.route_updates:
                await self.broadcast_route_updates()
                
        except Exception as e:
            self.logger.error(f"Error sending initial data: {str(e)}")
    
    async def handle_client_message(self, websocket, data):
        """Handle messages from WebSocket clients"""
        try:
            message_type = data.get("type")
            
            if message_type == "get_route":
                # Calculate route with real-time data
                source = RoutePoint(data["source"]["lat"], data["source"]["lng"])
                destination = RoutePoint(data["destination"]["lat"], data["destination"]["lng"])
                
                route_data = routing_service.get_transit_route(source, destination)
                
                # Enhance with real-time data
                enhanced_route = await self.enhance_route_with_realtime(route_data)
                
                response = {
                    "type": "route_response",
                    "route": enhanced_route
                }
                
                await websocket.send(json.dumps(response))
                
            elif message_type == "get_vehicles_near":
                # Get vehicles near a location
                lat = data["latitude"]
                lng = data["longitude"]
                radius_km = data.get("radius_km", 2.0)
                
                nearby_vehicles = self.get_vehicles_near_location(lat, lng, radius_km)
                
                response = {
                    "type": "nearby_vehicles",
                    "vehicles": nearby_vehicles
                }
                
                await websocket.send(json.dumps(response))
                
        except Exception as e:
            self.logger.error(f"Error handling client message: {str(e)}")
            await websocket.send(json.dumps({"error": "Internal server error"}))
    
    async def enhance_route_with_realtime(self, route_data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance route data with real-time information"""
        try:
            enhanced_route = route_data.copy()
            
            # Add real-time fare information
            if "routes" in enhanced_route:
                for mode, route_info in enhanced_route["routes"].items():
                    if mode in self.fare_updates:
                        fare_update = self.fare_updates[mode]
                        route_info["current_fare"] = {
                            "base_fare": fare_update.base_fare,
                            "per_km_rate": fare_update.per_km_rate,
                            "surge_multiplier": fare_update.surge_multiplier,
                            "estimated_cost": (fare_update.base_fare + 
                                             route_info["distance_km"] * fare_update.per_km_rate)
                        }
            
            # Add real-time vehicle positions for bus/metro routes
            enhanced_route["live_vehicles"] = []
            for vehicle_id, position in self.vehicle_positions.items():
                enhanced_route["live_vehicles"].append({
                    "vehicle_id": vehicle_id,
                    "route_id": position.route_id,
                    "latitude": position.latitude,
                    "longitude": position.longitude,
                    "occupancy": position.occupancy_status,
                    "delay_minutes": position.delay_minutes
                })
            

            
            return enhanced_route
            
        except Exception as e:
            self.logger.error(f"Error enhancing route with real-time data: {str(e)}")
            return route_data
    
    def get_vehicles_near_location(self, lat: float, lng: float, radius_km: float) -> List[Dict[str, Any]]:
        """Get vehicles near a specific location"""
        try:
            nearby_vehicles = []
            
            for vehicle_id, position in self.vehicle_positions.items():
                # Calculate distance using haversine formula
                distance = routing_service._calculate_haversine_distance(
                    lat, lng, position.latitude, position.longitude
                )
                
                if distance <= radius_km:
                    vehicle_data = asdict(position)
                    vehicle_data["timestamp"] = vehicle_data["timestamp"].isoformat()
                    vehicle_data["distance_km"] = round(distance, 2)
                    nearby_vehicles.append(vehicle_data)
            
            # Sort by distance
            nearby_vehicles.sort(key=lambda x: x["distance_km"])
            
            return nearby_vehicles
            
        except Exception as e:
            self.logger.error(f"Error getting vehicles near location: {str(e)}")
            return []
    
    async def start_streaming(self):
        """Start the Pathway streaming pipeline"""
        try:
            self.logger.info("Starting Pathway streaming pipeline...")
            
            # Start mock data generation
            await asyncio.gather(
                self.generate_mock_vehicles(),
                self.generate_mock_fares(),
                self.generate_mock_routes()
            )
            
        except Exception as e:
            self.logger.error(f"Error starting streaming pipeline: {str(e)}")

# Global streaming instance
pathway_streaming = PathwayTransitStreaming()