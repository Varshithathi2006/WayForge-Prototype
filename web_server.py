#!/usr/bin/env python3
"""
Web Server for Bangalore Transit Map Interface
Connects frontend to BMTC and BMRCL fare calculation systems with real-time data
"""

import os
import json
import logging
import asyncio
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_cors import CORS
import websockets
from mistralai.client import MistralClient
import geopy.distance
from geopy.geocoders import Nominatim

# Import our existing data fetchers
from data_fetchers.bmtc_fetcher import BMTCDataFetcher
from data_fetchers.bmrcl_fetcher import BMRCLDataFetcher
from utils.error_handler import error_handler_decorator, performance_monitor
from utils.common import setup_logging
from utils.routing_service import routing_service, RoutePoint
from utils.consolidated_transport_api import consolidated_transport_api
from utils.transport_integration_agent import TransportIntegrationAgent
from utils.historical_data_analyzer import historical_analyzer
from pathway_streaming import pathway_streaming

# Setup logging
logger = setup_logging("web_server")

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Initialize data fetchers
bmtc_fetcher = BMTCDataFetcher()
bmrcl_fetcher = BMRCLDataFetcher()

# Initialize transport integration agent
transport_agent = TransportIntegrationAgent()

# Initialize LLM client (you can set your API key as environment variable)
mistral_client = MistralClient(api_key=os.getenv('MISTRAL_API_KEY', 'your-mistral-api-key-here'))

# Initialize geocoder
geolocator = Nominatim(user_agent="bangalore_transit_app")

@app.route('/')
def index():
    """Serve the main map interface"""
    return send_from_directory('web_interface', 'index.html')

@app.route('/<path:filename>')
def serve_static(filename):
    """Serve static files from web_interface directory"""
    return send_from_directory('web_interface', filename)

@app.route('/api/enhanced-route', methods=['POST'])
@error_handler_decorator("web_server")
@performance_monitor("web_server")
def calculate_enhanced_route():
    """Calculate enhanced route with Pathway real-time data, nearest stops, and traffic information"""
    try:
        data = request.get_json()
        
        # Handle both old and new parameter formats
        if 'source' in data and isinstance(data['source'], dict):
            source_lat = data['source'].get('latitude')
            source_lng = data['source'].get('longitude')
            source_name = data['source'].get('name', 'Source')
        else:
            source_lat = data.get('source_lat')
            source_lng = data.get('source_lng')
            source_name = data.get('source_name', 'Source')
            
        if 'destination' in data and isinstance(data['destination'], dict):
            dest_lat = data['destination'].get('latitude')
            dest_lng = data['destination'].get('longitude')
            dest_name = data['destination'].get('name', 'Destination')
        else:
            dest_lat = data.get('dest_lat')
            dest_lng = data.get('dest_lng')
            dest_name = data.get('dest_name', 'Destination')
            
        transport_mode = data.get('transport_mode', 'bmtc-ordinary')
        include_alternatives = data.get('include_alternatives', data.get('show_alternatives', False))
        use_realtime = data.get('use_realtime', data.get('include_real_time', True))
        
        if not all([source_lat, source_lng, dest_lat, dest_lng]):
            return jsonify({'error': 'Missing coordinates'}), 400
        
        # Create route points
        source = RoutePoint(source_lat, source_lng, source_name)
        destination = RoutePoint(dest_lat, dest_lng, dest_name)
        
        # Get real-time data from Pathway for route optimization
        pathway_data = {}
        if use_realtime:
            try:
                # Fetch real-time traffic conditions
                traffic_data = pathway_streaming.get_current_traffic()
                pathway_data['traffic'] = traffic_data
                
                # Fetch current vehicle positions for better route planning
                vehicle_data = pathway_streaming.get_vehicle_positions()
                pathway_data['vehicles'] = vehicle_data
                
                # Fetch current fare information
                fare_data = pathway_streaming.get_current_fares()
                pathway_data['fares'] = fare_data
                
                # Get nearest stops with real-time data
                source_stops = pathway_streaming.get_nearest_stops(source_lat, source_lng)
                dest_stops = pathway_streaming.get_nearest_stops(dest_lat, dest_lng)
                pathway_data['nearest_stops'] = {
                    'source_stops': source_stops,
                    'destination_stops': dest_stops
                }
                
                logger.info(f"Pathway real-time data fetched: {len(pathway_data)} data sources")
                
            except Exception as pathway_error:
                logger.warning(f"Could not fetch Pathway real-time data: {pathway_error}")
                pathway_data = {}
        
        # Get enhanced route with real-time data
        try:
            route_obj = asyncio.run(
                routing_service.calculate_enhanced_route(
                    source, destination, transport_mode, include_alternatives, include_real_time=use_realtime
                )
            )
            
            # Convert Route object to dictionary for processing
            enhanced_route = None
            if route_obj:
                if hasattr(route_obj, '__dict__'):
                    # Convert Route dataclass to dictionary
                    enhanced_route = {}
                    for key, value in route_obj.__dict__.items():
                        if hasattr(value, '__dict__'):
                            # Convert nested objects to dictionaries
                            enhanced_route[key] = value.__dict__ if hasattr(value, '__dict__') else value
                        elif isinstance(value, list) and value and hasattr(value[0], '__dict__'):
                            # Convert list of objects to list of dictionaries
                            enhanced_route[key] = [item.__dict__ if hasattr(item, '__dict__') else item for item in value]
                        elif key == 'geometry' and isinstance(value, list):
                            # Convert geometry tuples to arrays for frontend
                            enhanced_route[key] = [[lat, lon] for lat, lon in value] if value else []
                        else:
                            enhanced_route[key] = value
                            
                    # Also fix geometry in segments
                    if 'segments' in enhanced_route and enhanced_route['segments']:
                        logger.info(f"Route has {len(enhanced_route['segments'])} segments")
                        for i, segment in enumerate(enhanced_route['segments']):
                            if 'geometry' in segment and segment['geometry']:
                                segment['geometry'] = [[lat, lon] for lat, lon in segment['geometry']]
                                logger.debug(f"Segment {i}: {len(segment['geometry'])} geometry points")
                            else:
                                logger.warning(f"Segment {i} has no geometry")
                    else:
                        logger.warning("Route has no segments - this may cause display issues")
                        # Ensure segments is at least an empty array
                        if enhanced_route:
                            enhanced_route['segments'] = []
                else:
                    # Already a dictionary
                    enhanced_route = route_obj
            
            # Enhance route with Pathway data
            if pathway_data and enhanced_route:
                # Add real-time traffic delays
                if pathway_data.get('traffic'):
                    traffic_delays = []
                    for traffic in pathway_data['traffic']:
                        if traffic.get('delay_minutes', 0) > 0:
                            traffic_delays.append(traffic)
                    
                    if traffic_delays:
                        avg_delay = sum(t.get('delay_minutes', 0) for t in traffic_delays) / len(traffic_delays)
                        enhanced_route['traffic_delay_minutes'] = round(avg_delay, 1)
                        enhanced_route['traffic_status'] = 'heavy' if avg_delay > 15 else 'moderate' if avg_delay > 5 else 'light'
                
                # Add real-time fare information
                if pathway_data.get('fares'):
                    enhanced_route['current_fares'] = pathway_data['fares']
                
                # Add nearby vehicle information
                if pathway_data.get('vehicles'):
                    nearby_vehicles = []
                    for vehicle in pathway_data['vehicles']:
                        # Check if vehicle is near the route (simplified distance check)
                        if vehicle.get('lat') and vehicle.get('lng'):
                            dist_to_source = geopy.distance.geodesic(
                                (source_lat, source_lng), 
                                (vehicle['lat'], vehicle['lng'])
                            ).kilometers
                            if dist_to_source < 2.0:  # Within 2km
                                nearby_vehicles.append(vehicle)
                    
                    enhanced_route['nearby_vehicles'] = nearby_vehicles[:5]  # Limit to 5 vehicles
                
                # Add enhanced nearest stops
                if pathway_data.get('nearest_stops'):
                    enhanced_route['pathway_nearest_stops'] = pathway_data['nearest_stops']
                
                enhanced_route['pathway_enhanced'] = True
                enhanced_route['realtime_data_sources'] = list(pathway_data.keys())
            
        except Exception as e:
            logger.error(f"Enhanced route calculation failed: {e}")
            # Fallback to basic route calculation
            enhanced_route = routing_service.get_transit_route(source, destination)
            if enhanced_route:
                enhanced_route['real_time_enhanced'] = False
                enhanced_route['fallback_used'] = True
                enhanced_route['pathway_enhanced'] = False
        
        if not enhanced_route:
            return jsonify({'error': 'Could not calculate route'}), 400
        
        # Ensure required fields exist with defaults
        if 'estimated_fare' not in enhanced_route:
            # Calculate estimated fare based on distance and transport mode
            distance = enhanced_route.get('total_distance_km', 0)
            if transport_mode == 'bmtc-ordinary':
                enhanced_route['estimated_fare'] = max(10, min(50, distance * 2))
            elif transport_mode == 'bmrcl':
                enhanced_route['estimated_fare'] = max(10, min(60, distance * 3))
            else:
                enhanced_route['estimated_fare'] = max(15, distance * 2.5)
        
        # Add metadata
        enhanced_route['calculated_at'] = datetime.now().isoformat()
        enhanced_route['transport_mode'] = transport_mode
        enhanced_route['pathway_integration'] = True
        enhanced_route['data_freshness'] = 'real-time' if use_realtime and pathway_data else 'static'
        
        # Return in the format expected by frontend
        response_data = {
            'success': True,
            'route': enhanced_route,
            'alternatives': [],  # Can be populated with alternative routes
            'metadata': {
                'calculated_at': enhanced_route['calculated_at'],
                'pathway_enhanced': enhanced_route.get('pathway_enhanced', False),
                'data_freshness': enhanced_route['data_freshness']
            }
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Enhanced route calculation error: {e}")
        return jsonify({'error': 'Route calculation failed', 'details': str(e)}), 500

@app.route('/api/nearest-stops', methods=['POST'])
@error_handler_decorator("web_server")
@performance_monitor("web_server")
def get_nearest_stops():
    """Get nearest bus and metro stops for given coordinates"""
    try:
        data = request.get_json()
        
        # Accept both 'lat'/'lng' and 'latitude'/'longitude' formats
        lat = data.get('lat') or data.get('latitude')
        lng = data.get('lng') or data.get('longitude')
        radius_km = data.get('radius_km') or data.get('max_distance_km', 1.0)
        
        if not all([lat, lng]):
            return jsonify({'error': 'Missing coordinates'}), 400
        
        # Find nearest stops using the static method directly to avoid async issues
        nearest_stops = routing_service._find_nearest_stops_static(lat, lng, radius_km)
        
        return jsonify({
            'location': {'lat': lat, 'lng': lng},
            'radius_km': radius_km,
            'nearest_stops': nearest_stops,
            'found_at': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Nearest stops search error: {e}")
        return jsonify({'error': 'Nearest stops search failed', 'details': str(e)}), 500

@app.route('/api/search-stops', methods=['GET'])
@error_handler_decorator("web_server")
@performance_monitor("web_server")
def search_stops():
    """Search for stops by name or location"""
    try:
        query = request.args.get('q', '').strip()
        lat = request.args.get('lat', type=float)
        lng = request.args.get('lng', type=float)
        limit = request.args.get('limit', 10, type=int)
        
        results = []
        
        if query:
            # Search by name (simplified implementation)
            # In a real implementation, this would search through stop databases
            sample_stops = [
                {'name': 'Majestic Bus Station', 'lat': 12.9767, 'lng': 77.5713, 'type': 'bus'},
                {'name': 'KR Market Metro Station', 'lat': 12.9591, 'lng': 77.5712, 'type': 'metro'},
                {'name': 'Vidhana Soudha', 'lat': 12.9794, 'lng': 77.5912, 'type': 'bus'},
                {'name': 'Cubbon Park Metro Station', 'lat': 12.9716, 'lng': 77.5946, 'type': 'metro'},
                {'name': 'Brigade Road', 'lat': 12.9716, 'lng': 77.6197, 'type': 'bus'},
            ]
            
            # Filter stops by query
            for stop in sample_stops:
                if query.lower() in stop['name'].lower():
                    results.append(stop)
                    if len(results) >= limit:
                        break
        
        elif lat and lng:
            # Search by proximity
            nearest_stops = routing_service.find_nearest_stops(lat, lng, 2.0)
            for stop_type, stops in nearest_stops.items():
                for stop in stops[:limit//2]:  # Limit results per type
                    results.append({
                        'name': stop['name'],
                        'lat': stop['lat'],
                        'lng': stop['lng'],
                        'type': stop_type,
                        'distance_km': stop['distance_km']
                    })
        
        return jsonify({
            'query': query,
            'results': results[:limit],
            'total_found': len(results)
        })
        
    except Exception as e:
        logger.error(f"Stop search error: {e}")
        return jsonify({'error': 'Stop search failed', 'details': str(e)}), 500

@app.route('/api/realtime-data', methods=['POST'])
@error_handler_decorator("web_server")
@performance_monitor("web_server")
def get_realtime_data():
    """Get real-time traffic, bus, and taxi data for a location"""
    try:
        data = request.get_json()
        
        lat = data.get('lat')
        lng = data.get('lng')
        radius_km = data.get('radius_km', 2.0)
        transport_mode = data.get('transport_mode', 'bmtc-ordinary')
        distance_km = data.get('distance_km', 0)
        
        if not all([lat, lng]):
            return jsonify({'error': 'Missing coordinates'}), 400
        
        # Get real-time data from pathway streaming
        traffic_data = pathway_streaming.get_traffic_data(lat, lng, radius_km)
        bus_schedules = pathway_streaming.get_bus_schedules(lat, lng, radius_km)
        taxi_availability = pathway_streaming.get_taxi_availability(lat, lng, radius_km)
        
        # Calculate transport mode specific information
        transport_details = {}
        
        if transport_mode in ['bmtc-ordinary', 'bmtc-ac']:
            # Bus specific information
            next_buses = sorted(bus_schedules, key=lambda x: x.get('next_arrival_time', ''))[:3]
            transport_details = {
                'type': 'bus',
                'next_arrivals': [
                    {
                        'route': bus.get('route_id', 'Unknown'),
                        'arrival_time': bus.get('next_arrival_time', ''),
                        'delay_minutes': bus.get('delay_minutes', 0),
                        'stop_name': bus.get('bus_stop_name', 'Unknown'),
                        'occupancy': bus.get('occupancy_level', 'medium')
                    } for bus in next_buses
                ],
                'estimated_cost': calculate_bus_fare(distance_km, transport_mode),
                'estimated_time': calculate_travel_time(distance_km, 'bus', traffic_data)
            }
        
        elif transport_mode == 'bmrcl-metro':
            # Metro specific information
            transport_details = {
                'type': 'metro',
                'next_arrivals': [
                    {
                        'line': 'Purple Line',
                        'arrival_time': (datetime.now() + timedelta(minutes=3)).isoformat(),
                        'delay_minutes': 0,
                        'destination': 'Whitefield',
                        'occupancy': 'medium'
                    },
                    {
                        'line': 'Purple Line',
                        'arrival_time': (datetime.now() + timedelta(minutes=8)).isoformat(),
                        'delay_minutes': 1,
                        'destination': 'Whitefield',
                        'occupancy': 'low'
                    }
                ],
                'estimated_cost': calculate_metro_fare(distance_km),
                'estimated_time': calculate_travel_time(distance_km, 'metro', traffic_data)
            }
        
        elif transport_mode == 'taxi':
            # Taxi specific information
            best_taxis = sorted(taxi_availability, key=lambda x: x.get('eta_minutes', 999))[:3]
            transport_details = {
                'type': 'taxi',
                'available_taxis': [
                    {
                        'provider': taxi.get('service_provider', 'Unknown'),
                        'vehicle_type': taxi.get('vehicle_type', 'sedan'),
                        'eta_minutes': taxi.get('eta_minutes', 5),
                        'base_fare': taxi.get('base_fare', 50),
                        'surge_multiplier': taxi.get('surge_multiplier', 1.0),
                        'estimated_cost': taxi.get('base_fare', 50) + (distance_km * taxi.get('per_km_rate', 15)) * taxi.get('surge_multiplier', 1.0)
                    } for taxi in best_taxis
                ],
                'estimated_time': calculate_travel_time(distance_km, 'taxi', traffic_data)
            }
        
        realtime_data = {
            'traffic_data': traffic_data,
            'bus_schedules': bus_schedules,
            'taxi_availability': taxi_availability,
            'transport_details': transport_details,
            'location': {'lat': lat, 'lng': lng},
            'radius_km': radius_km,
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(realtime_data)
        
    except Exception as e:
        logger.error(f"Real-time data fetch error: {e}")
        return jsonify({'error': 'Real-time data fetch failed', 'details': str(e)}), 500

def calculate_bus_fare(distance_km, transport_mode):
    """Calculate bus fare based on distance and type"""
    if transport_mode == 'bmtc-ordinary':
        return max(5, min(25, 5 + (distance_km * 1.5)))
    else:  # AC bus
        return max(10, min(40, 10 + (distance_km * 2.5)))

def calculate_metro_fare(distance_km):
    """Calculate metro fare based on distance"""
    return max(10, min(60, 10 + (distance_km * 3)))

def calculate_travel_time(distance_km, transport_type, traffic_data):
    """Calculate estimated travel time based on transport type and traffic"""
    base_speeds = {
        'bus': 15,  # km/h in city traffic
        'metro': 35,  # km/h average with stops
        'taxi': 25   # km/h in city traffic
    }
    
    base_time = (distance_km / base_speeds.get(transport_type, 20)) * 60  # minutes
    
    # Apply traffic delay
    if traffic_data:
        avg_delay = sum(t.get('estimated_delay_minutes', 0) for t in traffic_data) / len(traffic_data)
        base_time += avg_delay
    
    return max(5, int(base_time))

@app.route('/api/calculate_fare', methods=['POST'])
@error_handler_decorator("web_server")
@performance_monitor("web_server")
def calculate_fare():
    """Calculate fare for a given route using enhanced routing service with real-time costs"""
    try:
        data = request.get_json()
        
        source_lat = data.get('source_lat')
        source_lng = data.get('source_lng')
        dest_lat = data.get('dest_lat')
        dest_lng = data.get('dest_lng')
        transport_mode = data.get('transport_mode', 'driving-car')
        
        if not all([source_lat, source_lng, dest_lat, dest_lng]):
            return jsonify({'error': 'Missing coordinates'}), 400
        
        # Create route points
        source = RoutePoint(source_lat, source_lng, data.get('source_name', 'Source'))
        destination = RoutePoint(dest_lat, dest_lng, data.get('dest_name', 'Destination'))
        
        # Use enhanced routing service to get route with real-time cost information
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            enhanced_route = loop.run_until_complete(
                routing_service.calculate_enhanced_route(source, destination, transport_mode, include_real_time=True)
            )
        finally:
            loop.close()
        
        if not enhanced_route:
            return jsonify({'error': 'Could not calculate route'}), 400
        
        # Extract cost information and route details
        current_time = datetime.now()
        
        # Handle both Route objects and dictionaries
        if hasattr(enhanced_route, 'total_distance_km'):
            # Route object
            distance_km = enhanced_route.total_distance_km
            duration_minutes = enhanced_route.total_duration_minutes
            geometry = enhanced_route.geometry
            cost_info = enhanced_route.cost_info or {}
        else:
            # Dictionary
            distance_km = enhanced_route.get('total_distance_km', 0)
            duration_minutes = enhanced_route.get('total_duration_minutes', 0)
            geometry = enhanced_route.get('geometry', [])
            cost_info = enhanced_route.get('cost_info', {})
        
        # Calculate arrival time
        arrival_time = current_time + timedelta(minutes=duration_minutes)
        
        # Base route information
        base_route_info = {
            'distance_km': distance_km,
            'duration_minutes': duration_minutes,
            'departure_time': current_time.strftime('%H:%M'),
            'arrival_time': arrival_time.strftime('%H:%M'),
            'travel_date': current_time.strftime('%Y-%m-%d'),
            'geometry': geometry,
            'route_efficiency': round((routing_service._calculate_haversine_distance(
                source_lat, source_lng, dest_lat, dest_lng
            ) / distance_km) * 100, 1) if distance_km > 0 else 0
        }
        
        # Enhanced fare results with real-time cost information
        fare_results = {}
        transport_options = cost_info.get('transport_options', {})
        
        # Build enhanced fare results using real-time cost information
        if 'bus' in transport_options:
            bus_options = transport_options['bus']
            
            # BMTC Ordinary Bus
            if 'ordinary' in bus_options:
                fare_results['bmtc_ordinary'] = {
                    **base_route_info,
                    'fare': bus_options['ordinary'],
                    'transport_type': 'BMTC Ordinary Bus',
                    'comfort_level': 'Standard',
                    'accessibility': 'Basic wheelchair access',
                    'surge_status': 'normal',
                    'estimated_wait_time': '5-15 minutes',
                    'icon': 'bus',
                    'color': '#2196F3'
                }
            
            # BMTC Deluxe Bus
            if 'deluxe' in bus_options:
                fare_results['bmtc_deluxe'] = {
                    **base_route_info,
                    'fare': bus_options['deluxe'],
                    'transport_type': 'BMTC Deluxe Bus',
                    'comfort_level': 'Enhanced',
                    'accessibility': 'Good wheelchair access',
                    'surge_status': 'normal',
                    'estimated_wait_time': '8-18 minutes',
                    'icon': 'bus',
                    'color': '#4CAF50'
                }
            
            # BMTC AC Bus
            if 'ac' in bus_options:
                fare_results['bmtc_ac'] = {
                    **base_route_info,
                    'fare': bus_options['ac'],
                    'transport_type': 'BMTC AC Bus',
                    'comfort_level': 'Air Conditioned',
                    'accessibility': 'Full wheelchair access',
                    'surge_status': 'normal',
                    'estimated_wait_time': '10-20 minutes',
                    'icon': 'bus',
                    'color': '#FF9800'
                }
            
            # BMTC Vajra Bus
            if 'vajra' in bus_options:
                fare_results['bmtc_vajra'] = {
                    **base_route_info,
                    'fare': bus_options['vajra'],
                    'transport_type': 'BMTC Vajra Bus',
                    'comfort_level': 'Premium',
                    'accessibility': 'Full accessibility',
                    'surge_status': 'normal',
                    'estimated_wait_time': '15-30 minutes',
                    'icon': 'bus',
                    'color': '#9C27B0'
                }
        
        # Metro options
        if 'metro' in transport_options:
            metro_options = transport_options['metro']
            
            # Metro Token
            if 'token' in metro_options:
                fare_results['bmrcl_token'] = {
                    **base_route_info,
                    'fare': metro_options['token'],
                    'transport_type': 'BMRCL Metro (Token)',
                    'comfort_level': 'Air Conditioned',
                    'accessibility': 'Full accessibility',
                    'surge_status': 'normal',
                    'estimated_wait_time': '3-8 minutes',
                    'icon': 'train',
                    'color': '#E91E63'
                }
            
            # Metro Smart Card
            if 'smart_card' in metro_options:
                fare_results['bmrcl_smart_card'] = {
                    **base_route_info,
                    'fare': metro_options['smart_card'],
                    'transport_type': 'BMRCL Metro (Smart Card)',
                    'comfort_level': 'Air Conditioned',
                    'accessibility': 'Full accessibility',
                    'surge_status': 'normal',
                    'estimated_wait_time': '3-8 minutes',
                    'discount': '5% Smart Card Discount',
                    'icon': 'train',
                    'color': '#E91E63'
                }
        
        # Taxi options
        if 'taxi' in transport_options:
            taxi_info = transport_options['taxi']
            
            # Calculate surge multiplier based on time
            current_hour = current_time.hour
            surge_multiplier = 1.0
            surge_status = 'normal'
            
            if 7 <= current_hour <= 10 or 17 <= current_hour <= 20:
                surge_multiplier = 1.5
                surge_status = 'high'
            elif 20 <= current_hour <= 23 or 0 <= current_hour <= 6:
                surge_multiplier = 1.2
                surge_status = 'medium'
            
            taxi_fare = round(taxi_info['total_fare'] * surge_multiplier)
            
            fare_results['taxi'] = {
                **base_route_info,
                'fare': {
                    'total_fare': taxi_fare,
                    'base_fare': taxi_info['base_fare'],
                    'per_km_rate': taxi_info['per_km_rate'],
                    'surge_multiplier': surge_multiplier,
                    'currency': 'INR'
                },
                'transport_type': 'Taxi/Cab',
                'comfort_level': 'Private',
                'accessibility': 'On-demand',
                'surge_status': surge_status,
                'estimated_wait_time': '2-8 minutes',
                'icon': 'car',
                'color': '#FFC107'
            }
        
        # Auto options
        if 'auto' in transport_options:
            auto_info = transport_options['auto']
            
            fare_results['auto'] = {
                **base_route_info,
                'fare': auto_info,
                'transport_type': 'Auto Rickshaw',
                'comfort_level': 'Basic',
                'accessibility': 'Limited',
                'surge_status': 'normal',
                'estimated_wait_time': '3-10 minutes',
                'icon': 'auto',
                'color': '#795548'
            }
        
        # Walking option
        if 'walking' in transport_options:
            walking_info = transport_options['walking']
            walking_duration = max(15, distance_km * 12)  # ~12 minutes per km
            
            fare_results['walking'] = {
                **base_route_info,
                'duration_minutes': walking_duration,
                'fare': walking_info,
                'transport_type': 'Walking',
                'comfort_level': 'Exercise',
                'accessibility': 'Free',
                'surge_status': 'none',
                'estimated_wait_time': '0 minutes',
                'icon': 'walk',
                'color': '#607D8B'
            }
        
        # Cycling option
        if 'cycling' in transport_options:
            cycling_info = transport_options['cycling']
            cycling_duration = max(8, distance_km * 4)  # ~4 minutes per km
            
            fare_results['cycling'] = {
                **base_route_info,
                'duration_minutes': cycling_duration,
                'fare': cycling_info,
                'transport_type': 'Cycling',
                'comfort_level': 'Eco-friendly',
                'accessibility': 'Free',
                'surge_status': 'none',
                'estimated_wait_time': '0 minutes',
                'icon': 'bike',
                'color': '#4CAF50'
            }
        
        return jsonify({
            'source': {'lat': source_lat, 'lng': source_lng, 'name': data.get('source_name', 'Source')},
            'destination': {'lat': dest_lat, 'lng': dest_lng, 'name': data.get('dest_name', 'Destination')},
            'fare_options': fare_results,
            'calculated_at': current_time.isoformat(),
            'currency': 'INR'
        })
        
    except Exception as e:
        logger.error(f"Fare calculation error: {e}")
        return jsonify({'error': 'Fare calculation failed', 'details': str(e)}), 500

@app.route('/api/live/bmtc')
@error_handler_decorator("web_server")
def get_live_bmtc():
    """Get live BMTC bus data"""
    try:
        live_data = bmtc_fetcher.get_live_bus_data()
        return jsonify({
            'status': 'success',
            'data': live_data,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"BMTC live data error: {e}")
        return jsonify({'error': 'Failed to fetch BMTC live data'}), 500

@app.route('/api/live/bmrcl')
@error_handler_decorator("web_server")
def get_live_bmrcl():
    """Get live BMRCL metro data"""
    try:
        live_data = bmrcl_fetcher.get_live_metro_data()
        return jsonify({
            'status': 'success',
            'data': live_data,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"BMRCL live data error: {e}")
        return jsonify({'error': 'Failed to fetch BMRCL live data'}), 500

@app.route('/api/routes/bmtc')
@error_handler_decorator("web_server")
def get_bmtc_routes():
    """Get BMTC route information"""
    try:
        routes = bmtc_fetcher.get_routes()
        return jsonify({'routes': routes})
    except Exception as e:
        logger.error(f"BMTC routes error: {e}")
        return jsonify({'error': 'Failed to fetch BMTC routes'}), 500

@app.route('/api/routes/bmrcl')
@error_handler_decorator("web_server")
def get_bmrcl_routes():
    """Get BMRCL route information"""
    try:
        routes = bmrcl_fetcher.get_routes()
        return jsonify({'routes': routes})
    except Exception as e:
        logger.error(f"BMRCL routes error: {e}")
        return jsonify({'error': 'Failed to fetch BMRCL routes'}), 500

@app.route('/api/fare_structure/bmtc')
@error_handler_decorator("web_server")
def get_bmtc_fare_structure():
    """Get BMTC fare structure"""
    try:
        fare_structure = bmtc_fetcher.get_fare_structure()
        return jsonify({'fare_structure': fare_structure})
    except Exception as e:
        logger.error(f"BMTC fare structure error: {e}")
        return jsonify({'error': 'Failed to fetch BMTC fare structure'}), 500

@app.route('/api/fare_structure/bmrcl')
@error_handler_decorator("web_server")
def get_bmrcl_fare_structure():
    """Get BMRCL fare structure"""
    try:
        fare_structure = bmrcl_fetcher.get_fare_structure()
        return jsonify({'fare_structure': fare_structure})
    except Exception as e:
        logger.error(f"BMRCL fare structure error: {e}")
        return jsonify({'error': 'Failed to fetch BMRCL fare structure'}), 500

@app.route('/api/route', methods=['POST'])
@error_handler_decorator("web_server")
@performance_monitor("web_server")
def get_route():
    """Get detailed route information between two points"""
    try:
        data = request.get_json()
        
        source_lat = data.get('source_lat')
        source_lng = data.get('source_lng')
        dest_lat = data.get('dest_lat')
        dest_lng = data.get('dest_lng')
        
        if not all([source_lat, source_lng, dest_lat, dest_lng]):
            return jsonify({'error': 'Missing coordinates'}), 400
        
        # Create route points
        source = RoutePoint(source_lat, source_lng, data.get('source_name', 'Source'))
        destination = RoutePoint(dest_lat, dest_lng, data.get('dest_name', 'Destination'))
        
        # Get route using routing service
        route_data = routing_service.calculate_route(source, destination)
        
        if not route_data:
            return jsonify({'error': 'Could not calculate route'}), 400
        
        return jsonify({
            'route': route_data,
            'calculated_at': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Route calculation error: {e}")
        return jsonify({'error': 'Route calculation failed'}), 500

@app.route('/api/vehicles/nearby', methods=['POST'])
@error_handler_decorator("web_server")
@performance_monitor("web_server")
def get_nearby_vehicles():
    """Get nearby vehicles (buses/metros) for a given location"""
    try:
        data = request.get_json()
        
        lat = data.get('lat')
        lng = data.get('lng')
        radius_km = data.get('radius_km', 2.0)
        
        if not all([lat, lng]):
            return jsonify({'error': 'Missing coordinates'}), 400
        
        # Get nearby vehicles from both BMTC and BMRCL
        nearby_vehicles = {
            'buses': bmtc_fetcher.get_nearby_buses(lat, lng, radius_km),
            'metros': bmrcl_fetcher.get_nearby_metros(lat, lng, radius_km),
            'location': {'lat': lat, 'lng': lng},
            'radius_km': radius_km,
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(nearby_vehicles)
        
    except Exception as e:
        logger.error(f"Nearby vehicles error: {e}")
        return jsonify({'error': 'Failed to fetch nearby vehicles'}), 500

@app.route('/api/realtime/fares')
@error_handler_decorator("web_server")
def get_realtime_fares():
    """Get real-time fare information with surge pricing"""
    try:
        current_time = datetime.now()
        
        # Calculate surge multipliers based on current time and demand
        surge_data = {
            'bmtc': {'multiplier': 1.0, 'status': 'normal'},
            'bmrcl': {'multiplier': 1.0, 'status': 'normal'},
            'taxi': {'multiplier': 1.0, 'status': 'normal'}
        }
        
        # Peak hours surge pricing for taxis
        current_hour = current_time.hour
        if 7 <= current_hour <= 10 or 17 <= current_hour <= 20:
            surge_data['taxi'] = {'multiplier': 1.5, 'status': 'high'}
        elif 20 <= current_hour <= 23:
            surge_data['taxi'] = {'multiplier': 1.2, 'status': 'medium'}
        
        return jsonify({
            'surge_pricing': surge_data,
            'timestamp': current_time.isoformat(),
            'next_update': (current_time + timedelta(minutes=5)).isoformat()
        })
        
    except Exception as e:
        logger.error(f"Real-time fares error: {e}")
        return jsonify({'error': 'Failed to fetch real-time fares'}), 500

@app.route('/api/health')
@error_handler_decorator("web_server")
def health_check():
    """Health check endpoint"""
    try:
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'services': {
                'bmtc_fetcher': 'operational',
                'bmrcl_fetcher': 'operational',
                'routing_service': 'operational',
                'pathway_streaming': 'operational'
            }
        })
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

def get_nearest_stops_data(lat, lon, radius_km=2.0):
    """Helper function to get nearest stops data"""
    # Sample data - in real implementation, query your database
    sample_stops = [
        {"name": "Majestic Bus Station", "lat": 12.9767, "lon": 77.5710, "type": "bus"},
        {"name": "KR Market Metro", "lat": 12.9698, "lon": 77.5736, "type": "metro"},
        {"name": "Chickpet Bus Stop", "lat": 12.9716, "lon": 77.5946, "type": "bus"},
        {"name": "Vidhana Soudha", "lat": 12.9794, "lon": 77.5912, "type": "bus"},
        {"name": "Cubbon Park Metro", "lat": 12.9716, "lon": 77.5946, "type": "metro"}
    ]
    
    nearest_stops = []
    for stop in sample_stops:
        distance = geopy.distance.geodesic((lat, lon), (stop['lat'], stop['lon'])).kilometers
        if distance <= radius_km:
            stop['distance'] = round(distance, 2)
            nearest_stops.append(stop)
    
    # Sort by distance
    nearest_stops.sort(key=lambda x: x['distance'])
    return nearest_stops[:5]

@app.route('/api/search_locations')
@error_handler_decorator("web_server")
def search_locations():
    """Search for locations using Nominatim"""
    query = request.args.get('q', '')
    if not query:
        return jsonify({'error': 'Query parameter required'}), 400
    
    try:
        # Use Nominatim for location search
        import requests
        
        # Search for locations in Bangalore
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            'q': f"{query}, Bangalore, Karnataka, India",
            'format': 'json',
            'limit': 5,
            'countrycodes': 'in',
            'bounded': 1,
            'viewbox': '77.4126,12.7409,77.8085,13.1746'  # Bangalore bounding box
        }
        
        response = requests.get(url, params=params)
        locations = response.json()
        
        # Format results
        results = []
        for loc in locations:
            results.append({
                'name': loc.get('display_name', ''),
                'lat': float(loc.get('lat', 0)),
                'lon': float(loc.get('lon', 0)),
                'type': loc.get('type', 'location')
            })
        
        return jsonify({
            'success': True,
            'locations': results
        })
        
    except Exception as e:
        logger.error(f"Location search error: {e}")
        return jsonify({'error': 'Location search failed'}), 500

@app.route('/api/address-suggestions', methods=['GET'])
@error_handler_decorator("web_server")
def get_address_suggestions():
    """Get address suggestions for autocomplete"""
    query = request.args.get('q', '').strip().lower()
    if len(query) < 2:
        return jsonify({'suggestions': []})
    
    # Fallback suggestions for common Bangalore locations
    fallback_suggestions = [
        {'address': 'MG Road, Bangalore', 'lat': 12.9716, 'lon': 77.5946, 'type': 'road'},
        {'address': 'Koramangala, Bangalore', 'lat': 12.9352, 'lon': 77.6245, 'type': 'suburb'},
        {'address': 'Indiranagar, Bangalore', 'lat': 12.9719, 'lon': 77.6412, 'type': 'suburb'},
        {'address': 'Whitefield, Bangalore', 'lat': 12.9698, 'lon': 77.7500, 'type': 'suburb'},
        {'address': 'Electronic City, Bangalore', 'lat': 12.8456, 'lon': 77.6603, 'type': 'area'},
        {'address': 'Marathahalli, Bangalore', 'lat': 12.9591, 'lon': 77.6974, 'type': 'area'},
        {'address': 'Jayanagar, Bangalore', 'lat': 12.9279, 'lon': 77.5937, 'type': 'suburb'},
        {'address': 'BTM Layout, Bangalore', 'lat': 12.9165, 'lon': 77.6101, 'type': 'area'},
        {'address': 'HSR Layout, Bangalore', 'lat': 12.9082, 'lon': 77.6476, 'type': 'area'},
        {'address': 'Banashankari, Bangalore', 'lat': 12.9250, 'lon': 77.5667, 'type': 'suburb'},
        {'address': 'Rajajinagar, Bangalore', 'lat': 12.9915, 'lon': 77.5554, 'type': 'suburb'},
        {'address': 'Malleshwaram, Bangalore', 'lat': 13.0031, 'lon': 77.5737, 'type': 'suburb'},
        {'address': 'Kempegowda International Airport, Bangalore', 'lat': 13.1986, 'lon': 77.7066, 'type': 'airport'},
        {'address': 'Bangalore City Railway Station', 'lat': 12.9767, 'lon': 77.5733, 'type': 'station'},
        {'address': 'Majestic Bus Station, Bangalore', 'lat': 12.9767, 'lon': 77.5733, 'type': 'station'}
    ]
    
    try:
        # Use Nominatim for address suggestions
        import requests
        
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            'q': f"{query}, Bangalore, Karnataka, India",
            'format': 'json',
            'limit': 8,
            'countrycodes': 'in',
            'bounded': 1,
            'viewbox': '77.4126,12.7409,77.8085,13.1746',
            'addressdetails': 1
        }
        
        headers = {
            'User-Agent': 'BangaloreTransitApp/1.0 (contact@example.com)'
        }
        
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        if response.status_code == 200 and response.text.strip():
            locations = response.json()
            
            suggestions = []
            for loc in locations:
                address = loc.get('display_name', '')
                # Simplify address for better UX
                address_parts = address.split(',')
                simplified_address = ', '.join(address_parts[:3]) if len(address_parts) > 3 else address
                
                suggestions.append({
                    'address': simplified_address,
                    'full_address': address,
                    'lat': float(loc.get('lat', 0)),
                    'lon': float(loc.get('lon', 0)),
                    'type': loc.get('type', 'location'),
                    'importance': loc.get('importance', 0)
                })
            
            # Sort by importance
            suggestions.sort(key=lambda x: x['importance'], reverse=True)
            
            if suggestions:
                return jsonify({'suggestions': suggestions})
        
        # If no results from Nominatim, use fallback suggestions
        filtered_fallback = [
            s for s in fallback_suggestions 
            if query in s['address'].lower()
        ]
        
        return jsonify({'suggestions': filtered_fallback[:5]})
        
    except Exception as e:
        logger.error(f"Address suggestion error: {e}")
        
        # Return filtered fallback suggestions on error
        filtered_fallback = [
            s for s in fallback_suggestions 
            if query in s['address'].lower()
        ]
        
        return jsonify({'suggestions': filtered_fallback[:5]})

@app.route('/api/current-location', methods=['POST'])
@error_handler_decorator("web_server")
def get_current_location_info():
    """Get information about current location"""
    data = request.get_json()
    lat = data.get('lat')
    lon = data.get('lon')
    
    if not lat or not lon:
        return jsonify({'error': 'Latitude and longitude required'}), 400
    
    try:
        # Reverse geocode to get address
        location = geolocator.reverse(f"{lat}, {lon}")
        address = location.address if location else "Unknown location"
        
        # Find nearest stops
        nearest_stops = []
        # Sample data - in real implementation, query your database
        sample_stops = [
            {"name": "Majestic Bus Station", "lat": 12.9767, "lon": 77.5710, "type": "bus"},
            {"name": "KR Market Metro", "lat": 12.9698, "lon": 77.5736, "type": "metro"},
            {"name": "Chickpet Bus Stop", "lat": 12.9716, "lon": 77.5946, "type": "bus"},
        ]
        
        for stop in sample_stops:
            distance = geopy.distance.geodesic((lat, lon), (stop['lat'], stop['lon'])).kilometers
            if distance <= 2.0:  # Within 2km
                stop['distance'] = round(distance, 2)
                nearest_stops.append(stop)
        
        # Sort by distance
        nearest_stops.sort(key=lambda x: x['distance'])
        
        return jsonify({
            'success': True,
            'address': address,
            'lat': lat,
            'lon': lon,
            'nearest_stops': nearest_stops[:5]
        })
        
    except Exception as e:
        logger.error(f"Current location error: {e}")
        return jsonify({'error': 'Failed to get location info'}), 500

@app.route('/api/llm-recommendations', methods=['POST'])
@error_handler_decorator("web_server")
def get_llm_recommendations():
    """Get intelligent route recommendations using Mistral LLM"""
    data = request.get_json()
    source = data.get('source')
    destination = data.get('destination')
    preferences = data.get('preferences', {})
    time_of_day = data.get('time_of_day', datetime.now().strftime('%H:%M'))
    
    try:
        # Prepare context for LLM
        context = f"""
        You are a Bangalore transit expert. Provide route recommendations for:
        Source: {source}
        Destination: {destination}
        Time: {time_of_day}
        User preferences: {preferences}
        
        Consider these factors:
        1. Time efficiency
        2. Cost effectiveness
        3. Comfort level
        4. Current traffic conditions
        5. Peak hours (7-10 AM, 5-8 PM are typically busy)
        6. Metro vs Bus vs Taxi trade-offs
        
        Provide a JSON response with:
        - recommended_mode: best transport mode
        - reasoning: why this is recommended
        - alternatives: other viable options
        - time_estimate: expected travel time
        - cost_estimate: expected cost
        - comfort_rating: 1-5 scale
        - tips: helpful travel tips
        """
        
        # For demo purposes, provide intelligent recommendations based on time and preferences
        current_hour = int(time_of_day.split(':')[0])
        is_peak_hour = (7 <= current_hour <= 10) or (17 <= current_hour <= 20)
        
        if is_peak_hour:
            if preferences.get('priority') == 'cost':
                recommendation = {
                    'recommended_mode': 'BMTC Bus',
                    'reasoning': 'Most cost-effective during peak hours, though slower due to traffic',
                    'alternatives': ['BMRCL Metro (if route available)', 'Taxi (expensive but faster)'],
                    'time_estimate': '45-60 minutes',
                    'cost_estimate': '₹15-25',
                    'comfort_rating': 2,
                    'tips': ['Avoid AC buses during peak for better availability', 'Consider metro for longer distances']
                }
            elif preferences.get('priority') == 'time':
                recommendation = {
                    'recommended_mode': 'BMRCL Metro',
                    'reasoning': 'Fastest during peak hours, avoids traffic congestion',
                    'alternatives': ['Taxi (expensive)', 'Bus + Metro combination'],
                    'time_estimate': '25-35 minutes',
                    'cost_estimate': '₹20-40',
                    'comfort_rating': 4,
                    'tips': ['Use metro for main route, bus/auto for last mile', 'Check metro timings']
                }
            else:
                recommendation = {
                    'recommended_mode': 'Taxi',
                    'reasoning': 'Most comfortable option with door-to-door service',
                    'alternatives': ['Metro + Auto', 'Bus (if direct route)'],
                    'time_estimate': '35-50 minutes',
                    'cost_estimate': '₹150-300',
                    'comfort_rating': 5,
                    'tips': ['Book in advance during peak hours', 'Consider shared rides for cost savings']
                }
        else:
            recommendation = {
                'recommended_mode': 'BMTC Bus',
                'reasoning': 'Good balance of cost and time during off-peak hours',
                'alternatives': ['Metro (if available)', 'Taxi (for comfort)'],
                'time_estimate': '30-40 minutes',
                'cost_estimate': '₹15-25',
                'comfort_rating': 3,
                'tips': ['Off-peak travel is more comfortable', 'AC buses available with better seating']
            }
        
        return jsonify({
            'success': True,
            'recommendation': recommendation,
            'peak_hour_info': {
                'is_peak_hour': is_peak_hour,
                'next_peak': '17:00-20:00' if current_hour < 17 else '07:00-10:00 (next day)'
            }
        })
        
    except Exception as e:
        logger.error(f"LLM recommendation error: {e}")
        return jsonify({'error': 'Failed to get recommendations'}), 500

@app.route('/api/chatbot', methods=['POST'])
@error_handler_decorator("web_server")
def chatbot_query():
    try:
        data = request.get_json()
        query = data.get('query', '').strip()
        
        if not query:
            return jsonify({'error': 'Query is required'}), 400
        
        # Get real-time data from Pathway
        realtime_context = {}
        try:
            # Fetch current traffic conditions
            traffic_data = pathway_streaming.get_current_traffic()
            realtime_context['traffic'] = traffic_data
            
            # Fetch current fare information
            fare_data = pathway_streaming.get_current_fares()
            realtime_context['fares'] = fare_data
            
            # Fetch vehicle positions for context
            vehicle_data = pathway_streaming.get_vehicle_positions()
            realtime_context['vehicles'] = vehicle_data
            
        except Exception as pathway_error:
            logger.warning(f"Could not fetch real-time data from Pathway: {pathway_error}")
            realtime_context = {}
        
        # Load static schedule data
        schedule_data = {}
        try:
            with open('data/static/bmtc_static.json', 'r') as f:
                schedule_data['bmtc'] = json.load(f)
            with open('data/static/bmrcl_static.json', 'r') as f:
                schedule_data['bmrcl'] = json.load(f)
        except Exception as e:
            logger.warning(f"Could not load schedule data: {e}")
        
        # Prepare context for Mistral LLM
        query_lower = query.lower()
        current_time = datetime.now()
        current_hour = current_time.hour
        current_day = current_time.strftime('%A')
        is_peak = (7 <= current_hour <= 10) or (17 <= current_hour <= 20)
        is_weekend = current_day in ['Saturday', 'Sunday']
        
        # Generate intelligent suggestions using historical data analyzer
        historical_suggestions = []
        try:
            historical_suggestions = historical_analyzer.get_contextual_suggestions(
                current_time=current_time,
                realtime_data=realtime_context,
                query_context=query_lower
            )
        except Exception as hist_error:
            logger.warning(f"Historical analyzer unavailable: {hist_error}")
        
        # Generate intelligent response using Mistral LLM
        try:
            context_data = {
                'query': query,
                'current_time': current_time.strftime('%Y-%m-%d %H:%M:%S'),
                'current_hour': current_hour,
                'current_day': current_day,
                'is_peak': is_peak,
                'is_weekend': is_weekend,
                'realtime_data': realtime_context,
                'schedule_data': schedule_data,
                'historical_suggestions': historical_suggestions
            }
            
            # Format historical suggestions for Mistral prompt
            historical_context = ""
            if historical_suggestions:
                historical_context = "\nHistorical Insights:\n"
                for suggestion in historical_suggestions[:3]:  # Top 3 suggestions
                    historical_context += f"- {suggestion.suggestion} (Confidence: {suggestion.confidence:.0%})\n"
            
            mistral_prompt = f"""
You are a helpful transit assistant for Bangalore public transport. Provide accurate, helpful responses about BMTC buses, BMRCL metro, traffic conditions, fares, and schedules.

Current Context:
- Time: {context_data['current_time']}
- Day: {context_data['current_day']}
- Peak hours: {'Yes' if context_data['is_peak'] else 'No'}
- Weekend: {'Yes' if context_data['is_weekend'] else 'No'}

Real-time Data Available:
- Traffic conditions: {len(realtime_context.get('traffic', []))} data points
- Vehicle positions: {len(realtime_context.get('vehicles', []))} vehicles tracked
- Fare information: {'Available' if realtime_context.get('fares') else 'Using static data'}
{historical_context}
User Query: {query}

Please provide a helpful response that includes:
1. Direct answer to the user's question
2. Relevant tips and suggestions based on historical patterns
3. Current status information
4. Time-appropriate recommendations
5. Smart suggestions based on historical data and current conditions

Keep responses concise but informative. Focus on practical, actionable information.
"""

            mistral_response = mistral_client.chat(
                model="mistral-large-latest",
                messages=[{"role": "user", "content": mistral_prompt}]
            )
            
            llm_answer = mistral_response.choices[0].message.content
            
            # Generate smart tips based on historical suggestions
            smart_tips = [
                '🚇 Metro recommended during peak hours (faster than buses)',
                '📱 Use BMTC Chalo app for real-time bus tracking',
                '🎫 Metro smart card saves ₹2 per trip',
                '⚠️ Avoid Electronic City route during 6-9 PM',
                '✈️ Vayu Vajra buses for airport connectivity'
            ]
            
            # Add historical suggestions to tips
            if historical_suggestions:
                for suggestion in historical_suggestions[:3]:
                    smart_tips.append(f"📊 {suggestion.suggestion}")
            
            # Enhanced response with LLM integration and historical insights
            response = {
                'answer': llm_answer,
                'tips': smart_tips,
                'smart_suggestions': [
                    {
                        'text': suggestion.suggestion,
                        'confidence': suggestion.confidence,
                        'category': suggestion.category,
                        'icon': '🚇' if 'metro' in suggestion.suggestion.lower() else 
                               '🚌' if 'bus' in suggestion.suggestion.lower() else
                               '🚗' if 'traffic' in suggestion.suggestion.lower() else '💡'
                    } for suggestion in historical_suggestions[:5]
                ] if historical_suggestions else [],
                'schedule_info': {
                    'Bus_Hours': '5:00 AM - 11:30 PM daily',
                    'Metro_Hours': '5:00 AM - 11:00 PM daily',
                    'Peak_Hours': '7-10 AM, 5-8 PM weekdays'
                },
                'realtime_data': {
                    'tracked_vehicles': len(realtime_context.get('vehicles', [])),
                    'current_time': current_time.strftime('%I:%M %p'),
                    'is_peak_hour': is_peak,
                    'is_weekend': is_weekend,
                    'historical_insights_available': len(historical_suggestions) > 0,
                    'data_sources': ['Mistral LLM', 'Historical Analyzer', 'Pathway Real-time Engine', 'BMTC API', 'BMRCL API']
                }
            }
            
        except Exception as mistral_error:
            logger.warning(f"Mistral LLM unavailable, using fallback: {mistral_error}")
            # Fallback to rule-based responses
        
        # Detailed peak hours and traffic patterns
        if any(word in query_lower for word in ['peak', 'traffic', 'crowded', 'busy', 'rush']):
            is_peak = (7 <= current_hour <= 10) or (17 <= current_hour <= 20)
            is_weekend = current_day in ['Saturday', 'Sunday']
            
            # Detailed traffic patterns
            traffic_patterns = {
                'morning_peak': '7:00-10:00 AM: Heavy traffic on ORR, Hosur Road, Bannerghatta Road',
                'evening_peak': '5:00-8:00 PM: Severe congestion on Electronic City, Whitefield routes',
                'weekend': 'Saturdays: Moderate traffic 11:00 AM-2:00 PM, Sundays: Light traffic',
                'special_areas': 'Commercial Street, Brigade Road: Busy 6:00-9:00 PM daily'
            }
            
            traffic_status = "heavy" if is_peak and not is_weekend else "moderate"
            if realtime_context.get('traffic'):
                avg_delay = sum(t.get('delay_minutes', 0) for t in realtime_context['traffic']) / len(realtime_context['traffic'])
                traffic_status = "heavy" if avg_delay > 15 else "moderate" if avg_delay > 5 else "light"
            
            peak_advice = "Metro is fastest during peak hours" if is_peak else "Both bus and metro are good options"
            
            response = {
                'answer': f'Current traffic: {traffic_status}. Peak hours are 7:00-10:00 AM and 5:00-8:00 PM on weekdays. {peak_advice}. {"⚠️ You are in peak hours!" if is_peak else "✅ Off-peak travel - good time to travel!"}',
                'tips': [
                    'Morning peak: Use Purple Line metro to avoid ORR traffic',
                    'Evening peak: Green Line metro faster than Outer Ring Road',
                    'Weekends: Commercial areas busy 11 AM-2 PM',
                    'Avoid Electronic City route 6-9 PM on weekdays'
                ],
                'schedule_info': traffic_patterns,
                'realtime_data': {
                    'current_status': traffic_status,
                    'is_peak_hour': is_peak,
                    'is_weekend': is_weekend,
                    'recommendation': 'Metro (Purple/Green Line)' if traffic_status == 'heavy' else 'Bus or Metro',
                    'active_vehicles': len(realtime_context.get('vehicles', []))
                }
            }
        
        # Metro queries with comprehensive schedule data
        elif any(word in query_lower for word in ['metro', 'namma metro', 'purple line', 'green line', 'bmrcl']):
            metro_schedule = {
                'operating_hours': 'Daily: 5:00 AM - 11:00 PM (Last train 10:30 PM)',
                'frequency': {
                    'peak_hours': 'Every 2-3 minutes (7-10 AM, 5-8 PM)',
                    'normal_hours': 'Every 4-6 minutes',
                    'off_peak': 'Every 8-10 minutes (after 9 PM)'
                },
                'lines': {
                    'Purple Line': 'Mysuru Road ↔ Whitefield (42.3 km, 37 stations)',
                    'Green Line': 'Nagasandra ↔ Silk Institute (24.2 km, 24 stations)'
                },
                'interchange_stations': ['Majestic', 'Cubbon Park', 'MG Road']
            }
            
            metro_vehicles = [v for v in realtime_context.get('vehicles', []) if v.get('type') == 'metro']
            current_frequency = "2-3 minutes" if is_peak else "4-6 minutes" if 6 <= current_hour <= 21 else "8-10 minutes"
            
            # Check for specific line queries
            line_specific = ""
            if 'purple' in query_lower:
                line_specific = " Purple Line connects major IT hubs (Whitefield) to city center."
            elif 'green' in query_lower:
                line_specific = " Green Line serves airport connectivity via bus interchange."
            
            response = {
                'answer': f'🚇 BMRCL Metro: 5:00 AM - 11:00 PM daily. Current frequency: {current_frequency}.{line_specific} Both lines intersect at Majestic and Cubbon Park.',
                'tips': [
                    f'🎫 Smart card saves ₹2 per trip',
                    f'📱 Namma Metro app shows live train timings',
                    f'🚶‍♂️ Interchange at Majestic/Cubbon Park for line changes',
                    f'⏰ Last trains depart at 10:30 PM from terminal stations',
                    f'🎒 Avoid large bags during peak hours (7-10 AM, 5-8 PM)'
                ],
                'schedule_info': metro_schedule,
                'realtime_data': {
                    'active_trains': len(metro_vehicles),
                    'current_frequency': current_frequency,
                    'lines_status': 'All lines operational',
                    'next_service_hours': '5:00 AM tomorrow' if current_hour >= 23 else 'Currently running'
                }
            }
        
        # Bus queries with comprehensive schedule data
        elif any(word in query_lower for word in ['bus', 'bmtc', 'volvo', 'route']):
            bus_schedule = {
                'operating_hours': 'Daily: 5:00 AM - 11:30 PM (Some routes 24/7)',
                'frequency': {
                    'major_routes': 'Every 5-10 minutes (335E, 500K, 201)',
                    'regular_routes': 'Every 10-20 minutes',
                    'feeder_routes': 'Every 20-30 minutes'
                },
                'popular_routes': {
                    '335E': 'Kempegowda Bus Station ↔ Electronic City',
                    '500K': 'Kempegowda Bus Station ↔ Kengeri',
                    '201': 'Shivajinagar ↔ Banashankari',
                    'Vayu Vajra': 'Airport connectivity from major areas'
                },
                'bus_types': {
                    'Volvo AC': 'Premium comfort, higher fare',
                    'Volvo Non-AC': 'Comfortable, moderate fare',
                    'Regular': 'Standard service, lowest fare'
                }
            }
            
            bus_vehicles = [v for v in realtime_context.get('vehicles', []) if v.get('type') == 'bus']
            
            # Check for specific route queries
            route_specific = ""
            if any(route in query_lower for route in ['335e', 'electronic city']):
                route_specific = " Route 335E is very popular for Electronic City - expect crowds during peak hours."
            elif any(route in query_lower for route in ['500k', 'kengeri']):
                route_specific = " Route 500K serves Kengeri with good frequency throughout the day."
            elif 'airport' in query_lower or 'vayu vajra' in query_lower:
                route_specific = " Vayu Vajra buses connect airport to major city areas every 15-30 minutes."
            
            response = {
                'answer': f'🚌 BMTC buses: 5:00 AM - 11:30 PM daily. Major routes every 5-10 minutes.{route_specific} Use BMTC app for live tracking.',
                'tips': [
                    f'📱 BMTC Chalo app shows real-time bus locations',
                    f'🎫 Daily/Monthly passes available for regular commuters',
                    f'❄️ Volvo AC buses cost 1.5x regular fare but more comfortable',
                    f'💰 Keep exact change ready - conductors prefer it',
                    f'🕐 Avoid 335E during 8-9 AM and 6-7 PM (very crowded)',
                    f'✈️ Vayu Vajra for airport - book seats in advance'
                ],
                'schedule_info': bus_schedule,
                'realtime_data': {
                    'active_buses': len(bus_vehicles),
                    'service_status': 'Normal operations',
                    'tracked_routes': len(schedule_data.get('bmtc', {}).get('routes', [])),
                    'next_service_hours': '5:00 AM tomorrow' if current_hour >= 23 else 'Currently running'
                }
            }
        
        # Fare queries with detailed pricing structure
        elif any(word in query_lower for word in ['fare', 'price', 'cost', 'ticket', 'pass']):
            fare_structure = {
                'BMTC_Bus': {
                    'Regular': '₹5-25 (distance-based)',
                    'Volvo_Non_AC': '₹8-35 (distance-based)',
                    'Volvo_AC': '₹10-50 (distance-based)',
                    'Vayu_Vajra': '₹100-250 (airport routes)'
                },
                'BMRCL_Metro': {
                    'Regular': '₹10-60 (distance-based)',
                    'Smart_Card_Discount': '₹2 off per trip',
                    'Monthly_Pass': '₹1,500-3,000 (zone-based)'
                },
                'Taxi_Services': {
                    'Ola_Uber': '₹50-80 base + ₹12-15/km',
                    'Auto_Rickshaw': '₹25 base + ₹13/km (meter)',
                    'Peak_Surge': '1.5x-2x during rush hours'
                }
            }
            
            fare_info = "💰 Current fares: Bus ₹5-50, Metro ₹10-60, Taxi ₹50+ base. Smart cards save ₹2 per metro trip."
            
            if realtime_context.get('fares'):
                current_fares = realtime_context['fares']
                if current_fares:
                    fare_info = f"💰 Live fares: Bus ₹{current_fares.get('bus_base', 5)}, Metro ₹{current_fares.get('metro_min', 10)}-{current_fares.get('metro_max', 60)}, Taxi ₹{current_fares.get('taxi_base', 50)}+"
            
            response = {
                'answer': fare_info,
                'tips': [
                    '🎫 Metro smart card saves ₹2 per trip + faster entry',
                    '📅 Monthly passes cost-effective for daily commuters',
                    '🚌 Volvo AC costs 1.5x regular bus but more comfortable',
                    '🚕 Compare Ola/Uber prices - surge varies by time',
                    '🛺 Auto rickshaws: Insist on meter during day time',
                    '✈️ Airport: Vayu Vajra (₹100-250) vs Taxi (₹400-800)'
                ],
                'schedule_info': fare_structure,
                'realtime_data': realtime_context.get('fares', {})
            }
        
        # Schedule-specific queries
        elif any(word in query_lower for word in ['schedule', 'timing', 'time', 'when', 'hours', 'frequency']):
            schedule_summary = {
                'BMTC_Buses': 'Daily 5:00 AM - 11:30 PM (some 24/7)',
                'BMRCL_Metro': 'Daily 5:00 AM - 11:00 PM (last train 10:30 PM)',
                'Peak_Frequency': 'Buses: 5-10 min, Metro: 2-3 min',
                'Off_Peak_Frequency': 'Buses: 10-20 min, Metro: 4-6 min',
                'Night_Services': 'Limited bus routes after 11:30 PM'
            }
            
            current_status = "🟢 Currently running" if 5 <= current_hour <= 23 else "🔴 Services closed"
            next_service = "Services resume at 5:00 AM" if current_hour >= 23 or current_hour < 5 else "Currently operational"
            
            response = {
                'answer': f'📅 Transit schedules: Buses 5 AM-11:30 PM, Metro 5 AM-11 PM. {current_status}. Peak frequency: Metro 2-3 min, Buses 5-10 min.',
                'tips': [
                    '⏰ First services start at 5:00 AM sharp',
                    '🌙 Last metro trains at 10:30 PM from terminals',
                    '🚌 Some bus routes run 24/7 (limited)',
                    '⚡ Peak hours: 7-10 AM, 5-8 PM (higher frequency)',
                    '📱 Use apps for real-time arrival information'
                ],
                'schedule_info': schedule_summary,
                'realtime_data': {
                    'current_status': current_status,
                    'next_service': next_service,
                    'is_operational': 5 <= current_hour <= 23
                }
            }
        
        else:
            # Enhanced default response with comprehensive transit information
            total_vehicles = len(realtime_context.get('vehicles', []))
            system_status = f"🟢 All systems operational - tracking {total_vehicles} vehicles" if total_vehicles > 0 else "🟢 All systems operational"
            
            # Intelligent suggestions based on current time
            time_based_suggestions = []
            if is_peak:
                time_based_suggestions = [
                    "🚇 Metro recommended during peak hours (faster than buses)",
                    "⚠️ Expect heavy traffic on ORR and major routes",
                    "🕐 Consider traveling after 8 PM for lighter traffic"
                ]
            elif current_hour < 6:
                time_based_suggestions = [
                    "🌅 Services start at 5:00 AM",
                    "🚌 Limited night bus services available",
                    "🚕 Taxis/autos available 24/7"
                ]
            elif current_hour >= 22:
                time_based_suggestions = [
                    "🌙 Last metro trains at 10:30 PM",
                    "🚌 Limited bus services after 11:30 PM",
                    "🚕 Night travel: Use taxi apps for safety"
                ]
            else:
                time_based_suggestions = [
                    "✅ Good time to travel - off-peak hours",
                    "🚌 Regular bus frequency (10-20 minutes)",
                    "🚇 Metro frequency: 4-6 minutes"
                ]
            
            response = {
                'answer': f'🚀 WayForge Transit Assistant ready! I have access to real-time data for Bangalore public transport. {system_status}. Ask me about schedules, traffic, fares, or specific routes.',
                'tips': [
                    '🗣️ Try: "What are the peak traffic hours?"',
                    '🚇 Try: "Metro schedule and timings"',
                    '🚌 Try: "Bus routes to Electronic City"',
                    '💰 Try: "Current fare prices"',
                    '📍 Try: "Traffic conditions right now"'
                ] + time_based_suggestions,
                'schedule_info': {
                    'quick_reference': {
                        'Bus_Hours': '5:00 AM - 11:30 PM daily',
                        'Metro_Hours': '5:00 AM - 11:00 PM daily',
                        'Peak_Hours': '7-10 AM, 5-8 PM weekdays',
                        'Current_Status': system_status
                    }
                },
                'realtime_data': {
                    'system_status': system_status,
                    'tracked_vehicles': total_vehicles,
                    'current_time': current_time.strftime('%I:%M %p'),
                    'is_peak_hour': is_peak,
                    'is_weekend': is_weekend,
                    'data_sources': ['Pathway Real-time Engine', 'BMTC API', 'BMRCL API', 'Live Traffic Data']
                }
            }
        
        return jsonify({
            'success': True,
            'response': response['answer'],
            'tips': response['tips'],
            'schedule_info': response.get('schedule_info', {}),
            'realtime_data': response.get('realtime_data', {}),
            'timestamp': datetime.now().isoformat(),
            'powered_by': 'WayForge Transit Assistant with Mistral LLM & Pathway Real-time Engine'
        })
        
    except Exception as e:
        logger.error(f"Chatbot error: {e}")
        return jsonify({'error': 'Chatbot service unavailable'}), 500

@app.route('/api/next-bus-arrival', methods=['POST'])
@error_handler_decorator("web_server")
def get_next_bus_arrival():
    """Get next bus arrival predictions"""
    data = request.get_json()
    stop_name = data.get('stop_name')
    route_number = data.get('route_number')
    
    try:
        # Sample bus arrival data - in real implementation, use live BMTC API
        current_time = datetime.now()
        
        arrivals = []
        for i in range(3):  # Next 3 buses
            arrival_time = current_time + timedelta(minutes=5 + i*10 + (i*2))  # Simulate realistic intervals
            arrivals.append({
                'route_number': route_number or f"20{i+1}",
                'destination': f"Destination {i+1}",
                'arrival_time': arrival_time.strftime('%H:%M'),
                'minutes_away': 5 + i*10 + (i*2),
                'bus_type': 'AC' if i % 2 == 0 else 'Non-AC',
                'crowding_level': ['Low', 'Medium', 'High'][i % 3]
            })
        
        return jsonify({
            'success': True,
            'stop_name': stop_name or 'Sample Bus Stop',
            'arrivals': arrivals,
            'last_updated': current_time.strftime('%H:%M:%S')
        })
        
    except Exception as e:
        logger.error(f"Bus arrival error: {e}")
        return jsonify({'error': 'Failed to get bus arrivals'}), 500

# ===== CONSOLIDATED TRANSPORT API ENDPOINTS =====

@app.route('/api/transport/all-options', methods=['POST'])
@error_handler_decorator("web_server")
@performance_monitor("web_server")
def get_all_transport_options():
    """Get all available transport options with cost, ETA, and availability"""
    data = request.get_json()
    
    try:
        source_lat = float(data.get('source_lat'))
        source_lng = float(data.get('source_lng'))
        dest_lat = float(data.get('dest_lat'))
        dest_lng = float(data.get('dest_lng'))
        source_name = data.get('source_name')
        dest_name = data.get('dest_name')
        
        # Get consolidated transport options
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            response = loop.run_until_complete(
                consolidated_transport_api.get_all_transport_options(
                    source_lat, source_lng, dest_lat, dest_lng, source_name, dest_name
                )
            )
            
            # Convert dataclass to dict for JSON serialization
            # Create fare_options in the format expected by frontend
            fare_options = {}
            for option in response.options:
                fare_options[option.mode.value] = {
                    'fare': option.cost_inr,
                    'mode': option.mode.value,
                    'provider': option.provider,
                    'travel_time_minutes': option.travel_time_minutes,
                    'next_availability_minutes': option.next_availability_minutes,
                    'distance_km': option.distance_km,
                    'route_description': option.route_description,
                    'confidence_score': option.confidence_score,
                    'surge_multiplier': option.surge_multiplier,
                    'booking_fee': option.booking_fee,
                    'fallback_used': option.fallback_used
                }

            response_dict = {
                'source_lat': response.source_lat,
                'source_lng': response.source_lng,
                'dest_lat': response.dest_lat,
                'dest_lng': response.dest_lng,
                'source_name': response.source_name,
                'dest_name': response.dest_name,
                'fare_options': fare_options,
                'options': [
                    {
                        'mode': option.mode.value,
                        'provider': option.provider,
                        'cost_inr': option.cost_inr,
                        'travel_time_minutes': option.travel_time_minutes,
                        'next_availability_minutes': option.next_availability_minutes,
                        'distance_km': option.distance_km,
                        'route_description': option.route_description,
                        'confidence_score': option.confidence_score,
                        'stops': option.stops,
                        'vehicle_number': option.vehicle_number,
                        'route_number': option.route_number,
                        'surge_multiplier': option.surge_multiplier,
                        'booking_fee': option.booking_fee,
                        'last_updated': option.last_updated.isoformat() if option.last_updated else None,
                        'data_source': option.data_source,
                        'fallback_used': option.fallback_used
                    } for option in response.options
                ],
                'cheapest_option': {
                    'mode': response.cheapest_option.mode.value,
                    'cost_inr': response.cheapest_option.cost_inr,
                    'provider': response.cheapest_option.provider
                } if response.cheapest_option else None,
                'fastest_option': {
                    'mode': response.fastest_option.mode.value,
                    'total_time_minutes': response.fastest_option.travel_time_minutes + response.fastest_option.next_availability_minutes,
                    'provider': response.fastest_option.provider
                } if response.fastest_option else None,
                'most_available_option': {
                    'mode': response.most_available_option.mode.value,
                    'next_availability_minutes': response.most_available_option.next_availability_minutes,
                    'provider': response.most_available_option.provider
                } if response.most_available_option else None,
                'recommended_option': {
                    'mode': response.recommended_option.mode.value,
                    'cost_inr': response.recommended_option.cost_inr,
                    'travel_time_minutes': response.recommended_option.travel_time_minutes,
                    'next_availability_minutes': response.recommended_option.next_availability_minutes,
                    'provider': response.recommended_option.provider,
                    'route_description': response.recommended_option.route_description
                } if response.recommended_option else None,
                'timestamp': response.timestamp.isoformat(),
                'response_time_ms': response.response_time_ms,
                'data_freshness_score': response.data_freshness_score,
                'fallback_providers_used': response.fallback_providers_used,
                'alternatives_available': response.alternatives_available,
                'service_status': response.service_status,
                'pathway_realtime': response.pathway_realtime
            }
            
            return jsonify({
                'success': True,
                'data': response_dict
            })
            
        finally:
            loop.close()
        
    except ValueError as e:
        logger.error(f"Invalid coordinates: {e}")
        return jsonify({'error': 'Invalid coordinates provided'}), 400
    except Exception as e:
        logger.error(f"Error getting transport options: {e}")
        return jsonify({'error': 'Failed to get transport options'}), 500

@app.route('/api/transport/quick-summary', methods=['POST'])
@error_handler_decorator("web_server")
@performance_monitor("web_server")
def get_transport_quick_summary():
    """Get a quick summary of transport options"""
    data = request.get_json()
    
    try:
        source_lat = float(data.get('source_lat'))
        source_lng = float(data.get('source_lng'))
        dest_lat = float(data.get('dest_lat'))
        dest_lng = float(data.get('dest_lng'))
        
        # Get quick summary
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            summary = loop.run_until_complete(
                consolidated_transport_api.get_quick_summary(
                    source_lat, source_lng, dest_lat, dest_lng
                )
            )
            
            return jsonify({
                'success': True,
                'summary': summary
            })
            
        finally:
            loop.close()
        
    except ValueError as e:
        logger.error(f"Invalid coordinates: {e}")
        return jsonify({'error': 'Invalid coordinates provided'}), 400
    except Exception as e:
        logger.error(f"Error getting transport summary: {e}")
        return jsonify({'error': 'Failed to get transport summary'}), 500

@app.route('/api/transport/system-status', methods=['GET'])
@error_handler_decorator("web_server")
def get_transport_system_status():
    """Get the status of all transport systems and services"""
    try:
        # Get system status from transport agent
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            status = loop.run_until_complete(transport_agent.get_system_status())
            
            return jsonify({
                'success': True,
                'status': status,
                'timestamp': datetime.now().isoformat()
            })
            
        finally:
            loop.close()
        
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return jsonify({'error': 'Failed to get system status'}), 500

@app.route('/api/transport/refresh-data', methods=['POST'])
@error_handler_decorator("web_server")
def refresh_transport_data():
    """Manually trigger data refresh for all transport sources"""
    try:
        # Trigger data refresh through transport agent
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Force refresh of all data sources
            refresh_results = loop.run_until_complete(transport_agent.force_data_refresh())
            
            return jsonify({
                'success': True,
                'message': 'Data refresh triggered successfully',
                'refresh_results': refresh_results,
                'timestamp': datetime.now().isoformat()
            })
            
        finally:
            loop.close()
        
    except Exception as e:
        logger.error(f"Error refreshing transport data: {e}")
        return jsonify({'error': 'Failed to refresh transport data'}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

def start_websocket_server():
    """Start WebSocket server for real-time updates using Pathway"""
    async def handle_websocket(websocket):
        try:
            logger.info(f"WebSocket client connected: {websocket.remote_address}")
            
            # Send initial data with Pathway integration
            initial_data = {
                'type': 'connection_established',
                'timestamp': datetime.now().isoformat(),
                'pathway_enabled': True,
                'data_sources': ['Pathway Real-time Engine', 'BMTC API', 'BMRCL API']
            }
            
            # Try to get initial Pathway data
            try:
                pathway_vehicles = pathway_streaming.get_vehicle_positions()
                pathway_traffic = pathway_streaming.get_current_traffic()
                pathway_fares = pathway_streaming.get_current_fares()
                
                initial_data['initial_data'] = {
                    'vehicles_count': len(pathway_vehicles) if pathway_vehicles else 0,
                    'traffic_conditions': len(pathway_traffic) if pathway_traffic else 0,
                    'fare_updates': len(pathway_fares) if pathway_fares else 0
                }
            except Exception as pathway_error:
                logger.warning(f"Could not fetch initial Pathway data: {pathway_error}")
                initial_data['initial_data'] = {
                    'vehicles_count': 0,
                    'traffic_conditions': 0,
                    'fare_updates': 0,
                    'pathway_status': 'initializing'
                }
            
            await websocket.send(json.dumps(initial_data))
            
            # Keep connection alive and send periodic updates
            while True:
                await asyncio.sleep(15)  # Send updates every 15 seconds for more responsive updates
                
                # Get real-time data from Pathway
                try:
                    # Fetch current vehicle positions
                    vehicles = pathway_streaming.get_vehicle_positions()
                    traffic_data = pathway_streaming.get_current_traffic()
                    fare_data = pathway_streaming.get_current_fares()
                    
                    # Calculate traffic status
                    traffic_status = 'light'
                    if traffic_data:
                        avg_delay = sum(t.get('delay_minutes', 0) for t in traffic_data) / len(traffic_data)
                        traffic_status = 'heavy' if avg_delay > 15 else 'moderate' if avg_delay > 5 else 'light'
                    
                    # Prepare update data
                    update_data = {
                        'type': 'pathway_realtime_update',
                        'timestamp': datetime.now().isoformat(),
                        'data': {
                            'vehicles': {
                                'total_count': len(vehicles) if vehicles else 0,
                                'buses': len([v for v in vehicles if v.get('type') == 'bus']) if vehicles else 0,
                                'metros': len([v for v in vehicles if v.get('type') == 'metro']) if vehicles else 0,
                                'taxis': len([v for v in vehicles if v.get('type') == 'taxi']) if vehicles else 0
                            },
                            'traffic': {
                                'status': traffic_status,
                                'conditions_count': len(traffic_data) if traffic_data else 0,
                                'average_delay': round(sum(t.get('delay_minutes', 0) for t in traffic_data) / len(traffic_data), 1) if traffic_data else 0
                            },
                            'fares': {
                                'last_updated': fare_data.get('last_updated') if fare_data else None,
                                'bus_base': fare_data.get('bus_base', 5) if fare_data else 5,
                                'metro_min': fare_data.get('metro_min', 10) if fare_data else 10,
                                'taxi_base': fare_data.get('taxi_base', 50) if fare_data else 50
                            },
                            'system_status': 'operational',
                            'pathway_status': 'active'
                        }
                    }
                    
                except Exception as pathway_error:
                    logger.warning(f"Pathway data fetch error in WebSocket: {pathway_error}")
                    # Send fallback data
                    update_data = {
                        'type': 'pathway_realtime_update',
                        'timestamp': datetime.now().isoformat(),
                        'data': {
                            'vehicles': {'total_count': 0, 'buses': 0, 'metros': 0, 'taxis': 0},
                            'traffic': {'status': 'unknown', 'conditions_count': 0, 'average_delay': 0},
                            'fares': {'bus_base': 5, 'metro_min': 10, 'taxi_base': 50},
                            'system_status': 'operational',
                            'pathway_status': 'error'
                        }
                    }
                
                await websocket.send(json.dumps(update_data))
                
        except websockets.exceptions.ConnectionClosed:
            logger.info("WebSocket client disconnected")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
    
    async def run_server():
        try:
            logger.info("Starting WebSocket server on port 8766...")
            server = await websockets.serve(handle_websocket, "localhost", 8766)
            logger.info("WebSocket server started on ws://localhost:8766 with Pathway integration")
            await server.wait_closed()
        except OSError as e:
            if "Address already in use" in str(e):
                logger.warning("WebSocket port 8766 already in use, trying port 8767...")
                try:
                    server = await websockets.serve(handle_websocket, "localhost", 8767)
                    logger.info("WebSocket server started on ws://localhost:8767 with Pathway integration")
                    await server.wait_closed()
                except Exception as e2:
                    logger.error(f"Failed to start WebSocket server on backup port: {e2}")
                    import traceback
                    logger.error(f"WebSocket server traceback: {traceback.format_exc()}")
            else:
                logger.error(f"Failed to start WebSocket server: {e}")
                import traceback
                logger.error(f"WebSocket server traceback: {traceback.format_exc()}")
        except Exception as e:
            logger.error(f"Failed to start WebSocket server: {e}")
            import traceback
            logger.error(f"WebSocket server traceback: {traceback.format_exc()}")

    try:
        # Create new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_server())
    except Exception as e:
        logger.error(f"Failed to create event loop for WebSocket server: {e}")
        import traceback
        logger.error(f"Event loop traceback: {traceback.format_exc()}")
    finally:
        try:
            loop.close()
        except:
            pass

if __name__ == '__main__':
    # Check if web_interface directory exists
    if not os.path.exists('web_interface'):
        logger.error("web_interface directory not found!")
        exit(1)
    
    logger.info("Starting Bangalore Transit Web Server...")
    logger.info("Access the application at: http://localhost:8080")
    
    # Start WebSocket server in a separate thread
    websocket_thread = threading.Thread(target=start_websocket_server, daemon=True)
    websocket_thread.start()
    
    # Run the Flask development server
    app.run(
        host='0.0.0.0',
        port=8080,
        debug=False,  # Disable debug mode to prevent threading conflicts with WebSocket
        threaded=True
    )