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

# Import our existing data fetchers
from data_fetchers.bmtc_fetcher import BMTCDataFetcher
from data_fetchers.bmrcl_fetcher import BMRCLDataFetcher
from utils.error_handler import error_handler_decorator, performance_monitor
from utils.common import setup_logging
from utils.routing_service import routing_service, RoutePoint
from pathway_streaming import pathway_streaming

# Setup logging
logger = setup_logging("web_server")

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Initialize data fetchers
bmtc_fetcher = BMTCDataFetcher()
bmrcl_fetcher = BMRCLDataFetcher()

@app.route('/')
def index():
    """Serve the main map interface"""
    return send_from_directory('web_interface', 'index.html')

@app.route('/<path:filename>')
def serve_static(filename):
    """Serve static files from web_interface directory"""
    return send_from_directory('web_interface', filename)

@app.route('/api/calculate_fare', methods=['POST'])
@error_handler_decorator("web_server")
@performance_monitor("web_server")
def calculate_fare():
    """Calculate fare for a given route using real road distances"""
    try:
        data = request.get_json()
        
        source_lat = data.get('source_lat')
        source_lng = data.get('source_lng')
        dest_lat = data.get('dest_lat')
        dest_lng = data.get('dest_lng')
        transport_mode = data.get('transport_mode', 'bus')
        
        if not all([source_lat, source_lng, dest_lat, dest_lng]):
            return jsonify({'error': 'Missing coordinates'}), 400
        
        # Create route points
        source = RoutePoint(source_lat, source_lng, data.get('source_name', 'Source'))
        destination = RoutePoint(dest_lat, dest_lng, data.get('dest_name', 'Destination'))
        
        # Get comprehensive route information
        route_data = routing_service.get_transit_route(source, destination)
        
        if not route_data or 'routes' not in route_data:
            return jsonify({'error': 'Could not calculate route'}), 400
        
        # Calculate fares for all available transport modes with enhanced information
        fare_results = {}
        current_time = datetime.now()
        
        for mode, route_info in route_data['routes'].items():
            distance_km = route_info['distance_km']
            duration_minutes = route_info['duration_minutes']
            
            # Calculate arrival time
            arrival_time = current_time + timedelta(minutes=duration_minutes)
            
            # Enhanced route information
            enhanced_route_info = {
                **route_info,
                'road_distance_km': distance_km,  # Actual road distance
                'straight_line_distance_km': routing_service._calculate_haversine_distance(
                    source_lat, source_lng, dest_lat, dest_lng
                ),
                'journey_time_minutes': duration_minutes,
                'departure_time': current_time.strftime('%H:%M'),
                'arrival_time': arrival_time.strftime('%H:%M'),
                'travel_date': current_time.strftime('%Y-%m-%d'),
                'route_efficiency': round((routing_service._calculate_haversine_distance(
                    source_lat, source_lng, dest_lat, dest_lng
                ) / distance_km) * 100, 1) if distance_km > 0 else 0
            }
            
            if mode == 'bus' or transport_mode == 'bus':
                # Use BMTC fare calculator
                fare_breakdown = bmtc_fetcher.calculate_fare(
                    distance_km=distance_km,
                    bus_type='ordinary'  # Default to ordinary
                )
                fare_results['bus_ordinary'] = {
                    **fare_breakdown,
                    'route_info': enhanced_route_info,
                    'transport_details': {
                        'type': 'BMTC Ordinary Bus',
                        'average_speed_kmh': round((distance_km / duration_minutes) * 60, 1) if duration_minutes > 0 else 0,
                        'stops_estimated': max(1, int(distance_km / 0.5)),  # Approx 1 stop per 500m
                        'frequency_minutes': '5-15',
                        'comfort_level': 'Basic'
                    }
                }
                
                # Calculate for AC bus
                fare_breakdown_ac = bmtc_fetcher.calculate_fare(
                    distance_km=distance_km,
                    bus_type='ac'
                )
                fare_results['bus_ac'] = {
                    **fare_breakdown_ac,
                    'route_info': enhanced_route_info,
                    'transport_details': {
                        'type': 'BMTC AC Bus',
                        'average_speed_kmh': round((distance_km / duration_minutes) * 60, 1) if duration_minutes > 0 else 0,
                        'stops_estimated': max(1, int(distance_km / 0.6)),  # AC buses have fewer stops
                        'frequency_minutes': '10-20',
                        'comfort_level': 'Comfortable'
                    }
                }
                
            elif mode == 'metro' or transport_mode == 'metro':
                # Use BMRCL fare calculator
                fare_breakdown = bmrcl_fetcher.calculate_fare(distance_km=distance_km)
                
                # Get detailed metro route information
                metro_route_details = route_info.copy()
                
                # Add metro-specific details if available
                metro_specific_info = {
                    'type': 'Namma Metro',
                    'average_speed_kmh': round((distance_km / duration_minutes) * 60, 1) if duration_minutes > 0 else 35,
                    'stations_estimated': max(1, int(distance_km / 1.2)),  # Approx 1 station per 1.2km
                    'frequency_minutes': '3-8',
                    'comfort_level': 'Excellent',
                    'accessibility': 'Wheelchair accessible'
                }
                
                # Add metro station information if available
                if 'source_station' in route_info:
                    metro_specific_info['source_station'] = route_info['source_station']
                if 'dest_station' in route_info:
                    metro_specific_info['dest_station'] = route_info['dest_station']
                if 'metro_distance_km' in route_info:
                    metro_specific_info['metro_distance_km'] = route_info['metro_distance_km']
                    metro_route_details['metro_distance_km'] = route_info['metro_distance_km']
                if 'walking_to_metro_km' in route_info:
                    metro_specific_info['walking_to_metro_km'] = route_info['walking_to_metro_km']
                    metro_route_details['walking_to_metro_km'] = route_info['walking_to_metro_km']
                if 'walking_from_metro_km' in route_info:
                    metro_specific_info['walking_from_metro_km'] = route_info['walking_from_metro_km']
                    metro_route_details['walking_from_metro_km'] = route_info['walking_from_metro_km']
                
                fare_results['metro'] = {
                    **fare_breakdown,
                    'route_info': metro_route_details,
                    'transport_details': metro_specific_info
                }
            
            elif mode == 'driving-car':
                # Taxi/cab fare estimation
                base_fare = 25
                per_km_rate = 12
                surge_multiplier = pathway_streaming.fare_updates.get('taxi', type('obj', (object,), {'surge_multiplier': 1.0})).surge_multiplier
                
                total_fare = (base_fare + (distance_km * per_km_rate)) * surge_multiplier
                
                fare_results['taxi'] = {
                    'base_fare': base_fare,
                    'distance_fare': distance_km * per_km_rate,
                    'surge_multiplier': surge_multiplier,
                    'total_fare': total_fare,
                    'distance_km': distance_km,
                    'route_info': enhanced_route_info,
                    'transport_details': {
                        'type': 'Taxi/Cab',
                        'average_speed_kmh': round((distance_km / duration_minutes) * 60, 1) if duration_minutes > 0 else 25,
                        'traffic_conditions': 'Real-time adjusted',
                        'comfort_level': 'Premium',
                        'door_to_door': True,
                        'surge_status': 'High' if surge_multiplier > 1.5 else 'Normal' if surge_multiplier <= 1.2 else 'Medium'
                    }
                }
        
        # Add real-time enhancements
        enhanced_results = {}
        for mode, fare_data in fare_results.items():
            enhanced_results[mode] = fare_data
            
            # Add real-time fare adjustments if available
            if mode in pathway_streaming.fare_updates:
                fare_update = pathway_streaming.fare_updates[mode]
                enhanced_results[mode]['real_time_fare'] = {
                    'base_fare': fare_update.base_fare,
                    'per_km_rate': fare_update.per_km_rate,
                    'surge_multiplier': fare_update.surge_multiplier,
                    'total_fare': fare_update.base_fare + (distance_km * fare_update.per_km_rate)
                }
        
        return jsonify({
            'success': True,
            'route_data': route_data,
            'fare_results': enhanced_results,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error calculating fare: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/live/bmtc')
@error_handler_decorator("web_server")
def get_live_bmtc():
    """Get live BMTC vehicle positions"""
    try:
        live_data_path = 'data/live/bmtc_positions_live.json'
        if os.path.exists(live_data_path):
            with open(live_data_path, 'r') as f:
                data = json.load(f)
            return jsonify(data)
        else:
            return jsonify({'entity': []})
    except Exception as e:
        logger.error(f"Error loading BMTC live data: {str(e)}")
        return jsonify({'entity': []})

@app.route('/api/live/bmrcl')
@error_handler_decorator("web_server")
def get_live_bmrcl():
    """Get live BMRCL train positions"""
    try:
        live_data_path = 'data/live/bmrcl_positions_live.json'
        if os.path.exists(live_data_path):
            with open(live_data_path, 'r') as f:
                data = json.load(f)
            return jsonify(data)
        else:
            return jsonify({'entity': []})
    except Exception as e:
        logger.error(f"Error loading BMRCL live data: {str(e)}")
        return jsonify({'entity': []})

@app.route('/api/routes/bmtc')
@error_handler_decorator("web_server")
def get_bmtc_routes():
    """Get BMTC route information"""
    try:
        routes = bmtc_fetcher.fetch_routes()
        return jsonify(routes)
    except Exception as e:
        logger.error(f"Error fetching BMTC routes: {str(e)}")
        return jsonify({'routes': []})

@app.route('/api/routes/bmrcl')
@error_handler_decorator("web_server")
def get_bmrcl_routes():
    """Get BMRCL route information"""
    try:
        routes = bmrcl_fetcher.fetch_routes()
        return jsonify(routes)
    except Exception as e:
        logger.error(f"Error fetching BMRCL routes: {str(e)}")
        return jsonify({'routes': []})

@app.route('/api/fare_structure/bmtc')
@error_handler_decorator("web_server")
def get_bmtc_fare_structure():
    """Get BMTC fare structure"""
    try:
        fare_structure = bmtc_fetcher.get_fare_structure()
        return jsonify(fare_structure)
    except Exception as e:
        logger.error(f"Error fetching BMTC fare structure: {str(e)}")
        return jsonify({'error': 'Failed to fetch fare structure'})

@app.route('/api/fare_structure/bmrcl')
@error_handler_decorator("web_server")
def get_bmrcl_fare_structure():
    """Get BMRCL fare structure"""
    try:
        fare_structure = bmrcl_fetcher.get_fare_structure()
        return jsonify(fare_structure)
    except Exception as e:
        logger.error(f"Error fetching BMRCL fare structure: {str(e)}")
        return jsonify({'error': 'Failed to fetch fare structure'})

@app.route('/api/route', methods=['POST'])
@error_handler_decorator("web_server")
@performance_monitor("web_server")
def get_route():
    """Get detailed route information with real-time data"""
    try:
        data = request.get_json()
        
        source_lat = data.get('source_lat')
        source_lng = data.get('source_lng')
        dest_lat = data.get('dest_lat')
        dest_lng = data.get('dest_lng')
        
        if not all([source_lat, source_lng, dest_lat, dest_lng]):
            return jsonify({'error': 'Missing coordinates'}), 400
        
        source = RoutePoint(source_lat, source_lng, data.get('source_name', 'Source'))
        destination = RoutePoint(dest_lat, dest_lng, data.get('dest_name', 'Destination'))
        
        # Get route with real-time enhancements
        route_data = routing_service.get_transit_route(source, destination)
        
        # Add live vehicle information
        if route_data:
            route_data['live_vehicles'] = []
            for vehicle_id, position in pathway_streaming.vehicle_positions.items():
                route_data['live_vehicles'].append({
                    'vehicle_id': vehicle_id,
                    'route_id': position.route_id,
                    'latitude': position.latitude,
                    'longitude': position.longitude,
                    'occupancy': position.occupancy_status,
                    'delay_minutes': position.delay_minutes
                })
        
        return jsonify({
            'success': True,
            'route': route_data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting route: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/vehicles/nearby', methods=['POST'])
@error_handler_decorator("web_server")
@performance_monitor("web_server")
def get_nearby_vehicles():
    """Get vehicles near a specific location"""
    try:
        data = request.get_json()
        
        latitude = data.get('latitude')
        longitude = data.get('longitude')
        radius_km = data.get('radius_km', 2.0)
        
        if not all([latitude, longitude]):
            return jsonify({'error': 'Missing coordinates'}), 400
        
        nearby_vehicles = pathway_streaming.get_vehicles_near_location(latitude, longitude, radius_km)
        
        return jsonify({
            'success': True,
            'vehicles': nearby_vehicles,
            'search_location': {'latitude': latitude, 'longitude': longitude},
            'radius_km': radius_km,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting nearby vehicles: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/realtime/fares')
@error_handler_decorator("web_server")
def get_realtime_fares():
    """Get current real-time fare information"""
    try:
        current_fares = {}
        
        for route_type, fare_update in pathway_streaming.fare_updates.items():
            current_fares[route_type] = {
                'base_fare': fare_update.base_fare,
                'per_km_rate': fare_update.per_km_rate,
                'surge_multiplier': fare_update.surge_multiplier,
                'effective_time': fare_update.effective_time.isoformat(),
                'zone': fare_update.zone
            }
        
        return jsonify({
            'success': True,
            'fares': current_fares,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting real-time fares: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500



@app.route('/api/health')
@error_handler_decorator("web_server")
def health_check():
    """Health check endpoint"""
    try:
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'services': {
                'bmtc': 'operational',
                'bmrcl': 'operational',
                'routing': 'operational',
                'streaming': 'operational',
                'websocket_clients': len(pathway_streaming.websocket_clients)
            }
        })
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({'status': 'unhealthy'}), 500

@app.route('/api/search_locations')
@error_handler_decorator("web_server")
def search_locations():
    """Search for locations in Bangalore"""
    query = request.args.get('q', '')
    if len(query) < 2:
        return jsonify({'results': []})
    
    # Common Bangalore locations for quick search
    bangalore_locations = [
        {'name': 'Majestic Bus Station', 'lat': 12.9767, 'lng': 77.5703, 'type': 'transport'},
        {'name': 'Bangalore City Railway Station', 'lat': 12.9767, 'lng': 77.5703, 'type': 'transport'},
        {'name': 'Kempegowda International Airport', 'lat': 13.1986, 'lng': 77.7066, 'type': 'transport'},
        {'name': 'MG Road Metro Station', 'lat': 12.9759, 'lng': 77.6063, 'type': 'metro'},
        {'name': 'Cubbon Park Metro Station', 'lat': 12.9698, 'lng': 77.5936, 'type': 'metro'},
        {'name': 'Vidhana Soudha Metro Station', 'lat': 12.9794, 'lng': 77.5912, 'type': 'metro'},
        {'name': 'Electronic City', 'lat': 12.8456, 'lng': 77.6603, 'type': 'area'},
        {'name': 'Whitefield', 'lat': 12.9698, 'lng': 77.7500, 'type': 'area'},
        {'name': 'Koramangala', 'lat': 12.9279, 'lng': 77.6271, 'type': 'area'},
        {'name': 'Indiranagar', 'lat': 12.9719, 'lng': 77.6412, 'type': 'area'},
        {'name': 'Jayanagar', 'lat': 12.9237, 'lng': 77.5838, 'type': 'area'},
        {'name': 'BTM Layout', 'lat': 12.9165, 'lng': 77.6101, 'type': 'area'},
        {'name': 'HSR Layout', 'lat': 12.9081, 'lng': 77.6476, 'type': 'area'},
        {'name': 'Marathahalli', 'lat': 12.9591, 'lng': 77.6974, 'type': 'area'},
        {'name': 'Banashankari', 'lat': 12.9081, 'lng': 77.5737, 'type': 'area'}
    ]
    
    # Filter locations based on query
    query_lower = query.lower()
    results = [
        loc for loc in bangalore_locations 
        if query_lower in loc['name'].lower()
    ][:10]  # Limit to 10 results
    
    return jsonify({'results': results})

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

def start_websocket_server():
    """Start WebSocket server for real-time updates"""
    try:
        logger.info("Starting WebSocket server on port 8081...")
        
        async def run_websocket_server():
            await websockets.serve(
                pathway_streaming.handle_websocket_client,
                "localhost",
                8081
            )
            logger.info("WebSocket server started on ws://localhost:8081")
            
            # Start streaming pipeline
            await pathway_streaming.start_streaming()
        
        # Run WebSocket server in event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_websocket_server())
        
    except Exception as e:
        logger.error(f"Error starting WebSocket server: {str(e)}")

if __name__ == '__main__':
    # Ensure web_interface directory exists
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
        debug=True,
        threaded=True
    )