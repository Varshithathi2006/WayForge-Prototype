#!/usr/bin/env python3
"""
Consolidated Transport Data API
Provides unified access to all transport modes (BMTC, Metro, Taxi) with cost, ETA, and availability
Includes automatic fallback handling and real-time data integration
"""

import os
import json
import logging
import asyncio
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum

from .common import setup_logging
from .error_handler import error_handler_decorator, performance_monitor
from .transport_integration_agent import TransportIntegrationAgent
from .fallback_routing_providers import FallbackRoutingManager, RoutePoint
from .taxi_integration import TaxiIntegrationService, TaxiOption
from .data_freshness_validator import DataFreshnessValidator, DataSource

logger = setup_logging("consolidated_transport_api")

class TransportMode(Enum):
    """Transport mode enumeration"""
    BMTC_REGULAR = "bmtc_regular"
    BMTC_AC = "bmtc_ac"
    BMTC_VOLVO = "bmtc_volvo"
    METRO = "metro"
    TAXI_AUTO = "taxi_auto"
    TAXI_MINI = "taxi_mini"
    TAXI_SEDAN = "taxi_sedan"
    TAXI_SUV = "taxi_suv"
    TAXI_BIKE = "taxi_bike"
    WALKING = "walking"
    CYCLING = "cycling"

@dataclass
class TransportOption:
    """Unified transport option with cost, ETA, and availability"""
    mode: TransportMode
    provider: str  # 'bmtc', 'bmrcl', 'ola', 'uber', 'mock'
    cost_inr: float
    travel_time_minutes: int
    next_availability_minutes: int  # How soon can you start this journey
    distance_km: float
    route_description: str
    confidence_score: float  # 0-1, how reliable this data is
    
    # Additional details
    stops: List[str] = None
    vehicle_number: Optional[str] = None
    route_number: Optional[str] = None
    surge_multiplier: float = 1.0
    booking_fee: float = 0.0
    
    # Metadata
    last_updated: datetime = None
    data_source: str = "live"  # 'live', 'cached', 'estimated'
    fallback_used: bool = False

@dataclass
class ConsolidatedTransportResponse:
    """Complete response with all transport options"""
    source_lat: float
    source_lng: float
    dest_lat: float
    dest_lng: float
    source_name: Optional[str] = None
    dest_name: Optional[str] = None
    
    # All available options sorted by preference
    options: List[TransportOption] = None
    
    # Quick access to best options
    cheapest_option: Optional[TransportOption] = None
    fastest_option: Optional[TransportOption] = None
    most_available_option: Optional[TransportOption] = None
    recommended_option: Optional[TransportOption] = None
    
    # System status
    timestamp: datetime = None
    response_time_ms: float = 0.0
    data_freshness_score: float = 0.0
    fallback_providers_used: List[str] = None
    
    # Alternatives if main services fail
    alternatives_available: bool = True
    service_status: Dict[str, str] = None
    pathway_realtime: Optional[Dict[str, Any]] = None

class ConsolidatedTransportAPI:
    """Main API class that consolidates all transport data"""
    
    def __init__(self):
        # Initialize all service components
        self.integration_agent = TransportIntegrationAgent()
        self.routing_manager = FallbackRoutingManager()
        self.taxi_service = TaxiIntegrationService()
        self.freshness_validator = DataFreshnessValidator()
        
        # Pricing models for different transport modes
        self.pricing_models = {
            TransportMode.BMTC_REGULAR: {'base': 8, 'per_km': 1.5, 'max': 25},
            TransportMode.BMTC_AC: {'base': 12, 'per_km': 2.0, 'max': 35},
            TransportMode.BMTC_VOLVO: {'base': 15, 'per_km': 2.5, 'max': 45},
            TransportMode.METRO: {'base': 10, 'per_km': 3.0, 'max': 60},
        }
        
        # Average speeds for ETA calculation (km/h)
        self.avg_speeds = {
            TransportMode.BMTC_REGULAR: 18,
            TransportMode.BMTC_AC: 20,
            TransportMode.BMTC_VOLVO: 22,
            TransportMode.METRO: 35,
            TransportMode.WALKING: 5,
            TransportMode.CYCLING: 15
        }
        
        logger.info("Consolidated Transport API initialized")
    
    @error_handler_decorator("consolidated_transport_api")
    @performance_monitor("consolidated_transport_api")
    async def get_all_transport_options(self, source_lat: float, source_lng: float,
                                       dest_lat: float, dest_lng: float,
                                       source_name: Optional[str] = None,
                                       dest_name: Optional[str] = None) -> ConsolidatedTransportResponse:
        """Get all available transport options with cost, ETA, and availability"""
        
        start_time = datetime.now()
        logger.info(f"Getting transport options from ({source_lat:.4f}, {source_lng:.4f}) to ({dest_lat:.4f}, {dest_lng:.4f})")
        
        # Initialize response
        response = ConsolidatedTransportResponse(
            source_lat=source_lat,
            source_lng=source_lng,
            dest_lat=dest_lat,
            dest_lng=dest_lng,
            source_name=source_name,
            dest_name=dest_name,
            options=[],
            timestamp=start_time,
            fallback_providers_used=[],
            service_status={}
        )
        
        # Check data freshness first
        freshness_report = await self.freshness_validator.validate_all_sources()
        response.data_freshness_score = self.freshness_validator.get_system_health_score()
        
        # Auto-refresh stale data if needed
        if freshness_report.stale_sources or freshness_report.expired_sources:
            logger.info("Auto-refreshing stale data sources")
            await self.freshness_validator.auto_refresh_stale_data()
        
        # Gather all transport options concurrently
        tasks = [
            self._get_bmtc_options(source_lat, source_lng, dest_lat, dest_lng),
            self._get_metro_options(source_lat, source_lng, dest_lat, dest_lng),
            self._get_taxi_options(source_lat, source_lng, dest_lat, dest_lng),
            self._get_walking_cycling_options(source_lat, source_lng, dest_lat, dest_lng)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combine all options
        all_options = []
        for result in results:
            if isinstance(result, list):
                all_options.extend(result)
            elif isinstance(result, Exception):
                logger.error(f"Error getting transport options: {result}")
        
        # Sort options by a composite score (cost, time, availability)
        all_options.sort(key=self._calculate_option_score)
        response.options = all_options
        
        # Identify best options
        if all_options:
            response.cheapest_option = min(all_options, key=lambda x: x.cost_inr)
            response.fastest_option = min(all_options, key=lambda x: x.travel_time_minutes + x.next_availability_minutes)
            response.most_available_option = min(all_options, key=lambda x: x.next_availability_minutes)
            response.recommended_option = self._get_recommended_option(all_options)
        
        # Check if alternatives are available
        response.alternatives_available = len(all_options) > 0
        
        # Get service status
        response.service_status = await self._get_service_status()
        
        # Get Pathway real-time data
        logger.info("Attempting to fetch Pathway real-time data...")
        try:
            logger.info(f"Integration agent available: {self.integration_agent is not None}")
            pathway_data = await self.integration_agent._get_pathway_realtime_data(
                source_lat, source_lng, dest_lat, dest_lng
            )
            response.pathway_realtime = pathway_data
            logger.info(f"Successfully added Pathway real-time data to response: {pathway_data}")
        except Exception as e:
            logger.error(f"Failed to get Pathway real-time data: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            response.pathway_realtime = None
        
        # Calculate response time
        response.response_time_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        logger.info(f"Found {len(all_options)} transport options in {response.response_time_ms:.0f}ms")
        
        return response
    
    async def _get_bmtc_options(self, source_lat: float, source_lng: float,
                               dest_lat: float, dest_lng: float) -> List[TransportOption]:
        """Get BMTC bus options"""
        options = []
        
        try:
            # Get BMTC data from integration agent
            bmtc_data = await self.integration_agent.get_consolidated_transport_data(
                source_lat, source_lng, dest_lat, dest_lng
            )
            
            if bmtc_data and 'bmtc' in bmtc_data:
                bmtc_info = bmtc_data['bmtc']
                
                # Calculate distance for pricing
                distance_km = self._calculate_distance(source_lat, source_lng, dest_lat, dest_lng)
                
                # Generate options for different BMTC bus types
                bus_types = [
                    (TransportMode.BMTC_REGULAR, 'Regular'),
                    (TransportMode.BMTC_AC, 'AC'),
                    (TransportMode.BMTC_VOLVO, 'Volvo')
                ]
                
                for mode, bus_type in bus_types:
                    # Calculate fare
                    pricing = self.pricing_models[mode]
                    fare = min(pricing['base'] + (distance_km * pricing['per_km']), pricing['max'])
                    
                    # Calculate travel time
                    travel_time = (distance_km / self.avg_speeds[mode]) * 60
                    
                    # Get next availability from live data
                    next_availability = self._get_bmtc_next_availability(bmtc_info, bus_type)
                    
                    # Determine confidence based on data freshness
                    confidence = 0.9 if bmtc_info.get('data_fresh', False) else 0.6
                    
                    option = TransportOption(
                        mode=mode,
                        provider='bmtc',
                        cost_inr=round(fare, 2),
                        travel_time_minutes=int(travel_time),
                        next_availability_minutes=next_availability,
                        distance_km=round(distance_km, 2),
                        route_description=f"BMTC {bus_type} Bus",
                        confidence_score=confidence,
                        stops=bmtc_info.get('stops', []),
                        last_updated=datetime.now(),
                        data_source='live' if bmtc_info.get('data_fresh', False) else 'cached'
                    )
                    
                    options.append(option)
            
        except Exception as e:
            logger.error(f"Error getting BMTC options: {e}")
            # Fallback to estimated options
            options.extend(await self._get_estimated_bmtc_options(source_lat, source_lng, dest_lat, dest_lng))
        
        return options
    
    async def _get_metro_options(self, source_lat: float, source_lng: float,
                                dest_lat: float, dest_lng: float) -> List[TransportOption]:
        """Get Metro (BMRCL) options"""
        options = []
        
        try:
            # Get Metro data from integration agent
            metro_data = await self.integration_agent.get_consolidated_transport_data(
                source_lat, source_lng, dest_lat, dest_lng
            )
            
            if metro_data and 'metro' in metro_data:
                metro_info = metro_data['metro']
                
                # Calculate distance and fare
                distance_km = self._calculate_distance(source_lat, source_lng, dest_lat, dest_lng)
                pricing = self.pricing_models[TransportMode.METRO]
                fare = min(pricing['base'] + (distance_km * pricing['per_km']), pricing['max'])
                
                # Calculate travel time
                travel_time = (distance_km / self.avg_speeds[TransportMode.METRO]) * 60
                
                # Get next train availability
                next_availability = self._get_metro_next_availability(metro_info)
                
                # Determine confidence
                confidence = 0.9 if metro_info.get('data_fresh', False) else 0.6
                
                option = TransportOption(
                    mode=TransportMode.METRO,
                    provider='bmrcl',
                    cost_inr=round(fare, 2),
                    travel_time_minutes=int(travel_time),
                    next_availability_minutes=next_availability,
                    distance_km=round(distance_km, 2),
                    route_description="Namma Metro",
                    confidence_score=confidence,
                    stops=metro_info.get('stations', []),
                    last_updated=datetime.now(),
                    data_source='live' if metro_info.get('data_fresh', False) else 'cached'
                )
                
                options.append(option)
            
        except Exception as e:
            logger.error(f"Error getting Metro options: {e}")
            # Fallback to estimated options
            options.extend(await self._get_estimated_metro_options(source_lat, source_lng, dest_lat, dest_lng))
        
        return options
    
    async def _get_taxi_options(self, source_lat: float, source_lng: float,
                               dest_lat: float, dest_lng: float) -> List[TransportOption]:
        """Get taxi options from all providers"""
        options = []
        
        try:
            # Get taxi data from taxi service
            taxi_response = await self.taxi_service.get_taxi_options(source_lat, source_lng, dest_lat, dest_lng)
            
            if taxi_response.success:
                for taxi_option in taxi_response.options:
                    # Map taxi types to transport modes
                    mode_mapping = {
                        'auto': TransportMode.TAXI_AUTO,
                        'mini': TransportMode.TAXI_MINI,
                        'sedan': TransportMode.TAXI_SEDAN,
                        'suv': TransportMode.TAXI_SUV,
                        'bike': TransportMode.TAXI_BIKE
                    }
                    
                    mode = mode_mapping.get(taxi_option.vehicle_type.lower(), TransportMode.TAXI_MINI)
                    
                    # Determine confidence based on provider
                    confidence = 0.9 if taxi_option.provider in ['uber', 'ola'] else 0.7
                    
                    option = TransportOption(
                        mode=mode,
                        provider=taxi_option.provider,
                        cost_inr=taxi_option.total_fare,
                        travel_time_minutes=taxi_option.eta_minutes,
                        next_availability_minutes=2,  # Taxis are usually available quickly
                        distance_km=taxi_option.distance_km,
                        route_description=f"{taxi_option.provider.title()} {taxi_option.vehicle_type.title()}",
                        confidence_score=confidence,
                        surge_multiplier=taxi_option.surge_multiplier,
                        booking_fee=taxi_option.booking_fee,
                        last_updated=datetime.now(),
                        data_source='live',
                        fallback_used=taxi_option.provider == 'mock'
                    )
                    
                    options.append(option)
            
        except Exception as e:
            logger.error(f"Error getting taxi options: {e}")
        
        return options
    
    async def _get_walking_cycling_options(self, source_lat: float, source_lng: float,
                                          dest_lat: float, dest_lng: float) -> List[TransportOption]:
        """Get walking and cycling options"""
        options = []
        
        try:
            distance_km = self._calculate_distance(source_lat, source_lng, dest_lat, dest_lng)
            
            # Walking option (if distance is reasonable)
            if distance_km <= 5:  # Up to 5km for walking
                walking_time = (distance_km / self.avg_speeds[TransportMode.WALKING]) * 60
                
                walking_option = TransportOption(
                    mode=TransportMode.WALKING,
                    provider='self',
                    cost_inr=0.0,
                    travel_time_minutes=int(walking_time),
                    next_availability_minutes=0,
                    distance_km=round(distance_km, 2),
                    route_description="Walking",
                    confidence_score=1.0,
                    last_updated=datetime.now(),
                    data_source='calculated'
                )
                
                options.append(walking_option)
            
            # Cycling option (if distance is reasonable)
            if distance_km <= 15:  # Up to 15km for cycling
                cycling_time = (distance_km / self.avg_speeds[TransportMode.CYCLING]) * 60
                
                cycling_option = TransportOption(
                    mode=TransportMode.CYCLING,
                    provider='self',
                    cost_inr=0.0,
                    travel_time_minutes=int(cycling_time),
                    next_availability_minutes=0,
                    distance_km=round(distance_km, 2),
                    route_description="Cycling",
                    confidence_score=1.0,
                    last_updated=datetime.now(),
                    data_source='calculated'
                )
                
                options.append(cycling_option)
            
        except Exception as e:
            logger.error(f"Error getting walking/cycling options: {e}")
        
        return options
    
    async def _get_estimated_bmtc_options(self, source_lat: float, source_lng: float,
                                         dest_lat: float, dest_lng: float) -> List[TransportOption]:
        """Get estimated BMTC options when live data is unavailable"""
        options = []
        distance_km = self._calculate_distance(source_lat, source_lng, dest_lat, dest_lng)
        
        bus_types = [
            (TransportMode.BMTC_REGULAR, 'Regular'),
            (TransportMode.BMTC_AC, 'AC')
        ]
        
        for mode, bus_type in bus_types:
            pricing = self.pricing_models[mode]
            fare = min(pricing['base'] + (distance_km * pricing['per_km']), pricing['max'])
            travel_time = (distance_km / self.avg_speeds[mode]) * 60
            
            option = TransportOption(
                mode=mode,
                provider='bmtc',
                cost_inr=round(fare, 2),
                travel_time_minutes=int(travel_time),
                next_availability_minutes=8,  # Estimated wait time
                distance_km=round(distance_km, 2),
                route_description=f"BMTC {bus_type} Bus (Estimated)",
                confidence_score=0.5,
                last_updated=datetime.now(),
                data_source='estimated',
                fallback_used=True
            )
            
            options.append(option)
        
        return options
    
    async def _get_estimated_metro_options(self, source_lat: float, source_lng: float,
                                          dest_lat: float, dest_lng: float) -> List[TransportOption]:
        """Get estimated Metro options when live data is unavailable"""
        options = []
        distance_km = self._calculate_distance(source_lat, source_lng, dest_lat, dest_lng)
        
        pricing = self.pricing_models[TransportMode.METRO]
        fare = min(pricing['base'] + (distance_km * pricing['per_km']), pricing['max'])
        travel_time = (distance_km / self.avg_speeds[TransportMode.METRO]) * 60
        
        option = TransportOption(
            mode=TransportMode.METRO,
            provider='bmrcl',
            cost_inr=round(fare, 2),
            travel_time_minutes=int(travel_time),
            next_availability_minutes=5,  # Estimated wait time
            distance_km=round(distance_km, 2),
            route_description="Namma Metro (Estimated)",
            confidence_score=0.5,
            last_updated=datetime.now(),
            data_source='estimated',
            fallback_used=True
        )
        
        options.append(option)
        return options
    
    def _get_bmtc_next_availability(self, bmtc_info: Dict, bus_type: str) -> int:
        """Calculate next BMTC bus availability"""
        try:
            live_buses = bmtc_info.get('live_buses', [])
            
            # Filter buses by type if possible
            relevant_buses = [bus for bus in live_buses if bus_type.lower() in bus.get('type', '').lower()]
            
            if not relevant_buses:
                relevant_buses = live_buses
            
            if relevant_buses:
                # Find the closest bus
                min_eta = min(bus.get('eta_minutes', 10) for bus in relevant_buses)
                return max(1, min_eta)  # At least 1 minute
            else:
                # Default estimate based on time of day
                current_hour = datetime.now().hour
                if 7 <= current_hour <= 10 or 17 <= current_hour <= 20:  # Peak hours
                    return 5
                else:
                    return 8
                    
        except Exception as e:
            logger.error(f"Error calculating BMTC availability: {e}")
            return 8
    
    def _get_metro_next_availability(self, metro_info: Dict) -> int:
        """Calculate next Metro train availability"""
        try:
            live_trains = metro_info.get('live_trains', [])
            
            if live_trains:
                # Find the next train
                min_eta = min(train.get('eta_minutes', 5) for train in live_trains)
                return max(1, min_eta)
            else:
                # Metro frequency is usually 3-7 minutes
                current_hour = datetime.now().hour
                if 7 <= current_hour <= 10 or 17 <= current_hour <= 20:  # Peak hours
                    return 3
                else:
                    return 5
                    
        except Exception as e:
            logger.error(f"Error calculating Metro availability: {e}")
            return 5
    
    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate Haversine distance between two points"""
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
    
    def _calculate_option_score(self, option: TransportOption) -> float:
        """Calculate a composite score for sorting options"""
        # Normalize factors (lower is better)
        cost_score = option.cost_inr / 100.0  # Normalize to 0-1 range
        time_score = (option.travel_time_minutes + option.next_availability_minutes) / 60.0
        availability_score = option.next_availability_minutes / 30.0
        confidence_penalty = (1.0 - option.confidence_score) * 0.5
        
        # Weighted composite score
        composite_score = (cost_score * 0.3 + 
                          time_score * 0.4 + 
                          availability_score * 0.2 + 
                          confidence_penalty * 0.1)
        
        return composite_score
    
    def _get_recommended_option(self, options: List[TransportOption]) -> Optional[TransportOption]:
        """Get the recommended option based on multiple factors"""
        if not options:
            return None
        
        # Score each option
        scored_options = []
        for option in options:
            score = 0
            
            # Prefer reliable data sources
            if option.confidence_score > 0.8:
                score += 20
            elif option.confidence_score > 0.6:
                score += 10
            
            # Prefer faster options
            total_time = option.travel_time_minutes + option.next_availability_minutes
            if total_time < 30:
                score += 15
            elif total_time < 60:
                score += 10
            
            # Prefer cost-effective options
            if option.cost_inr < 50:
                score += 15
            elif option.cost_inr < 100:
                score += 10
            
            # Prefer available options
            if option.next_availability_minutes < 5:
                score += 10
            elif option.next_availability_minutes < 10:
                score += 5
            
            # Bonus for public transport
            if option.mode in [TransportMode.BMTC_REGULAR, TransportMode.BMTC_AC, TransportMode.METRO]:
                score += 5
            
            scored_options.append((option, score))
        
        # Return the highest scored option
        return max(scored_options, key=lambda x: x[1])[0]
    
    async def _get_service_status(self) -> Dict[str, str]:
        """Get status of all services"""
        status = {}
        
        try:
            # Get freshness report
            freshness_report = await self.freshness_validator.validate_all_sources()
            
            for source, info in freshness_report.sources.items():
                if info.status.value == 'fresh':
                    status[source.value] = 'healthy'
                elif info.status.value == 'stale':
                    status[source.value] = 'degraded'
                else:
                    status[source.value] = 'unhealthy'
            
            # Get routing provider status
            routing_status = self.routing_manager.get_provider_status()
            status['routing'] = 'healthy' if routing_status['available_providers'] else 'unhealthy'
            
            # Get taxi provider status
            taxi_status = self.taxi_service.get_provider_status()
            status['taxi'] = 'healthy' if taxi_status['available_providers'] else 'unhealthy'
            
        except Exception as e:
            logger.error(f"Error getting service status: {e}")
            status['error'] = str(e)
        
        return status
    
    async def get_quick_summary(self, source_lat: float, source_lng: float,
                               dest_lat: float, dest_lng: float) -> Dict[str, Any]:
        """Get a quick summary of transport options"""
        try:
            response = await self.get_all_transport_options(source_lat, source_lng, dest_lat, dest_lng)
            
            summary = {
                'total_options': len(response.options),
                'cheapest': {
                    'mode': response.cheapest_option.mode.value if response.cheapest_option else None,
                    'cost': response.cheapest_option.cost_inr if response.cheapest_option else None
                },
                'fastest': {
                    'mode': response.fastest_option.mode.value if response.fastest_option else None,
                    'time': response.fastest_option.travel_time_minutes if response.fastest_option else None
                },
                'recommended': {
                    'mode': response.recommended_option.mode.value if response.recommended_option else None,
                    'cost': response.recommended_option.cost_inr if response.recommended_option else None,
                    'time': response.recommended_option.travel_time_minutes if response.recommended_option else None
                },
                'system_health': response.data_freshness_score,
                'response_time_ms': response.response_time_ms
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting quick summary: {e}")
            return {'error': str(e)}

# Global instance
consolidated_transport_api = ConsolidatedTransportAPI()

async def test_consolidated_api():
    """Test the consolidated transport API"""
    logger.info("Testing consolidated transport API...")
    
    # Test coordinates (Bangalore city center to Whitefield)
    source_lat, source_lng = 12.9716, 77.5946
    dest_lat, dest_lng = 12.9698, 77.7500
    
    # Get all options
    response = await consolidated_transport_api.get_all_transport_options(
        source_lat, source_lng, dest_lat, dest_lng,
        "City Center", "Whitefield"
    )
    
    logger.info(f"Found {len(response.options)} transport options:")
    
    for option in response.options[:5]:  # Show top 5
        logger.info(f"  {option.mode.value}: ₹{option.cost_inr:.2f}, "
                   f"{option.travel_time_minutes + option.next_availability_minutes}min total, "
                   f"confidence: {option.confidence_score:.2f}")
    
    if response.recommended_option:
        rec = response.recommended_option
        logger.info(f"Recommended: {rec.mode.value} - ₹{rec.cost_inr:.2f}, {rec.travel_time_minutes}min")
    
    # Test quick summary
    summary = await consolidated_transport_api.get_quick_summary(source_lat, source_lng, dest_lat, dest_lng)
    logger.info(f"Quick summary: {summary}")

if __name__ == "__main__":
    asyncio.run(test_consolidated_api())