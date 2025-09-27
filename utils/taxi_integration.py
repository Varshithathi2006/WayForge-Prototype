#!/usr/bin/env python3
"""
Taxi Integration Service for Ola/Uber APIs with Mock Pricing Fallback
Provides real-time taxi fare and ETA data with automatic fallback mechanisms
"""

import os
import requests
import json
import logging
import asyncio
import random
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import time
from datetime import datetime, timedelta

from .common import setup_logging
from .error_handler import error_handler_decorator, performance_monitor

logger = setup_logging("taxi_integration")

@dataclass
class TaxiOption:
    """Represents a taxi option with fare and ETA"""
    provider: str  # 'ola', 'uber', 'mock'
    vehicle_type: str  # 'mini', 'sedan', 'suv', 'auto', 'bike'
    fare_inr: float
    eta_minutes: int
    distance_km: float
    surge_multiplier: float = 1.0
    available: bool = True
    booking_fee: float = 0.0
    total_fare: float = 0.0

    def __post_init__(self):
        """Calculate total fare including booking fee and surge"""
        self.total_fare = (self.fare_inr * self.surge_multiplier) + self.booking_fee

@dataclass
class TaxiResponse:
    """Response containing all available taxi options"""
    source_lat: float
    source_lng: float
    dest_lat: float
    dest_lng: float
    options: List[TaxiOption]
    timestamp: datetime
    success: bool = True
    error_message: Optional[str] = None

class OlaAPIProvider:
    """Ola API integration (Note: Ola doesn't have public API, this is a mock structure)"""
    
    def __init__(self):
        self.api_key = os.getenv('OLA_API_KEY', '')
        self.base_url = "https://api.ola.com/v1"  # Hypothetical URL
        self.session = requests.Session()
        
    @error_handler_decorator("ola_api")
    @performance_monitor("ola_api")
    async def get_fare_estimate(self, source_lat: float, source_lng: float, 
                               dest_lat: float, dest_lng: float) -> List[TaxiOption]:
        """Get fare estimate from Ola API"""
        if not self.api_key:
            logger.warning("Ola API key not configured")
            return []
        
        try:
            # Note: This is a mock implementation as Ola doesn't have public API
            # In real implementation, you'd integrate with Ola's partner API
            
            headers = {
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                'pickup_lat': source_lat,
                'pickup_lng': source_lng,
                'drop_lat': dest_lat,
                'drop_lng': dest_lng
            }
            
            response = self.session.post(
                f"{self.base_url}/fare_estimate",
                headers=headers,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                return self._parse_ola_response(data)
            else:
                logger.error(f"Ola API error: {response.status_code}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Ola API request error: {e}")
            return []
        except Exception as e:
            logger.error(f"Ola API error: {e}")
            return []
    
    def _parse_ola_response(self, data: Dict) -> List[TaxiOption]:
        """Parse Ola API response"""
        options = []
        
        for estimate in data.get('estimates', []):
            option = TaxiOption(
                provider='ola',
                vehicle_type=estimate.get('category', 'unknown'),
                fare_inr=estimate.get('fare', 0),
                eta_minutes=estimate.get('eta', 0),
                distance_km=estimate.get('distance', 0),
                surge_multiplier=estimate.get('surge_multiplier', 1.0),
                booking_fee=estimate.get('booking_fee', 0)
            )
            options.append(option)
        
        return options

class UberAPIProvider:
    """Uber API integration"""
    
    def __init__(self):
        self.client_id = os.getenv('UBER_CLIENT_ID', '')
        self.client_secret = os.getenv('UBER_CLIENT_SECRET', '')
        self.server_token = os.getenv('UBER_SERVER_TOKEN', '')
        self.base_url = "https://api.uber.com/v1.2"
        self.session = requests.Session()
        
    @error_handler_decorator("uber_api")
    @performance_monitor("uber_api")
    async def get_fare_estimate(self, source_lat: float, source_lng: float, 
                               dest_lat: float, dest_lng: float) -> List[TaxiOption]:
        """Get fare estimate from Uber API"""
        if not self.server_token:
            logger.warning("Uber server token not configured")
            return []
        
        try:
            headers = {
                'Authorization': f'Token {self.server_token}',
                'Accept-Language': 'en_US',
                'Content-Type': 'application/json'
            }
            
            params = {
                'start_latitude': source_lat,
                'start_longitude': source_lng,
                'end_latitude': dest_lat,
                'end_longitude': dest_lng
            }
            
            # Get price estimates
            price_response = self.session.get(
                f"{self.base_url}/estimates/price",
                headers=headers,
                params=params,
                timeout=10
            )
            
            # Get time estimates
            time_params = {
                'start_latitude': source_lat,
                'start_longitude': source_lng
            }
            
            time_response = self.session.get(
                f"{self.base_url}/estimates/time",
                headers=headers,
                params=time_params,
                timeout=10
            )
            
            if price_response.status_code == 200 and time_response.status_code == 200:
                price_data = price_response.json()
                time_data = time_response.json()
                return self._parse_uber_response(price_data, time_data)
            else:
                logger.error(f"Uber API error: Price {price_response.status_code}, Time {time_response.status_code}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Uber API request error: {e}")
            return []
        except Exception as e:
            logger.error(f"Uber API error: {e}")
            return []
    
    def _parse_uber_response(self, price_data: Dict, time_data: Dict) -> List[TaxiOption]:
        """Parse Uber API response"""
        options = []
        
        # Create lookup for time estimates
        time_lookup = {}
        for time_est in time_data.get('times', []):
            time_lookup[time_est.get('product_id')] = time_est.get('estimate', 0) // 60  # Convert to minutes
        
        for price_est in price_data.get('prices', []):
            product_id = price_est.get('product_id')
            
            # Extract fare (take average of low and high estimate)
            low_estimate = price_est.get('low_estimate', 0)
            high_estimate = price_est.get('high_estimate', 0)
            fare_usd = (low_estimate + high_estimate) / 2
            
            # Convert USD to INR (approximate rate: 1 USD = 83 INR)
            fare_inr = fare_usd * 83
            
            option = TaxiOption(
                provider='uber',
                vehicle_type=price_est.get('localized_display_name', 'unknown'),
                fare_inr=fare_inr,
                eta_minutes=time_lookup.get(product_id, 0),
                distance_km=price_est.get('distance', 0),
                surge_multiplier=price_est.get('surge_multiplier', 1.0)
            )
            options.append(option)
        
        return options

class MockTaxiProvider:
    """Mock taxi provider for fallback pricing"""
    
    def __init__(self):
        # Base rates per km for different vehicle types in Bangalore
        self.base_rates = {
            'auto': {'base': 25, 'per_km': 15, 'per_min': 2},
            'mini': {'base': 50, 'per_km': 12, 'per_min': 2},
            'sedan': {'base': 80, 'per_km': 18, 'per_min': 3},
            'suv': {'base': 120, 'per_km': 25, 'per_min': 4},
            'bike': {'base': 20, 'per_km': 8, 'per_min': 1.5}
        }
        
        # Average speeds in Bangalore (km/h)
        self.avg_speeds = {
            'auto': 20,
            'mini': 25,
            'sedan': 25,
            'suv': 22,
            'bike': 30
        }
    
    @error_handler_decorator("mock_taxi")
    @performance_monitor("mock_taxi")
    async def get_fare_estimate(self, source_lat: float, source_lng: float, 
                               dest_lat: float, dest_lng: float) -> List[TaxiOption]:
        """Generate mock fare estimates based on distance and time"""
        try:
            # Calculate distance using Haversine formula
            distance_km = self._calculate_distance(source_lat, source_lng, dest_lat, dest_lng)
            
            options = []
            
            for vehicle_type, rates in self.base_rates.items():
                # Calculate base fare
                base_fare = rates['base']
                distance_fare = distance_km * rates['per_km']
                
                # Calculate time-based fare
                avg_speed = self.avg_speeds[vehicle_type]
                travel_time_minutes = (distance_km / avg_speed) * 60
                
                # Add traffic factor (20-50% extra time during peak hours)
                current_hour = datetime.now().hour
                if 8 <= current_hour <= 10 or 17 <= current_hour <= 20:  # Peak hours
                    traffic_factor = random.uniform(1.2, 1.5)
                else:
                    traffic_factor = random.uniform(1.0, 1.2)
                
                travel_time_minutes *= traffic_factor
                time_fare = travel_time_minutes * rates['per_min']
                
                total_fare = base_fare + distance_fare + time_fare
                
                # Add some randomness for surge pricing
                surge_multiplier = random.uniform(1.0, 1.8) if random.random() < 0.3 else 1.0
                
                # ETA includes waiting time
                eta_minutes = int(travel_time_minutes + random.uniform(2, 8))
                
                option = TaxiOption(
                    provider='mock',
                    vehicle_type=vehicle_type,
                    fare_inr=round(total_fare, 2),
                    eta_minutes=eta_minutes,
                    distance_km=round(distance_km, 2),
                    surge_multiplier=round(surge_multiplier, 2),
                    booking_fee=random.choice([0, 5, 10])  # Random booking fee
                )
                
                options.append(option)
            
            logger.info(f"Generated {len(options)} mock taxi options for {distance_km:.2f}km trip")
            return options
            
        except Exception as e:
            logger.error(f"Mock taxi provider error: {e}")
            return []
    
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

class TaxiIntegrationService:
    """Main service for taxi integration with fallback mechanisms"""
    
    def __init__(self):
        self.providers = {
            'ola': OlaAPIProvider(),
            'uber': UberAPIProvider(),
            'mock': MockTaxiProvider()
        }
        
        # Provider priority (try real APIs first, then fallback to mock)
        self.provider_priority = ['uber', 'ola', 'mock']
        
        # Provider health tracking
        self.provider_health = {name: True for name in self.providers.keys()}
        self.provider_failures = {name: 0 for name in self.providers.keys()}
        
        # Cache for recent requests (5 minute cache)
        self.cache = {}
        self.cache_duration = 300  # 5 minutes
        
        logger.info("Taxi Integration Service initialized")
    
    @error_handler_decorator("taxi_integration")
    @performance_monitor("taxi_integration")
    async def get_taxi_options(self, source_lat: float, source_lng: float, 
                              dest_lat: float, dest_lng: float) -> TaxiResponse:
        """Get taxi options with automatic fallback"""
        
        # Check cache first
        cache_key = f"{source_lat:.4f},{source_lng:.4f},{dest_lat:.4f},{dest_lng:.4f}"
        cached_result = self._get_cached_result(cache_key)
        if cached_result:
            logger.info("Returning cached taxi options")
            return cached_result
        
        all_options = []
        successful_providers = []
        
        # Try each provider
        for provider_name in self.provider_priority:
            if not self.provider_health[provider_name]:
                logger.debug(f"Skipping unhealthy provider: {provider_name}")
                continue
            
            try:
                logger.info(f"Fetching taxi options from {provider_name}")
                provider = self.providers[provider_name]
                
                options = await provider.get_fare_estimate(source_lat, source_lng, dest_lat, dest_lng)
                
                if options:
                    all_options.extend(options)
                    successful_providers.append(provider_name)
                    
                    # Reset failure count on success
                    self.provider_failures[provider_name] = 0
                    self.provider_health[provider_name] = True
                    
                    logger.info(f"Got {len(options)} options from {provider_name}")
                else:
                    await self._handle_provider_failure(provider_name, "No options returned")
                    
            except Exception as e:
                logger.error(f"Error with taxi provider {provider_name}: {e}")
                await self._handle_provider_failure(provider_name, str(e))
        
        # If no options from any provider, ensure we have mock data
        if not all_options:
            logger.warning("No taxi options from any provider, generating emergency mock data")
            mock_options = await self.providers['mock'].get_fare_estimate(
                source_lat, source_lng, dest_lat, dest_lng
            )
            all_options.extend(mock_options)
        
        # Sort options by total fare
        all_options.sort(key=lambda x: x.total_fare)
        
        response = TaxiResponse(
            source_lat=source_lat,
            source_lng=source_lng,
            dest_lat=dest_lat,
            dest_lng=dest_lng,
            options=all_options,
            timestamp=datetime.now(),
            success=len(all_options) > 0
        )
        
        # Cache the result
        self._cache_result(cache_key, response)
        
        logger.info(f"Returning {len(all_options)} taxi options from providers: {successful_providers}")
        return response
    
    async def _handle_provider_failure(self, provider_name: str, error_message: str):
        """Handle provider failure and update health status"""
        self.provider_failures[provider_name] += 1
        
        # Mark provider as unhealthy after 3 consecutive failures
        if self.provider_failures[provider_name] >= 3:
            self.provider_health[provider_name] = False
            logger.warning(f"Marked taxi provider {provider_name} as unhealthy after {self.provider_failures[provider_name]} failures")
        
        logger.error(f"Taxi provider {provider_name} failed: {error_message}")
    
    def _get_cached_result(self, cache_key: str) -> Optional[TaxiResponse]:
        """Get cached result if still valid"""
        if cache_key in self.cache:
            cached_data, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.cache_duration:
                return cached_data
            else:
                # Remove expired cache entry
                del self.cache[cache_key]
        return None
    
    def _cache_result(self, cache_key: str, response: TaxiResponse):
        """Cache the result"""
        self.cache[cache_key] = (response, time.time())
        
        # Clean old cache entries (keep only last 100)
        if len(self.cache) > 100:
            oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k][1])
            del self.cache[oldest_key]
    
    async def health_check_providers(self):
        """Perform health check on all taxi providers"""
        logger.info("Performing health check on taxi providers")
        
        # Test coordinates (Bangalore city center to airport)
        test_source_lat, test_source_lng = 12.9716, 77.5946
        test_dest_lat, test_dest_lng = 13.1986, 77.7066
        
        for provider_name, provider in self.providers.items():
            try:
                options = await provider.get_fare_estimate(
                    test_source_lat, test_source_lng, test_dest_lat, test_dest_lng
                )
                
                if options:
                    self.provider_health[provider_name] = True
                    self.provider_failures[provider_name] = 0
                    logger.info(f"Taxi provider {provider_name} health check: PASSED")
                else:
                    self.provider_failures[provider_name] += 1
                    if self.provider_failures[provider_name] >= 3:
                        self.provider_health[provider_name] = False
                    logger.warning(f"Taxi provider {provider_name} health check: FAILED")
                    
            except Exception as e:
                self.provider_failures[provider_name] += 1
                if self.provider_failures[provider_name] >= 3:
                    self.provider_health[provider_name] = False
                logger.error(f"Taxi provider {provider_name} health check error: {e}")
    
    def get_provider_status(self) -> Dict[str, Any]:
        """Get status of all taxi providers"""
        return {
            'provider_health': self.provider_health,
            'provider_failures': self.provider_failures,
            'available_providers': [name for name, healthy in self.provider_health.items() if healthy],
            'cache_size': len(self.cache)
        }
    
    def get_cheapest_option(self, response: TaxiResponse) -> Optional[TaxiOption]:
        """Get the cheapest taxi option"""
        if response.options:
            return min(response.options, key=lambda x: x.total_fare)
        return None
    
    def get_fastest_option(self, response: TaxiResponse) -> Optional[TaxiOption]:
        """Get the fastest taxi option"""
        if response.options:
            return min(response.options, key=lambda x: x.eta_minutes)
        return None
    
    def filter_by_vehicle_type(self, response: TaxiResponse, vehicle_type: str) -> List[TaxiOption]:
        """Filter options by vehicle type"""
        return [opt for opt in response.options if opt.vehicle_type.lower() == vehicle_type.lower()]

# Global instance
taxi_service = TaxiIntegrationService()

async def test_taxi_integration():
    """Test taxi integration service"""
    logger.info("Testing taxi integration service...")
    
    # Test coordinates (Bangalore city center to Whitefield)
    source_lat, source_lng = 12.9716, 77.5946
    dest_lat, dest_lng = 12.9698, 77.7500
    
    response = await taxi_service.get_taxi_options(source_lat, source_lng, dest_lat, dest_lng)
    
    if response.success:
        logger.info(f"Got {len(response.options)} taxi options:")
        for option in response.options:
            logger.info(f"  {option.provider} {option.vehicle_type}: ₹{option.total_fare:.2f}, {option.eta_minutes}min")
        
        # Test helper methods
        cheapest = taxi_service.get_cheapest_option(response)
        fastest = taxi_service.get_fastest_option(response)
        
        if cheapest:
            logger.info(f"Cheapest: {cheapest.provider} {cheapest.vehicle_type} - ₹{cheapest.total_fare:.2f}")
        if fastest:
            logger.info(f"Fastest: {fastest.provider} {fastest.vehicle_type} - {fastest.eta_minutes}min")
    else:
        logger.error("Failed to get taxi options")
    
    # Print provider status
    status = taxi_service.get_provider_status()
    logger.info(f"Provider status: {status}")

if __name__ == "__main__":
    asyncio.run(test_taxi_integration())