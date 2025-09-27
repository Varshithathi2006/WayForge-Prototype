#!/usr/bin/env python3
"""
Transport Integration Agent
Monitors logs, detects issues, and automatically fixes problems to ensure real-time transit data availability
"""

import os
import json
import logging
import asyncio
import time
import requests
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
import re

from .common import setup_logging
from .error_handler import error_handler_decorator, performance_monitor
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from pathway_streaming import PathwayTransitStreaming

logger = setup_logging("transport_integration_agent")

class ServiceStatus(Enum):
    """Service status enumeration"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

@dataclass
class ServiceHealth:
    """Service health information"""
    service_name: str
    status: ServiceStatus
    last_check: datetime
    error_count: int = 0
    last_error: Optional[str] = None
    response_time_ms: float = 0.0
    uptime_percentage: float = 100.0

class TransportIntegrationAgent:
    """Main agent for monitoring and maintaining transport data integrity"""
    
    def __init__(self):
        self.service_health = {}
        self.api_credentials = {
            'bmtc': {'api_key': os.getenv('BMTC_API_KEY', 'demo-key')},
            'bmrcl': {'api_key': os.getenv('BMRCL_API_KEY', 'demo-key')},
            'ors': {'api_key': os.getenv('ORS_API_KEY', 'demo-key')},
            'google': {'api_key': os.getenv('GOOGLE_MAPS_API_KEY', 'demo-key')},
            'mapbox': {'api_key': os.getenv('MAPBOX_API_KEY', 'demo-key')},
            'uber': {'api_key': os.getenv('UBER_API_KEY', 'demo-key')},
            'ola': {'api_key': os.getenv('OLA_API_KEY', 'demo-key')}
        }
        
        # Initialize service health tracking
        services = ['bmtc', 'bmrcl', 'routing', 'taxi', 'data_freshness']
        for service in services:
            self.service_health[service] = ServiceHealth(
                service_name=service,
                status=ServiceStatus.UNKNOWN,
                last_check=datetime.now()
            )
        
        # Initialize Pathway streaming for real-time data
        try:
            self.pathway_streaming = PathwayTransitStreaming()
            logger.info("Pathway streaming initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Pathway streaming: {str(e)}")
            self.pathway_streaming = None
        
        logger.info("Transport Integration Agent initialized")
    
    @error_handler_decorator("transport_integration_agent")
    @performance_monitor("transport_integration_agent")
    async def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        
        # Update service health
        await self._check_all_services()
        
        status = {
            'overall_health': self._calculate_overall_health(),
            'services': {
                name: {
                    'status': health.status.value,
                    'last_check': health.last_check.isoformat(),
                    'error_count': health.error_count,
                    'response_time_ms': health.response_time_ms,
                    'uptime_percentage': health.uptime_percentage
                }
                for name, health in self.service_health.items()
            },
            'api_credentials_status': await self._check_api_credentials(),
            'timestamp': datetime.now().isoformat()
        }
        
        return status
    
    @error_handler_decorator("transport_integration_agent")
    async def get_consolidated_transport_data(self, source_lat: float, source_lng: float,
                                            dest_lat: float, dest_lng: float) -> Dict[str, Any]:
        """Get consolidated transport data from all sources with real-time Pathway integration"""
        
        # Get real-time data from Pathway if available
        real_time_data = {}
        if self.pathway_streaming:
            try:
                real_time_data = await self._get_pathway_realtime_data(source_lat, source_lng, dest_lat, dest_lng)
            except Exception as e:
                logger.warning(f"Pathway real-time data fetch failed: {str(e)}")
        
        data = {
            'bmtc': await self._get_bmtc_data(source_lat, source_lng, dest_lat, dest_lng),
            'metro': await self._get_metro_data(source_lat, source_lng, dest_lat, dest_lng),
            'taxi': await self._get_taxi_data(source_lat, source_lng, dest_lat, dest_lng),
            'routing': await self._get_routing_data(source_lat, source_lng, dest_lat, dest_lng),
            'pathway_realtime': real_time_data
        }
        
        return data
    
    @error_handler_decorator("transport_integration_agent")
    async def force_data_refresh(self) -> Dict[str, Any]:
        """Force refresh of all data sources"""
        
        refresh_results = {}
        
        try:
            # Refresh BMTC data
            refresh_results['bmtc'] = await self._refresh_bmtc_data()
            
            # Refresh Metro data
            refresh_results['metro'] = await self._refresh_metro_data()
            
            # Refresh taxi data
            refresh_results['taxi'] = await self._refresh_taxi_data()
            
            # Refresh routing data
            refresh_results['routing'] = await self._refresh_routing_data()
            
            logger.info("All data sources refreshed successfully")
            
        except Exception as e:
            logger.error(f"Error during data refresh: {e}")
            refresh_results['error'] = str(e)
        
        return refresh_results
    
    async def _check_all_services(self):
        """Check health of all services"""
        
        tasks = [
            self._check_bmtc_health(),
            self._check_metro_health(),
            self._check_routing_health(),
            self._check_taxi_health(),
            self._check_data_freshness_health()
        ]
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _check_bmtc_health(self):
        """Check BMTC service health"""
        start_time = time.time()
        
        try:
            # Simulate BMTC health check
            await asyncio.sleep(0.1)  # Simulate API call
            
            response_time = (time.time() - start_time) * 1000
            
            self.service_health['bmtc'] = ServiceHealth(
                service_name='bmtc',
                status=ServiceStatus.HEALTHY,
                last_check=datetime.now(),
                response_time_ms=response_time,
                uptime_percentage=95.0
            )
            
        except Exception as e:
            self.service_health['bmtc'].status = ServiceStatus.UNHEALTHY
            self.service_health['bmtc'].error_count += 1
            self.service_health['bmtc'].last_error = str(e)
            logger.error(f"BMTC health check failed: {e}")
    
    async def _check_metro_health(self):
        """Check Metro service health"""
        start_time = time.time()
        
        try:
            # Simulate Metro health check
            await asyncio.sleep(0.1)  # Simulate API call
            
            response_time = (time.time() - start_time) * 1000
            
            self.service_health['bmrcl'] = ServiceHealth(
                service_name='bmrcl',
                status=ServiceStatus.HEALTHY,
                last_check=datetime.now(),
                response_time_ms=response_time,
                uptime_percentage=98.0
            )
            
        except Exception as e:
            self.service_health['bmrcl'].status = ServiceStatus.UNHEALTHY
            self.service_health['bmrcl'].error_count += 1
            self.service_health['bmrcl'].last_error = str(e)
            logger.error(f"Metro health check failed: {e}")
    
    async def _check_routing_health(self):
        """Check routing service health"""
        start_time = time.time()
        
        try:
            # Simulate routing health check
            await asyncio.sleep(0.1)  # Simulate API call
            
            response_time = (time.time() - start_time) * 1000
            
            self.service_health['routing'] = ServiceHealth(
                service_name='routing',
                status=ServiceStatus.HEALTHY,
                last_check=datetime.now(),
                response_time_ms=response_time,
                uptime_percentage=92.0
            )
            
        except Exception as e:
            self.service_health['routing'].status = ServiceStatus.UNHEALTHY
            self.service_health['routing'].error_count += 1
            self.service_health['routing'].last_error = str(e)
            logger.error(f"Routing health check failed: {e}")
    
    async def _check_taxi_health(self):
        """Check taxi service health"""
        start_time = time.time()
        
        try:
            # Simulate taxi health check
            await asyncio.sleep(0.1)  # Simulate API call
            
            response_time = (time.time() - start_time) * 1000
            
            self.service_health['taxi'] = ServiceHealth(
                service_name='taxi',
                status=ServiceStatus.HEALTHY,
                last_check=datetime.now(),
                response_time_ms=response_time,
                uptime_percentage=88.0
            )
            
        except Exception as e:
            self.service_health['taxi'].status = ServiceStatus.UNHEALTHY
            self.service_health['taxi'].error_count += 1
            self.service_health['taxi'].last_error = str(e)
            logger.error(f"Taxi health check failed: {e}")
    
    async def _check_data_freshness_health(self):
        """Check data freshness health"""
        start_time = time.time()
        
        try:
            # Simulate data freshness check
            await asyncio.sleep(0.05)  # Simulate check
            
            response_time = (time.time() - start_time) * 1000
            
            self.service_health['data_freshness'] = ServiceHealth(
                service_name='data_freshness',
                status=ServiceStatus.HEALTHY,
                last_check=datetime.now(),
                response_time_ms=response_time,
                uptime_percentage=99.0
            )
            
        except Exception as e:
            self.service_health['data_freshness'].status = ServiceStatus.UNHEALTHY
            self.service_health['data_freshness'].error_count += 1
            self.service_health['data_freshness'].last_error = str(e)
            logger.error(f"Data freshness health check failed: {e}")
    
    async def _check_api_credentials(self) -> Dict[str, str]:
        """Check status of API credentials"""
        
        status = {}
        
        for service, creds in self.api_credentials.items():
            if creds.get('api_key') and creds['api_key'] != 'demo-key':
                status[service] = 'configured'
            else:
                status[service] = 'demo_mode'
        
        return status
    
    def _calculate_overall_health(self) -> str:
        """Calculate overall system health"""
        
        healthy_count = sum(1 for health in self.service_health.values() 
                          if health.status == ServiceStatus.HEALTHY)
        total_count = len(self.service_health)
        
        health_percentage = (healthy_count / total_count) * 100
        
        if health_percentage >= 80:
            return 'healthy'
        elif health_percentage >= 60:
            return 'degraded'
        else:
            return 'unhealthy'
    
    async def _get_bmtc_data(self, source_lat: float, source_lng: float,
                           dest_lat: float, dest_lng: float) -> Dict[str, Any]:
        """Get BMTC data"""
        
        try:
            # Simulate BMTC data fetch
            await asyncio.sleep(0.2)
            
            return {
                'status': 'success',
                'data_fresh': True,
                'live_buses': [
                    {'route': '201', 'eta_minutes': 5, 'type': 'regular'},
                    {'route': '202A', 'eta_minutes': 8, 'type': 'ac'},
                    {'route': '500V', 'eta_minutes': 12, 'type': 'volvo'}
                ],
                'stops': ['Stop A', 'Stop B', 'Stop C'],
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting BMTC data: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def _get_metro_data(self, source_lat: float, source_lng: float,
                            dest_lat: float, dest_lng: float) -> Dict[str, Any]:
        """Get Metro data"""
        
        try:
            # Simulate Metro data fetch
            await asyncio.sleep(0.15)
            
            return {
                'status': 'success',
                'data_fresh': True,
                'live_trains': [
                    {'line': 'Purple', 'eta_minutes': 3, 'direction': 'Whitefield'},
                    {'line': 'Green', 'eta_minutes': 7, 'direction': 'Silk Institute'}
                ],
                'stations': ['Station A', 'Station B', 'Station C'],
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting Metro data: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def _get_taxi_data(self, source_lat: float, source_lng: float,
                           dest_lat: float, dest_lng: float) -> Dict[str, Any]:
        """Get taxi data"""
        
        try:
            # Simulate taxi data fetch
            await asyncio.sleep(0.1)
            
            return {
                'status': 'success',
                'options': [
                    {'provider': 'uber', 'type': 'mini', 'eta_minutes': 4, 'fare': 120},
                    {'provider': 'ola', 'type': 'sedan', 'eta_minutes': 6, 'fare': 150},
                    {'provider': 'mock', 'type': 'auto', 'eta_minutes': 2, 'fare': 80}
                ],
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting taxi data: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def _get_routing_data(self, source_lat: float, source_lng: float,
                              dest_lat: float, dest_lng: float) -> Dict[str, Any]:
        """Get routing data"""
        
        try:
            # Simulate routing data fetch
            await asyncio.sleep(0.1)
            
            return {
                'status': 'success',
                'routes': [
                    {'provider': 'ors', 'distance_km': 15.2, 'duration_minutes': 35},
                    {'provider': 'google', 'distance_km': 14.8, 'duration_minutes': 32},
                    {'provider': 'mapbox', 'distance_km': 15.0, 'duration_minutes': 34}
                ],
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting routing data: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def _refresh_bmtc_data(self) -> Dict[str, Any]:
        """Refresh BMTC data"""
        try:
            await asyncio.sleep(0.3)  # Simulate refresh
            return {'status': 'success', 'message': 'BMTC data refreshed'}
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    async def _refresh_metro_data(self) -> Dict[str, Any]:
        """Refresh Metro data"""
        try:
            await asyncio.sleep(0.2)  # Simulate refresh
            return {'status': 'success', 'message': 'Metro data refreshed'}
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    async def _refresh_taxi_data(self) -> Dict[str, Any]:
        """Refresh taxi data"""
        try:
            await asyncio.sleep(0.1)  # Simulate refresh
            return {'status': 'success', 'message': 'Taxi data refreshed'}
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    async def _refresh_routing_data(self) -> Dict[str, Any]:
        """Refresh routing data"""
        try:
            await asyncio.sleep(0.15)  # Simulate refresh
            return {'status': 'success', 'message': 'Routing data refreshed'}
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    async def _get_pathway_realtime_data(self, source_lat: float, source_lng: float,
                                       dest_lat: float, dest_lng: float) -> Dict[str, Any]:
        """Get real-time data from Pathway streaming"""
        try:
            logger.info(f"Fetching Pathway real-time data for route: ({source_lat}, {source_lng}) -> ({dest_lat}, {dest_lng})")
            
            if not self.pathway_streaming:
                logger.warning("Pathway streaming not initialized")
                return {'status': 'unavailable', 'message': 'Pathway streaming not initialized'}
            
            logger.info("Pathway streaming is available, calling get_realtime_updates")
            
            # Get real-time transit updates from Pathway
            real_time_updates = await self.pathway_streaming.get_realtime_updates(
                source_lat, source_lng, dest_lat, dest_lng
            )
            
            logger.info(f"Pathway real-time updates received: {real_time_updates}")
            
            result = {
                'status': 'success',
                'real_time_buses': real_time_updates.get('buses', []),
                'real_time_metros': real_time_updates.get('metros', []),
                'traffic_conditions': real_time_updates.get('traffic', {}),
                'service_alerts': real_time_updates.get('alerts', []),
                'last_updated': datetime.now().isoformat()
            }
            
            logger.info(f"Pathway data result: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error getting Pathway real-time data: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def _check_data_freshness(self) -> Dict[str, Any]:
        """Check freshness of data from all sources"""
        try:
            freshness_data = {
                'bmtc_last_update': datetime.now() - timedelta(minutes=2),
                'metro_last_update': datetime.now() - timedelta(minutes=1),
                'taxi_last_update': datetime.now() - timedelta(seconds=30),
                'routing_last_update': datetime.now() - timedelta(minutes=5)
            }
            
            # Check if any data is stale (older than 10 minutes)
            stale_threshold = timedelta(minutes=10)
            current_time = datetime.now()
            
            freshness_status = {}
            for source, last_update in freshness_data.items():
                age = current_time - last_update
                freshness_status[source] = {
                    'age_minutes': age.total_seconds() / 60,
                    'is_fresh': age < stale_threshold,
                    'last_update': last_update.isoformat()
                }
            
            return {
                'status': 'success',
                'freshness': freshness_status,
                'overall_fresh': all(status['is_fresh'] for status in freshness_status.values())
            }
            
        except Exception as e:
            logger.error(f"Error checking data freshness: {e}")
            return {'status': 'error', 'error': str(e)}

# Test function
async def test_transport_integration_agent():
    """Test the transport integration agent"""
    logger.info("Testing Transport Integration Agent...")
    
    agent = TransportIntegrationAgent()
    
    # Test system status
    status = await agent.get_system_status()
    logger.info(f"System status: {status['overall_health']}")
    
    # Test consolidated data
    data = await agent.get_consolidated_transport_data(12.9716, 77.5946, 12.9698, 77.7500)
    logger.info(f"Got data for {len(data)} transport modes")
    
    # Test data refresh
    refresh_results = await agent.force_data_refresh()
    logger.info(f"Data refresh results: {refresh_results}")

if __name__ == "__main__":
    asyncio.run(test_transport_integration_agent())