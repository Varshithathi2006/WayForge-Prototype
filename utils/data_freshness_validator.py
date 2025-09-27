#!/usr/bin/env python3
"""
Data Freshness Validator for BMTC and BMRCL Real-time Data
Ensures data is fresh (≤30 seconds) and triggers automatic refreshes when needed
"""

import os
import requests
import json
import logging
import asyncio
import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

from .common import setup_logging
from .error_handler import error_handler_decorator, performance_monitor

logger = setup_logging("data_freshness_validator")

class DataSource(Enum):
    """Enumeration of data sources"""
    BMTC = "bmtc"
    BMRCL = "bmrcl"
    TAXI = "taxi"
    ROUTING = "routing"

class FreshnessStatus(Enum):
    """Enumeration of freshness status"""
    FRESH = "fresh"
    STALE = "stale"
    EXPIRED = "expired"
    UNAVAILABLE = "unavailable"

@dataclass
class DataFreshnessInfo:
    """Information about data freshness"""
    source: DataSource
    endpoint: str
    last_update: Optional[datetime]
    age_seconds: float
    status: FreshnessStatus
    data_count: int = 0
    error_message: Optional[str] = None
    response_time_ms: float = 0.0

@dataclass
class FreshnessReport:
    """Complete freshness report for all data sources"""
    timestamp: datetime
    sources: Dict[DataSource, DataFreshnessInfo]
    overall_status: FreshnessStatus
    stale_sources: List[DataSource]
    expired_sources: List[DataSource]
    recommendations: List[str]

class DataFreshnessValidator:
    """Validates data freshness across all transport data sources"""
    
    def __init__(self, base_url: str = "http://localhost:5000"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        
        # Freshness thresholds (in seconds)
        self.thresholds = {
            DataSource.BMTC: 30,      # 30 seconds for BMTC live data
            DataSource.BMRCL: 30,     # 30 seconds for BMRCL live data
            DataSource.TAXI: 60,      # 1 minute for taxi data (less frequent updates)
            DataSource.ROUTING: 300   # 5 minutes for routing data (more stable)
        }
        
        # API endpoints to check
        self.endpoints = {
            DataSource.BMTC: "/api/live/bmtc",
            DataSource.BMRCL: "/api/live/bmrcl",
            DataSource.TAXI: "/api/taxi/options",
            DataSource.ROUTING: "/api/health"
        }
        
        # Historical data for trend analysis
        self.freshness_history = {source: [] for source in DataSource}
        self.max_history_size = 100
        
        logger.info("Data Freshness Validator initialized")
    
    @error_handler_decorator("data_freshness_validator")
    @performance_monitor("data_freshness_validator")
    async def validate_all_sources(self) -> FreshnessReport:
        """Validate freshness of all data sources"""
        logger.info("Starting comprehensive data freshness validation")
        
        sources_info = {}
        stale_sources = []
        expired_sources = []
        recommendations = []
        
        # Check each data source
        for source in DataSource:
            try:
                freshness_info = await self._check_source_freshness(source)
                sources_info[source] = freshness_info
                
                # Track stale and expired sources
                if freshness_info.status == FreshnessStatus.STALE:
                    stale_sources.append(source)
                elif freshness_info.status in [FreshnessStatus.EXPIRED, FreshnessStatus.UNAVAILABLE]:
                    expired_sources.append(source)
                
                # Add to history
                self._add_to_history(source, freshness_info)
                
            except Exception as e:
                logger.error(f"Error checking {source.value}: {e}")
                sources_info[source] = DataFreshnessInfo(
                    source=source,
                    endpoint=self.endpoints[source],
                    last_update=None,
                    age_seconds=float('inf'),
                    status=FreshnessStatus.UNAVAILABLE,
                    error_message=str(e)
                )
                expired_sources.append(source)
        
        # Determine overall status
        overall_status = self._determine_overall_status(sources_info)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(sources_info, stale_sources, expired_sources)
        
        report = FreshnessReport(
            timestamp=datetime.now(),
            sources=sources_info,
            overall_status=overall_status,
            stale_sources=stale_sources,
            expired_sources=expired_sources,
            recommendations=recommendations
        )
        
        logger.info(f"Freshness validation complete. Overall status: {overall_status.value}")
        logger.info(f"Stale sources: {[s.value for s in stale_sources]}")
        logger.info(f"Expired sources: {[s.value for s in expired_sources]}")
        
        return report
    
    async def _check_source_freshness(self, source: DataSource) -> DataFreshnessInfo:
        """Check freshness of a specific data source"""
        endpoint = self.endpoints[source]
        url = f"{self.base_url}{endpoint}"
        
        start_time = time.time()
        
        try:
            # Special handling for different endpoints
            if source == DataSource.TAXI:
                # For taxi, we need to provide coordinates
                params = {
                    'source_lat': 12.9716,
                    'source_lng': 77.5946,
                    'dest_lat': 12.9698,
                    'dest_lng': 77.7500
                }
                response = self.session.get(url, params=params, timeout=10)
            else:
                response = self.session.get(url, timeout=10)
            
            response_time_ms = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                data = response.json()
                return self._analyze_response_freshness(source, endpoint, data, response_time_ms)
            else:
                logger.error(f"HTTP {response.status_code} for {source.value}")
                return DataFreshnessInfo(
                    source=source,
                    endpoint=endpoint,
                    last_update=None,
                    age_seconds=float('inf'),
                    status=FreshnessStatus.UNAVAILABLE,
                    error_message=f"HTTP {response.status_code}",
                    response_time_ms=response_time_ms
                )
                
        except requests.exceptions.Timeout:
            logger.error(f"Timeout checking {source.value}")
            return DataFreshnessInfo(
                source=source,
                endpoint=endpoint,
                last_update=None,
                age_seconds=float('inf'),
                status=FreshnessStatus.UNAVAILABLE,
                error_message="Request timeout",
                response_time_ms=(time.time() - start_time) * 1000
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {source.value}: {e}")
            return DataFreshnessInfo(
                source=source,
                endpoint=endpoint,
                last_update=None,
                age_seconds=float('inf'),
                status=FreshnessStatus.UNAVAILABLE,
                error_message=str(e),
                response_time_ms=(time.time() - start_time) * 1000
            )
    
    def _analyze_response_freshness(self, source: DataSource, endpoint: str, 
                                   data: Dict, response_time_ms: float) -> DataFreshnessInfo:
        """Analyze response data to determine freshness"""
        
        last_update = None
        data_count = 0
        age_seconds = float('inf')
        
        try:
            # Extract timestamp and data count based on source type
            if source == DataSource.BMTC:
                if 'live_data' in data:
                    live_data = data['live_data']
                    data_count = len(live_data)
                    
                    # Find most recent update
                    if live_data:
                        timestamps = []
                        for vehicle in live_data:
                            if 'last_updated' in vehicle:
                                try:
                                    ts = datetime.fromisoformat(vehicle['last_updated'].replace('Z', '+00:00'))
                                    timestamps.append(ts)
                                except:
                                    pass
                        
                        if timestamps:
                            last_update = max(timestamps)
                            age_seconds = (datetime.now() - last_update.replace(tzinfo=None)).total_seconds()
            
            elif source == DataSource.BMRCL:
                if 'live_data' in data:
                    live_data = data['live_data']
                    data_count = len(live_data)
                    
                    # Find most recent update
                    if live_data:
                        timestamps = []
                        for train in live_data:
                            if 'last_updated' in train:
                                try:
                                    ts = datetime.fromisoformat(train['last_updated'].replace('Z', '+00:00'))
                                    timestamps.append(ts)
                                except:
                                    pass
                        
                        if timestamps:
                            last_update = max(timestamps)
                            age_seconds = (datetime.now() - last_update.replace(tzinfo=None)).total_seconds()
            
            elif source == DataSource.TAXI:
                if 'options' in data:
                    data_count = len(data['options'])
                    
                    # For taxi data, use the response timestamp
                    if 'timestamp' in data:
                        try:
                            last_update = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                            age_seconds = (datetime.now() - last_update.replace(tzinfo=None)).total_seconds()
                        except:
                            pass
            
            elif source == DataSource.ROUTING:
                # For routing/health endpoint, check if service is responsive
                if 'status' in data and data['status'] == 'healthy':
                    last_update = datetime.now()
                    age_seconds = 0
                    data_count = 1
            
            # If we couldn't extract timestamp, assume data is stale
            if last_update is None:
                age_seconds = float('inf')
            
        except Exception as e:
            logger.error(f"Error analyzing response for {source.value}: {e}")
            age_seconds = float('inf')
        
        # Determine freshness status
        threshold = self.thresholds[source]
        
        if age_seconds == float('inf'):
            status = FreshnessStatus.UNAVAILABLE
        elif age_seconds <= threshold:
            status = FreshnessStatus.FRESH
        elif age_seconds <= threshold * 2:  # Grace period
            status = FreshnessStatus.STALE
        else:
            status = FreshnessStatus.EXPIRED
        
        return DataFreshnessInfo(
            source=source,
            endpoint=endpoint,
            last_update=last_update,
            age_seconds=age_seconds,
            status=status,
            data_count=data_count,
            response_time_ms=response_time_ms
        )
    
    def _determine_overall_status(self, sources_info: Dict[DataSource, DataFreshnessInfo]) -> FreshnessStatus:
        """Determine overall system freshness status"""
        statuses = [info.status for info in sources_info.values()]
        
        # If any critical source is unavailable, overall is unavailable
        critical_sources = [DataSource.BMTC, DataSource.BMRCL]
        for source in critical_sources:
            if source in sources_info and sources_info[source].status == FreshnessStatus.UNAVAILABLE:
                return FreshnessStatus.UNAVAILABLE
        
        # If any source is expired, overall is expired
        if FreshnessStatus.EXPIRED in statuses:
            return FreshnessStatus.EXPIRED
        
        # If any source is stale, overall is stale
        if FreshnessStatus.STALE in statuses:
            return FreshnessStatus.STALE
        
        # All sources are fresh
        return FreshnessStatus.FRESH
    
    def _generate_recommendations(self, sources_info: Dict[DataSource, DataFreshnessInfo], 
                                 stale_sources: List[DataSource], 
                                 expired_sources: List[DataSource]) -> List[str]:
        """Generate recommendations based on freshness analysis"""
        recommendations = []
        
        # Recommendations for expired sources
        for source in expired_sources:
            if source == DataSource.BMTC:
                recommendations.append("Refresh BMTC data fetcher - restart service or check API credentials")
            elif source == DataSource.BMRCL:
                recommendations.append("Refresh BMRCL data fetcher - restart service or check API credentials")
            elif source == DataSource.TAXI:
                recommendations.append("Refresh taxi integration - check Ola/Uber API status")
            elif source == DataSource.ROUTING:
                recommendations.append("Restart routing service - check ORS/fallback providers")
        
        # Recommendations for stale sources
        for source in stale_sources:
            recommendations.append(f"Monitor {source.value} data source - approaching staleness threshold")
        
        # Performance recommendations
        for source, info in sources_info.items():
            if info.response_time_ms > 5000:  # > 5 seconds
                recommendations.append(f"Optimize {source.value} API response time ({info.response_time_ms:.0f}ms)")
            
            if info.data_count == 0 and info.status != FreshnessStatus.UNAVAILABLE:
                recommendations.append(f"Investigate empty data response from {source.value}")
        
        # General recommendations
        if len(expired_sources) > 1:
            recommendations.append("Multiple services failing - check network connectivity and API keys")
        
        if not recommendations:
            recommendations.append("All data sources are fresh and healthy")
        
        return recommendations
    
    def _add_to_history(self, source: DataSource, freshness_info: DataFreshnessInfo):
        """Add freshness info to historical data"""
        history = self.freshness_history[source]
        history.append({
            'timestamp': datetime.now(),
            'age_seconds': freshness_info.age_seconds,
            'status': freshness_info.status,
            'data_count': freshness_info.data_count,
            'response_time_ms': freshness_info.response_time_ms
        })
        
        # Keep only recent history
        if len(history) > self.max_history_size:
            history.pop(0)
    
    async def trigger_data_refresh(self, source: DataSource) -> bool:
        """Trigger data refresh for a specific source"""
        logger.info(f"Triggering data refresh for {source.value}")
        
        try:
            # Different refresh strategies for different sources
            if source == DataSource.BMTC:
                return await self._refresh_bmtc_data()
            elif source == DataSource.BMRCL:
                return await self._refresh_bmrcl_data()
            elif source == DataSource.TAXI:
                return await self._refresh_taxi_data()
            elif source == DataSource.ROUTING:
                return await self._refresh_routing_data()
            
        except Exception as e:
            logger.error(f"Error refreshing {source.value}: {e}")
            return False
        
        return False
    
    async def _refresh_bmtc_data(self) -> bool:
        """Refresh BMTC data"""
        try:
            # Call the refresh endpoint
            url = f"{self.base_url}/api/refresh/bmtc"
            response = self.session.post(url, timeout=30)
            
            if response.status_code == 200:
                logger.info("BMTC data refresh triggered successfully")
                return True
            else:
                logger.error(f"BMTC refresh failed: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"BMTC refresh error: {e}")
            return False
    
    async def _refresh_bmrcl_data(self) -> bool:
        """Refresh BMRCL data"""
        try:
            # Call the refresh endpoint
            url = f"{self.base_url}/api/refresh/bmrcl"
            response = self.session.post(url, timeout=30)
            
            if response.status_code == 200:
                logger.info("BMRCL data refresh triggered successfully")
                return True
            else:
                logger.error(f"BMRCL refresh failed: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"BMRCL refresh error: {e}")
            return False
    
    async def _refresh_taxi_data(self) -> bool:
        """Refresh taxi data"""
        try:
            # Clear taxi cache
            url = f"{self.base_url}/api/taxi/clear_cache"
            response = self.session.post(url, timeout=10)
            
            if response.status_code == 200:
                logger.info("Taxi data cache cleared successfully")
                return True
            else:
                logger.error(f"Taxi cache clear failed: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Taxi refresh error: {e}")
            return False
    
    async def _refresh_routing_data(self) -> bool:
        """Refresh routing data"""
        try:
            # Restart routing service health check
            url = f"{self.base_url}/api/routing/health_check"
            response = self.session.post(url, timeout=10)
            
            if response.status_code == 200:
                logger.info("Routing service health check triggered")
                return True
            else:
                logger.error(f"Routing health check failed: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Routing refresh error: {e}")
            return False
    
    async def auto_refresh_stale_data(self) -> Dict[DataSource, bool]:
        """Automatically refresh all stale/expired data sources"""
        logger.info("Starting automatic refresh of stale data")
        
        report = await self.validate_all_sources()
        refresh_results = {}
        
        # Refresh expired sources first
        for source in report.expired_sources:
            logger.info(f"Auto-refreshing expired source: {source.value}")
            success = await self.trigger_data_refresh(source)
            refresh_results[source] = success
            
            if success:
                # Wait a bit for the refresh to take effect
                await asyncio.sleep(2)
        
        # Refresh stale sources
        for source in report.stale_sources:
            logger.info(f"Auto-refreshing stale source: {source.value}")
            success = await self.trigger_data_refresh(source)
            refresh_results[source] = success
            
            if success:
                await asyncio.sleep(2)
        
        logger.info(f"Auto-refresh completed. Results: {refresh_results}")
        return refresh_results
    
    def get_freshness_trends(self, source: DataSource, hours: int = 1) -> Dict[str, Any]:
        """Get freshness trends for a data source"""
        history = self.freshness_history[source]
        
        if not history:
            return {'error': 'No historical data available'}
        
        # Filter recent history
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_history = [h for h in history if h['timestamp'] > cutoff_time]
        
        if not recent_history:
            return {'error': f'No data in the last {hours} hours'}
        
        # Calculate statistics
        ages = [h['age_seconds'] for h in recent_history if h['age_seconds'] != float('inf')]
        response_times = [h['response_time_ms'] for h in recent_history]
        data_counts = [h['data_count'] for h in recent_history]
        
        status_counts = {}
        for h in recent_history:
            status = h['status'].value
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            'source': source.value,
            'period_hours': hours,
            'total_checks': len(recent_history),
            'avg_age_seconds': sum(ages) / len(ages) if ages else None,
            'max_age_seconds': max(ages) if ages else None,
            'avg_response_time_ms': sum(response_times) / len(response_times) if response_times else None,
            'avg_data_count': sum(data_counts) / len(data_counts) if data_counts else None,
            'status_distribution': status_counts
        }
    
    def get_system_health_score(self) -> float:
        """Calculate overall system health score (0-100)"""
        try:
            # Get latest freshness info for all sources
            total_score = 0
            total_weight = 0
            
            # Weights for different sources
            weights = {
                DataSource.BMTC: 30,    # Critical
                DataSource.BMRCL: 30,   # Critical
                DataSource.TAXI: 20,    # Important
                DataSource.ROUTING: 20  # Important
            }
            
            for source in DataSource:
                history = self.freshness_history[source]
                if not history:
                    continue
                
                latest = history[-1]
                weight = weights[source]
                
                # Score based on status
                if latest['status'] == FreshnessStatus.FRESH:
                    score = 100
                elif latest['status'] == FreshnessStatus.STALE:
                    score = 70
                elif latest['status'] == FreshnessStatus.EXPIRED:
                    score = 30
                else:  # UNAVAILABLE
                    score = 0
                
                # Adjust score based on response time
                if latest['response_time_ms'] > 5000:
                    score *= 0.8  # Penalty for slow response
                elif latest['response_time_ms'] > 2000:
                    score *= 0.9
                
                total_score += score * weight
                total_weight += weight
            
            return total_score / total_weight if total_weight > 0 else 0
            
        except Exception as e:
            logger.error(f"Error calculating health score: {e}")
            return 0

# Global instance
data_freshness_validator = DataFreshnessValidator()

async def test_data_freshness_validator():
    """Test the data freshness validator"""
    logger.info("Testing data freshness validator...")
    
    # Validate all sources
    report = await data_freshness_validator.validate_all_sources()
    
    logger.info(f"Validation complete. Overall status: {report.overall_status.value}")
    
    for source, info in report.sources.items():
        logger.info(f"{source.value}: {info.status.value}, age: {info.age_seconds:.1f}s, count: {info.data_count}")
    
    if report.recommendations:
        logger.info("Recommendations:")
        for rec in report.recommendations:
            logger.info(f"  - {rec}")
    
    # Test auto-refresh
    if report.stale_sources or report.expired_sources:
        logger.info("Testing auto-refresh...")
        refresh_results = await data_freshness_validator.auto_refresh_stale_data()
        logger.info(f"Refresh results: {refresh_results}")
    
    # Get health score
    health_score = data_freshness_validator.get_system_health_score()
    logger.info(f"System health score: {health_score:.1f}/100")

if __name__ == "__main__":
    asyncio.run(test_data_freshness_validator())