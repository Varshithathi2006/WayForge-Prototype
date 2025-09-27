#!/usr/bin/env python3
"""
Historical Data Analyzer for WayForge Transit System
Analyzes past traffic patterns, transit usage, and generates intelligent suggestions
"""

import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import numpy as np
from dataclasses import dataclass
import logging

from utils.common import setup_logging
from utils.error_handler import error_handler_decorator, performance_monitor

logger = setup_logging("historical_analyzer")

@dataclass
class TrafficPattern:
    """Historical traffic pattern data"""
    time_period: str  # "morning_peak", "evening_peak", "off_peak", "night"
    route_segment: str
    avg_congestion_level: float
    avg_delay_minutes: float
    frequency_count: int
    day_of_week: str

@dataclass
class TransitUsagePattern:
    """Historical transit usage patterns"""
    route_type: str  # "bus", "metro", "taxi"
    route_id: str
    time_period: str
    avg_occupancy: float
    avg_delay_minutes: float
    popularity_score: float
    day_of_week: str

@dataclass
class SmartSuggestion:
    """Intelligent suggestion based on historical and real-time data"""
    suggestion_type: str  # "route", "timing", "mode", "fare"
    message: str
    confidence_score: float
    reasoning: str
    data_sources: List[str]
    priority: str  # "high", "medium", "low"

class HistoricalDataAnalyzer:
    """Analyzes historical transit and traffic data to generate intelligent suggestions"""
    
    def __init__(self):
        self.traffic_patterns = []
        self.transit_patterns = []
        self.peak_hours = {
            "morning": (7, 10),
            "evening": (17, 20),
            "lunch": (12, 14)
        }
        self.load_historical_data()
    
    @error_handler_decorator("historical_analyzer")
    def load_historical_data(self):
        """Load and process historical data from various sources"""
        try:
            # Load traffic patterns (simulated historical data)
            self.traffic_patterns = self._generate_traffic_patterns()
            self.transit_patterns = self._generate_transit_patterns()
            logger.info("Historical data loaded successfully")
        except Exception as e:
            logger.error(f"Error loading historical data: {e}")
            self.traffic_patterns = []
            self.transit_patterns = []
    
    def _generate_traffic_patterns(self) -> List[TrafficPattern]:
        """Generate realistic traffic patterns based on Bangalore traffic data"""
        patterns = []
        
        # Major routes in Bangalore with typical congestion patterns
        major_routes = [
            "Outer Ring Road", "Hosur Road", "Bannerghatta Road", 
            "Electronic City Flyover", "Silk Board Junction", "Hebbal Flyover",
            "Airport Road", "Whitefield Road", "Sarjapur Road", "Marathahalli Bridge"
        ]
        
        time_periods = ["morning_peak", "evening_peak", "off_peak", "night"]
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        
        for route in major_routes:
            for day in days:
                for period in time_periods:
                    # Realistic congestion levels based on Bangalore traffic
                    if period == "morning_peak":
                        congestion = np.random.uniform(0.7, 0.95)
                        delay = np.random.uniform(15, 45)
                    elif period == "evening_peak":
                        congestion = np.random.uniform(0.8, 0.98)
                        delay = np.random.uniform(20, 60)
                    elif period == "off_peak":
                        congestion = np.random.uniform(0.3, 0.6)
                        delay = np.random.uniform(5, 15)
                    else:  # night
                        congestion = np.random.uniform(0.1, 0.3)
                        delay = np.random.uniform(0, 5)
                    
                    # Weekend adjustments
                    if day in ["saturday", "sunday"]:
                        congestion *= 0.7
                        delay *= 0.6
                    
                    patterns.append(TrafficPattern(
                        time_period=period,
                        route_segment=route,
                        avg_congestion_level=congestion,
                        avg_delay_minutes=delay,
                        frequency_count=np.random.randint(50, 200),
                        day_of_week=day
                    ))
        
        return patterns
    
    def _generate_transit_patterns(self) -> List[TransitUsagePattern]:
        """Generate realistic transit usage patterns"""
        patterns = []
        
        # BMTC routes
        bmtc_routes = ["356", "500K", "201", "335E", "G4", "AS4", "KBS1", "V335"]
        # BMRCL routes
        bmrcl_routes = ["Purple Line", "Green Line"]
        
        time_periods = ["morning_peak", "evening_peak", "off_peak", "night"]
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        
        for route in bmtc_routes:
            for day in days:
                for period in time_periods:
                    if period in ["morning_peak", "evening_peak"]:
                        occupancy = np.random.uniform(0.8, 0.95)
                        delay = np.random.uniform(5, 20)
                        popularity = np.random.uniform(0.7, 0.9)
                    else:
                        occupancy = np.random.uniform(0.3, 0.6)
                        delay = np.random.uniform(2, 10)
                        popularity = np.random.uniform(0.4, 0.7)
                    
                    patterns.append(TransitUsagePattern(
                        route_type="bus",
                        route_id=route,
                        time_period=period,
                        avg_occupancy=occupancy,
                        avg_delay_minutes=delay,
                        popularity_score=popularity,
                        day_of_week=day
                    ))
        
        for route in bmrcl_routes:
            for day in days:
                for period in time_periods:
                    if period in ["morning_peak", "evening_peak"]:
                        occupancy = np.random.uniform(0.7, 0.9)
                        delay = np.random.uniform(1, 5)
                        popularity = np.random.uniform(0.8, 0.95)
                    else:
                        occupancy = np.random.uniform(0.2, 0.5)
                        delay = np.random.uniform(0, 3)
                        popularity = np.random.uniform(0.5, 0.8)
                    
                    patterns.append(TransitUsagePattern(
                        route_type="metro",
                        route_id=route,
                        time_period=period,
                        avg_occupancy=occupancy,
                        avg_delay_minutes=delay,
                        popularity_score=popularity,
                        day_of_week=day
                    ))
        
        return patterns
    
    @error_handler_decorator("historical_analyzer")
    @performance_monitor("historical_analyzer")
    def get_current_time_context(self) -> Dict[str, Any]:
        """Get current time context for analysis"""
        now = datetime.now()
        hour = now.hour
        day_name = now.strftime("%A").lower()
        
        # Determine time period
        if 7 <= hour <= 10:
            time_period = "morning_peak"
        elif 17 <= hour <= 20:
            time_period = "evening_peak"
        elif 12 <= hour <= 14:
            time_period = "lunch"
        elif 22 <= hour or hour <= 5:
            time_period = "night"
        else:
            time_period = "off_peak"
        
        return {
            "current_hour": hour,
            "day_of_week": day_name,
            "time_period": time_period,
            "is_weekend": day_name in ["saturday", "sunday"],
            "is_peak_hour": time_period in ["morning_peak", "evening_peak"]
        }
    
    @error_handler_decorator("historical_analyzer")
    def analyze_traffic_patterns(self, current_context: Dict[str, Any]) -> List[SmartSuggestion]:
        """Analyze traffic patterns and generate suggestions"""
        suggestions = []
        
        current_period = current_context["time_period"]
        current_day = current_context["day_of_week"]
        
        # Find relevant traffic patterns
        relevant_patterns = [
            p for p in self.traffic_patterns 
            if p.time_period == current_period and p.day_of_week == current_day
        ]
        
        if not relevant_patterns:
            return suggestions
        
        # Analyze high congestion routes
        high_congestion_routes = [
            p for p in relevant_patterns 
            if p.avg_congestion_level > 0.7
        ]
        
        if high_congestion_routes:
            worst_routes = sorted(high_congestion_routes, 
                                key=lambda x: x.avg_congestion_level, reverse=True)[:3]
            
            route_names = [r.route_segment for r in worst_routes]
            avg_delay = sum(r.avg_delay_minutes for r in worst_routes) / len(worst_routes)
            
            suggestions.append(SmartSuggestion(
                suggestion_type="traffic",
                message=f"⚠️ Heavy traffic expected on {', '.join(route_names[:2])}. Average delay: {avg_delay:.0f} minutes",
                confidence_score=0.85,
                reasoning=f"Historical data shows high congestion during {current_period} on {current_day}",
                data_sources=["historical_traffic_patterns"],
                priority="high"
            ))
        
        # Time-based suggestions
        if current_period == "morning_peak":
            suggestions.append(SmartSuggestion(
                suggestion_type="timing",
                message="🚇 Metro recommended during morning peak (2-3x faster than buses)",
                confidence_score=0.9,
                reasoning="Historical data shows metro is significantly faster during morning rush",
                data_sources=["historical_transit_patterns"],
                priority="high"
            ))
        
        elif current_period == "evening_peak":
            suggestions.append(SmartSuggestion(
                suggestion_type="timing",
                message="🕐 Consider traveling after 8 PM for 40% less traffic",
                confidence_score=0.8,
                reasoning="Traffic reduces significantly after evening peak hours",
                data_sources=["historical_traffic_patterns"],
                priority="medium"
            ))
        
        return suggestions
    
    @error_handler_decorator("historical_analyzer")
    def analyze_transit_patterns(self, current_context: Dict[str, Any]) -> List[SmartSuggestion]:
        """Analyze transit usage patterns and generate suggestions"""
        suggestions = []
        
        current_period = current_context["time_period"]
        current_day = current_context["day_of_week"]
        
        # Find relevant transit patterns
        relevant_patterns = [
            p for p in self.transit_patterns 
            if p.time_period == current_period and p.day_of_week == current_day
        ]
        
        if not relevant_patterns:
            return suggestions
        
        # Compare bus vs metro performance
        bus_patterns = [p for p in relevant_patterns if p.route_type == "bus"]
        metro_patterns = [p for p in relevant_patterns if p.route_type == "metro"]
        
        if bus_patterns and metro_patterns:
            avg_bus_delay = sum(p.avg_delay_minutes for p in bus_patterns) / len(bus_patterns)
            avg_metro_delay = sum(p.avg_delay_minutes for p in metro_patterns) / len(metro_patterns)
            
            if avg_metro_delay < avg_bus_delay * 0.5:
                suggestions.append(SmartSuggestion(
                    suggestion_type="mode",
                    message=f"🚇 Metro is {avg_bus_delay/avg_metro_delay:.1f}x more reliable right now",
                    confidence_score=0.85,
                    reasoning=f"Metro avg delay: {avg_metro_delay:.1f}min vs Bus: {avg_bus_delay:.1f}min",
                    data_sources=["historical_transit_patterns"],
                    priority="high"
                ))
        
        # High occupancy warnings
        high_occupancy_routes = [
            p for p in relevant_patterns 
            if p.avg_occupancy > 0.8
        ]
        
        if high_occupancy_routes and current_context["is_peak_hour"]:
            suggestions.append(SmartSuggestion(
                suggestion_type="timing",
                message="🚌 Buses are typically crowded now. Consider metro or wait 30 minutes",
                confidence_score=0.75,
                reasoning="High occupancy patterns during peak hours",
                data_sources=["historical_transit_patterns"],
                priority="medium"
            ))
        
        return suggestions
    
    @error_handler_decorator("historical_analyzer")
    def generate_contextual_suggestions(self, realtime_data: Dict[str, Any] = None) -> List[SmartSuggestion]:
        """Generate comprehensive suggestions based on historical and real-time data"""
        current_context = self.get_current_time_context()
        suggestions = []
        
        # Get historical pattern suggestions
        traffic_suggestions = self.analyze_traffic_patterns(current_context)
        transit_suggestions = self.analyze_transit_patterns(current_context)
        
        suggestions.extend(traffic_suggestions)
        suggestions.extend(transit_suggestions)
        
        # Add real-time data context if available
        if realtime_data:
            realtime_suggestions = self._analyze_realtime_data(realtime_data, current_context)
            suggestions.extend(realtime_suggestions)
        
        # Add general time-based suggestions
        general_suggestions = self._get_general_suggestions(current_context)
        suggestions.extend(general_suggestions)
        
        # Sort by priority and confidence
        suggestions.sort(key=lambda x: (
            {"high": 3, "medium": 2, "low": 1}[x.priority],
            x.confidence_score
        ), reverse=True)
        
        return suggestions[:5]  # Return top 5 suggestions
    
    def _analyze_realtime_data(self, realtime_data: Dict[str, Any], context: Dict[str, Any]) -> List[SmartSuggestion]:
        """Analyze real-time data and generate suggestions"""
        suggestions = []
        
        # Analyze current traffic
        if "traffic" in realtime_data and realtime_data["traffic"]:
            traffic_data = realtime_data["traffic"]
            if isinstance(traffic_data, list) and len(traffic_data) > 0:
                avg_delay = sum(t.get("delay_minutes", 0) for t in traffic_data) / len(traffic_data)
                
                if avg_delay > 20:
                    suggestions.append(SmartSuggestion(
                        suggestion_type="traffic",
                        message=f"🚨 Live traffic shows {avg_delay:.0f}min delays. Consider alternate routes",
                        confidence_score=0.95,
                        reasoning="Real-time traffic data indicates severe delays",
                        data_sources=["realtime_traffic"],
                        priority="high"
                    ))
        
        # Analyze vehicle availability
        if "vehicles" in realtime_data and realtime_data["vehicles"]:
            vehicle_count = len(realtime_data["vehicles"])
            if vehicle_count < 5:
                suggestions.append(SmartSuggestion(
                    suggestion_type="availability",
                    message=f"⚠️ Limited vehicles available ({vehicle_count} tracked). Expect longer waits",
                    confidence_score=0.8,
                    reasoning="Low vehicle count indicates reduced service frequency",
                    data_sources=["realtime_vehicles"],
                    priority="medium"
                ))
        
        return suggestions
    
    def _get_general_suggestions(self, context: Dict[str, Any]) -> List[SmartSuggestion]:
        """Get general suggestions based on time context"""
        suggestions = []
        
        if context["is_weekend"]:
            suggestions.append(SmartSuggestion(
                suggestion_type="timing",
                message="🌅 Weekend services start later. First buses/metros after 6 AM",
                confidence_score=0.7,
                reasoning="Weekend schedule adjustments",
                data_sources=["schedule_data"],
                priority="low"
            ))
        
        if context["current_hour"] >= 22:
            suggestions.append(SmartSuggestion(
                suggestion_type="timing",
                message="🌙 Limited night services. Last metro at 11 PM, limited bus routes",
                confidence_score=0.9,
                reasoning="Night service schedule limitations",
                data_sources=["schedule_data"],
                priority="high"
            ))
        
        return suggestions

# Global instance
historical_analyzer = HistoricalDataAnalyzer()