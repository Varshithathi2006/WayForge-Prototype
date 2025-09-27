#!/usr/bin/env python3
"""
Enhanced Distance Calculator for WayForge Transit System
Provides accurate distance calculations with path segmentation and cumulative tracking
"""

import math
import logging
from typing import List, Tuple, Dict, Any, Optional
from dataclasses import dataclass
from geopy.distance import geodesic
import numpy as np

try:
    from .common import setup_logging
except ImportError:
    # Fallback for when running as standalone script
    import logging
    def setup_logging(name):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
try:
    from .error_handler import error_handler_decorator, performance_monitor
except ImportError:
    # Fallback decorators for standalone script
    def error_handler_decorator(service_name):
        def decorator(func):
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"Error in {service_name}: {str(e)}")
                    raise
            return wrapper
        return decorator
    
    def performance_monitor(func):
        def wrapper(*args, **kwargs):
            import time
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            print(f"{func.__name__} took {end_time - start_time:.3f} seconds")
            return result
        return wrapper

logger = setup_logging("enhanced_distance_calculator")

@dataclass
class PathSegment:
    """Represents a segment of a path with detailed distance information"""
    start_point: Tuple[float, float]  # (lat, lng)
    end_point: Tuple[float, float]    # (lat, lng)
    distance_km: float
    segment_type: str  # 'walking', 'driving', 'transit', 'cycling'
    transport_mode: Optional[str] = None
    road_type: Optional[str] = None  # 'highway', 'arterial', 'local', 'pedestrian'
    elevation_gain: float = 0.0
    traffic_factor: float = 1.0  # Multiplier for traffic conditions

@dataclass
class PathAnalysis:
    """Complete analysis of a path with detailed distance breakdown"""
    total_distance_km: float
    straight_line_distance_km: float
    path_efficiency: float  # ratio of straight line to actual path
    segments: List[PathSegment]
    cumulative_distances: List[float]
    elevation_profile: List[float]
    transport_modes: List[str]
    estimated_accuracy: float  # confidence in distance calculation (0-1)

class EnhancedDistanceCalculator:
    """Enhanced distance calculator with path segmentation and accuracy improvements"""
    
    def __init__(self):
        self.logger = logger
        
        # Earth's radius in kilometers
        self.EARTH_RADIUS_KM = 6371.0
        
        # Road type multipliers for more accurate distance estimation
        self.road_multipliers = {
            'highway': 1.05,      # Highways are relatively straight
            'arterial': 1.15,     # Main roads with some curves
            'local': 1.25,        # Local roads with more turns
            'pedestrian': 1.35,   # Walking paths with more detours
            'default': 1.20       # Default multiplier
        }
        
        # Transport mode speed factors for time-based distance validation
        self.speed_factors = {
            'walking': 5.0,       # km/h
            'cycling': 15.0,      # km/h
            'driving': 35.0,      # km/h
            'transit': 25.0,      # km/h
            'metro': 40.0,        # km/h
            'bus': 20.0          # km/h
        }

    @error_handler_decorator("enhanced_distance_calculator")
    @performance_monitor
    def calculate_haversine_distance(self, lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """
        Calculate precise Haversine distance between two points
        Uses high-precision formula for better accuracy
        """
        # Convert to radians
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lng = math.radians(lng2 - lng1)
        
        # Haversine formula with high precision
        a = (math.sin(delta_lat / 2) ** 2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * 
             math.sin(delta_lng / 2) ** 2)
        
        # Use atan2 for better numerical stability
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return self.EARTH_RADIUS_KM * c

    @error_handler_decorator("enhanced_distance_calculator")
    def calculate_geodesic_distance(self, lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """
        Calculate geodesic distance using geopy for highest accuracy
        This accounts for Earth's ellipsoid shape
        """
        try:
            distance = geodesic((lat1, lng1), (lat2, lng2)).kilometers
            return distance
        except Exception as e:
            self.logger.warning(f"Geodesic calculation failed, falling back to Haversine: {e}")
            return self.calculate_haversine_distance(lat1, lng1, lat2, lng2)

    @error_handler_decorator("enhanced_distance_calculator")
    def calculate_path_distance(self, geometry: List[Tuple[float, float]], 
                              transport_mode: str = 'driving',
                              use_geodesic: bool = True) -> PathAnalysis:
        """
        Calculate accurate distance for a complete path with segmentation
        
        Args:
            geometry: List of (lat, lng) coordinates defining the path
            transport_mode: Type of transport for appropriate calculations
            use_geodesic: Whether to use geodesic (more accurate) or Haversine calculation
        
        Returns:
            PathAnalysis with detailed distance breakdown
        """
        if len(geometry) < 2:
            raise ValueError("Path must contain at least 2 points")
        
        segments = []
        cumulative_distances = [0.0]
        total_distance = 0.0
        
        # Calculate distance function based on preference
        distance_func = self.calculate_geodesic_distance if use_geodesic else self.calculate_haversine_distance
        
        # Process each segment
        for i in range(len(geometry) - 1):
            start_point = geometry[i]
            end_point = geometry[i + 1]
            
            # Calculate base distance
            segment_distance = distance_func(
                start_point[0], start_point[1],
                end_point[0], end_point[1]
            )
            
            # Apply road type multiplier based on segment characteristics
            road_type = self._determine_road_type(start_point, end_point, transport_mode)
            multiplier = self.road_multipliers.get(road_type, self.road_multipliers['default'])
            
            # Adjust for transport mode
            if transport_mode == 'walking':
                # Walking paths may have more detours
                multiplier *= 1.1
            elif transport_mode == 'cycling':
                # Cycling may use more direct routes
                multiplier *= 0.95
            
            adjusted_distance = segment_distance * multiplier
            total_distance += adjusted_distance
            cumulative_distances.append(total_distance)
            
            # Create segment
            segment = PathSegment(
                start_point=start_point,
                end_point=end_point,
                distance_km=adjusted_distance,
                segment_type=transport_mode,
                transport_mode=transport_mode,
                road_type=road_type
            )
            segments.append(segment)
        
        # Calculate straight-line distance for efficiency analysis
        straight_line_distance = distance_func(
            geometry[0][0], geometry[0][1],
            geometry[-1][0], geometry[-1][1]
        )
        
        # Calculate path efficiency
        path_efficiency = straight_line_distance / total_distance if total_distance > 0 else 0
        
        # Estimate accuracy based on number of points and path characteristics
        estimated_accuracy = self._estimate_accuracy(geometry, segments, transport_mode)
        
        return PathAnalysis(
            total_distance_km=total_distance,
            straight_line_distance_km=straight_line_distance,
            path_efficiency=path_efficiency,
            segments=segments,
            cumulative_distances=cumulative_distances,
            elevation_profile=[],  # Could be enhanced with elevation data
            transport_modes=[transport_mode],
            estimated_accuracy=estimated_accuracy
        )

    def _determine_road_type(self, start_point: Tuple[float, float], 
                           end_point: Tuple[float, float], 
                           transport_mode: str) -> str:
        """
        Determine road type based on segment characteristics
        This is a simplified heuristic - could be enhanced with actual road data
        """
        distance = self.calculate_haversine_distance(
            start_point[0], start_point[1],
            end_point[0], end_point[1]
        )
        
        if transport_mode == 'walking':
            return 'pedestrian'
        elif transport_mode == 'cycling':
            return 'local' if distance < 2.0 else 'arterial'
        elif transport_mode in ['driving', 'taxi']:
            if distance > 5.0:
                return 'highway'
            elif distance > 1.0:
                return 'arterial'
            else:
                return 'local'
        else:
            return 'default'

    def _estimate_accuracy(self, geometry: List[Tuple[float, float]], 
                          segments: List[PathSegment], 
                          transport_mode: str) -> float:
        """
        Estimate the accuracy of distance calculation based on various factors
        """
        base_accuracy = 0.85  # Base accuracy for simple calculations
        
        # More points generally mean higher accuracy
        point_factor = min(1.0, len(geometry) / 50.0) * 0.1
        
        # Shorter segments generally mean higher accuracy
        avg_segment_length = sum(s.distance_km for s in segments) / len(segments) if segments else 0
        segment_factor = max(0, 0.05 - avg_segment_length * 0.01)
        
        # Transport mode affects accuracy
        mode_factor = {
            'walking': 0.05,    # Walking paths are well-defined
            'driving': 0.0,     # Driving routes can vary
            'cycling': 0.03,    # Cycling has some flexibility
            'transit': 0.08     # Transit routes are fixed
        }.get(transport_mode, 0.0)
        
        return min(0.98, base_accuracy + point_factor + segment_factor + mode_factor)

    @error_handler_decorator("enhanced_distance_calculator")
    def calculate_multi_modal_distance(self, route_segments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate cumulative distance for multi-modal routes
        
        Args:
            route_segments: List of route segments with different transport modes
        
        Returns:
            Dictionary with detailed distance breakdown by mode and total
        """
        total_distance = 0.0
        mode_distances = {}
        cumulative_by_mode = []
        
        for i, segment in enumerate(route_segments):
            mode = segment.get('transport_mode', 'unknown')
            geometry = segment.get('geometry', [])
            
            if len(geometry) >= 2:
                # Calculate distance for this segment
                path_analysis = self.calculate_path_distance(geometry, mode)
                segment_distance = path_analysis.total_distance_km
                
                # Update totals
                total_distance += segment_distance
                mode_distances[mode] = mode_distances.get(mode, 0) + segment_distance
                
                # Add cumulative tracking
                cumulative_by_mode.append({
                    'segment_index': i,
                    'mode': mode,
                    'segment_distance': segment_distance,
                    'cumulative_total': total_distance,
                    'path_efficiency': path_analysis.path_efficiency,
                    'estimated_accuracy': path_analysis.estimated_accuracy
                })
            else:
                # Fallback for segments without geometry
                fallback_distance = segment.get('distance_km', 0)
                total_distance += fallback_distance
                mode_distances[mode] = mode_distances.get(mode, 0) + fallback_distance
                
                cumulative_by_mode.append({
                    'segment_index': i,
                    'mode': mode,
                    'segment_distance': fallback_distance,
                    'cumulative_total': total_distance,
                    'path_efficiency': 0.8,  # Estimated
                    'estimated_accuracy': 0.7  # Lower accuracy for fallback
                })
        
        return {
            'total_distance_km': total_distance,
            'distance_by_mode': mode_distances,
            'cumulative_tracking': cumulative_by_mode,
            'number_of_modes': len(mode_distances),
            'primary_mode': max(mode_distances.items(), key=lambda x: x[1])[0] if mode_distances else 'unknown',
            'overall_accuracy': sum(seg['estimated_accuracy'] for seg in cumulative_by_mode) / len(cumulative_by_mode) if cumulative_by_mode else 0.0
        }

    @error_handler_decorator("enhanced_distance_calculator")
    def validate_distance_calculation(self, calculated_distance: float, 
                                    geometry: List[Tuple[float, float]], 
                                    transport_mode: str,
                                    expected_duration_minutes: Optional[float] = None) -> Dict[str, Any]:
        """
        Validate distance calculation using multiple methods and cross-checks
        
        Returns validation results with confidence score
        """
        validation_results = {
            'is_valid': True,
            'confidence_score': 1.0,
            'warnings': [],
            'alternative_calculations': {}
        }
        
        if len(geometry) < 2:
            validation_results['is_valid'] = False
            validation_results['warnings'].append("Insufficient geometry points")
            return validation_results
        
        # Calculate straight-line distance
        straight_line = self.calculate_geodesic_distance(
            geometry[0][0], geometry[0][1],
            geometry[-1][0], geometry[-1][1]
        )
        validation_results['alternative_calculations']['straight_line_km'] = straight_line
        
        # Check if calculated distance is reasonable compared to straight line
        if calculated_distance < straight_line:
            validation_results['is_valid'] = False
            validation_results['warnings'].append("Calculated distance is less than straight-line distance")
            validation_results['confidence_score'] *= 0.3
        elif calculated_distance > straight_line * 3.0:
            validation_results['warnings'].append("Calculated distance seems unusually high")
            validation_results['confidence_score'] *= 0.7
        
        # Validate against expected duration if provided
        if expected_duration_minutes:
            expected_speed = self.speed_factors.get(transport_mode, 25.0)
            duration_based_distance = (expected_duration_minutes / 60.0) * expected_speed
            validation_results['alternative_calculations']['duration_based_km'] = duration_based_distance
            
            distance_ratio = calculated_distance / duration_based_distance if duration_based_distance > 0 else 0
            if distance_ratio < 0.5 or distance_ratio > 2.0:
                validation_results['warnings'].append("Distance-duration mismatch detected")
                validation_results['confidence_score'] *= 0.8
        
        # Calculate alternative using Haversine for comparison
        haversine_analysis = self.calculate_path_distance(geometry, transport_mode, use_geodesic=False)
        validation_results['alternative_calculations']['haversine_km'] = haversine_analysis.total_distance_km
        
        # Check consistency between methods
        geodesic_haversine_ratio = calculated_distance / haversine_analysis.total_distance_km if haversine_analysis.total_distance_km > 0 else 0
        if abs(geodesic_haversine_ratio - 1.0) > 0.1:  # More than 10% difference
            validation_results['warnings'].append("Significant difference between calculation methods")
            validation_results['confidence_score'] *= 0.9
        
        return validation_results

# Create global instance
enhanced_distance_calculator = EnhancedDistanceCalculator()