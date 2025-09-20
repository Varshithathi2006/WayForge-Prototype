"""
Enhanced Error Handling and Monitoring for Bangalore Transit Pipeline
Provides comprehensive error handling, monitoring, and alerting capabilities
"""

import json
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable
from functools import wraps
from dataclasses import dataclass, asdict
import logging

from .common import setup_logging, get_current_timestamp, save_json_file

@dataclass
class ErrorEvent:
    """Data class for error events"""
    timestamp: float
    component: str
    error_type: str
    error_message: str
    stack_trace: str
    context: Dict[str, Any]
    severity: str  # 'low', 'medium', 'high', 'critical'
    resolved: bool = False

@dataclass
class PerformanceMetric:
    """Data class for performance metrics"""
    timestamp: float
    component: str
    operation: str
    duration_ms: float
    success: bool
    metadata: Dict[str, Any]

class ErrorHandler:
    """Centralized error handling and monitoring"""
    
    def __init__(self, component_name: str):
        self.component_name = component_name
        self.logger = setup_logging(f"error_handler.{component_name}")
        self.error_events: List[ErrorEvent] = []
        self.performance_metrics: List[PerformanceMetric] = []
        self.error_counts = {}
        self.setup_error_logging()
    
    def setup_error_logging(self):
        """Setup error-specific logging configuration"""
        # Create error log file
        error_log_path = Path("logs") / f"errors_{self.component_name}.log"
        error_log_path.parent.mkdir(exist_ok=True)
        
        # Add file handler for errors
        error_handler = logging.FileHandler(error_log_path)
        error_handler.setLevel(logging.ERROR)
        error_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        error_handler.setFormatter(error_formatter)
        self.logger.addHandler(error_handler)
    
    def handle_error(self, 
                    error: Exception, 
                    context: Dict[str, Any] = None,
                    severity: str = 'medium') -> ErrorEvent:
        """
        Handle and log an error event
        
        Args:
            error: The exception that occurred
            context: Additional context information
            severity: Error severity level
            
        Returns:
            ErrorEvent object
        """
        context = context or {}
        
        error_event = ErrorEvent(
            timestamp=get_current_timestamp(),
            component=self.component_name,
            error_type=type(error).__name__,
            error_message=str(error),
            stack_trace=traceback.format_exc(),
            context=context,
            severity=severity
        )
        
        # Store error event
        self.error_events.append(error_event)
        
        # Update error counts
        error_key = f"{error_event.error_type}:{error_event.component}"
        self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1
        
        # Log error based on severity
        log_message = f"[{severity.upper()}] {error_event.error_type}: {error_event.error_message}"
        
        if severity == 'critical':
            self.logger.critical(log_message, extra={'context': context})
        elif severity == 'high':
            self.logger.error(log_message, extra={'context': context})
        elif severity == 'medium':
            self.logger.warning(log_message, extra={'context': context})
        else:
            self.logger.info(log_message, extra={'context': context})
        
        # Save error to file for critical errors
        if severity in ['critical', 'high']:
            self._save_error_event(error_event)
        
        return error_event
    
    def _save_error_event(self, error_event: ErrorEvent):
        """Save error event to JSON file"""
        try:
            timestamp_str = datetime.fromtimestamp(error_event.timestamp).strftime('%Y%m%d_%H%M%S')
            filename = f"logs/error_event_{self.component_name}_{timestamp_str}.json"
            save_json_file(asdict(error_event), filename)
        except Exception as e:
            self.logger.error(f"Failed to save error event: {e}")
    
    def record_performance(self, 
                          operation: str, 
                          duration_ms: float, 
                          success: bool = True,
                          metadata: Dict[str, Any] = None):
        """
        Record performance metrics
        
        Args:
            operation: Name of the operation
            duration_ms: Duration in milliseconds
            success: Whether operation was successful
            metadata: Additional metadata
        """
        metadata = metadata or {}
        
        metric = PerformanceMetric(
            timestamp=get_current_timestamp(),
            component=self.component_name,
            operation=operation,
            duration_ms=duration_ms,
            success=success,
            metadata=metadata
        )
        
        self.performance_metrics.append(metric)
        
        # Log performance if slow or failed
        if not success or duration_ms > 5000:  # Log if > 5 seconds
            status = "SUCCESS" if success else "FAILED"
            self.logger.warning(f"Performance Alert - {operation}: {duration_ms:.2f}ms [{status}]")
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of errors"""
        total_errors = len(self.error_events)
        
        if total_errors == 0:
            return {"total_errors": 0, "error_types": {}, "severity_breakdown": {}}
        
        # Count by error type
        error_types = {}
        severity_breakdown = {}
        
        for event in self.error_events:
            error_types[event.error_type] = error_types.get(event.error_type, 0) + 1
            severity_breakdown[event.severity] = severity_breakdown.get(event.severity, 0) + 1
        
        return {
            "total_errors": total_errors,
            "error_types": error_types,
            "severity_breakdown": severity_breakdown,
            "most_recent_error": asdict(self.error_events[-1]) if self.error_events else None
        }
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get summary of performance metrics"""
        if not self.performance_metrics:
            return {"total_operations": 0}
        
        total_ops = len(self.performance_metrics)
        successful_ops = sum(1 for m in self.performance_metrics if m.success)
        
        # Calculate average duration by operation
        operation_stats = {}
        for metric in self.performance_metrics:
            if metric.operation not in operation_stats:
                operation_stats[metric.operation] = {
                    "count": 0,
                    "total_duration": 0,
                    "success_count": 0
                }
            
            stats = operation_stats[metric.operation]
            stats["count"] += 1
            stats["total_duration"] += metric.duration_ms
            if metric.success:
                stats["success_count"] += 1
        
        # Calculate averages
        for operation, stats in operation_stats.items():
            stats["avg_duration_ms"] = stats["total_duration"] / stats["count"]
            stats["success_rate"] = stats["success_count"] / stats["count"]
        
        return {
            "total_operations": total_ops,
            "success_rate": successful_ops / total_ops,
            "operation_stats": operation_stats
        }

def error_handler_decorator(component_name: str, severity: str = 'medium'):
    """
    Decorator for automatic error handling
    
    Args:
        component_name: Name of the component
        severity: Default error severity
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            error_handler = ErrorHandler(component_name)
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                
                # Record successful performance
                duration_ms = (time.time() - start_time) * 1000
                error_handler.record_performance(
                    operation=func.__name__,
                    duration_ms=duration_ms,
                    success=True
                )
                
                return result
                
            except Exception as e:
                # Record failed performance
                duration_ms = (time.time() - start_time) * 1000
                error_handler.record_performance(
                    operation=func.__name__,
                    duration_ms=duration_ms,
                    success=False
                )
                
                # Handle error
                context = {
                    "function": func.__name__,
                    "args": str(args)[:200],  # Limit length
                    "kwargs": str(kwargs)[:200]
                }
                error_handler.handle_error(e, context, severity)
                
                # Re-raise the exception
                raise
        
        return wrapper
    return decorator

def performance_monitor(component_name: str):
    """
    Decorator for performance monitoring
    
    Args:
        component_name: Name of the component
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            error_handler = ErrorHandler(component_name)
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                success = True
            except Exception as e:
                result = None
                success = False
                raise
            finally:
                duration_ms = (time.time() - start_time) * 1000
                error_handler.record_performance(
                    operation=func.__name__,
                    duration_ms=duration_ms,
                    success=success
                )
            
            return result
        
        return wrapper
    return decorator

class HealthChecker:
    """System health monitoring"""
    
    def __init__(self):
        self.logger = setup_logging("health_checker")
        self.components = {}
    
    def register_component(self, name: str, check_function: Callable) -> None:
        """Register a component for health checking"""
        self.components[name] = check_function
    
    def check_component_health(self, component_name: str) -> Dict[str, Any]:
        """Check health of a specific component"""
        if component_name not in self.components:
            return {"status": "unknown", "message": "Component not registered"}
        
        try:
            check_function = self.components[component_name]
            result = check_function()
            
            if isinstance(result, bool):
                return {"status": "healthy" if result else "unhealthy"}
            elif isinstance(result, dict):
                return result
            else:
                return {"status": "healthy", "details": str(result)}
                
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "error_type": type(e).__name__
            }
    
    def check_all_components(self) -> Dict[str, Dict[str, Any]]:
        """Check health of all registered components"""
        results = {}
        
        for component_name in self.components:
            results[component_name] = self.check_component_health(component_name)
        
        return results
    
    def get_system_health_summary(self) -> Dict[str, Any]:
        """Get overall system health summary"""
        component_results = self.check_all_components()
        
        total_components = len(component_results)
        healthy_components = sum(
            1 for result in component_results.values() 
            if result.get("status") == "healthy"
        )
        
        overall_status = "healthy" if healthy_components == total_components else "degraded"
        if healthy_components == 0:
            overall_status = "critical"
        
        return {
            "overall_status": overall_status,
            "healthy_components": healthy_components,
            "total_components": total_components,
            "health_percentage": (healthy_components / total_components * 100) if total_components > 0 else 0,
            "component_details": component_results,
            "timestamp": get_current_timestamp()
        }

# Global health checker instance
health_checker = HealthChecker()

def register_health_check(component_name: str):
    """Decorator to register a function as a health check"""
    def decorator(func: Callable):
        health_checker.register_component(component_name, func)
        return func
    return decorator