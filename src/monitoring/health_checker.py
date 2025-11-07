"""Health check and monitoring module for PDF Bank Statement Processing System."""

import os
import sys
import time
import psutil
import platform
import redis
from typing import Dict, List, Optional, Any
from pathlib import Path
import subprocess
from datetime import datetime, timedelta

from src.config.settings import Settings
from src.utils.logger import get_logger


class HealthChecker:
    """Health checker for monitoring system components."""
    
    def __init__(self, settings: Settings):
        """Initialize health checker with settings."""
        self.settings = settings
        self.logger = get_logger(self.__class__.__name__)
        
        # Component checkers
        self.components = {
            'system': self._check_system_health,
            'redis': self._check_redis_health,
            'disk_space': self._check_disk_space,
            'memory': self._check_memory_usage,
            'cpu': self._check_cpu_usage,
            'dependencies': self._check_dependencies,
            'file_permissions': self._check_file_permissions,
            'environment': self._check_environment,
        }
    
    def run_health_check(self) -> Dict[str, Any]:
        """Run comprehensive health check."""
        start_time = time.time()
        health_data = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'uptime': self._get_uptime(),
            'version': self._get_version(),
            'components': {},
            'metrics': {},
            'alerts': [],
        }
        
        component_issues = []
        
        # Check all components
        for component_name, check_func in self.components.items():
            try:
                result = check_func()
                health_data['components'][component_name] = result
                
                if result['status'] != 'healthy':
                    component_issues.append(component_name)
                    health_data['alerts'].append(
                        f"Component {component_name}: {result.get('message', 'Unknown issue')}"
                    )
                    
            except Exception as e:
                self.logger.error(f"Health check failed for {component_name}: {e}")
                health_data['components'][component_name] = {
                    'status': 'error',
                    'message': str(e),
                    'timestamp': datetime.now().isoformat(),
                }
                component_issues.append(component_name)
        
        # Check task processing health
        task_health = self._check_task_health()
        health_data['components']['tasks'] = task_health
        
        if task_health['status'] != 'healthy':
            component_issues.append('tasks')
            health_data['alerts'].append(f"Task processing: {task_health.get('message', 'Issues detected')}")
        
        # Collect performance metrics
        health_data['metrics'] = self._collect_metrics()
        
        # Determine overall health
        if component_issues:
            if len(component_issues) > len(self.components) // 2:
                health_data['status'] = 'unhealthy'
            else:
                health_data['status'] = 'degraded'
        
        # Add processing time
        health_data['check_duration'] = round(time.time() - start_time, 3)
        
        return health_data
    
    def _check_system_health(self) -> Dict[str, Any]:
        """Check basic system health."""
        try:
            system_info = {
                'platform': platform.platform(),
                'python_version': sys.version,
                'architecture': platform.architecture(),
                'processor': platform.processor(),
            }
            
            return {
                'status': 'healthy',
                'info': system_info,
                'timestamp': datetime.now().isoformat(),
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f"System check failed: {e}",
                'timestamp': datetime.now().isoformat(),
            }
    
    def _check_redis_health(self) -> Dict[str, Any]:
        """Check Redis connectivity and health."""
        try:
            # Test Redis connection
            client = redis.Redis(
                host=os.getenv('REDIS_HOST', 'localhost'),
                port=int(os.getenv('REDIS_PORT', '6379')),
                db=int(os.getenv('REDIS_DB', '0')),
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            
            # Test connection
            client.ping()
            
            # Get Redis info
            info = client.info()
            
            return {
                'status': 'healthy',
                'info': {
                    'version': info.get('redis_version'),
                    'connected_clients': info.get('connected_clients'),
                    'used_memory': info.get('used_memory_human'),
                    'uptime_in_seconds': info.get('uptime_in_seconds'),
                },
                'timestamp': datetime.now().isoformat(),
            }
            
        except redis.ConnectionError:
            return {
                'status': 'unhealthy',
                'message': 'Cannot connect to Redis',
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f"Redis check failed: {e}",
                'timestamp': datetime.now().isoformat(),
            }
    
    def _check_disk_space(self) -> Dict[str, Any]:
        """Check disk space availability."""
        try:
            disk_usage = psutil.disk_usage('/')
            free_gb = disk_usage.free / (1024**3)
            total_gb = disk_usage.total / (1024**3)
            used_percent = (disk_usage.used / disk_usage.total) * 100
            
            status = 'healthy'
            if used_percent > 90:
                status = 'unhealthy'
            elif used_percent > 80:
                status = 'degraded'
            
            return {
                'status': status,
                'info': {
                    'total_gb': round(total_gb, 2),
                    'free_gb': round(free_gb, 2),
                    'used_percent': round(used_percent, 2),
                },
                'timestamp': datetime.now().isoformat(),
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f"Disk space check failed: {e}",
                'timestamp': datetime.now().isoformat(),
            }
    
    def _check_memory_usage(self) -> Dict[str, Any]:
        """Check memory usage."""
        try:
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()
            
            status = 'healthy'
            if memory.percent > 90:
                status = 'unhealthy'
            elif memory.percent > 80:
                status = 'degraded'
            
            return {
                'status': status,
                'info': {
                    'total_gb': round(memory.total / (1024**3), 2),
                    'available_gb': round(memory.available / (1024**3), 2),
                    'used_percent': round(memory.percent, 2),
                    'swap_total_gb': round(swap.total / (1024**3), 2),
                    'swap_used_percent': round(swap.percent, 2),
                },
                'timestamp': datetime.now().isoformat(),
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f"Memory check failed: {e}",
                'timestamp': datetime.now().isoformat(),
            }
    
    def _check_cpu_usage(self) -> Dict[str, Any]:
        """Check CPU usage."""
        try:
            # Get CPU usage over 1 second
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_count = psutil.cpu_count()
            load_avg = os.getloadavg() if hasattr(os, 'getloadavg') else None
            
            status = 'healthy'
            if cpu_percent > 90:
                status = 'unhealthy'
            elif cpu_percent > 80:
                status = 'degraded'
            
            return {
                'status': status,
                'info': {
                    'usage_percent': cpu_percent,
                    'count': cpu_count,
                    'load_average': load_avg,
                },
                'timestamp': datetime.now().isoformat(),
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f"CPU check failed: {e}",
                'timestamp': datetime.now().isoformat(),
            }
    
    def _check_dependencies(self) -> Dict[str, Any]:
        """Check critical dependencies."""
        dependencies = {
            'PyPDF2': 'PyPDF2',
            'pdfplumber': 'pdfplumber',
            'pandas': 'pandas',
            'openpyxl': 'openpyxl',
            'celery': 'celery',
            'redis': 'redis',
            'psutil': 'psutil',
        }
        
        missing_deps = []
        version_info = {}
        
        for name, package in dependencies.items():
            try:
                module = __import__(package)
                version = getattr(module, '__version__', 'unknown')
                version_info[name] = version
            except ImportError:
                missing_deps.append(name)
        
        status = 'healthy'
        if missing_deps:
            status = 'unhealthy'
        
        return {
            'status': status,
            'info': {
                'available': list(version_info.keys()),
                'missing': missing_deps,
                'versions': version_info,
            },
            'timestamp': datetime.now().isoformat(),
        }
    
    def _check_file_permissions(self) -> Dict[str, Any]:
        """Check file permissions for critical directories."""
        critical_paths = [
            self.settings.output_dir,
            'logs',
            'reports',
        ]
        
        permission_issues = []
        path_status = {}
        
        for path in critical_paths:
            try:
                path_obj = Path(path)
                
                # Check if path exists
                if not path_obj.exists():
                    permission_issues.append(f"{path}: does not exist")
                    path_status[path] = {'exists': False, 'writable': False}
                    continue
                
                # Check if writable
                is_writable = os.access(path, os.W_OK)
                path_status[path] = {'exists': True, 'writable': is_writable}
                
                if not is_writable:
                    permission_issues.append(f"{path}: not writable")
                    
            except Exception as e:
                permission_issues.append(f"{path}: {e}")
                path_status[path] = {'exists': False, 'writable': False, 'error': str(e)}
        
        status = 'healthy'
        if permission_issues:
            status = 'unhealthy'
        
        return {
            'status': status,
            'info': {
                'paths': path_status,
                'issues': permission_issues,
            },
            'timestamp': datetime.now().isoformat(),
        }
    
    def _check_environment(self) -> Dict[str, Any]:
        """Check environment configuration."""
        required_vars = [
            'PDF_PASSWORD',
            'CURRENCY_SYMBOL',
            'DEFAULT_CURRENCY',
        ]
        
        optional_vars = [
            'REDIS_HOST',
            'REDIS_PORT',
            'LOG_LEVEL',
        ]
        
        missing_required = []
        env_vars = {}
        
        for var in required_vars:
            value = os.getenv(var)
            env_vars[var] = value
            if not value:
                missing_required.append(var)
        
        for var in optional_vars:
            env_vars[var] = os.getenv(var)
        
        status = 'healthy'
        if missing_required:
            status = 'unhealthy'
        
        return {
            'status': status,
            'info': {
                'environment_variables': env_vars,
                'missing_required': missing_required,
            },
            'timestamp': datetime.now().isoformat(),
        }
    
    def _check_task_health(self) -> Dict[str, Any]:
        """Check Celery task processing health."""
        try:
            from src.tasks.celery_app import create_celery_app
            
            app = create_celery_app()
            
            # Check if we can access the app
            if not app:
                return {
                    'status': 'unhealthy',
                    'message': 'Cannot create Celery app',
                    'timestamp': datetime.now().isoformat(),
                }
            
            # Check worker status
            inspect = app.control.inspect()
            active_tasks = inspect.active()
            reserved_tasks = inspect.reserved()
            
            return {
                'status': 'healthy',
                'info': {
                    'active_tasks': sum(len(tasks) for tasks in (active_tasks or {}).values()),
                    'reserved_tasks': sum(len(tasks) for tasks in (reserved_tasks or {}).values()),
                },
                'timestamp': datetime.now().isoformat(),
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f"Task health check failed: {e}",
                'timestamp': datetime.now().isoformat(),
            }
    
    def _collect_metrics(self) -> Dict[str, Any]:
        """Collect performance metrics."""
        try:
            process = psutil.Process()
            
            return {
                'process': {
                    'cpu_percent': process.cpu_percent(),
                    'memory_mb': round(process.memory_info().rss / (1024**2), 2),
                    'memory_percent': process.memory_percent(),
                    'open_files': len(process.open_files()),
                    'threads': process.num_threads(),
                },
                'system': {
                    'cpu_count': psutil.cpu_count(),
                    'boot_time': datetime.fromtimestamp(psutil.boot_time()).isoformat(),
                },
            }
            
        except Exception as e:
            return {
                'error': f"Metrics collection failed: {e}",
            }
    
    def _get_uptime(self) -> str:
        """Get system uptime."""
        try:
            boot_time = psutil.boot_time()
            uptime_delta = datetime.now() - datetime.fromtimestamp(boot_time)
            return str(uptime_delta).split('.')[0]  # Remove microseconds
        except Exception:
            return "unknown"
    
    def _get_version(self) -> str:
        """Get application version."""
        try:
            import pkg_resources
            return pkg_resources.get_distribution("pdf-bank-statement-processor").version
        except Exception:
            return "1.0.0"  # Default version
    
    def get_component_health(self, component_name: str) -> Dict[str, Any]:
        """Get health status for a specific component."""
        if component_name not in self.components:
            return {
                'status': 'error',
                'message': f'Unknown component: {component_name}',
            }
        
        try:
            return self.components[component_name]()
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Component check failed: {e}',
            }
    
    def is_healthy(self) -> bool:
        """Quick health check - returns True if system is healthy."""
        health_data = self.run_health_check()
        return health_data['status'] == 'healthy'
    
    def get_health_summary(self) -> str:
        """Get human-readable health summary."""
        health_data = self.run_health_check()
        
        summary = f"System Health: {health_data['status'].upper()}\n"
        summary += f"Uptime: {health_data['uptime']}\n"
        summary += f"Version: {health_data['version']}\n\n"
        
        for component, data in health_data['components'].items():
            status_icon = "✓" if data['status'] == 'healthy' else "✗"
            summary += f"{status_icon} {component.capitalize()}: {data['status']}\n"
        
        if health_data['alerts']:
            summary += f"\nAlerts ({len(health_data['alerts'])}):\n"
            for alert in health_data['alerts']:
                summary += f"  - {alert}\n"
        
        return summary


class MonitoringEndpoint:
    """HTTP-like monitoring endpoint for external access."""
    
    def __init__(self, settings: Settings):
        """Initialize monitoring endpoint."""
        self.settings = settings
        self.health_checker = HealthChecker(settings)
        self.logger = get_logger(self.__class__.__name__)
    
    def health_check(self) -> Dict[str, Any]:
        """Return JSON health check response."""
        return self.health_checker.run_health_check()
    
    def metrics(self) -> Dict[str, Any]:
        """Return JSON metrics response."""
        return self.health_checker._collect_metrics()
    
    def ready(self) -> Dict[str, Any]:
        """Kubernetes readiness probe equivalent."""
        try:
            is_ready = self.health_checker.is_healthy()
            return {
                'ready': is_ready,
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:
            return {
                'ready': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
            }
    
    def live(self) -> Dict[str, Any]:
        """Kubernetes liveness probe equivalent."""
        return {
            'alive': True,
            'timestamp': datetime.now().isoformat(),
            'uptime': self.health_checker._get_uptime(),
        }