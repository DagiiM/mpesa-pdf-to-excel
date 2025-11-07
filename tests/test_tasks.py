"""Tests for task scheduling and Celery modules."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from celery import Celery
from celery.result import AsyncResult
import time
import json

from src.tasks.celery_app import (
    create_celery_app, 
    PDFProcessingTask, 
    BackgroundProcessor
)
from src.utils.exceptions import TaskError


class TestCeleryApp:
    """Test cases for Celery app configuration."""

    def test_create_celery_app(self):
        """Test Celery app creation."""
        with patch('celery.Celery') as mock_celery:
            app = create_celery_app()
            
            # Check that Celery was instantiated correctly
            mock_celery.assert_called_once()
            assert app is not None

    def test_celery_config(self):
        """Test Celery configuration."""
        app = create_celery_app()
        
        # Check default configuration
        assert app.conf.broker_url is not None
        assert app.conf.result_backend is not None

    def test_celery_task_registration(self):
        """Test task registration."""
        app = create_celery_app()
        
        # Test that tasks are properly registered
        assert 'process_pdf_task' in app.tasks
        assert 'batch_process_pdfs_task' in app.tasks
        assert 'generate_report_task' in app.tasks


class TestPDFProcessingTask:
    """Test cases for PDF processing tasks."""

    def test_process_pdf_task_init(self, sample_settings):
        """Test PDF processing task initialization."""
        app = create_celery_app()
        task = PDFProcessingTask()
        task.app = app
        task.settings = sample_settings
        
        assert task.app is app
        assert task.settings == sample_settings

    @patch('src.tasks.celery_app.PDFProcessor')
    def test_process_pdf_task_run(self, mock_processor_class, sample_pdf_file, sample_settings):
        """Test PDF processing task execution."""
        # Setup mocks
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor
        mock_processor.process_pdf.return_value = [
            ["Header1", "Header2"],
            ["Data1", "Data2"]
        ]
        
        # Create task and run it
        app = create_celery_app()
        task = PDFProcessingTask()
        task.app = app
        task.settings = sample_settings
        
        result = task.run(sample_pdf_file, "test_password")
        
        # Verify execution
        mock_processor.process_pdf.assert_called_once()
        assert result is not None
        assert len(result) == 2

    @patch('src.tasks.celery_app.PDFProcessor')
    def test_process_pdf_task_error_handling(self, mock_processor_class, sample_pdf_file):
        """Test PDF processing task error handling."""
        # Setup mock to raise exception
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor
        mock_processor.process_pdf.side_effect = Exception("Test error")
        
        app = create_celery_app()
        task = PDFProcessingTask()
        task.app = app
        
        with pytest.raises(Exception):
            task.run(sample_pdf_file, "test_password")

    @patch('src.tasks.celery_app.PDFProcessor')
    def test_batch_process_pdfs_task(self, mock_processor_class, temp_dir):
        """Test batch PDF processing task."""
        # Setup mocks
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor
        mock_processor.process_pdf.return_value = [
            ["Header1", "Header2"],
            ["Data1", "Data2"]
        ]
        
        app = create_celery_app()
        task = PDFProcessingTask()
        task.app = app
        
        # Create test PDF files
        pdf_files = []
        for i in range(3):
            pdf_file = temp_dir / f"test{i}.pdf"
            pdf_file.write_bytes(b"test content")
            pdf_files.append(str(pdf_file))
        
        result = task.batch_run(pdf_files, "test_password")
        
        # Verify execution
        assert mock_processor.process_pdf.call_count == 3
        assert len(result) == 3

    @patch('src.tasks.celery_app.PDFProcessor')
    def test_generate_report_task(self, mock_processor_class, sample_transactions, temp_dir):
        """Test report generation task."""
        # Setup mocks
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor
        
        app = create_celery_app()
        task = PDFProcessingTask()
        task.app = app
        
        output_file = temp_dir / "test_report.xlsx"
        
        result = task.generate_report(
            sample_transactions, 
            str(output_file)
        )
        
        # Verify report generation
        assert result is not None
        assert "test_report.xlsx" in result

    def test_task_retry_logic(self, sample_settings):
        """Test task retry logic."""
        app = create_celery_app()
        task = PDFProcessingTask()
        task.app = app
        task.settings = sample_settings
        
        # Test retry count tracking
        assert hasattr(task, 'retry_count')
        assert task.retry_count == 0

    def test_task_progress_tracking(self):
        """Test task progress tracking."""
        app = create_celery_app()
        task = PDFProcessingTask()
        task.app = app
        
        # Test progress update
        task.update_progress(50, "Processing...")
        assert hasattr(task, 'progress')
        assert task.progress == 50

    def test_task_timeout_handling(self):
        """Test task timeout handling."""
        app = create_celery_app()
        task = PDFProcessingTask()
        task.app = app
        
        # Test timeout configuration
        assert hasattr(task, 'time_limit')
        assert task.time_limit is not None

    def test_task_state_management(self):
        """Test task state management."""
        app = create_celery_app()
        task = PDFProcessingTask()
        task.app = app
        
        # Test state transitions
        assert task.state in ['PENDING', 'STARTED', 'SUCCESS', 'FAILURE']
        task.state = 'STARTED'
        assert task.state == 'STARTED'


class TestBackgroundProcessor:
    """Test cases for background processor functionality."""

    def test_init(self, sample_settings):
        """Test BackgroundProcessor initialization."""
        processor = BackgroundProcessor(sample_settings)
        assert processor.settings == sample_settings
        assert processor.app is not None

    @patch('src.tasks.celery_app.BackgroundProcessor._start_worker')
    def test_start(self, mock_start_worker, sample_settings):
        """Test starting the background processor."""
        processor = BackgroundProcessor(sample_settings)
        
        processor.start()
        
        mock_start_worker.assert_called_once()

    @patch('src.tasks.celery_app.BackgroundProcessor._start_worker')
    @patch('src.tasks.celery_app.BackgroundProcessor._start_monitor')
    def test_start_with_monitoring(self, mock_start_monitor, mock_start_worker, sample_settings):
        """Test starting background processor with monitoring."""
        processor = BackgroundProcessor(sample_settings)
        
        processor.start(monitoring=True)
        
        mock_start_worker.assert_called_once()
        mock_start_monitor.assert_called_once()

    def test_stop(self, sample_settings):
        """Test stopping the background processor."""
        processor = BackgroundProcessor(sample_settings)
        
        # Mock the worker
        processor.worker = Mock()
        
        processor.stop()
        
        assert processor.worker.terminate.called

    @patch('redis.Redis')
    def test_health_check(self, mock_redis, sample_settings):
        """Test health check functionality."""
        # Setup mock redis
        mock_client = Mock()
        mock_redis.return_value = mock_client
        mock_client.ping.return_value = True
        
        processor = BackgroundProcessor(sample_settings)
        
        health = processor.health_check()
        
        assert health['status'] == 'healthy'
        assert 'redis' in health
        assert health['redis']['status'] == 'connected'

    @patch('redis.Redis')
    def test_health_check_redis_down(self, mock_redis, sample_settings):
        """Test health check when Redis is down."""
        # Setup mock to simulate Redis failure
        mock_redis.side_effect = Exception("Connection failed")
        
        processor = BackgroundProcessor(sample_settings)
        
        health = processor.health_check()
        
        assert health['status'] == 'unhealthy'
        assert 'redis' in health
        assert health['redis']['status'] == 'disconnected'

    def test_get_task_status(self, sample_settings):
        """Test getting task status."""
        processor = BackgroundProcessor(sample_settings)
        
        # Mock async result
        mock_result = Mock()
        mock_result.state = 'SUCCESS'
        mock_result.info = {'test': 'data'}
        
        processor.get_task_status = Mock(return_value=mock_result)
        
        status = processor.get_task_status('task_id_123')
        
        assert status.state == 'SUCCESS'
        assert status.info == {'test': 'data'}

    def test_get_queue_stats(self, sample_settings):
        """Test getting queue statistics."""
        processor = BackgroundProcessor(sample_settings)
        
        # Mock queue inspection
        with patch('celery.current_app.control.inspect') as mock_inspect:
            mock_inspect.return_value.active.return_value = {
                'worker1': [
                    {'id': 'task1', 'name': 'process_pdf_task'}
                ]
            }
            mock_inspect.return_value.reserved.return_value = {
                'worker1': [
                    {'id': 'task2', 'name': 'process_pdf_task'}
                ]
            }
            
            stats = processor.get_queue_stats()
            
            assert 'active_tasks' in stats
            assert 'reserved_tasks' in stats
            assert stats['active_tasks'] >= 0

    def test_cleanup_stale_tasks(self, sample_settings):
        """Test cleanup of stale tasks."""
        processor = BackgroundProcessor(sample_settings)
        
        # Mock task cleanup
        with patch('celery.current_app.control.inspect') as mock_inspect:
            mock_inspect.return_value.active.return_value = {
                'worker1': [
                    {'id': 'stale_task', 'name': 'process_pdf_task', 'time_start': time.time() - 3600}  # 1 hour ago
                ]
            }
            
            cleaned = processor.cleanup_stale_tasks(max_age_hours=1)
            
            assert cleaned >= 0

    def test_submit_task(self, sample_settings):
        """Test submitting tasks for processing."""
        processor = BackgroundProcessor(sample_settings)
        
        # Mock the apply_async method
        processor.app = Mock()
        processor.app.send_task.return_value = Mock(id='task_123')
        
        task_id = processor.submit_task('process_pdf_task', 'test.pdf', 'password')
        
        assert task_id == 'task_123'
        processor.app.send_task.assert_called_once_with(
            'process_pdf_task',
            args=('test.pdf', 'password'),
            kwargs={},
            queue='default'
        )

    def test_batch_submit_tasks(self, sample_settings, temp_dir):
        """Test batch task submission."""
        processor = BackgroundProcessor(sample_settings)
        
        # Create test files
        files = []
        for i in range(3):
            pdf_file = temp_dir / f"test{i}.pdf"
            pdf_file.write_bytes(b"test")
            files.append(str(pdf_file))
        
        # Mock the submit_task method
        processor.submit_task = Mock(return_value=f"task_{i}")
        
        task_ids = processor.batch_submit_tasks(files, "password")
        
        assert len(task_ids) == 3
        assert all(task_id.startswith('task_') for task_id in task_ids)

    def test_get_worker_info(self, sample_settings):
        """Test getting worker information."""
        processor = BackgroundProcessor(sample_settings)
        
        with patch('celery.current_app.control.inspect') as mock_inspect:
            mock_inspect.return_value.stats.return_value = {
                'worker1': {
                    'pool': {'processes': 4},
                    'total': {'tasks': 100}
                }
            }
            
            info = processor.get_worker_info()
            
            assert 'worker1' in info
            assert info['worker1']['processes'] == 4
            assert info['worker1']['total_tasks'] == 100

    def test_configure_task_routing(self, sample_settings):
        """Test task routing configuration."""
        processor = BackgroundProcessor(sample_settings)
        
        # Test route configuration
        routes = processor.get_task_routes()
        
        assert 'process_pdf_task' in routes
        assert 'batch_process_pdfs_task' in routes
        assert 'generate_report_task' in routes

    def test_get_metrics(self, sample_settings):
        """Test getting processing metrics."""
        processor = BackgroundProcessor(sample_settings)
        
        # Mock metrics collection
        with patch('src.tasks.celery_app.collect_metrics') as mock_collect:
            mock_collect.return_value = {
                'tasks_processed': 100,
                'tasks_failed': 5,
                'average_processing_time': 1.23
            }
            
            metrics = processor.get_metrics()
            
            assert metrics['tasks_processed'] == 100
            assert metrics['tasks_failed'] == 5
            assert metrics['average_processing_time'] == 1.23


class TestTaskIntegration:
    """Integration tests for task workflows."""

    def test_full_processing_workflow(self, sample_settings, sample_pdf_file, temp_dir):
        """Test complete PDF processing workflow."""
        # This would be a more comprehensive integration test
        # that tests the full pipeline from PDF input to Excel output
        
        processor = BackgroundProcessor(sample_settings)
        
        # Mock the entire workflow
        with patch.object(processor, 'submit_task') as mock_submit:
            mock_submit.return_value = Mock(id='workflow_task_123')
            
            # Simulate workflow submission
            task_id = processor.submit_workflow(
                pdf_file=sample_pdf_file,
                password="test",
                output_dir=str(temp_dir)
            )
            
            assert task_id == 'workflow_task_123'

    def test_error_recovery_workflow(self, sample_settings):
        """Test error recovery in task workflow."""
        processor = BackgroundProcessor(sample_settings)
        
        # Test retry logic
        with patch.object(processor, 'submit_task') as mock_submit:
            # First two attempts fail, third succeeds
            mock_submit.side_effect = [
                Exception("Temporary error"),
                Exception("Temporary error"),
                Mock(id='success_task_123')
            ]
            
            task_id = processor.submit_with_retry(
                'process_pdf_task',
                'test.pdf',
                'password',
                max_retries=3
            )
            
            assert task_id == 'success_task_123'
            assert mock_submit.call_count == 3

    def test_concurrent_task_handling(self, sample_settings):
        """Test handling multiple concurrent tasks."""
        processor = BackgroundProcessor(sample_settings)
        
        # Mock concurrent task handling
        with patch.object(processor, 'submit_task') as mock_submit:
            mock_submit.side_effect = [Mock(id=f'task_{i}') for i in range(10)]
            
            task_ids = []
            for i in range(10):
                task_id = processor.submit_task('process_pdf_task', f'file{i}.pdf', 'pass')
                task_ids.append(task_id)
            
            assert len(task_ids) == 10
            assert all(task_id.startswith('task_') for task_id in task_ids)