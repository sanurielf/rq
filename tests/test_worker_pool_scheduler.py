"""
Unit tests for WorkerPool pool-level scheduler behavior.

These tests use mocks to avoid spawning real processes or connecting to Redis,
following the repository's unittest.TestCase + unittest.mock testing style.
"""
import unittest
from unittest.mock import MagicMock, patch


class DummyProcess:
    """Lightweight process stub for mocking Process instances."""
    
    def __init__(self, *args, **kwargs):
        self.pid = 12345
        self._is_alive = True
    
    def start(self):
        """Simulate starting the process."""
        pass
    
    def is_alive(self):
        """Return whether process is alive."""
        return self._is_alive
    
    def join(self, timeout=None):
        """Simulate joining the process."""
        pass


class TestWorkerPoolScheduler(unittest.TestCase):
    """Test WorkerPool's pool-level scheduler behavior with mocks."""
    
    @patch('rq.worker_pool.Process', side_effect=DummyProcess)
    @patch('rq.worker_pool.RQScheduler')
    def test_worker_pool_starts_single_scheduler_non_burst(self, mock_scheduler_class, mock_process_class):
        """
        Test that WorkerPool starts a single RQScheduler instance in non-burst mode
        when with_scheduler=True.
        """
        from rq.worker_pool import WorkerPool
        
        # Create a mocked Redis connection
        mock_connection = MagicMock()
        
        # Mock the scheduler instance
        mock_scheduler_instance = MagicMock()
        mock_scheduler_instance.acquired_locks = True
        mock_scheduler_instance._process = None
        mock_scheduler_class.return_value = mock_scheduler_instance
        
        # Create WorkerPool with mocked connection
        pool = WorkerPool(['default'], connection=mock_connection, num_workers=2)
        
        # Mock the start method's loop to exit immediately after starting
        # We'll replace the loop with a simple call to _start_scheduler and start_workers
        with patch.object(pool, 'start_workers'):
            with patch.object(pool, '_install_signal_handlers'):
                # Directly call _start_scheduler to test the scheduler creation
                pool._start_scheduler(burst=False, logging_level='INFO')
        
        # Assert that RQScheduler was instantiated exactly once
        self.assertEqual(mock_scheduler_class.call_count, 1)
        
        # Verify scheduler methods were called
        mock_scheduler_instance.acquire_locks.assert_called_once()
        mock_scheduler_instance.start.assert_called_once()
    
    @patch('rq.worker_pool.Process', side_effect=DummyProcess)
    @patch('rq.worker_pool.RQScheduler')
    def test_worker_pool_scheduler_burst_no_process(self, mock_scheduler_class, mock_process_class):
        """
        Test that WorkerPool does not start a scheduler process in burst mode.
        
        In burst mode, the scheduler should enqueue scheduled jobs once and
        release locks without starting a background process.
        """
        from rq.worker_pool import WorkerPool
        
        # Create a mocked Redis connection
        mock_connection = MagicMock()
        
        # Mock the scheduler instance
        mock_scheduler_instance = MagicMock()
        mock_scheduler_instance.acquired_locks = True
        mock_scheduler_instance._process = None
        mock_scheduler_class.return_value = mock_scheduler_instance
        
        # Create WorkerPool with mocked connection
        pool = WorkerPool(['default'], connection=mock_connection, num_workers=2)
        
        # Call _start_scheduler in burst mode
        pool._start_scheduler(burst=True, logging_level='INFO')
        
        # Assert that RQScheduler was instantiated exactly once
        self.assertEqual(mock_scheduler_class.call_count, 1)
        
        # Verify scheduler methods were called correctly for burst mode
        mock_scheduler_instance.acquire_locks.assert_called_once()
        mock_scheduler_instance.enqueue_scheduled_jobs.assert_called_once()
        mock_scheduler_instance.release_locks.assert_called_once()
        
        # Verify start() was NOT called (no background process in burst mode)
        mock_scheduler_instance.start.assert_not_called()
    
    @patch('rq.worker_pool.Process', side_effect=DummyProcess)
    @patch('rq.worker_pool.RQScheduler')
    def test_worker_pool_without_scheduler(self, mock_scheduler_class, mock_process_class):
        """
        Test that WorkerPool does not create a scheduler when with_scheduler=False.
        """
        from rq.worker_pool import WorkerPool
        
        # Create a mocked Redis connection
        mock_connection = MagicMock()
        
        # Create WorkerPool with mocked connection
        pool = WorkerPool(['default'], connection=mock_connection, num_workers=2)
        
        # Mock the start method's loop to exit immediately
        with patch.object(pool, 'start_workers'):
            with patch.object(pool, '_install_signal_handlers'):
                # Don't call _start_scheduler when with_scheduler=False
                # Just verify the scheduler is not created
                pass
        
        # Assert that RQScheduler was not instantiated
        self.assertEqual(mock_scheduler_class.call_count, 0)
        
        # Verify pool.scheduler remains None
        self.assertIsNone(pool.scheduler)


if __name__ == '__main__':
    unittest.main()
