
"""
Unit tests for the core views, specifically the Health Check.
"""
import http
import json
from unittest.mock import MagicMock, patch

from django.test import RequestFactory, SimpleTestCase

from core.views import health_check


class HealthCheckTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()

    def _assert_response(self, response, expected_status, db_status_check, celery_status_check):
        assert response.status_code == expected_status
        content = json.loads(response.content)
        assert content['status'] == ('healthy' if expected_status == http.HTTPStatus.OK else 'unhealthy')
        
        # Check Database status
        if db_status_check == 'ok':
            assert content['components']['database'] == 'ok'
        else:
            assert 'error' in content['components']['database']

        # Check Celery status
        if celery_status_check == 'ok':
            assert content['components']['celery'] == 'ok'
        else:
            assert 'error' in content['components']['celery']

    @patch('core.views.connection.ensure_connection')
    @patch('core.views.current_app.connection_or_acquire')
    def test_health_check_success(self, mock_celery_conn, mock_db_conn):
        """Test that the health check returns 200 OK when all services are up."""
        mock_db_conn.return_value = None
        mock_celery_conn.return_value.__enter__.return_value = MagicMock()
        
        response = health_check(self.factory.get('/health/'))
        self._assert_response(response, http.HTTPStatus.OK, 'ok', 'ok')

    @patch('core.views.connection.ensure_connection')
    @patch('core.views.current_app.connection_or_acquire')
    def test_health_check_db_failure(self, mock_celery_conn, mock_db_conn):
        """Test that DB failure returns 503 and reflects error status."""
        mock_db_conn.side_effect = Exception("DB Fail")
        mock_celery_conn.return_value.__enter__.return_value = MagicMock()
        
        response = health_check(self.factory.get('/health/'))
        self._assert_response(response, http.HTTPStatus.SERVICE_UNAVAILABLE, 'error', 'ok')

    @patch('core.views.connection.ensure_connection')
    @patch('core.views.current_app.connection_or_acquire')
    def test_health_check_celery_failure(self, mock_celery_conn, mock_db_conn):
        """Test that Celery failure returns 503 and reflects error status."""
        mock_db_conn.return_value = None
        mock_celery_conn.side_effect = Exception("Broker Fail")
        
        response = health_check(self.factory.get('/health/'))
        self._assert_response(response, http.HTTPStatus.SERVICE_UNAVAILABLE, 'ok', 'error')
