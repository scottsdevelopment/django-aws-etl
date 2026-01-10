from unittest.mock import MagicMock, patch

import pytest
from django.test import RequestFactory

from core.views import health_check

HTTP_OK = 200
HTTP_SERVICE_UNAVAILABLE = 503

@pytest.fixture
def rf():
    return RequestFactory()

def test_health_check_success(rf):
    request = rf.get('/health/')
    
    with patch('core.views.connection.ensure_connection'), \
         patch('core.views.current_app.connection_or_acquire') as mock_celery:
        
        mock_conn = MagicMock()
        mock_celery.return_value.__enter__.return_value = mock_conn
        
        response = health_check(request)
        
        assert response.status_code == HTTP_OK
        data = response.content.decode()
        assert '"status": "healthy"' in data

def test_health_check_db_failure(rf):
    request = rf.get('/health/')
    
    with patch('core.views.connection.ensure_connection', side_effect=Exception("DB Down")), \
         patch('core.views.current_app.connection_or_acquire') as mock_celery:
        
        mock_conn = MagicMock()
        mock_celery.return_value.__enter__.return_value = mock_conn
        
        response = health_check(request)
        
        assert response.status_code == HTTP_SERVICE_UNAVAILABLE
        data = response.content.decode()
        assert '"status": "unhealthy"' in data
        assert 'DB Down' in data

def test_health_check_celery_failure(rf):
    request = rf.get('/health/')
    
    with patch('core.views.connection.ensure_connection'), \
         patch('core.views.current_app.connection_or_acquire', side_effect=Exception("Celery Down")):
        
        response = health_check(request)
        
        assert response.status_code == HTTP_SERVICE_UNAVAILABLE
        data = response.content.decode()
        assert '"status": "unhealthy"' in data
        assert 'Celery Down' in data
