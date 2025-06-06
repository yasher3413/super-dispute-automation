import requests
import json
from typing import Dict, List, Optional, Tuple
from config import Config
from logger import dispute_logger
import time

class CustomerProfileIntegration:
    """Handle all Customer Profile operations for dispute automation"""
    
    def __init__(self):
        self.base_url = Config.CP_BASE_URL
        self.api_key = Config.CP_API_KEY
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        })
        self.test_connection()
    
    def test_connection(self):
        """Test connection to Customer Profile API"""
        try:
            # Test with a simple ping or health check endpoint
            response = self.session.get(f"{self.base_url}/health", timeout=10)
            if response.status_code == 200:
                dispute_logger.log_system_connection('Customer Profile', 'success')
            else:
                dispute_logger.log_system_connection('Customer Profile', 'error', 
                                                   f"HTTP {response.status_code}")
        except Exception as e:
            dispute_logger.log_system_connection('Customer Profile', 'error', str(e))
    
    def get_booking_details(self, client_reference_id: str) -> Optional[Dict]:
        """
        Retrieve booking details from Customer Profile
        Returns booking information including error status
        """
        try:
            # Search for booking by client reference ID
            endpoint = f"{self.base_url}/bookings/search"
            params = {
                'client_reference_id': client_reference_id,
                'include_logs': True,
                'include_errors': True
            }
            
            response = self.session.get(endpoint, params=params, timeout=30)
            
            if response.status_code == 200:
                booking_data = response.json()
                
                if booking_data and 'data' in booking_data:
                    dispute_logger.info(f"Successfully retrieved booking details for {client_reference_id}")
                    return booking_data['data']
                else:
                    dispute_logger.warning(f"No booking found for client reference: {client_reference_id}")
                    return None
            
            elif response.status_code == 404:
                dispute_logger.warning(f"Booking not found in CP for client reference: {client_reference_id}")
                return None
            
            else:
                dispute_logger.error(f"CP API error for {client_reference_id}: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            dispute_logger.error(f"Error retrieving booking details from CP: {str(e)}")
            return None
    
    def get_cancellation_logs(self, client_reference_id: str) -> Optional[List[Dict]]:
        """
        Get cancellation logs and error messages from Customer Profile
        """
        try:
            endpoint = f"{self.base_url}/bookings/{client_reference_id}/cancellation-logs"
            
            response = self.session.get(endpoint, timeout=30)
            
            if response.status_code == 200:
                logs_data = response.json()
                
                if logs_data and 'logs' in logs_data:
                    dispute_logger.info(f"Successfully retrieved cancellation logs for {client_reference_id}")
                    return logs_data['logs']
                else:
                    dispute_logger.warning(f"No cancellation logs found for: {client_reference_id}")
                    return []
            
            elif response.status_code == 404:
                dispute_logger.warning(f"No cancellation logs found in CP for: {client_reference_id}")
                return []
            
            else:
                dispute_logger.error(f"CP API error getting cancellation logs: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            dispute_logger.error(f"Error retrieving cancellation logs from CP: {str(e)}")
            return None
    
    def get_error_messages(self, client_reference_id: str) -> Optional[List[Dict]]:
        """
        Get error messages associated with a booking
        """
        try:
            endpoint = f"{self.base_url}/bookings/{client_reference_id}/errors"
            
            response = self.session.get(endpoint, timeout=30)
            
            if response.status_code == 200:
                error_data = response.json()
                
                if error_data and 'errors' in error_data:
                    dispute_logger.info(f"Successfully retrieved error messages for {client_reference_id}")
                    return error_data['errors']
                else:
                    dispute_logger.warning(f"No error messages found for: {client_reference_id}")
                    return []
            
            elif response.status_code == 404:
                dispute_logger.warning(f"No error messages found in CP for: {client_reference_id}")
                return []
            
            else:
                dispute_logger.error(f"CP API error getting error messages: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            dispute_logger.error(f"Error retrieving error messages from CP: {str(e)}")
            return None
    
    def validate_booking_status(self, client_reference_id: str) -> Tuple[bool, Optional[str], Optional[Dict]]:
        """
        Validate booking status and detect common error patterns
        Returns: (is_valid, error_type, booking_data)
        """
        try:
            booking_data = self.get_booking_details(client_reference_id)
            
            if not booking_data:
                return False, 'BOOKING_NOT_FOUND', None
            
            # Check for common error patterns in booking status
            booking_status = booking_data.get('status', '').upper()
            booking_errors = booking_data.get('errors', [])
            
            # Check for specific error patterns
            detected_error = None
            
            # Check booking status
            if booking_status in ['FAILED', 'CANCELLED', 'ERROR']:
                detected_error = f'{booking_status}_STATUS'
            
            # Check error messages
            if booking_errors:
                for error in booking_errors:
                    error_message = error.get('message', '').upper()
                    error_code = error.get('code', '').upper()
                    
                    # Check against known error patterns
                    for pattern in Config.ERROR_PATTERNS:
                        if pattern in error_message or pattern in error_code:
                            detected_error = pattern
                            break
                    
                    # Check for generic error patterns
                    if not detected_error:
                        if 'SUPPLIER' in error_message and 'CONFIRMATION' in error_message:
                            detected_error = 'SUPPLIER_CONFIRMATION_ERROR'
                        elif 'CONNECTION' in error_message:
                            detected_error = 'CONNECTION_ERROR'
                        elif 'TIMEOUT' in error_message:
                            detected_error = 'TIMEOUT_ERROR'
                        elif 'PROVIDER' in error_message:
                            detected_error = 'PROVIDER_ERROR'
            
            # Get cancellation logs for additional context
            cancellation_logs = self.get_cancellation_logs(client_reference_id)
            if cancellation_logs:
                booking_data['cancellation_logs'] = cancellation_logs
                
                # Check cancellation logs for errors
                for log in cancellation_logs:
                    log_message = log.get('message', '').upper()
                    for pattern in Config.ERROR_PATTERNS:
                        if pattern in log_message:
                            detected_error = pattern
                            break
            
            # Determine if booking is valid (not in error state)
            is_valid = detected_error is None and booking_status not in ['FAILED', 'CANCELLED', 'ERROR']
            
            return is_valid, detected_error, booking_data
            
        except Exception as e:
            dispute_logger.error(f"Error validating booking status: {str(e)}")
            return False, 'VALIDATION_ERROR', None
    
    def search_related_bookings(self, client_reference_id: str) -> Optional[List[Dict]]:
        """
        Search for related bookings (for rebooking scenarios)
        """
        try:
            # First get the original booking details
            booking_data = self.get_booking_details(client_reference_id)
            
            if not booking_data:
                return None
            
            # Extract search criteria from original booking
            guest_email = booking_data.get('guest_email')
            check_in_date = booking_data.get('check_in_date')
            property_id = booking_data.get('property_id')
            
            if not guest_email:
                return None
            
            # Search for related bookings
            endpoint = f"{self.base_url}/bookings/search"
            params = {
                'guest_email': guest_email,
                'check_in_date': check_in_date,
                'property_id': property_id,
                'limit': 10
            }
            
            response = self.session.get(endpoint, params=params, timeout=30)
            
            if response.status_code == 200:
                search_results = response.json()
                
                if search_results and 'data' in search_results:
                    # Filter out the original booking
                    related_bookings = [
                        booking for booking in search_results['data']
                        if booking.get('client_reference_id') != client_reference_id
                    ]
                    
                    dispute_logger.info(f"Found {len(related_bookings)} related bookings for {client_reference_id}")
                    return related_bookings
                
                return []
            
            else:
                dispute_logger.error(f"Error searching related bookings: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            dispute_logger.error(f"Error searching related bookings: {str(e)}")
            return None
    
    def get_comprehensive_booking_info(self, client_reference_id: str) -> Dict:
        """
        Get comprehensive booking information combining all CP data
        """
        try:
            # Get main booking details
            is_valid, error_type, booking_data = self.validate_booking_status(client_reference_id)
            
            # Get additional information
            error_messages = self.get_error_messages(client_reference_id)
            related_bookings = self.search_related_bookings(client_reference_id)
            
            comprehensive_info = {
                'client_reference_id': client_reference_id,
                'is_valid': is_valid,
                'detected_error_type': error_type,
                'booking_data': booking_data,
                'error_messages': error_messages or [],
                'related_bookings': related_bookings or [],
                'timestamp': time.time()
            }
            
            return comprehensive_info
            
        except Exception as e:
            dispute_logger.error(f"Error getting comprehensive booking info: {str(e)}")
            return {
                'client_reference_id': client_reference_id,
                'is_valid': False,
                'detected_error_type': 'CP_ERROR',
                'booking_data': None,
                'error_messages': [],
                'related_bookings': [],
                'timestamp': time.time(),
                'error': str(e)
            } 