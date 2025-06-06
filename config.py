import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration class for the automated dispute handling system"""
    
    # Smartsheet Configuration
    SMARTSHEET_ACCESS_TOKEN = os.getenv('SMARTSHEET_ACCESS_TOKEN')
    SMARTSHEET_SHEET_ID = os.getenv('SMARTSHEET_SHEET_ID')
    
    # Snowflake Configuration
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'db_apps')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'public')
    
    # Customer Profile Configuration
    CP_BASE_URL = os.getenv('CP_BASE_URL', 'https://api.customerprofile.super.com')
    CP_API_KEY = os.getenv('CP_API_KEY')
    
    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'dispute_automation.log')
    
    # Business Rules
    DISPUTE_COLUMNS = {
        'round_1_notes': 'Round 1: Super Additional Notes',
        'round_1_supplier_comments': 'Round 1: Supplier comments',
        'round_1_status': 'Round 1: Status',
        'round_1_completion': 'Round 1: Completion',
        'client_reference': 'Client Reference Number'
    }
    
    # Error patterns to look for
    ERROR_PATTERNS = [
        'SUPPLIER_CONFIRMATION_ERROR',
        'CONNECTION_ERROR',
        'TIMEOUT_ERROR',
        'PROVIDER_ERROR',
        'BOOKING_FAILED'
    ]
    
    # Dispute trigger messages
    DISPUTE_TRIGGERS = [
        'The booking was marked as invalid by the supplier, please provide the logs.',
        'The booking was marked by supplier as provider error. Please provide logs.',
        'booking was marked as invalid',
        'provider error',
        'supplier error'
    ]
    
    # Status updates
    STATUS_UPDATES = {
        'supplier_comments_from': 'in escalation process',
        'supplier_comments_to': 'reviewed by ST technical team',
        'status_from': 'escalation',
        'status_to': 'will not pay',
        'completion_from': 'need help',
        'completion_to': 'ready to submit'
    }
    
    @classmethod
    def validate_config(cls):
        """Validate that all required configuration values are present"""
        required_vars = [
            'SMARTSHEET_ACCESS_TOKEN',
            'SMARTSHEET_SHEET_ID',
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_USER',
            'SNOWFLAKE_PASSWORD',
            'CP_API_KEY'
        ]
        
        missing_vars = []
        for var in required_vars:
            if not getattr(cls, var):
                missing_vars.append(var)
        
        if missing_vars:
            raise ValueError(f"Missing required configuration variables: {', '.join(missing_vars)}")
        
        return True 