import logging
import os
from datetime import datetime
from config import Config

class DisputeLogger:
    """Centralized logging system for the dispute automation program"""
    
    def __init__(self):
        self.setup_logger()
        self.audit_log = []
    
    def setup_logger(self):
        """Setup the main logger with file and console handlers"""
        self.logger = logging.getLogger('dispute_automation')
        self.logger.setLevel(getattr(logging, Config.LOG_LEVEL))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Create formatters
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # File handler
        file_handler = logging.FileHandler(Config.LOG_FILE)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # Audit log file handler
        audit_formatter = logging.Formatter('%(asctime)s - %(message)s')
        audit_handler = logging.FileHandler('audit_log.txt')
        audit_handler.setLevel(logging.INFO)
        audit_handler.setFormatter(audit_formatter)
        
        self.audit_logger = logging.getLogger('audit')
        self.audit_logger.setLevel(logging.INFO)
        self.audit_logger.addHandler(audit_handler)
    
    def info(self, message):
        """Log info message"""
        self.logger.info(message)
    
    def error(self, message):
        """Log error message"""
        self.logger.error(message)
    
    def warning(self, message):
        """Log warning message"""
        self.logger.warning(message)
    
    def debug(self, message):
        """Log debug message"""
        self.logger.debug(message)
    
    def audit(self, action, client_reference, details=None):
        """Log audit trail for actions taken"""
        audit_entry = {
            'timestamp': datetime.now().isoformat(),
            'action': action,
            'client_reference': client_reference,
            'details': details or {}
        }
        
        self.audit_log.append(audit_entry)
        
        audit_message = f"Action: {action} | Client Reference: {client_reference}"
        if details:
            audit_message += f" | Details: {details}"
        
        self.audit_logger.info(audit_message)
        self.logger.info(f"AUDIT: {audit_message}")
    
    def log_system_connection(self, system_name, status, error_msg=None):
        """Log system connection attempts"""
        if status == 'success':
            self.info(f"Successfully connected to {system_name}")
        else:
            self.error(f"Failed to connect to {system_name}: {error_msg}")
    
    def log_row_processing(self, row_id, client_reference, action, status):
        """Log processing of individual Smartsheet rows"""
        message = f"Row {row_id} | Client Ref: {client_reference} | Action: {action} | Status: {status}"
        self.info(message)
        
        if status == 'error':
            self.audit('ROW_PROCESSING_ERROR', client_reference, {
                'row_id': row_id,
                'action': action
            })
        else:
            self.audit('ROW_PROCESSED', client_reference, {
                'row_id': row_id,
                'action': action,
                'status': status
            })
    
    def log_query_execution(self, query_type, client_reference, status, error_msg=None):
        """Log SQL query executions"""
        if status == 'success':
            self.info(f"Successfully executed {query_type} query for client reference: {client_reference}")
            self.audit('QUERY_EXECUTED', client_reference, {'query_type': query_type})
        else:
            self.error(f"Failed to execute {query_type} query for {client_reference}: {error_msg}")
            self.audit('QUERY_ERROR', client_reference, {
                'query_type': query_type,
                'error': error_msg
            })
    
    def get_audit_summary(self):
        """Get summary of all audit activities"""
        return {
            'total_actions': len(self.audit_log),
            'actions_by_type': {},
            'processed_references': list(set([entry['client_reference'] for entry in self.audit_log])),
            'audit_log': self.audit_log
        }

# Global logger instance
dispute_logger = DisputeLogger() 