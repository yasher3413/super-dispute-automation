#!/usr/bin/env python3
"""
Automated Dispute Handling Program for Super.com
Main automation engine that orchestrates the dispute handling process
"""

import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json
import pandas as pd

# Import our modules
from config import Config
from logger import dispute_logger
from smartsheet_integration import SmartsheetIntegration
from snowflake_integration import SnowflakeIntegration
from customer_profile_integration import CustomerProfileIntegration

class DisputeAutomationEngine:
    """Main automation engine for handling disputes"""
    
    def __init__(self):
        """Initialize the automation engine with all integrations"""
        self.smartsheet = None
        self.snowflake = None
        self.customer_profile = None
        self.processing_stats = {
            'total_rows': 0,
            'processed': 0,
            'errors': 0,
            'skipped': 0,
            'updated': 0
        }
        
        # Initialize integrations
        self.initialize_integrations()
    
    def initialize_integrations(self):
        """Initialize all system integrations"""
        try:
            dispute_logger.info("Initializing automation engine...")
            
            # Validate configuration
            Config.validate_config()
            
            # Initialize integrations
            dispute_logger.info("Connecting to Smartsheet...")
            self.smartsheet = SmartsheetIntegration()
            
            dispute_logger.info("Connecting to Snowflake...")
            self.snowflake = SnowflakeIntegration()
            
            dispute_logger.info("Connecting to Customer Profile...")
            self.customer_profile = CustomerProfileIntegration()
            
            dispute_logger.info("All integrations initialized successfully")
            
        except Exception as e:
            dispute_logger.error(f"Failed to initialize integrations: {str(e)}")
            raise
    
    def run_automation(self) -> Dict:
        """
        Main method to run the complete dispute automation process
        Returns summary of processing results
        """
        start_time = time.time()
        dispute_logger.info("Starting dispute automation process...")
        
        try:
            # Get all dispute rows from Smartsheet
            dispute_rows = self.smartsheet.get_dispute_rows()
            self.processing_stats['total_rows'] = len(dispute_rows)
            
            if not dispute_rows:
                dispute_logger.info("No dispute rows found to process")
                return self._generate_summary(start_time)
            
            dispute_logger.info(f"Found {len(dispute_rows)} dispute rows to process")
            
            # Process each dispute row
            for row in dispute_rows:
                try:
                    self._process_dispute_row(row)
                except Exception as e:
                    dispute_logger.error(f"Error processing row {row['row_id']}: {str(e)}")
                    self.processing_stats['errors'] += 1
                    continue
            
            # Generate final summary
            summary = self._generate_summary(start_time)
            dispute_logger.info(f"Automation completed. Processed: {self.processing_stats['processed']}, "
                              f"Updated: {self.processing_stats['updated']}, "
                              f"Errors: {self.processing_stats['errors']}")
            
            return summary
            
        except Exception as e:
            dispute_logger.error(f"Critical error in automation process: {str(e)}")
            raise
        
        finally:
            # Cleanup connections
            self._cleanup_connections()
    
    def _process_dispute_row(self, row: Dict) -> bool:
        """
        Process a single dispute row through the complete workflow
        Returns True if row was successfully processed and updated
        """
        client_reference = row['client_reference']
        row_id = row['row_id']
        
        dispute_logger.info(f"Processing row {row_id} for client reference: {client_reference}")
        
        try:
            # Step 1: Validate row eligibility
            is_eligible, eligibility_reason = self.smartsheet.validate_row_eligibility(row)
            
            if not is_eligible:
                dispute_logger.info(f"Row {row_id} skipped: {eligibility_reason}")
                self.processing_stats['skipped'] += 1
                return False
            
            # Step 2: Get booking information from Customer Profile
            dispute_logger.info(f"Validating booking in Customer Profile for {client_reference}")
            cp_info = self.customer_profile.get_comprehensive_booking_info(client_reference)
            
            # Step 3: Get booking logs from Snowflake
            dispute_logger.info(f"Retrieving booking logs from Snowflake for {client_reference}")
            logs_df, detected_error = self.snowflake.get_booking_logs(client_reference)
            
            # Step 4: Analyze and determine the error type
            final_error_type = self._determine_error_type(cp_info, detected_error, logs_df)
            
            # Step 5: Save logs to file if available
            log_file_path = None
            if logs_df is not None and not logs_df.empty:
                log_file_path = self.snowflake.save_logs_to_file(logs_df, client_reference)
            
            # Step 6: Create status updates
            has_logs = log_file_path is not None
            updates = self.smartsheet.create_status_updates(final_error_type, has_logs)
            
            # Step 7: Handle rebooking scenarios
            rebooking_info = self.snowflake.check_rebooking_scenario(client_reference)
            if rebooking_info and rebooking_info.get('is_rebooking'):
                updates = self._handle_rebooking_scenario(updates, rebooking_info, client_reference)
            
            # Step 8: Update Smartsheet
            success = self.smartsheet.update_row(row_id, updates, log_file_path)
            
            if success:
                dispute_logger.log_row_processing(row_id, client_reference, 'update', 'success')
                self.processing_stats['updated'] += 1
                self.processing_stats['processed'] += 1
                
                # Log audit trail
                dispute_logger.audit('ROW_UPDATED', client_reference, {
                    'row_id': row_id,
                    'error_type': final_error_type,
                    'has_logs': has_logs,
                    'updates_applied': list(updates.keys())
                })
                
                return True
            else:
                dispute_logger.log_row_processing(row_id, client_reference, 'update', 'failed')
                self.processing_stats['errors'] += 1
                return False
                
        except Exception as e:
            dispute_logger.error(f"Error processing dispute row {row_id}: {str(e)}")
            dispute_logger.log_row_processing(row_id, client_reference, 'process', 'error')
            self.processing_stats['errors'] += 1
            return False
    
    def _determine_error_type(self, cp_info: Dict, snowflake_error: Optional[str], 
                            logs_df: Optional[pd.DataFrame]) -> Optional[str]:
        """
        Determine the final error type based on all available information
        """
        # Priority order: Snowflake detected error, CP detected error, generic error
        
        # First check Snowflake logs
        if snowflake_error:
            return snowflake_error
        
        # Then check Customer Profile
        if cp_info and cp_info.get('detected_error_type'):
            return cp_info['detected_error_type']
        
        # Check if booking is invalid in CP
        if cp_info and not cp_info.get('is_valid'):
            return 'BOOKING_INVALID'
        
        # Check if we have any logs but no specific error detected
        if logs_df is not None and not logs_df.empty:
            return 'PROVIDER_ERROR'  # Generic provider error
        
        # No specific error detected
        return None
    
    def _handle_rebooking_scenario(self, updates: Dict, rebooking_info: Dict, 
                                 client_reference: str) -> Dict:
        """
        Handle special messaging for rebooking scenarios
        """
        if rebooking_info.get('cancel_count', 0) > 0:
            # This is a cancellation and rebooking scenario
            cancellation_info = self.snowflake.get_cancellation_info(client_reference)
            
            if cancellation_info:
                old_booking_id = cancellation_info.get('supplier_order_id', 'N/A')
                new_booking_id = client_reference  # Current reference is the new booking
                
                updates['round_1_notes'] = (
                    f"We have attempted to cancel this booking before the last day for a full refund - "
                    f"please find the attachments. We then immediately booked {new_booking_id} "
                    f"after canceling {old_booking_id}."
                )
        
        return updates
    
    def _generate_summary(self, start_time: float) -> Dict:
        """Generate processing summary"""
        end_time = time.time()
        duration = end_time - start_time
        
        summary = {
            'timestamp': datetime.now().isoformat(),
            'duration_seconds': round(duration, 2),
            'statistics': self.processing_stats.copy(),
            'audit_summary': dispute_logger.get_audit_summary()
        }
        
        return summary
    
    def _cleanup_connections(self):
        """Clean up all system connections"""
        try:
            if self.snowflake:
                self.snowflake.close_connection()
            dispute_logger.info("Connections cleaned up successfully")
        except Exception as e:
            dispute_logger.error(f"Error during cleanup: {str(e)}")
    
    def process_single_client_reference(self, client_reference: str) -> Dict:
        """
        Process a single client reference (useful for testing or manual processing)
        """
        dispute_logger.info(f"Processing single client reference: {client_reference}")
        
        try:
            # Find the row in Smartsheet
            row = self.smartsheet.get_row_by_client_reference(client_reference)
            
            if not row:
                return {
                    'success': False,
                    'error': f'No row found for client reference: {client_reference}'
                }
            
            # Process the row
            success = self._process_dispute_row(row)
            
            return {
                'success': success,
                'client_reference': client_reference,
                'row_id': row['row_id'],
                'processing_stats': self.processing_stats
            }
            
        except Exception as e:
            dispute_logger.error(f"Error processing single client reference: {str(e)}")
            return {
                'success': False,
                'client_reference': client_reference,
                'error': str(e)
            }
    
    def generate_report(self) -> Dict:
        """Generate a detailed report of the automation system status"""
        try:
            report = {
                'timestamp': datetime.now().isoformat(),
                'system_status': {
                    'smartsheet': 'connected' if self.smartsheet else 'disconnected',
                    'snowflake': 'connected' if self.snowflake else 'disconnected',
                    'customer_profile': 'connected' if self.customer_profile else 'disconnected'
                },
                'configuration': {
                    'sheet_id': Config.SMARTSHEET_SHEET_ID,
                    'snowflake_database': Config.SNOWFLAKE_DATABASE,
                    'cp_base_url': Config.CP_BASE_URL,
                    'log_level': Config.LOG_LEVEL,
                    'error_patterns': Config.ERROR_PATTERNS,
                    'dispute_triggers': Config.DISPUTE_TRIGGERS
                },
                'processing_stats': self.processing_stats,
                'audit_summary': dispute_logger.get_audit_summary() if hasattr(dispute_logger, 'get_audit_summary') else {}
            }
            
            return report
            
        except Exception as e:
            dispute_logger.error(f"Error generating report: {str(e)}")
            raise

def main():
    """Main entry point for the automation program"""
    try:
        print("=" * 60)
        print("Super.com Automated Dispute Handling System")
        print("=" * 60)
        
        # Initialize and run automation
        automation_engine = DisputeAutomationEngine()
        
        # Check if specific client reference is provided as command line argument
        if len(sys.argv) > 1:
            client_reference = sys.argv[1]
            print(f"Processing single client reference: {client_reference}")
            result = automation_engine.process_single_client_reference(client_reference)
            print(json.dumps(result, indent=2))
        else:
            # Run full automation
            print("Starting full automation process...")
            summary = automation_engine.run_automation()
            
            print("\n" + "=" * 60)
            print("AUTOMATION SUMMARY")
            print("=" * 60)
            print(json.dumps(summary, indent=2))
        
        print("\nAutomation completed successfully!")
        
    except KeyboardInterrupt:
        print("\nAutomation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Critical error: {str(e)}")
        dispute_logger.error(f"Critical error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 