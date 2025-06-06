import smartsheet
import pandas as pd
from typing import List, Dict, Optional, Tuple
from config import Config
from logger import dispute_logger
import re
import json
import time

class SmartsheetIntegration:
    """Handle all Smartsheet operations for dispute automation"""
    
    def __init__(self):
        self.client = None
        self.sheet = None
        self.columns = {}
        self.connect()
    
    def connect(self):
        """Establish connection to Smartsheet"""
        try:
            self.client = smartsheet.Smartsheet(Config.SMARTSHEET_ACCESS_TOKEN)
            self.client.errors_as_exceptions(True)
            
            # Get the sheet
            self.sheet = self.client.Sheets.get_sheet(Config.SMARTSHEET_SHEET_ID)
            
            # Map column names to IDs
            self._map_columns()
            
            dispute_logger.log_system_connection('Smartsheet', 'success')
        except Exception as e:
            dispute_logger.log_system_connection('Smartsheet', 'error', str(e))
            raise
    
    def _map_columns(self):
        """Create mapping of column names to column IDs"""
        for column in self.sheet.columns:
            self.columns[column.title] = column.id
        
        # Verify required columns exist
        required_columns = list(Config.DISPUTE_COLUMNS.values())
        missing_columns = []
        
        for col_name in required_columns:
            if col_name not in self.columns:
                missing_columns.append(col_name)
        
        if missing_columns:
            raise ValueError(f"Missing required columns in Smartsheet: {missing_columns}")
    
    def get_dispute_rows(self) -> List[Dict]:
        """Get all rows that contain dispute trigger messages"""
        dispute_rows = []
        
        try:
            # Refresh sheet data
            self.sheet = self.client.Sheets.get_sheet(Config.SMARTSHEET_SHEET_ID)
            
            notes_column_id = self.columns[Config.DISPUTE_COLUMNS['round_1_notes']]
            client_ref_column_id = self.columns[Config.DISPUTE_COLUMNS['client_reference']]
            
            for row in self.sheet.rows:
                row_data = self._extract_row_data(row)
                
                # Check if this row has a dispute trigger message
                notes_value = row_data.get('round_1_notes', '').lower()
                client_reference = row_data.get('client_reference', '')
                
                if self._is_dispute_row(notes_value) and client_reference:
                    dispute_rows.append({
                        'row_id': row.id,
                        'row_number': row.row_number,
                        'client_reference': client_reference,
                        'current_notes': row_data.get('round_1_notes', ''),
                        'supplier_comments': row_data.get('round_1_supplier_comments', ''),
                        'status': row_data.get('round_1_status', ''),
                        'completion': row_data.get('round_1_completion', ''),
                        'raw_data': row_data
                    })
            
            dispute_logger.info(f"Found {len(dispute_rows)} rows with dispute triggers")
            return dispute_rows
            
        except Exception as e:
            dispute_logger.error(f"Error retrieving dispute rows: {str(e)}")
            raise
    
    def _extract_row_data(self, row) -> Dict:
        """Extract data from a Smartsheet row"""
        row_data = {}
        
        for cell in row.cells:
            # Find column name by ID
            column_name = None
            for col_title, col_id in self.columns.items():
                if col_id == cell.column_id:
                    column_name = col_title
                    break
            
            if column_name:
                # Map to our internal column names
                for internal_name, external_name in Config.DISPUTE_COLUMNS.items():
                    if external_name == column_name:
                        row_data[internal_name] = cell.display_value or cell.value or ''
                        break
        
        return row_data
    
    def _is_dispute_row(self, notes_text: str) -> bool:
        """Check if a row contains dispute trigger messages"""
        if not notes_text:
            return False
        
        for trigger in Config.DISPUTE_TRIGGERS:
            if trigger.lower() in notes_text:
                return True
        
        return False
    
    def update_row(self, row_id: int, updates: Dict[str, str], attachment_path: Optional[str] = None) -> bool:
        """Update a Smartsheet row with new data and optionally attach a file"""
        try:
            # Prepare cell updates
            cells_to_update = []
            
            for internal_col, value in updates.items():
                if internal_col in Config.DISPUTE_COLUMNS:
                    column_name = Config.DISPUTE_COLUMNS[internal_col]
                    column_id = self.columns.get(column_name)
                    
                    if column_id:
                        cells_to_update.append({
                            'column_id': column_id,
                            'value': value
                        })
            
            if cells_to_update:
                # Create the row update object
                row_to_update = smartsheet.models.Row()
                row_to_update.id = row_id
                row_to_update.cells = cells_to_update
                
                # Update the row
                response = self.client.Sheets.update_rows(
                    Config.SMARTSHEET_SHEET_ID,
                    [row_to_update]
                )
                
                if response.result:
                    dispute_logger.info(f"Successfully updated row {row_id}")
                    
                    # Attach file if provided
                    if attachment_path:
                        self._attach_file_to_row(row_id, attachment_path)
                    
                    return True
                else:
                    dispute_logger.error(f"Failed to update row {row_id}")
                    return False
            
        except Exception as e:
            dispute_logger.error(f"Error updating row {row_id}: {str(e)}")
            return False
    
    def _attach_file_to_row(self, row_id: int, file_path: str) -> bool:
        """Attach a file to a specific row"""
        try:
            # Attach file to row
            response = self.client.Attachments.attach_file_to_row(
                Config.SMARTSHEET_SHEET_ID,
                row_id,
                file_path
            )
            
            if response.result:
                dispute_logger.info(f"Successfully attached file to row {row_id}")
                return True
            else:
                dispute_logger.error(f"Failed to attach file to row {row_id}")
                return False
                
        except Exception as e:
            dispute_logger.error(f"Error attaching file to row {row_id}: {str(e)}")
            return False
    
    def get_row_by_client_reference(self, client_reference: str) -> Optional[Dict]:
        """Find a row by client reference number"""
        try:
            dispute_rows = self.get_dispute_rows()
            
            for row in dispute_rows:
                if row['client_reference'] == client_reference:
                    return row
            
            return None
            
        except Exception as e:
            dispute_logger.error(f"Error finding row for client reference {client_reference}: {str(e)}")
            return None
    
    def create_status_updates(self, error_type: str, has_logs: bool = True) -> Dict[str, str]:
        """Create the appropriate status updates based on error type and logs"""
        updates = {}
        
        # Standard status updates
        updates['round_1_supplier_comments'] = Config.STATUS_UPDATES['supplier_comments_to']
        updates['round_1_status'] = Config.STATUS_UPDATES['status_to']
        updates['round_1_completion'] = Config.STATUS_UPDATES['completion_to']
        
        # Update notes based on error type
        if has_logs:
            if error_type:
                updates['round_1_notes'] = (
                    f"The booking was not confirmed by the supplier, please find the logs attached. "
                    f"Our request log was met with an {error_type.upper()} ERROR."
                )
            else:
                updates['round_1_notes'] = (
                    "The booking was not confirmed by the supplier, please find the logs attached. "
                    "Our request log shows a provider error."
                )
        else:
            updates['round_1_notes'] = (
                "Technical review completed. No logs available for this booking."
            )
        
        return updates
    
    def validate_row_eligibility(self, row_data: Dict) -> Tuple[bool, str]:
        """Validate if a row is eligible for automated processing"""
        supplier_comments = row_data.get('supplier_comments', '').lower()
        status = row_data.get('status', '').lower()
        completion = row_data.get('completion', '').lower()
        
        # Check if already processed
        if (supplier_comments == Config.STATUS_UPDATES['supplier_comments_to'].lower() and
            status == Config.STATUS_UPDATES['status_to'].lower() and
            completion == Config.STATUS_UPDATES['completion_to'].lower()):
            return False, "Already processed"
        
        # Check if in correct initial state
        if (supplier_comments != Config.STATUS_UPDATES['supplier_comments_from'].lower() or
            status != Config.STATUS_UPDATES['status_from'].lower() or
            completion != Config.STATUS_UPDATES['completion_from'].lower()):
            return False, "Not in expected initial state"
        
        return True, "Eligible for processing" 