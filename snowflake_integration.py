import snowflake.connector
import pandas as pd
from typing import Dict, List, Optional, Tuple
import json
import csv
import os
from datetime import datetime
from config import Config
from logger import dispute_logger

class SnowflakeIntegration:
    """Handle all Snowflake operations for dispute automation"""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.connect()
    
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            self.connection = snowflake.connector.connect(
                user=Config.SNOWFLAKE_USER,
                password=Config.SNOWFLAKE_PASSWORD,
                account=Config.SNOWFLAKE_ACCOUNT,
                warehouse=Config.SNOWFLAKE_WAREHOUSE,
                database=Config.SNOWFLAKE_DATABASE,
                schema=Config.SNOWFLAKE_SCHEMA
            )
            
            self.cursor = self.connection.cursor()
            dispute_logger.log_system_connection('Snowflake', 'success')
            
        except Exception as e:
            dispute_logger.log_system_connection('Snowflake', 'error', str(e))
            raise
    
    def get_booking_logs(self, client_reference_id: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        Retrieve booking logs for a given client reference ID
        Returns: (DataFrame with logs, detected error type)
        """
        try:
            # The corrected SQL query from the prompt
            query = """
            --retrieve booking logs w/client reference id
            set client_reference_id = (%s);
            set booked_at = (select min(created_at)
                             from db_apps.public.booking_patch_events
                             where client_reference_id = %s);
            set cancelled_at = (select min(created_at)
                                 from db_apps.public.booking_patch_events
                                 where client_reference_id = %s
                                     and reason ilike '%%cancel%%');

            select bpe.created_at
                , bpe.supplier
                , bpe.client_reference_id
                , bpe.supplier_order_id
                , bpe.supplier_order_reference
                , pe.call
                , bpe.status
                , bpe.detailed_status
                , bpe.reason
                , listagg(case when pe.type = 'request' then pe.body end) as request
                , listagg(case when pe.type = 'response' then pe.body end) as response
                , concat('https://snaptravel.freshdesk.com/a/tickets/', fdt.id) as ops_ticket
            from db_apps.public.booking_patch_events bpe
            left join db_apps.schema_unified_events.provider_event pe
                on bpe.client_reference_id = pe.client_reference_id and bpe.supplier = pe.supplier
            left join db_apps.freshdesk.ticket fdt
                on (bpe.client_reference_id = fdt.custom_cf_client_reference_id
                         or bpe.supplier_order_reference = fdt.custom_cf_supplier_reference_id
                         or bpe.supplier_order_id = fdt.custom_cf_supplier_order_id)
                     and fdt.type in ('T:ER', 'ER')
            where 1=1
                and bpe.client_reference_id = %s
                and pe.event_created_at between dateadd(min, -10, $booked_at) and dateadd(min, 5, $booked_at)
                and pe.call = 'book'
                and bpe.reason not ilike '%%cancel%%'
            group by 1,2,3,4,5,6,7,8,9,12

            union

            select bpe.created_at
                , bpe.supplier
                , bpe.client_reference_id
                , bpe.supplier_order_id
                , bpe.supplier_order_reference
                , pe.call
                , bpe.status
                , bpe.detailed_status
                , bpe.reason
                , listagg(case when pe.type = 'request' then pe.body end) as request
                , listagg(case when pe.type = 'response' then pe.body end) as response
                , concat('https://snaptravel.freshdesk.com/a/tickets/', fdt.id) as ops_ticket
            from db_apps.public.booking_patch_events bpe
            left join db_apps.schema_unified_events.provider_event pe
                on bpe.client_reference_id = pe.client_reference_id and bpe.supplier = pe.supplier
            left join db_apps.freshdesk.ticket fdt
                on (bpe.client_reference_id = fdt.custom_cf_client_reference_id
                         or bpe.supplier_order_reference = fdt.custom_cf_supplier_reference_id
                         or bpe.supplier_order_id = fdt.custom_cf_supplier_order_id)
                     and fdt.type in ('T:ERC', 'ERC')
            where 1=1
                and bpe.client_reference_id = %s
                and pe.event_created_at between dateadd(min, -10, $cancelled_at) and dateadd(min, 5, $cancelled_at)
                and pe.call = 'cancel'
                and bpe.reason ilike '%%cancel%%'
            group by 1,2,3,4,5,6,7,8,9,12
            order by created_at
            """
            
            # Execute the query
            self.cursor.execute(query, (client_reference_id, client_reference_id, client_reference_id, client_reference_id, client_reference_id))
            
            # Fetch results
            results = self.cursor.fetchall()
            columns = [desc[0] for desc in self.cursor.description]
            
            if results:
                # Convert to DataFrame
                df = pd.DataFrame(results, columns=columns)
                
                # Detect error type
                error_type = self._detect_error_type(df)
                
                dispute_logger.log_query_execution('booking_logs', client_reference_id, 'success')
                return df, error_type
            
            else:
                dispute_logger.warning(f"No logs found for client reference: {client_reference_id}")
                return None, None
                
        except Exception as e:
            dispute_logger.log_query_execution('booking_logs', client_reference_id, 'error', str(e))
            return None, None
    
    def _detect_error_type(self, df: pd.DataFrame) -> Optional[str]:
        """Detect the type of error from the booking logs"""
        if df.empty:
            return None
        
        # Check various columns for error patterns
        text_columns = ['reason', 'detailed_status', 'response', 'request']
        
        for column in text_columns:
            if column in df.columns:
                # Convert to string and check for error patterns
                column_text = df[column].astype(str).str.upper()
                
                for error_pattern in Config.ERROR_PATTERNS:
                    if column_text.str.contains(error_pattern, na=False).any():
                        return error_pattern
                
                # Check for generic error indicators
                if column_text.str.contains('ERROR', na=False).any():
                    # Try to extract the specific error type
                    for idx, text in enumerate(column_text):
                        if 'ERROR' in text:
                            # Extract error type from text
                            words = text.split()
                            for i, word in enumerate(words):
                                if 'ERROR' in word and i > 0:
                                    return words[i-1] + '_ERROR'
                    return 'GENERAL_ERROR'
        
        return None
    
    def save_logs_to_file(self, df: pd.DataFrame, client_reference_id: str) -> str:
        """Save logs DataFrame to a CSV file and return the file path"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"booking_logs_{client_reference_id}_{timestamp}.csv"
            filepath = os.path.join("logs", filename)
            
            # Create logs directory if it doesn't exist
            os.makedirs("logs", exist_ok=True)
            
            # Save to CSV
            df.to_csv(filepath, index=False)
            
            dispute_logger.info(f"Saved booking logs to {filepath}")
            return filepath
            
        except Exception as e:
            dispute_logger.error(f"Error saving logs to file: {str(e)}")
            return None
    
    def get_booking_details(self, client_reference_id: str) -> Optional[Dict]:
        """Get basic booking details for a client reference"""
        try:
            query = """
            select 
                client_reference_id,
                supplier,
                supplier_order_id,
                supplier_order_reference,
                status,
                detailed_status,
                created_at,
                reason
            from db_apps.public.booking_patch_events
            where client_reference_id = %s
            order by created_at desc
            limit 1
            """
            
            self.cursor.execute(query, (client_reference_id,))
            result = self.cursor.fetchone()
            
            if result:
                columns = [desc[0] for desc in self.cursor.description]
                booking_details = dict(zip(columns, result))
                
                dispute_logger.log_query_execution('booking_details', client_reference_id, 'success')
                return booking_details
            
            return None
            
        except Exception as e:
            dispute_logger.log_query_execution('booking_details', client_reference_id, 'error', str(e))
            return None
    
    def get_cancellation_info(self, client_reference_id: str) -> Optional[Dict]:
        """Get cancellation information for rebooking scenarios"""
        try:
            query = """
            select 
                client_reference_id,
                supplier,
                supplier_order_id,
                supplier_order_reference,
                status,
                detailed_status,
                created_at,
                reason
            from db_apps.public.booking_patch_events
            where client_reference_id = %s
                and reason ilike '%cancel%'
            order by created_at desc
            limit 1
            """
            
            self.cursor.execute(query, (client_reference_id,))
            result = self.cursor.fetchone()
            
            if result:
                columns = [desc[0] for desc in self.cursor.description]
                cancellation_info = dict(zip(columns, result))
                
                dispute_logger.log_query_execution('cancellation_info', client_reference_id, 'success')
                return cancellation_info
            
            return None
            
        except Exception as e:
            dispute_logger.log_query_execution('cancellation_info', client_reference_id, 'error', str(e))
            return None
    
    def check_rebooking_scenario(self, client_reference_id: str) -> Optional[Dict]:
        """Check if this is a rebooking scenario with cancellation and new booking"""
        try:
            query = """
            with booking_events as (
                select 
                    client_reference_id,
                    supplier_order_id,
                    created_at,
                    reason,
                    case when reason ilike '%cancel%' then 'cancel' else 'book' end as event_type
                from db_apps.public.booking_patch_events
                where client_reference_id = %s
                order by created_at
            )
            select 
                count(case when event_type = 'book' then 1 end) as book_count,
                count(case when event_type = 'cancel' then 1 end) as cancel_count,
                min(case when event_type = 'book' then created_at end) as first_booking_time,
                max(case when event_type = 'cancel' then created_at end) as last_cancel_time,
                max(case when event_type = 'book' then created_at end) as last_booking_time
            from booking_events
            """
            
            self.cursor.execute(query, (client_reference_id,))
            result = self.cursor.fetchone()
            
            if result:
                columns = [desc[0] for desc in self.cursor.description]
                rebooking_info = dict(zip(columns, result))
                
                # Determine if this is a rebooking scenario
                if (rebooking_info['book_count'] > 1 or 
                    (rebooking_info['cancel_count'] > 0 and rebooking_info['book_count'] > 0)):
                    rebooking_info['is_rebooking'] = True
                else:
                    rebooking_info['is_rebooking'] = False
                
                return rebooking_info
            
            return None
            
        except Exception as e:
            dispute_logger.error(f"Error checking rebooking scenario: {str(e)}")
            return None
    
    def close_connection(self):
        """Close the Snowflake connection"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            dispute_logger.info("Snowflake connection closed")
        except Exception as e:
            dispute_logger.error(f"Error closing Snowflake connection: {str(e)}")
    
    def __del__(self):
        """Destructor to ensure connection is closed"""
        self.close_connection() 