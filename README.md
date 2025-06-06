# Super.com Automated Dispute Handling System

An automated solution for processing supplier disputes related to rebooker bookings across Smartsheet, Snowflake, and Customer Profile systems.

## Overview

This system automates the manual dispute handling process by:

1. **Identifying disputes** in Smartsheet using trigger messages
2. **Validating bookings** in Customer Profile to check for errors  
3. **Retrieving logs** from Snowflake using the provided SQL template
4. **Analyzing errors** and determining error types
5. **Updating Smartsheet** with appropriate status changes and attached logs
6. **Maintaining audit trails** for all actions taken

## Features

- **Automated dispute detection** using configurable trigger messages
- **Multi-system integration** with Smartsheet, Snowflake, and Customer Profile
- **Error pattern recognition** for common booking failures
- **Rebooking scenario handling** with specialized messaging
- **Comprehensive logging and audit trails**
- **Error handling and recovery** for system failures
- **Single booking processing** for testing and manual intervention
- **Status reporting** and processing summaries

## System Requirements

- Python 3.8+
- Access to Smartsheet API
- Snowflake account and credentials
- Customer Profile API access
- Network connectivity to all systems

## Installation

1. **Clone or download** this repository to your local machine

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**:
   - Copy `sample_environment.txt` to `.env`
   - Fill in your actual credentials and configuration values
   
4. **Create logs directory**:
   ```bash
   mkdir logs
   ```

## Configuration

### Environment Variables

Create a `.env` file with the following variables:

```env
# Smartsheet Configuration
SMARTSHEET_ACCESS_TOKEN=your_smartsheet_access_token_here
SMARTSHEET_SHEET_ID=your_smartsheet_sheet_id_here

# Snowflake Configuration  
SNOWFLAKE_ACCOUNT=your_snowflake_account_identifier
SNOWFLAKE_USER=your_snowflake_username
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=db_apps
SNOWFLAKE_SCHEMA=public

# Customer Profile Configuration
CP_BASE_URL=https://api.customerprofile.super.com
CP_API_KEY=your_customer_profile_api_key

# Logging Configuration (Optional)
LOG_LEVEL=INFO
LOG_FILE=dispute_automation.log
```

### Smartsheet Setup

Ensure your Smartsheet has the following columns:
- `Round 1: Super Additional Notes`
- `Round 1: Supplier comments`
- `Round 1: Status`
- `Round 1: Completion`
- `Client Reference Number`

### Dispute Trigger Messages

The system looks for these messages in the notes column:
- "The booking was marked as invalid by the supplier, please provide the logs."
- "The booking was marked by supplier as provider error. Please provide logs."
- "booking was marked as invalid"
- "provider error"
- "supplier error"

## Usage

### Full Automation

Run the complete automation process on all eligible rows:

```bash
python dispute_automation.py
```

### Single Client Reference

Process a specific client reference for testing:

```bash
python dispute_automation.py CLIENT_REF_123456
```

### Manual Testing

You can also import and use the classes directly:

```python
from dispute_automation import DisputeAutomationEngine

# Initialize the engine
engine = DisputeAutomationEngine()

# Process a single client reference
result = engine.process_single_client_reference("CLIENT_REF_123456")

# Generate a status report
report = engine.generate_report()
```

## System Architecture

### Core Components

1. **`config.py`** - Configuration management and environment variables
2. **`logger.py`** - Centralized logging and audit trail system
3. **`smartsheet_integration.py`** - Smartsheet API operations
4. **`snowflake_integration.py`** - Snowflake database queries
5. **`customer_profile_integration.py`** - Customer Profile API operations
6. **`dispute_automation.py`** - Main orchestration engine

### Processing Workflow

1. **Discovery Phase**
   - Scan Smartsheet for rows with dispute trigger messages
   - Extract client reference numbers
   - Validate row eligibility for processing

2. **Investigation Phase**
   - Query Customer Profile for booking validation
   - Execute Snowflake queries to retrieve booking logs
   - Analyze error patterns and determine error types

3. **Resolution Phase**
   - Generate appropriate status updates and messages
   - Save booking logs to files
   - Update Smartsheet rows with new statuses and attachments
   - Handle special rebooking scenarios

4. **Audit Phase**
   - Log all actions taken
   - Generate processing summaries
   - Maintain audit trails for compliance

## Error Handling

The system includes comprehensive error handling for:

- **Network connectivity issues** with automatic retries
- **API rate limiting** with appropriate delays
- **Authentication failures** with clear error messages
- **Data validation errors** with detailed logging
- **System timeouts** with graceful recovery

## Logging and Monitoring

### Log Files

- **`dispute_automation.log`** - Main application log
- **`audit_log.txt`** - Audit trail of all actions taken
- **`logs/booking_logs_*.csv`** - Exported booking logs for each processed dispute

### Log Levels

- **INFO** - Normal operations and progress updates
- **WARNING** - Non-critical issues that don't stop processing
- **ERROR** - Errors that prevent processing of individual items
- **DEBUG** - Detailed debugging information (when enabled)

## Security Considerations

- **Environment variables** for all sensitive credentials
- **No hardcoded passwords** or API keys in source code
- **Audit logging** for all system actions
- **Connection cleanup** to prevent resource leaks
- **Input validation** to prevent injection attacks

## Error Types Detected

The system can detect and classify these error types:

- `SUPPLIER_CONFIRMATION_ERROR`
- `CONNECTION_ERROR`
- `TIMEOUT_ERROR`
- `PROVIDER_ERROR`
- `BOOKING_FAILED`
- `BOOKING_INVALID`
- `VALIDATION_ERROR`

## Status Updates Applied

When processing disputes, the system updates:

- **Supplier Comments**: `"in escalation process"` → `"reviewed by ST technical team"`
- **Status**: `"escalation"` → `"will not pay"`
- **Completion**: `"need help"` → `"ready to submit"`
- **Notes**: Custom message based on error type and available logs

## Rebooking Scenarios

For cancellation and rebooking situations, the system:

1. Detects multiple booking events for the same client reference
2. Identifies cancellation and rebooking patterns
3. Generates specialized messaging referencing both bookings
4. Provides context about the cancellation and rebooking timeline

## Troubleshooting

### Common Issues

1. **"Missing required configuration variables"**
   - Check that all required environment variables are set in `.env`

2. **"Failed to connect to [System]"**
   - Verify network connectivity and credentials
   - Check if the service is accessible from your network

3. **"No dispute rows found to process"**
   - Verify Smartsheet contains rows with trigger messages
   - Check that client reference numbers are populated

4. **"Query execution failed"**
   - Verify Snowflake credentials and permissions
   - Check that the database and schema exist

### Debug Mode

Enable debug logging by setting:
```env
LOG_LEVEL=DEBUG
```

### Testing Connection

Test individual system connections:

```python
from smartsheet_integration import SmartsheetIntegration
from snowflake_integration import SnowflakeIntegration  
from customer_profile_integration import CustomerProfileIntegration

# Test each integration
smartsheet = SmartsheetIntegration()
snowflake = SnowflakeIntegration()
cp = CustomerProfileIntegration()
```

## Performance Considerations

- **Batch processing** of multiple rows in a single run
- **Connection pooling** for database operations
- **Rate limiting compliance** for API calls
- **Memory optimization** for large log datasets
- **Parallel processing** where possible

## Compliance and Audit

The system maintains detailed audit logs including:

- **All actions taken** on each dispute
- **Error conditions** and recovery attempts
- **System connections** and status changes
- **Processing timestamps** and durations
- **User attribution** for manual operations

## Support and Maintenance

### Regular Maintenance

1. **Log rotation** to manage disk space
2. **Credential updates** as needed
3. **Dependency updates** for security patches
4. **Configuration reviews** for business rule changes

### Monitoring

Monitor these metrics:
- **Processing success rate**
- **Error frequency by type**
- **Processing time per dispute**
- **System availability**

## Version History

- **v1.0** - Initial release with core automation features
- Support for Smartsheet, Snowflake, and Customer Profile integration
- Comprehensive error detection and handling
- Audit logging and reporting capabilities

## Contact

For questions, issues, or feature requests related to this automation system, please contact the Super.com technical team. 