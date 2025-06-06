#!/usr/bin/env python3
"""
Simple run script for the Super.com Dispute Automation System
Provides easy command-line interface with options
"""

import argparse
import sys
import json
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(
        description='Super.com Automated Dispute Handling System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_automation.py                          # Run full automation
  python run_automation.py --client CLIENT_123     # Process single client
  python run_automation.py --report               # Generate system report
  python run_automation.py --test                 # Test connections only
        """
    )
    
    parser.add_argument(
        '--client', 
        type=str, 
        help='Process a single client reference ID'
    )
    
    parser.add_argument(
        '--report', 
        action='store_true',
        help='Generate and display system status report'
    )
    
    parser.add_argument(
        '--test', 
        action='store_true',
        help='Test system connections without processing'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    # Import after argument parsing to show help even if imports fail
    try:
        from dispute_automation import DisputeAutomationEngine
        from logger import dispute_logger
    except ImportError as e:
        print(f"Error importing modules: {e}")
        print("Make sure all dependencies are installed: pip install -r requirements.txt")
        sys.exit(1)
    except Exception as e:
        print(f"Error initializing system: {e}")
        print("Check your .env file and configuration")
        sys.exit(1)
    
    print("=" * 60)
    print("Super.com Automated Dispute Handling System")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        # Initialize the automation engine
        if args.verbose:
            print("Initializing automation engine...")
        
        engine = DisputeAutomationEngine()
        
        if args.test:
            # Test connections only
            print("Testing system connections...")
            report = engine.generate_report()
            
            print("\nSystem Status:")
            for system, status in report['system_status'].items():
                status_icon = "✓" if status == "connected" else "✗"
                print(f"  {status_icon} {system.title()}: {status}")
            
            if all(status == "connected" for status in report['system_status'].values()):
                print("\n✓ All systems connected successfully!")
                sys.exit(0)
            else:
                print("\n✗ Some systems failed to connect. Check your configuration.")
                sys.exit(1)
        
        elif args.report:
            # Generate status report
            print("Generating system status report...")
            report = engine.generate_report()
            
            print("\n" + "=" * 40)
            print("SYSTEM STATUS REPORT")
            print("=" * 40)
            print(json.dumps(report, indent=2, default=str))
        
        elif args.client:
            # Process single client reference
            print(f"Processing single client reference: {args.client}")
            result = engine.process_single_client_reference(args.client)
            
            print("\n" + "=" * 40)
            print("PROCESSING RESULT")
            print("=" * 40)
            print(json.dumps(result, indent=2, default=str))
            
            if result.get('success'):
                print(f"\n✓ Successfully processed client reference: {args.client}")
            else:
                print(f"\n✗ Failed to process client reference: {args.client}")
                if 'error' in result:
                    print(f"Error: {result['error']}")
        
        else:
            # Run full automation
            print("Starting full automation process...")
            if args.verbose:
                print("This will process all eligible dispute rows in Smartsheet.")
            
            summary = engine.run_automation()
            
            print("\n" + "=" * 60)
            print("AUTOMATION SUMMARY")
            print("=" * 60)
            print(json.dumps(summary, indent=2, default=str))
            
            stats = summary.get('statistics', {})
            total = stats.get('total_rows', 0)
            processed = stats.get('processed', 0)
            updated = stats.get('updated', 0)
            errors = stats.get('errors', 0)
            
            print(f"\n📊 Summary:")
            print(f"   Total rows found: {total}")
            print(f"   Successfully processed: {processed}")
            print(f"   Successfully updated: {updated}")
            print(f"   Errors encountered: {errors}")
            
            if errors == 0 and updated > 0:
                print(f"\n✓ Automation completed successfully!")
            elif errors > 0:
                print(f"\n⚠ Automation completed with {errors} errors. Check logs for details.")
            else:
                print(f"\n ℹ No rows were eligible for processing.")
        
        print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except KeyboardInterrupt:
        print("\n\nAutomation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nCritical error: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 