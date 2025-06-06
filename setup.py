#!/usr/bin/env python3
"""
Setup script for Super.com Dispute Automation System
Helps with initial installation and configuration
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

def print_header(title):
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)

def print_step(step, description):
    print(f"\n[{step}] {description}")

def check_python_version():
    """Check if Python version is 3.8 or higher"""
    print_step("1", "Checking Python version...")
    
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print(f"❌ Python {version.major}.{version.minor} detected. Python 3.8+ is required.")
        return False
    
    print(f"✅ Python {version.major}.{version.minor}.{version.micro} detected.")
    return True

def install_dependencies():
    """Install required Python packages"""
    print_step("2", "Installing dependencies...")
    
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Dependencies installed successfully.")
        return True
    except subprocess.CalledProcessError:
        print("❌ Failed to install dependencies.")
        print("Try running manually: pip install -r requirements.txt")
        return False

def create_directories():
    """Create necessary directories"""
    print_step("3", "Creating directories...")
    
    directories = ["logs"]
    
    for directory in directories:
        try:
            Path(directory).mkdir(exist_ok=True)
            print(f"✅ Created directory: {directory}/")
        except Exception as e:
            print(f"❌ Failed to create directory {directory}: {e}")
            return False
    
    return True

def setup_environment_file():
    """Setup environment configuration file"""
    print_step("4", "Setting up environment configuration...")
    
    env_file = Path(".env")
    sample_file = Path("sample_environment.txt")
    
    if env_file.exists():
        print("⚠️  .env file already exists. Skipping creation.")
        print("   If you need to reconfigure, delete .env and run setup again.")
        return True
    
    if not sample_file.exists():
        print("❌ sample_environment.txt not found.")
        return False
    
    try:
        shutil.copy(sample_file, env_file)
        print("✅ Created .env file from template.")
        print("⚠️  IMPORTANT: Edit .env file with your actual credentials before running the automation.")
        return True
    except Exception as e:
        print(f"❌ Failed to create .env file: {e}")
        return False

def verify_files():
    """Verify all required files are present"""
    print_step("5", "Verifying installation...")
    
    required_files = [
        "config.py",
        "logger.py", 
        "smartsheet_integration.py",
        "snowflake_integration.py",
        "customer_profile_integration.py",
        "dispute_automation.py",
        "run_automation.py",
        "requirements.txt",
        ".env"
    ]
    
    missing_files = []
    for file in required_files:
        if not Path(file).exists():
            missing_files.append(file)
    
    if missing_files:
        print("❌ Missing required files:")
        for file in missing_files:
            print(f"   - {file}")
        return False
    
    print("✅ All required files are present.")
    return True

def test_imports():
    """Test if all modules can be imported"""
    print_step("6", "Testing module imports...")
    
    modules = [
        "smartsheet",
        "snowflake.connector", 
        "pandas",
        "requests",
        "dotenv"
    ]
    
    failed_imports = []
    for module in modules:
        try:
            __import__(module)
            print(f"✅ {module}")
        except ImportError:
            print(f"❌ {module}")
            failed_imports.append(module)
    
    if failed_imports:
        print("\n❌ Some dependencies failed to import. Try reinstalling:")
        print("   pip install -r requirements.txt")
        return False
    
    return True

def print_next_steps():
    """Print next steps for the user"""
    print_header("SETUP COMPLETE")
    
    print("🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Edit the .env file with your actual credentials:")
    print("   - Smartsheet access token and sheet ID")
    print("   - Snowflake account, username, and password")
    print("   - Customer Profile API key")
    print()
    print("2. Test your configuration:")
    print("   python run_automation.py --test")
    print()
    print("3. Process a single client reference for testing:")
    print("   python run_automation.py --client YOUR_CLIENT_REF")
    print()
    print("4. Run the full automation:")
    print("   python run_automation.py")
    print()
    print("5. For more options:")
    print("   python run_automation.py --help")

def main():
    """Main setup function"""
    print_header("Super.com Dispute Automation System Setup")
    
    print("This script will help you set up the automated dispute handling system.")
    print("It will install dependencies, create directories, and configure the environment.")
    
    # Check if user wants to proceed
    response = input("\nDo you want to proceed with setup? (y/N): ").strip().lower()
    if response not in ['y', 'yes']:
        print("Setup cancelled.")
        sys.exit(0)
    
    success = True
    
    # Run setup steps
    success &= check_python_version()
    success &= install_dependencies()
    success &= create_directories()
    success &= setup_environment_file()
    success &= verify_files()
    success &= test_imports()
    
    if success:
        print_next_steps()
    else:
        print_header("SETUP FAILED")
        print("❌ Setup encountered errors. Please review the messages above and try again.")
        sys.exit(1)

if __name__ == "__main__":
    main() 