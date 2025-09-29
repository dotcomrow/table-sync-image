#!/usr/bin/env python3
"""
Validate that all packages from requirements.txt can be imported.
This script is used during Docker build to ensure all dependencies are properly installed.
"""

import re
import importlib
import sys
from pathlib import Path


def main():
    """Validate all importable packages from requirements.txt"""
    
    # Read requirements.txt
    requirements_file = Path(__file__).parent / 'requirements.txt'
    
    try:
        with open(requirements_file, 'r') as f:
            requirements = f.read().strip().split('\n')
    except FileNotFoundError:
        print(f"Error: requirements.txt not found at {requirements_file}")
        sys.exit(1)
    
    # Extract importable package names from requirements
    packages = []
    for req in requirements:
        if req.strip() and not req.startswith('#'):
            # Remove version specifiers and extras
            package_name = re.split(r'[>=<~!]', req.split('[')[0])[0].strip()
            
            # Map package names to their import names
            import_map = {
                'psycopg': 'psycopg',
                'psycopg2-binary': 'psycopg2',
                'google-cloud-bigquery': 'google.cloud.bigquery',
                'google-cloud-storage': 'google.cloud.storage',
                'google-auth': 'google.auth',
                'kafka-python': 'kafka',
                'python-dotenv': 'dotenv',
                'asyncio': None,  # Built-in, skip validation
            }
            
            import_name = import_map.get(package_name, package_name)
            if import_name:
                packages.append((package_name, import_name))
    
    # Validate each package can be imported
    failed_imports = []
    successful_imports = []
    
    print("Validating package imports from requirements.txt...")
    print("-" * 50)
    
    for package_name, import_name in packages:
        try:
            importlib.import_module(import_name)
            print(f"✓ {package_name} -> {import_name}")
            successful_imports.append(package_name)
        except ImportError as e:
            print(f"✗ {package_name} -> {import_name}: {e}")
            failed_imports.append(f'{package_name}: {e}')
    
    print("-" * 50)
    
    if failed_imports:
        print(f"ERROR: Failed to import {len(failed_imports)} packages:")
        for failure in failed_imports:
            print(f"  - {failure}")
        sys.exit(1)
    else:
        print(f"SUCCESS: All {len(successful_imports)} packages imported successfully")
        return 0


if __name__ == "__main__":
    main()