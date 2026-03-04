#!/usr/bin/env python3
"""
TTL File Quality Checker for Virtuoso Compatibility
Checks Turtle files for common issues before loading into Virtuoso
"""

import re
import sys
import os
from pathlib import Path
from collections import Counter
import argparse
from typing import List, Tuple, Set

def check_file_size(filepath: str) -> Tuple[bool, str]:
    """Check if file size is reasonable"""
    size = os.path.getsize(filepath)
    size_mb = size / (1024 * 1024)
    
    if size_mb > 1000:  # > 1GB
        return False, f"⚠️  Very large file: {size_mb:.2f} MB - may take time to load"
    elif size_mb > 100:
        return True, f"📦 Large file: {size_mb:.2f} MB"
    else:
        return True, f"✅ Size: {size_mb:.2f} MB"

def check_encoding(filepath: str) -> Tuple[bool, str]:
    """Check if file is UTF-8 encoded"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            f.read(10000)  # Read first 10k chars
        return True, "✅ UTF-8 encoding OK"
    except UnicodeDecodeError:
        return False, "❌ Not UTF-8 encoded - Virtuoso expects UTF-8"

def check_basic_syntax(filepath: str, sample_size: int = 100) -> Tuple[bool, List[str], Set[str]]:
    """
    Check basic TTL syntax patterns
    Returns: (is_valid, issues_found, prefixes_found)
    """
    issues = []
    prefixes = set()
    line_num = 0
    in_subject = False
    open_quotes = False
    open_brackets = 0
    open_parentheses = 0
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= sample_size * 10:  # Don't check entire huge file
                    break
                line_num = i + 1
                line = line.strip()
                
                if not line or line.startswith('#'):
                    continue
                
                # Track brackets and quotes
                open_brackets += line.count('[') - line.count(']')
                open_parentheses += line.count('(') - line.count(')')
                
                # Check for unbalanced quotes (simplified)
                quote_count = line.count('"')
                if quote_count % 2 != 0:
                    open_quotes = not open_quotes
                
                # Extract prefixes
                if line.startswith('@prefix'):
                    parts = line.split()
                    if len(parts) >= 2:
                        prefix = parts[1].rstrip(':')
                        prefixes.add(prefix)
                
                # Check for common syntax errors
                if '""' in line and '"""' not in line:
                    issues.append(f"Line {line_num}: Possible empty string")
                
                if line.endswith(('a', ';', ',', '.')):
                    if len(line) > 1 and line[-2].isalpha() and line[-1] in '.;,':
                        issues.append(f"Line {line_num}: Missing space before punctuation")
                
                # Check for invalid characters
                invalid_chars = re.findall(r'[^\x00-\x7F]', line)
                if invalid_chars:
                    issues.append(f"Line {line_num}: Non-ASCII characters: {set(invalid_chars)}")
    
    except Exception as e:
        issues.append(f"Error reading file: {str(e)}")
        return False, issues, prefixes
    
    # Check balance at end
    if open_brackets != 0:
        issues.append(f"Unbalanced brackets: [{open_brackets}]")
    if open_parentheses != 0:
        issues.append(f"Unbalanced parentheses: ({open_parentheses})")
    if open_quotes:
        issues.append("Unbalanced quotes")
    
    return len(issues) == 0, issues, prefixes

def check_virtuoso_specific(filepath: str) -> List[str]:
    """Check for Virtuoso-specific issues"""
    issues = []
    long_lines = 0
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                # Virtuoso has line length limits
                if len(line) > 32000:
                    long_lines += 1
                    if long_lines <= 5:  # Only report first 5
                        issues.append(f"Line {i+1}: Very long line ({len(line)} chars) - may cause Virtuoso issues")
                
                # Check for problematic characters
                if '\x00' in line:
                    issues.append(f"Line {i+1}: Contains null bytes")
                
                # Check for common Virtuoso syntax issues
                if '\\u' in line or '\\U' in line:
                    issues.append(f"Line {i+1}: Contains escaped Unicode - Virtuoso may need special handling")
    
    except Exception as e:
        issues.append(f"Error in Virtuoso check: {str(e)}")
    
    if long_lines > 5:
        issues.append(f"... and {long_lines - 5} more very long lines")
    
    return issues

def check_blank_nodes(filepath: str, sample_size: int = 1000) -> Tuple[int, List[str]]:
    """Check for blank nodes which can cause issues"""
    blank_count = 0
    examples = []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= sample_size:
                    break
                if '[]' in line or '_:b' in line or re.search(r'_:genid\d+', line):
                    blank_count += 1
                    if len(examples) < 5:
                        examples.append(line.strip())
    
    except Exception as e:
        print(f"Error checking blank nodes: {e}")
    
    return blank_count, examples

def generate_report(filepath: str, verbose: bool = False):
    """Generate comprehensive quality report"""
    print(f"\n{'='*60}")
    print(f"📊 QUALITY REPORT: {Path(filepath).name}")
    print(f"{'='*60}")
    
    # Check 1: File exists and size
    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        return
    
    size_ok, size_msg = check_file_size(filepath)
    print(f"\n📁 FILE INFO:")
    print(f"  {size_msg}")
    
    # Check 2: Encoding
    enc_ok, enc_msg = check_encoding(filepath)
    print(f"  {enc_msg}")
    
    # Check 3: Basic syntax
    print(f"\n🔍 SYNTAX CHECK:")
    syntax_ok, syntax_issues, prefixes = check_basic_syntax(filepath)
    
    if prefixes:
        print(f"  📌 Prefixes found: {', '.join(sorted(prefixes))}")
    
    if syntax_issues:
        print(f"  ⚠️  Found {len(syntax_issues)} potential issues:")
        for issue in syntax_issues[:10]:  # Show first 10
            print(f"    • {issue}")
        if len(syntax_issues) > 10:
            print(f"    • ... and {len(syntax_issues) - 10} more")
    else:
        print("  ✅ No basic syntax errors found in sample")
    
    # Check 4: Virtuoso-specific
    print(f"\n🎯 VIRTUOSO CHECKS:")
    virtuoso_issues = check_virtuoso_specific(filepath)
    if virtuoso_issues:
        for issue in virtuoso_issues:
            print(f"  ⚠️  {issue}")
    else:
        print("  ✅ No Virtuoso-specific issues found")
    
    # Check 5: Blank nodes
    blank_count, blank_examples = check_blank_nodes(filepath)
    if blank_count > 0:
        print(f"\n🔷 BLANK NODES:")
        print(f"  Found {blank_count} blank nodes in sample")
        if blank_examples:
            print("  Examples:")
            for ex in blank_examples[:3]:
                print(f"    • {ex}")
        if blank_count > 100:
            print("  ⚠️  Many blank nodes - may cause JOIN issues in Virtuoso")
    
    # Summary
    print(f"\n{'='*60}")
    if not syntax_ok or virtuoso_issues:
        print("🔴 RECOMMENDATION: Review issues before loading")
    else:
        print("🟢 RECOMMENDATION: File looks good for loading")
    print(f"{'='*60}\n")

def check_all_ttl_files(directory: str = ".", pattern: str = "*.ttl"):
    """Check all TTL files in directory"""
    ttl_files = list(Path(directory).glob(pattern))
    
    if not ttl_files:
        print("No TTL files found")
        return
    
    print(f"\nFound {len(ttl_files)} TTL files to check")
    
    summary = []
    for ttl_file in ttl_files:
        if ttl_file.name in ['.gitignore']:  # Skip any non-TTL files that match pattern
            continue
        generate_report(str(ttl_file))
        summary.append((ttl_file.name, os.path.getsize(ttl_file)))
    
    # Print summary table
    print("\n📋 SUMMARY TABLE")
    print("-" * 60)
    print(f"{'File':<30} {'Size (MB)':>10} {'Status':>15}")
    print("-" * 60)
    for name, size in sorted(summary):
        size_mb = size / (1024 * 1024)
        status = "✅" if size_mb < 100 else "⚠️"
        print(f"{name[:30]:<30} {size_mb:>10.2f} {status:>15}")

def main():
    parser = argparse.ArgumentParser(description='Validate TTL files for Virtuoso compatibility')
    parser.add_argument('files', nargs='*', help='Specific TTL files to check')
    parser.add_argument('--dir', '-d', default='.', help='Directory to scan for TTL files')
    parser.add_argument('--pattern', '-p', default='*.ttl', help='File pattern (default: *.ttl)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.files:
        for file in args.files:
            if os.path.exists(file):
                generate_report(file, args.verbose)
            else:
                print(f"File not found: {file}")
    else:
        check_all_ttl_files(args.dir, args.pattern)

if __name__ == "__main__":
    main()