#!/usr/bin/env python3

import os
import hashlib
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

def md5sum(filename):
    """Calculate the MD5 checksum of a file."""
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def verify_md5(file_path, expected_md5):
    """Verify if the MD5 checksum of the file matches the expected checksum."""
    return md5sum(file_path) == expected_md5

def process_md5_file(md5_file_path):
    """Process a single MD5.txt file and verify the checksums of the listed files."""
    results = []
    with open(md5_file_path, 'r') as md5_file:
        for line in md5_file:
            expected_md5, filename = line.strip().split()
            file_path = os.path.join(os.path.dirname(md5_file_path), filename)
            if os.path.exists(file_path):
                if verify_md5(file_path, expected_md5):
                    results.append((file_path, 'OK'))
                else:
                    results.append((file_path, 'MD5 mismatch'))
            else:
                results.append((file_path, 'File not found'))
    return results

def log_results(results):
    """Log the results of MD5 checksum verification."""
    for file_path, status in results:
        if status == 'OK':
            logging.info(f"OK: {file_path}")
        else:
            logging.error(f"FAIL: {file_path} ({status})")

def check_md5_files(root_dir, max_workers=4):
    """Check the integrity of files based on MD5 checksums provided in MD5.txt files."""
    md5_files = []
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            if file == 'MD5.txt':
                md5_files.append(os.path.join(subdir, file))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_md5_file = {executor.submit(process_md5_file, md5_file): md5_file for md5_file in md5_files}
        for future in as_completed(future_to_md5_file):
            try:
                results = future.result()
                log_results(results)
            except Exception as exc:
                logging.error(f"Exception occurred: {exc}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Check MD5 checksums of files.')
    parser.add_argument('root_directory', type=str, help='Root directory to start checking MD5 checksums')
    parser.add_argument('--log', type=str, default='md5_check.log', help='Log file name')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers')

    args = parser.parse_args()

    # Set up logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[
                            logging.FileHandler(args.log, mode='w'),  # ensure it writes to the file
                            logging.StreamHandler()
                        ])

    check_md5_files(args.root_directory, args.workers)
