#!/usr/bin/env python3
import gzip
import shutil

# Create compressed versions of JSON test files
files_to_compress = [
    '/home/runner/work/wvlet/wvlet/spec/basic/person.json',
    '/home/runner/work/wvlet/wvlet/spec/basic/books.json'
]

for json_file in files_to_compress:
    gz_file = json_file + '.gz'
    print(f"Compressing {json_file} to {gz_file}")
    
    with open(json_file, 'rb') as f_in:
        with gzip.open(gz_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    print(f"Created {gz_file}")

print("All files compressed successfully")