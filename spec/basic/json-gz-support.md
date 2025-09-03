# JSON.gz File Support

This directory demonstrates support for loading compressed JSON files in Wvlet.

## Features

- **Gzip Compression Support**: Load `.json.gz` files directly using `from 'data.json.gz'`
- **Automatic Decompression**: Files are decompressed automatically during loading
- **Schema Inference**: Same schema inference as regular JSON files
- **Cross-Platform**: Supported on JVM platform, with appropriate error messages on JS/Native

## Usage

```wv
from 'data.json.gz'
```

## Platform Support

- **JVM**: Full support with java.util.zip.GZIPInputStream
- **Scala.js**: Not supported (browser limitation)
- **Scala Native**: Planned for future implementation

## Examples

The implementation works with the same JSON data as regular JSON files, but compressed:

```json
[
  {"id":1, "name": "alice", "age": 10 },
  {"id":2, "name": "bob", "age": 24 },
  {"id":3, "name": "clark", "age": 40 }
]
```

Can be compressed to `data.json.gz` and loaded directly in Wvlet queries.

## Technical Implementation

- **TypeResolver**: Extended to recognize `.json.gz` file extensions
- **JSONAnalyzer**: New `analyzeJSONGzFile()` method for gzip decompression
- **IOCompat**: Platform-specific `readAsGzString()` implementations