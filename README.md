# Admintool

A command-line administration and debugging tool for TidesDB databases. The admintool provides interactive and scripted access to database operations, SSTable/WAL inspection, and integrity verification.

## Features

- Database Management - Open, close, and inspect TidesDB databases
- Column Family Operations - Create, drop, list, and get statistics for column families
- Key-Value Operations - Put, get, delete, scan, range queries, and prefix searches
- SSTable Analysis - List, inspect, dump entries, show statistics, and list keys from SSTable files
- WAL Analysis - List, inspect, dump entries, and verify integrity of WAL files
- Maintenance - Trigger compaction, flush memtables, verify integrity

## Building

The admintool is built as part of the TidesDB project:

```bash
cd tidesdb
mkdir build && cd build
cmake ..
make admintool
```

The binary will be located at `build/admintool/admintool`.

## Usage

### Interactive Mode

```bash
./admintool
```

This starts an interactive shell where you can enter commands.

### Command-Line Options

```bash
./admintool [options]

Options:
  -h, --help              Show help message
  -v, --version           Show version
  -d, --directory <path>  Open database at path on startup
  -c, --command <cmd>     Execute command and exit
```

### Examples

```bash
# Open database and enter interactive mode
./admintool -d /path/to/mydb

# Execute a single command
./admintool -d /path/to/mydb -c "cf-list"

# Dump SSTable without opening database
./admintool -c "sstable-dump /path/to/mydb/mycf/L1_1.klog 100"
```

## Commands Reference

### Database Commands

| Command | Description |
|---------|-------------|
| `open <path>` | Open or create a database at the specified path |
| `close` | Close the currently open database |
| `info` | Show database information including column families and cache stats |

**Example:**
```
admintool> open /tmp/testdb
Opened database at '/tmp/testdb'

admintool(/tmp/testdb)> info
Database Information:
  Path: /tmp/testdb
  Column Families: 2
    - default
    - users
  Block Cache:
    Enabled: yes
    Entries: 128
    Size: 4194304 bytes
    Hits: 1024
    Misses: 256
    Hit Rate: 80.00%
```

### Column Family Commands

| Command | Description |
|---------|-------------|
| `cf-list` | List all column families |
| `cf-create <name>` | Create a column family with default configuration |
| `cf-drop <name>` | Drop a column family |
| `cf-rename <old> <new>` | Rename a column family |
| `cf-stats <name>` | Show detailed statistics for a column family |
| `cf-status <name>` | Show flush/compaction status for a column family |

**Example:**
```
admintool(/tmp/testdb)> cf-create users
Created column family 'users'

admintool(/tmp/testdb)> cf-rename users customers
Renamed column family 'users' to 'customers'

admintool(/tmp/testdb)> cf-status customers
Column Family Status: customers
  Flushing: no
  Compacting: yes

admintool(/tmp/testdb)> cf-stats customers
Column Family: customers
  Memtable Size: 1048576 bytes
  Levels: 5
  Total Keys: 50000
  Total Data Size: 134217728 bytes (128.00 MB)
  Avg Key Size: 12.5 bytes
  Avg Value Size: 256.0 bytes
  Read Amplification: 2.50
  Cache Hit Rate: 85.00%
  Configuration:
    Write Buffer Size: 67108864 bytes
    Level Size Ratio: 10
    Min Levels: 5
    Dividing Level Offset: 2
    Compression: lz4
    Bloom Filter: enabled (FPR: 0.0100)
    Block Indexes: enabled (sample ratio: 1, prefix len: 16)
    Sync Mode: interval (interval: 128000 us)
    KLog Value Threshold: 512 bytes
    Min Disk Space: 104857600 bytes
    L1 File Count Trigger: 4
    L0 Queue Stall Threshold: 20
    Default Isolation Level: read_committed
    Skip List Max Level: 12
    Skip List Probability: 0.25
    Comparator: memcmp
  Level 1: 2 SSTables, 134217728 bytes, 25000 keys
  Level 2: 0 SSTables, 0 bytes, 0 keys
  ...
```

### Key-Value Commands

| Command | Description |
|---------|-------------|
| `put <cf> <key> <value>` | Insert or update a key-value pair |
| `get <cf> <key>` | Retrieve a value by key |
| `delete <cf> <key>` | Delete a key |
| `scan <cf> [limit]` | Scan all keys (default limit: 100) |
| `range <cf> <start> <end> [limit]` | Scan keys in a range (inclusive) |
| `prefix <cf> <prefix> [limit]` | Scan keys with a given prefix |

**Examples**
```
admintool(/tmp/testdb)> put users user:1001 '{"name":"Alice","age":30}'
OK

admintool(/tmp/testdb)> get users user:1001
{"name":"Alice","age":30}

admintool(/tmp/testdb)> put users user:1002 '{"name":"Bob","age":25}'
OK

admintool(/tmp/testdb)> put users user:1003 '{"name":"Charlie","age":35}'
OK

admintool(/tmp/testdb)> range users user:1001 user:1002
1) "user:1001" -> "{"name":"Alice","age":30}"
2) "user:1002" -> "{"name":"Bob","age":25}"
(2 entries in range)

admintool(/tmp/testdb)> prefix users user:
1) "user:1001" -> "{"name":"Alice","age":30}"
2) "user:1002" -> "{"name":"Bob","age":25}"
3) "user:1003" -> "{"name":"Charlie","age":35}"
(3 entries with prefix)

admintool(/tmp/testdb)> delete users user:1002
OK
```

### SSTable Analysis Commands

| Command | Description |
|---------|-------------|
| `sstable-list <cf>` | List all SSTable files in a column family |
| `sstable-info <path>` | Show basic information about an SSTable file |
| `sstable-dump <path> [limit]` | Dump SSTable entries (default limit: 1000) |
| `sstable-dump-full <klog> [vlog] [limit]` | Dump entries with vlog value retrieval and checksum info |
| `sstable-stats <path>` | Show detailed statistics for an SSTable |
| `sstable-keys <path> [limit]` | List only keys from an SSTable |
| `sstable-checksum <path>` | Verify all block checksums (xxHash32) |
| `bloom-stats <path>` | Show bloom filter statistics (size, fill ratio, estimated FPR) |

**Examples**
```
admintool(/tmp/testdb)> sstable-list users
SSTables in 'users':
  L1_1.klog (2097152 bytes)
  L1_2.klog (1048576 bytes)
(2 SSTables)

admintool(/tmp/testdb)> sstable-info /tmp/testdb/users/L1_1.klog
SSTable: /tmp/testdb/users/L1_1.klog
  File Size: 2097152 bytes
  Block Count: 64
  Last Modified: 1736560000
  First Block Size: 32768 bytes
  Last Block Size (metadata): 4096 bytes

admintool(/tmp/testdb)> sstable-dump /tmp/testdb/users/L1_1.klog 5
SSTable Entries (limit: 5):
1) [blk:0] seq=1 key="user:1001" value="{"name":"Alice","age":30}"
2) [blk:0] seq=3 key="user:1003" value="{"name":"Charlie","age":35}"
3) [blk:0] [DEL] seq=4 key="user:1002"
4) [blk:1] seq=5 key="user:1004" value="{"name":"Diana","age":28}"
5) [blk:1] [TTL:1736646400] seq=6 key="session:abc123" value="token_data"

(5 entries dumped from 2 blocks)

admintool(/tmp/testdb)> sstable-stats /tmp/testdb/users/L1_1.klog
SSTable Statistics: /tmp/testdb/users/L1_1.klog
  File Size: 2097152 bytes (2.00 MB)
  Block Count: 64
  Total Entries: 10000
  Tombstones: 150 (1.5%)
  TTL Entries: 500
  VLog References: 25
  Sequence Range: 1 - 10500
  Key Sizes: min=8 max=64 avg=12.5
  Value Sizes: min=0 max=4096 avg=128.3

admintool(/tmp/testdb)> sstable-keys /tmp/testdb/users/L1_1.klog 5
SSTable Keys (limit: 5):
1) "user:1001"
2) "user:1002" [DEL]
3) "user:1003"
4) "user:1004"
5) "user:1005"

(5 keys listed)
Key Range: "user:1001" to "user:1005"

admintool(/tmp/testdb)> sstable-checksum /tmp/testdb/users/L1_1.klog
Verifying checksums: /tmp/testdb/users/L1_1.klog
  File Size: 2097152 bytes

Checksum Verification Results:
  Total Blocks: 64
  Valid: 64
  Invalid: 0
  Status: OK

admintool(/tmp/testdb)> sstable-dump-full /tmp/testdb/users/L1_1.klog /tmp/testdb/users/L1_1.vlog 3
SSTable Full Dump (limit: 3):
  KLog: /tmp/testdb/users/L1_1.klog
  VLog: /tmp/testdb/users/L1_1.vlog

1) [blk:0] seq=1 key="user:1001" value="{"name":"Alice","age":30}"
2) [blk:0] [VLOG:8192] seq=2 key="user:large" value="This is a large value retrieved from vlog..."
3) [blk:0] [DEL] seq=3 key="user:deleted"

(3 entries from 1 blocks)

admintool(/tmp/testdb)> bloom-stats /tmp/testdb/users/L1_1.klog
Bloom Filter Statistics: /tmp/testdb/users/L1_1.klog
  Serialized Size: 4096 bytes
  Filter Size (m): 95850 bits (11.70 KB)
  Hash Functions (k): 7
  Storage Words: 1498 (uint64_t)
  Bits Set: 47925
  Fill Ratio: 50.00%
  Estimated FPR: 0.007813 (0.7813%)
```

### WAL Analysis Commands

| Command | Description |
|---------|-------------|
| `wal-list <cf>` | List all WAL files in a column family |
| `wal-info <path>` | Show basic information about a WAL file |
| `wal-dump <path> [limit]` | Dump WAL entries (default limit: 1000) |
| `wal-verify <path>` | Verify WAL integrity and report corruption |
| `wal-checksum <path>` | Verify all block checksums (xxHash32) |

**Examples**
```
admintool(/tmp/testdb)> wal-list users
WAL files in 'users':
  wal_0.log (524288 bytes)
(1 WAL files)

admintool(/tmp/testdb)> wal-info /tmp/testdb/users/wal_0.log
WAL: /tmp/testdb/users/wal_0.log
  File Size: 524288 bytes
  Block Count (entries): 1000
  Last Modified: 1736560500

admintool(/tmp/testdb)> wal-dump /tmp/testdb/users/wal_0.log 5
WAL Entries (limit: 5):
1) [PUT] seq=10501 key="user:2001" value="{"name":"Eve","age":22}"
2) [PUT] seq=10502 key="user:2002" value="{"name":"Frank","age":40}"
3) [DELETE] seq=10503 key="user:1005"
4) [PUT] [TTL:1736646400] seq=10504 key="cache:temp" value="temporary_data"
5) [PUT] seq=10505 key="user:2003" value="{"name":"Grace","age":33}"

(5 WAL entries dumped)

admintool(/tmp/testdb)> wal-verify /tmp/testdb/users/wal_0.log
Verifying WAL: /tmp/testdb/users/wal_0.log
  File Size: 524288 bytes
  Valid Entries: 1000
  Corrupted Entries: 0
  Sequence Range: 10501 - 11500
  Last Valid Position: 524200
  Status: OK

admintool(/tmp/testdb)> wal-checksum /tmp/testdb/users/wal_0.log
Verifying checksums: /tmp/testdb/users/wal_0.log
  File Size: 524288 bytes

Checksum Verification Results:
  Total Blocks: 1000
  Valid: 1000
  Invalid: 0
  Status: OK
```

### Level and Verification Commands

| Command | Description |
|---------|-------------|
| `level-info <cf>` | Show per-level SSTable details |
| `verify <cf>` | Verify integrity of all files in a column family |

**Examples**
```
admintool(/tmp/testdb)> level-info users
Level Information for 'users':
  Memtable Size: 2097152 bytes (2.00 MB)
  Number of Levels: 5

  Level 1:
    SSTables: 4
    Size: 268435456 bytes (256.00 MB)
  Level 2:
    SSTables: 2
    Size: 536870912 bytes (512.00 MB)
  Level 3:
    SSTables: 1
    Size: 1073741824 bytes (1024.00 MB)
  Level 4:
    SSTables: 0
    Size: 0 bytes (0.00 MB)
  Level 5:
    SSTables: 0
    Size: 0 bytes (0.00 MB)

  Total SSTables: 7
  Total Disk Size: 1879048192 bytes (1792.00 MB)

admintool(/tmp/testdb)> verify users
Verifying column family 'users'...

Verification Results:
  SSTables: 7 total, 7 valid, 0 invalid
  WAL Files: 1 total, 1 valid, 0 invalid
  Status: OK
```

### Maintenance Commands

| Command | Description |
|---------|-------------|
| `compact <cf>` | Trigger compaction for a column family |
| `flush <cf>` | Flush memtable to disk |
| `backup <path>` | Create a database backup at the specified path |

**Examples**
```
admintool(/tmp/testdb)> flush users
Memtable flushed for 'users'

admintool(/tmp/testdb)> compact users
Compaction triggered for 'users'

admintool(/tmp/testdb)> backup /tmp/testdb_backup
Creating backup at '/tmp/testdb_backup'...
Backup completed successfully.
```

### Other Commands

| Command | Description                    |
|---------|--------------------------------|
| `version` | Show TidesDB version installed |
| `help` | Show help message              |
| `quit` / `exit` | Exit the admintool             |

**Example:**
```
admintool> version
TidesDB version 7.4.0
```

## Large File Handling

When working with large SSTable or WAL files (>100 MB), the admintool automatically:
- Displays a warning about file size
- Applies the default limit (1000 entries) to prevent excessive output
- Allows explicit limit override via command argument

```
admintool> sstable-dump /path/to/large_sstable.klog
Warning: Large file (512 MB). Limiting to 1000 entries. Use explicit limit to override.
SSTable Entries (limit: 1000):
...
```

## Entry Flags

When dumping SSTable or WAL entries, the following flags may appear:

| Flag | Description |
|------|-------------|
| `[DEL]` | Tombstone (deleted key) |
| `[TTL:timestamp]` | Entry has time-to-live expiration |
| `[VLOG:offset]` | Value stored in separate vlog file |
| `[blk:N]` | Block number within SSTable |
| `CHECKSUM_ERR` | Block checksum verification failed (in `sstable-dump-full`) |
| `READ_ERR` | Failed to read vlog value |
| `NO_VLOG_FILE` | Vlog path not provided for entry with vlog reference |

## Checksum Verification

TidesDB uses xxHash32 checksums for all blocks in SSTable (klog) and WAL files. The block format is:

```
[size: 4 bytes][checksum: 4 bytes][data: variable][footer_size: 4 bytes][magic: 4 bytes]
```

The `sstable-checksum` and `wal-checksum` commands verify each block by:
1. Reading the stored checksum from the block header
2. Computing xxHash32 over the block data
3. Comparing stored vs computed checksums

When corruption is detected:
```
admintool> sstable-checksum /path/to/corrupted.klog
Verifying checksums: /path/to/corrupted.klog
  File Size: 1048576 bytes

  Block 42 @ offset 524288: CHECKSUM MISMATCH
    Size: 8192 bytes
    Stored:   0x1A2B3C4D
    Computed: 0x5E6F7A8B

Checksum Verification Results:
  Total Blocks: 64
  Valid: 63
  Invalid: 1
  Status: CORRUPTED
```

## VLog Value Retrieval

When values exceed the configured threshold, TidesDB stores them in a separate vlog file. The `sstable-dump-full` command can retrieve these values:

```bash
# Dump with vlog value retrieval
./admintool -c "sstable-dump-full /path/cf/L1_1.klog /path/cf/L1_1.vlog 10"
```

The vlog offset stored in the klog entry points to the block header in the vlog file. The admintool reads the block, verifies its checksum, and displays the value.

## Use Cases

### Debugging Data Issues
```bash
# Find all keys with a specific prefix
./admintool -d /path/to/db -c "prefix mycf error:"

# Check if a specific key exists
./admintool -d /path/to/db -c "get mycf problematic_key"
```

### Analyzing SSTable Contents
```bash
# Get statistics without opening the database
./admintool -c "sstable-stats /path/to/db/cf/L1_1.klog"

# List all keys to understand data distribution
./admintool -c "sstable-keys /path/to/db/cf/L1_1.klog 10000"
```

### Recovery and Verification
```bash
# Verify WAL integrity after crash
./admintool -c "wal-verify /path/to/db/cf/wal_*.log"

# Check all files in a column family
./admintool -d /path/to/db -c "verify mycf"
```

### Monitoring and Maintenance
```bash
# Check level distribution
./admintool -d /path/to/db -c "level-info mycf"

# Trigger manual compaction
./admintool -d /path/to/db -c "compact mycf"
```

