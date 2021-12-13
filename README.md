# sqlite3-druid-json-ext
## Overview
Implementation of SQLite virtual table for reading Druid json results.

## Notes
This is a developement proof-of-concept code which is not production ready.

## Building (on Mac)
### Dependencies 
1. Clone the official sqlite repo, from tag version-3.34.0
2. Build base SQLite:
```sh
mkdir bld
cd bld
../sqlite/configure
gcc -Os -I. -DSQLITE_THREADSAFE=0 -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_EXPLAIN_COMMENTS -DHAVE_USLEEP -DHAVE_READLINE shell.c sqlite3.c -ldl -lm -lreadline -lncurses -o sqlite3
```
### Building the extension
```sh
gcc -g -I PATH_TO_ORIGINAL_SQLITE_BLD -fPIC -dynamiclib druid_json.c -o druid_json.dylib
```
* Replace `PATH_TO_ORIGINAL_SQLITE_BLD` with the path for your original SQLite bld directory

## Usage
### Load extension
```sql
.load ./druid_json
```

### Map druid results
Option #1 - All columns are treated as strings
```sql
CREATE VIRTUAL TABLE temp.my_druid_result USING druid_json(filename=FILENAME);
```

Option #2 - Provide a list of metrics to treat them as FLOATs
```sql
CREATE VIRTUAL TABLE temp.my_druid_result USING druid_json(
      filename = "../raw_result.json",
      metrics = "clicks,impressions,cost"
);
```

### Loading in Python
```python
import sqlite3
con = sqlite3.connect(db_file)
con.enable_load_extension(True)
con.load_extension("PATH/TO/druid_json")
```
