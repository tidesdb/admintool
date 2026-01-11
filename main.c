/**
 *
 * Copyright (C) TidesDB
 *
 * Original Author: Alex Gaetano Padula
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <tidesdb/block_manager.h>
#include <tidesdb/bloom_filter.h>
#include <tidesdb/compat.h>
#include <tidesdb/compress.h>
#include <tidesdb/tidesdb.h>
#include <tidesdb/tidesdb_version.h>
#include <tidesdb/xxhash.h>

#if defined(_MSC_VER) && !defined(__MINGW32__) && !defined(__MINGW64__)
#ifndef close
#define close _close
#endif
#ifndef fstat
#define fstat _fstat
#endif
#ifndef stat
#define stat _stat
#endif
#endif

#ifndef S_ISREG
#define S_ISREG(m) (((m)&S_IFMT) == S_IFREG)
#endif

#define TDB_KV_FLAG_TOMBSTONE 0x01
#define TDB_KV_FLAG_HAS_TTL 0x02
#define TDB_KV_FLAG_HAS_VLOG 0x04
#define TDB_KV_FLAG_DELTA_SEQ 0x08

#define ADMINTOOL_DEFAULT_DUMP_LIMIT 1000
#define ADMINTOOL_LARGE_FILE_THRESHOLD (100 * 1024 * 1024)

static inline uint32_t compute_block_checksum(const void *data,
                                              const size_t size) {
  return XXH32(data, size, 0);
}

#define ADMINTOOL_MAX_INPUT 4096
#define ADMINTOOL_MAX_ARGS 64
#define ADMINTOOL_PROMPT "admintool> "
#define ADMINTOOL_PROMPT_DB "admintool(%s)> "

static tidesdb_t *g_db = NULL;
static char g_db_path[1024] = {0};

static void print_usage(void) {
  printf("Usage: admintool [options]\n\n");
  printf("Options:\n");
  printf("  -h, --help              Show this help message\n");
  printf("  -v, --version           Show version\n");
  printf("  -d, --directory <path>  Open database at path\n");
  printf("  -c, --command <cmd>     Execute command and exit\n\n");
  printf("Interactive Commands:\n");
  printf("  open <path>             Open/create database at path\n");
  printf("  close                   Close current database\n");
  printf("  info                    Show database information\n\n");
  printf("  cf-list                 List all column families\n");
  printf("  cf-create <name>        Create column family with defaults\n");
  printf("  cf-drop <name>          Drop column family\n");
  printf("  cf-stats <name>         Show column family statistics\n\n");
  printf("  put <cf> <key> <value>  Put key-value pair\n");
  printf("  get <cf> <key>          Get value by key\n");
  printf("  delete <cf> <key>       Delete key\n");
  printf("  scan <cf> [limit]       Scan all keys (default limit: 100)\n");
  printf("  range <cf> <start> <end> [limit]  Scan keys in range\n");
  printf("  prefix <cf> <prefix> [limit]      Scan keys with prefix\n\n");
  printf("  sstable-list <cf>       List SSTables in column family\n");
  printf("  sstable-info <path>     Inspect SSTable file\n");
  printf("  sstable-dump <path> [limit]       Dump SSTable entries\n");
  printf("  sstable-dump-full <klog> [vlog] [limit]  Dump with vlog values\n");
  printf("  sstable-stats <path>    Show SSTable statistics\n");
  printf("  sstable-keys <path> [limit]       List SSTable keys only\n");
  printf("  sstable-checksum <path> Verify block checksums\n");
  printf("  bloom-stats <path>      Show bloom filter statistics\n\n");
  printf("  wal-list <cf>           List WAL files in column family\n");
  printf("  wal-info <path>         Inspect WAL file\n");
  printf("  wal-dump <path> [limit] Dump WAL entries\n");
  printf("  wal-verify <path>       Verify WAL integrity\n");
  printf("  wal-checksum <path>     Verify WAL block checksums\n\n");
  printf("  level-info <cf>         Show per-level SSTable details\n");
  printf("  verify <cf>             Verify column family integrity\n\n");
  printf("  compact <cf>            Trigger compaction\n");
  printf("  flush <cf>              Flush memtable to disk\n\n");
  printf("  version                 Show TidesDB version\n");
  printf("  help                    Show this help\n");
  printf("  quit, exit              Exit admintool\n");
}

static char *trim_whitespace(char *str) {
  while (isspace((unsigned char)*str))
    str++;
  if (*str == 0)
    return str;

  char *end = str + strlen(str) - 1;
  while (end > str && isspace((unsigned char)*end))
    end--;
  end[1] = '\0';

  return str;
}

static int parse_args(char *line, char **argv) {
  int argc = 0;
  char *p = line;
  char quote_char = 0;

  while (*p && argc < ADMINTOOL_MAX_ARGS) {
    while (isspace((unsigned char)*p))
      p++;
    if (*p == '\0')
      break;

    if (*p == '"' || *p == '\'') {
      quote_char = *p;
      p++;
      argv[argc++] = p;
      while (*p && !(*p == quote_char && *(p - 1) != '\\')) {
        p++;
      }
      if (*p == quote_char) {
        *p = '\0';
        p++;
      }
    } else {
      argv[argc++] = p;
      while (*p && !isspace((unsigned char)*p)) {
        p++;
      }
      if (*p) {
        *p = '\0';
        p++;
      }
    }
  }
  return argc;
}

static const char *error_to_string(const int err) {
  switch (err) {
  case TDB_SUCCESS:
    return "Success";
  case TDB_ERR_MEMORY:
    return "Memory allocation failed";
  case TDB_ERR_INVALID_ARGS:
    return "Invalid arguments";
  case TDB_ERR_NOT_FOUND:
    return "Not found";
  case TDB_ERR_IO:
    return "I/O error";
  case TDB_ERR_CORRUPTION:
    return "Data corruption";
  case TDB_ERR_EXISTS:
    return "Already exists";
  case TDB_ERR_CONFLICT:
    return "Transaction conflict";
  case TDB_ERR_TOO_LARGE:
    return "Value too large";
  case TDB_ERR_MEMORY_LIMIT:
    return "Memory limit exceeded";
  case TDB_ERR_INVALID_DB:
    return "Invalid database";
  case TDB_ERR_LOCKED:
    return "Database locked";
  default:
    return "Unknown error";
  }
}

static const char *compression_to_string(const int algo) {
  switch (algo) {
  case NO_COMPRESSION:
    return "none";
#ifndef __sun
  case SNAPPY_COMPRESSION:
    return "snappy";
#endif
  case LZ4_COMPRESSION:
    return "lz4";
  case ZSTD_COMPRESSION:
    return "zstd";
  default:
    return "unknown";
  }
}

static const char *sync_mode_to_string(const int mode) {
  switch (mode) {
  case TDB_SYNC_NONE:
    return "none";
  case TDB_SYNC_FULL:
    return "full";
  case TDB_SYNC_INTERVAL:
    return "interval";
  default:
    return "unknown";
  }
}

static int cmd_open(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: open <path>\n");
    return -1;
  }

  if (g_db != NULL) {
    printf("Database already open. Close it first with 'close'.\n");
    return -1;
  }

  tidesdb_config_t config = tidesdb_default_config();
  config.db_path = argv[1];
  config.log_level = TDB_LOG_NONE;

  const int ret = tidesdb_open(&config, &g_db);
  if (ret != TDB_SUCCESS) {
    printf("Failed to open database: %s\n", error_to_string(ret));
    return ret;
  }

  strncpy(g_db_path, argv[1], sizeof(g_db_path) - 1);
  g_db_path[sizeof(g_db_path) - 1] = '\0';
  printf("Opened database at '%s'\n", g_db_path);
  return 0;
}

static int cmd_close(int argc, char **argv) {
  (void)argc;
  (void)argv;

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  const int ret = tidesdb_close(g_db);
  if (ret != TDB_SUCCESS) {
    printf("Failed to close database: %s\n", error_to_string(ret));
    return ret;
  }

  g_db = NULL;
  printf("Database closed.\n");
  g_db_path[0] = '\0';
  return 0;
}

static int cmd_info(int argc, char **argv) {
  (void)argc;
  (void)argv;

  if (g_db == NULL) {
    printf("No database is open. Use 'open <path>' first.\n");
    return -1;
  }

  printf("Database Information:\n");
  printf("  Path: %s\n", g_db_path);

  char **cf_names = NULL;
  int cf_count = 0;
  const int ret = tidesdb_list_column_families(g_db, &cf_names, &cf_count);
  if (ret == TDB_SUCCESS) {
    printf("  Column Families: %d\n", cf_count);
    for (int i = 0; i < cf_count; i++) {
      printf("    - %s\n", cf_names[i]);
      free(cf_names[i]);
    }
    free(cf_names);
  }

  tidesdb_cache_stats_t cache_stats;
  if (tidesdb_get_cache_stats(g_db, &cache_stats) == TDB_SUCCESS) {
    printf("  Block Cache:\n");
    printf("    Enabled: %s\n", cache_stats.enabled ? "yes" : "no");
    if (cache_stats.enabled) {
      printf("    Entries: %zu\n", cache_stats.total_entries);
      printf("    Size: %zu bytes\n", cache_stats.total_bytes);
      printf("    Hits: %" PRIu64 "\n", cache_stats.hits);
      printf("    Misses: %" PRIu64 "\n", cache_stats.misses);
      printf("    Hit Rate: %.2f%%\n", cache_stats.hit_rate * 100.0);
    }
  }

  return 0;
}

static int cmd_cf_list(const int argc, char **argv) {
  (void)argc;
  (void)argv;

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  char **cf_names = NULL;
  int cf_count = 0;
  const int ret = tidesdb_list_column_families(g_db, &cf_names, &cf_count);
  if (ret != TDB_SUCCESS) {
    printf("Failed to list column families: %s\n", error_to_string(ret));
    return ret;
  }

  if (cf_count == 0) {
    printf("No column families found.\n");
  } else {
    printf("Column Families (%d):\n", cf_count);
    for (int i = 0; i < cf_count; i++) {
      printf("  %s\n", cf_names[i]);
      free(cf_names[i]);
    }
  }
  free(cf_names);
  return 0;
}

static int cmd_cf_create(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: cf-create <name>\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  const tidesdb_column_family_config_t config =
      tidesdb_default_column_family_config();
  const int ret = tidesdb_create_column_family(g_db, argv[1], &config);
  if (ret != TDB_SUCCESS) {
    printf("Failed to create column family: %s\n", error_to_string(ret));
    return ret;
  }

  printf("Created column family '%s'\n", argv[1]);
  return 0;
}

static int cmd_cf_drop(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: cf-drop <name>\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  const int ret = tidesdb_drop_column_family(g_db, argv[1]);
  if (ret != TDB_SUCCESS) {
    printf("Failed to drop column family: %s\n", error_to_string(ret));
    return ret;
  }

  printf("Dropped column family '%s'\n", argv[1]);
  return 0;
}

static int cmd_cf_stats(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: cf-stats <name>\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  tidesdb_column_family_t *cf = tidesdb_get_column_family(g_db, argv[1]);
  if (cf == NULL) {
    printf("Column family '%s' not found.\n", argv[1]);
    return -1;
  }

  tidesdb_stats_t *stats = NULL;
  const int ret = tidesdb_get_stats(cf, &stats);
  if (ret != TDB_SUCCESS) {
    printf("Failed to get stats: %s\n", error_to_string(ret));
    return ret;
  }

  printf("Column Family: %s\n", argv[1]);
  printf("  Memtable Size: %zu bytes\n", stats->memtable_size);
  printf("  Levels: %d\n", stats->num_levels);

  if (stats->config) {
    printf("  Configuration:\n");
    printf("    Write Buffer Size: %zu bytes\n",
           stats->config->write_buffer_size);
    printf("    Level Size Ratio: %zu\n", stats->config->level_size_ratio);
    printf("    Min Levels: %d\n", stats->config->min_levels);
    printf("    Compression: %s\n",
           compression_to_string(stats->config->compression_algorithm));
    printf("    Bloom Filter: %s (FPR: %.4f)\n",
           stats->config->enable_bloom_filter ? "enabled" : "disabled",
           stats->config->bloom_fpr);
    printf("    Block Indexes: %s\n",
           stats->config->enable_block_indexes ? "enabled" : "disabled");
    printf("    Sync Mode: %s\n",
           sync_mode_to_string(stats->config->sync_mode));
  }

  for (int i = 0; i < stats->num_levels; i++) {
    printf("  Level %d: %d SSTables, %zu bytes\n", i + 1,
           stats->level_num_sstables[i], stats->level_sizes[i]);
  }

  tidesdb_free_stats(stats);
  return 0;
}

static int cmd_put(const int argc, char **argv) {
  if (argc < 4) {
    printf("Usage: put <cf> <key> <value>\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  tidesdb_column_family_t *cf = tidesdb_get_column_family(g_db, argv[1]);
  if (cf == NULL) {
    printf("Column family '%s' not found.\n", argv[1]);
    return -1;
  }

  tidesdb_txn_t *txn = NULL;
  int ret = tidesdb_txn_begin(g_db, &txn);
  if (ret != TDB_SUCCESS) {
    printf("Failed to begin transaction: %s\n", error_to_string(ret));
    return ret;
  }

  ret = tidesdb_txn_put(txn, cf, (const uint8_t *)argv[2], strlen(argv[2]),
                        (const uint8_t *)argv[3], strlen(argv[3]), 0);
  if (ret != TDB_SUCCESS) {
    printf("Failed to put: %s\n", error_to_string(ret));
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return ret;
  }

  ret = tidesdb_txn_commit(txn);
  if (ret != TDB_SUCCESS) {
    printf("Failed to commit: %s\n", error_to_string(ret));
    tidesdb_txn_free(txn);
    return ret;
  }

  tidesdb_txn_free(txn);
  printf("OK\n");
  return 0;
}

static int cmd_get(const int argc, char **argv) {
  if (argc < 3) {
    printf("Usage: get <cf> <key>\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  tidesdb_column_family_t *cf = tidesdb_get_column_family(g_db, argv[1]);
  if (cf == NULL) {
    printf("Column family '%s' not found.\n", argv[1]);
    return -1;
  }

  tidesdb_txn_t *txn = NULL;
  int ret = tidesdb_txn_begin(g_db, &txn);
  if (ret != TDB_SUCCESS) {
    printf("Failed to begin transaction: %s\n", error_to_string(ret));
    return ret;
  }

  uint8_t *value = NULL;
  size_t value_size = 0;
  ret = tidesdb_txn_get(txn, cf, (const uint8_t *)argv[2], strlen(argv[2]),
                        &value, &value_size);
  if (ret != TDB_SUCCESS) {
    if (ret == TDB_ERR_NOT_FOUND) {
      printf("(nil)\n");
    } else {
      printf("Failed to get: %s\n", error_to_string(ret));
    }
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return ret;
  }

  printf("%.*s\n", (int)value_size, (char *)value);
  free(value);
  tidesdb_txn_rollback(txn);
  tidesdb_txn_free(txn);
  return 0;
}

static int cmd_delete(const int argc, char **argv) {
  if (argc < 3) {
    printf("Usage: delete <cf> <key>\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  tidesdb_column_family_t *cf = tidesdb_get_column_family(g_db, argv[1]);
  if (cf == NULL) {
    printf("Column family '%s' not found.\n", argv[1]);
    return -1;
  }

  tidesdb_txn_t *txn = NULL;
  int ret = tidesdb_txn_begin(g_db, &txn);
  if (ret != TDB_SUCCESS) {
    printf("Failed to begin transaction: %s\n", error_to_string(ret));
    return ret;
  }

  ret = tidesdb_txn_delete(txn, cf, (const uint8_t *)argv[2], strlen(argv[2]));
  if (ret != TDB_SUCCESS) {
    printf("Failed to delete: %s\n", error_to_string(ret));
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return ret;
  }

  ret = tidesdb_txn_commit(txn);
  if (ret != TDB_SUCCESS) {
    printf("Failed to commit: %s\n", error_to_string(ret));
    tidesdb_txn_free(txn);
    return ret;
  }

  tidesdb_txn_free(txn);
  printf("OK\n");
  return 0;
}

static int cmd_scan(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: scan <cf> [limit]\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  int limit = 100;
  if (argc >= 3) {
    char *endptr;
    const long parsed = strtol(argv[2], &endptr, 10);
    if (*endptr == '\0' && parsed > 0) {
      limit = (int)parsed;
    }
  }

  tidesdb_column_family_t *cf = tidesdb_get_column_family(g_db, argv[1]);
  if (cf == NULL) {
    printf("Column family '%s' not found.\n", argv[1]);
    return -1;
  }

  tidesdb_txn_t *txn = NULL;
  int ret = tidesdb_txn_begin(g_db, &txn);
  if (ret != TDB_SUCCESS) {
    printf("Failed to begin transaction: %s\n", error_to_string(ret));
    return ret;
  }

  tidesdb_iter_t *iter = NULL;
  ret = tidesdb_iter_new(txn, cf, &iter);
  if (ret != TDB_SUCCESS) {
    printf("Failed to create iterator: %s\n", error_to_string(ret));
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return ret;
  }

  ret = tidesdb_iter_seek_to_first(iter);
  if (ret != TDB_SUCCESS) {
    printf("Failed to seek: %s\n", error_to_string(ret));
    tidesdb_iter_free(iter);
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return ret;
  }

  int count = 0;
  while (tidesdb_iter_valid(iter) && count < limit) {
    uint8_t *key = NULL;
    size_t key_size = 0;
    uint8_t *value = NULL;
    size_t value_size = 0;

    if (tidesdb_iter_key(iter, &key, &key_size) == TDB_SUCCESS &&
        tidesdb_iter_value(iter, &value, &value_size) == TDB_SUCCESS) {
      printf("%d) \"%.*s\" -> \"%.*s\"\n", count + 1, (int)key_size,
             (char *)key, (int)value_size, (char *)value);
      count++;
    }

    if (tidesdb_iter_next(iter) != TDB_SUCCESS)
      break;
  }

  if (count == 0) {
    printf("(empty)\n");
  } else {
    printf("(%d entries)\n", count);
  }

  tidesdb_iter_free(iter);
  tidesdb_txn_rollback(txn);
  tidesdb_txn_free(txn);
  return 0;
}

static int cmd_range(const int argc, char **argv) {
  if (argc < 4) {
    printf("Usage: range <cf> <start_key> <end_key> [limit]\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  int limit = 100;
  if (argc >= 5) {
    char *endptr;
    const long parsed = strtol(argv[4], &endptr, 10);
    if (*endptr == '\0' && parsed > 0) {
      limit = (int)parsed;
    }
  }

  tidesdb_column_family_t *cf = tidesdb_get_column_family(g_db, argv[1]);
  if (cf == NULL) {
    printf("Column family '%s' not found.\n", argv[1]);
    return -1;
  }

  const char *start_key = argv[2];
  const char *end_key = argv[3];
  const size_t start_key_size = strlen(start_key);
  const size_t end_key_size = strlen(end_key);

  tidesdb_txn_t *txn = NULL;
  int ret = tidesdb_txn_begin(g_db, &txn);
  if (ret != TDB_SUCCESS) {
    printf("Failed to begin transaction: %s\n", error_to_string(ret));
    return ret;
  }

  tidesdb_iter_t *iter = NULL;
  ret = tidesdb_iter_new(txn, cf, &iter);
  if (ret != TDB_SUCCESS) {
    printf("Failed to create iterator: %s\n", error_to_string(ret));
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return ret;
  }

  ret = tidesdb_iter_seek(iter, (const uint8_t *)start_key, start_key_size);
  if (ret != TDB_SUCCESS) {
    printf("(empty range)\n");
    tidesdb_iter_free(iter);
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return 0;
  }

  int count = 0;
  while (tidesdb_iter_valid(iter) && count < limit) {
    uint8_t *key = NULL;
    size_t key_size = 0;
    uint8_t *value = NULL;
    size_t value_size = 0;

    if (tidesdb_iter_key(iter, &key, &key_size) == TDB_SUCCESS) {
      if (key_size > end_key_size ||
          memcmp(key, end_key,
                 key_size < end_key_size ? key_size : end_key_size) > 0) {
        break;
      }

      if (tidesdb_iter_value(iter, &value, &value_size) == TDB_SUCCESS) {
        printf("%d) \"%.*s\" -> \"%.*s\"\n", count + 1, (int)key_size,
               (char *)key, (int)value_size, (char *)value);
        count++;
      }
    }

    if (tidesdb_iter_next(iter) != TDB_SUCCESS)
      break;
  }

  if (count == 0) {
    printf("(empty range)\n");
  } else {
    printf("(%d entries in range)\n", count);
  }

  tidesdb_iter_free(iter);
  tidesdb_txn_rollback(txn);
  tidesdb_txn_free(txn);
  return 0;
}

static int cmd_prefix(const int argc, char **argv) {
  if (argc < 3) {
    printf("Usage: prefix <cf> <prefix> [limit]\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  int limit = 100;
  if (argc >= 4) {
    char *endptr;
    const long parsed = strtol(argv[3], &endptr, 10);
    if (*endptr == '\0' && parsed > 0) {
      limit = (int)parsed;
    }
  }

  tidesdb_column_family_t *cf = tidesdb_get_column_family(g_db, argv[1]);
  if (cf == NULL) {
    printf("Column family '%s' not found.\n", argv[1]);
    return -1;
  }

  const char *prefix = argv[2];
  const size_t prefix_size = strlen(prefix);

  tidesdb_txn_t *txn = NULL;
  int ret = tidesdb_txn_begin(g_db, &txn);
  if (ret != TDB_SUCCESS) {
    printf("Failed to begin transaction: %s\n", error_to_string(ret));
    return ret;
  }

  tidesdb_iter_t *iter = NULL;
  ret = tidesdb_iter_new(txn, cf, &iter);
  if (ret != TDB_SUCCESS) {
    printf("Failed to create iterator: %s\n", error_to_string(ret));
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return ret;
  }

  ret = tidesdb_iter_seek(iter, (const uint8_t *)prefix, prefix_size);
  if (ret != TDB_SUCCESS) {
    printf("(no keys with prefix)\n");
    tidesdb_iter_free(iter);
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return 0;
  }

  int count = 0;
  while (tidesdb_iter_valid(iter) && count < limit) {
    uint8_t *key = NULL;
    size_t key_size = 0;
    uint8_t *value = NULL;
    size_t value_size = 0;

    if (tidesdb_iter_key(iter, &key, &key_size) == TDB_SUCCESS) {
      if (key_size < prefix_size || memcmp(key, prefix, prefix_size) != 0) {
        break;
      }

      if (tidesdb_iter_value(iter, &value, &value_size) == TDB_SUCCESS) {
        printf("%d) \"%.*s\" -> \"%.*s\"\n", count + 1, (int)key_size,
               (char *)key, (int)value_size, (char *)value);
        count++;
      }
    }

    if (tidesdb_iter_next(iter) != TDB_SUCCESS)
      break;
  }

  if (count == 0) {
    printf("(no keys with prefix)\n");
  } else {
    printf("(%d entries with prefix)\n", count);
  }

  tidesdb_iter_free(iter);
  tidesdb_txn_rollback(txn);
  tidesdb_txn_free(txn);
  return 0;
}

static int cmd_sstable_list(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: sstable-list <cf>\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  const tidesdb_column_family_t *cf = tidesdb_get_column_family(g_db, argv[1]);
  if (cf == NULL) {
    printf("Column family '%s' not found.\n", argv[1]);
    return -1;
  }

  char cf_path[2048];
  snprintf(cf_path, sizeof(cf_path), "%s/%s", g_db_path, argv[1]);

  DIR *dir = opendir(cf_path);
  if (dir == NULL) {
    printf("Cannot open column family directory: %s\n", strerror(errno));
    return -1;
  }

  printf("SSTables in '%s':\n", argv[1]);
  int count = 0;
  struct dirent *entry;
  while ((entry = readdir(dir)) != NULL) {
    if (strstr(entry->d_name, ".klog") != NULL) {
      char full_path[4096];
      snprintf(full_path, sizeof(full_path), "%s/%s", cf_path, entry->d_name);

      struct stat st;
      if (stat(full_path, &st) == 0) {
        printf("  %s (%ld bytes)\n", entry->d_name, (long)st.st_size);
        count++;
      }
    }
  }
  closedir(dir);

  if (count == 0) {
    printf("  (no SSTables found)\n");
  } else {
    printf("(%d SSTables)\n", count);
  }

  return 0;
}

static int cmd_sstable_info(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: sstable-info <klog_path>\n");
    return -1;
  }

  block_manager_t *bm = NULL;
  if (block_manager_open(&bm, argv[1], BLOCK_MANAGER_SYNC_NONE) != 0) {
    printf("Failed to open SSTable file: %s\n", argv[1]);
    return -1;
  }

  uint64_t file_size = 0;
  block_manager_get_size(bm, &file_size);

  const int block_count = block_manager_count_blocks(bm);

  printf("SSTable: %s\n", argv[1]);
  printf("  File Size: %" PRIu64 " bytes\n", file_size);
  printf("  Block Count: %d\n", block_count);
  printf("  Last Modified: %ld\n", (long)block_manager_last_modified(bm));

  if (block_count > 0) {
    block_manager_cursor_t *cursor = NULL;
    if (block_manager_cursor_init(&cursor, bm) == 0) {
      if (block_manager_cursor_goto_first(cursor) == 0) {
        block_manager_block_t *first_block = block_manager_cursor_read(cursor);
        if (first_block) {
          printf("  First Block Size: %" PRIu64 " bytes\n", first_block->size);
          block_manager_block_release(first_block);
        }
      }

      if (block_manager_cursor_goto_last(cursor) == 0) {
        block_manager_block_t *last_block = block_manager_cursor_read(cursor);
        if (last_block) {
          printf("  Last Block Size (metadata): %" PRIu64 " bytes\n",
                 last_block->size);
          block_manager_block_release(last_block);
        }
      }
      block_manager_cursor_free(cursor);
    }
  }

  block_manager_close(bm);
  return 0;
}

static int decode_varint_safe(const uint8_t *data, uint64_t *value,
                              const size_t max_bytes) {
  *value = 0;
  size_t i = 0;
  int shift = 0;
  while (i < max_bytes && i < 10) {
    uint8_t byte = data[i];
    *value |= (uint64_t)(byte & 0x7F) << shift;
    i++;
    if ((byte & 0x80) == 0)
      return (int)i;
    shift += 7;
  }
  return -1;
}

static int cmd_sstable_dump(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: sstable-dump <klog_path> [limit]\n");
    return -1;
  }

  int limit = ADMINTOOL_DEFAULT_DUMP_LIMIT;
  if (argc >= 3) {
    char *endptr;
    const long parsed = strtol(argv[2], &endptr, 10);
    if (*endptr == '\0' && parsed > 0) {
      limit = (int)parsed;
    }
  }

  struct stat st;
  if (stat(argv[1], &st) == 0 && st.st_size > ADMINTOOL_LARGE_FILE_THRESHOLD) {
    printf("Warning: Large file (%" PRId64 " MB). ",
           (int64_t)(st.st_size / (1024 * 1024)));
    printf("Limiting to %d entries. Use explicit limit to override.\n", limit);
  }

  block_manager_t *bm = NULL;
  if (block_manager_open(&bm, argv[1], BLOCK_MANAGER_SYNC_NONE) != 0) {
    printf("Failed to open SSTable file: %s\n", argv[1]);
    return -1;
  }

  block_manager_cursor_t *cursor = NULL;
  if (block_manager_cursor_init(&cursor, bm) != 0) {
    printf("Failed to create cursor\n");
    block_manager_close(bm);
    return -1;
  }

  if (block_manager_cursor_goto_first(cursor) != 0) {
    printf("(empty SSTable)\n");
    block_manager_cursor_free(cursor);
    block_manager_close(bm);
    return 0;
  }

  printf("SSTable Entries (limit: %d):\n", limit);
  int total_entries = 0;
  int block_num = 0;

  while (total_entries < limit) {
    block_manager_block_t *block = block_manager_cursor_read(cursor);
    if (!block)
      break;

    if (block->size < 4) {
      block_manager_block_release(block);
      if (block_manager_cursor_next(cursor) != 0)
        break;
      block_num++;
      continue;
    }

    const uint8_t *ptr = (const uint8_t *)block->data;
    size_t remaining = block->size;

    uint64_t prev_seq = 0;
    int entry_in_block = 0;

    while (remaining > 0 && total_entries < limit) {
      if (remaining < 1)
        break;

      const uint8_t flags = *ptr++;
      remaining--;

      uint64_t key_size, value_size, seq_value;

      int bytes_read = decode_varint_safe(ptr, &key_size, remaining);
      if (bytes_read < 0 || (size_t)bytes_read > remaining)
        break;
      ptr += bytes_read;
      remaining -= bytes_read;

      bytes_read = decode_varint_safe(ptr, &value_size, remaining);
      if (bytes_read < 0 || (size_t)bytes_read > remaining)
        break;
      ptr += bytes_read;
      remaining -= bytes_read;

      bytes_read = decode_varint_safe(ptr, &seq_value, remaining);
      if (bytes_read < 0 || (size_t)bytes_read > remaining)
        break;
      ptr += bytes_read;
      remaining -= bytes_read;

      uint64_t seq = seq_value;
      if (flags & TDB_KV_FLAG_DELTA_SEQ) {
        seq = prev_seq + seq_value;
      }
      prev_seq = seq;

      int64_t ttl = 0;
      if (flags & TDB_KV_FLAG_HAS_TTL) {
        if (remaining < sizeof(int64_t))
          break;
        memcpy(&ttl, ptr, sizeof(int64_t));
        ptr += sizeof(int64_t);
        remaining -= sizeof(int64_t);
      }

      uint64_t vlog_offset = 0;
      if (flags & TDB_KV_FLAG_HAS_VLOG) {
        bytes_read = decode_varint_safe(ptr, &vlog_offset, remaining);
        if (bytes_read < 0 || (size_t)bytes_read > remaining)
          break;
        ptr += bytes_read;
        remaining -= bytes_read;
      }

      if (remaining < key_size)
        break;
      const uint8_t *key = ptr;
      ptr += key_size;
      remaining -= key_size;

      const uint8_t *value = NULL;
      if (!(flags & TDB_KV_FLAG_HAS_VLOG) && value_size > 0) {
        if (remaining < value_size)
          break;
        value = ptr;
        ptr += value_size;
        remaining -= value_size;
      }

      total_entries++;
      printf("%d) [blk:%d] ", total_entries, block_num);

      if (flags & TDB_KV_FLAG_TOMBSTONE)
        printf("[DEL] ");
      if (flags & TDB_KV_FLAG_HAS_TTL)
        printf("[TTL:%" PRId64 "] ", ttl);
      if (flags & TDB_KV_FLAG_HAS_VLOG)
        printf("[VLOG:%" PRIu64 "] ", vlog_offset);

      printf("seq=%" PRIu64 " key=\"%.*s\"", seq, (int)key_size, (char *)key);

      if (value && value_size > 0) {
        if (value_size <= 64) {
          printf(" value=\"%.*s\"", (int)value_size, (char *)value);
        } else {
          printf(" value=(%zu bytes)", (size_t)value_size);
        }
      } else if (flags & TDB_KV_FLAG_HAS_VLOG) {
        printf(" value=(in vlog, %zu bytes)", (size_t)value_size);
      }
      printf("\n");

      entry_in_block++;
    }

    block_manager_block_release(block);
    if (block_manager_cursor_next(cursor) != 0)
      break;
    block_num++;
  }

  printf("\n(%d entries dumped from %d blocks)\n", total_entries,
         block_num + 1);

  block_manager_cursor_free(cursor);
  block_manager_close(bm);
  return 0;
}

static int cmd_sstable_stats(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: sstable-stats <klog_path>\n");
    return -1;
  }

  block_manager_t *bm = NULL;
  if (block_manager_open(&bm, argv[1], BLOCK_MANAGER_SYNC_NONE) != 0) {
    printf("Failed to open SSTable file: %s\n", argv[1]);
    return -1;
  }

  uint64_t file_size = 0;
  block_manager_get_size(bm, &file_size);

  block_manager_cursor_t *cursor = NULL;
  if (block_manager_cursor_init(&cursor, bm) != 0) {
    printf("Failed to create cursor\n");
    block_manager_close(bm);
    return -1;
  }

  if (block_manager_cursor_goto_first(cursor) != 0) {
    printf("(empty SSTable)\n");
    block_manager_cursor_free(cursor);
    block_manager_close(bm);
    return 0;
  }

  uint64_t total_entries = 0;
  uint64_t tombstone_count = 0;
  uint64_t ttl_count = 0;
  uint64_t vlog_count = 0;
  uint64_t min_seq = UINT64_MAX;
  uint64_t max_seq = 0;
  uint64_t total_key_size = 0;
  uint64_t total_value_size = 0;
  uint64_t min_key_size = UINT64_MAX;
  uint64_t max_key_size = 0;
  uint64_t min_value_size = UINT64_MAX;
  uint64_t max_value_size = 0;
  int block_count = 0;

  while (1) {
    block_manager_block_t *block = block_manager_cursor_read(cursor);
    if (!block)
      break;

    block_count++;

    if (block->size < 4) {
      block_manager_block_release(block);
      if (block_manager_cursor_next(cursor) != 0)
        break;
      continue;
    }

    const uint8_t *ptr = (const uint8_t *)block->data;
    size_t remaining = block->size;
    uint64_t prev_seq = 0;

    while (remaining > 0) {
      if (remaining < 1)
        break;

      const uint8_t flags = *ptr++;
      remaining--;

      uint64_t key_size, value_size, seq_value;

      int bytes_read = decode_varint_safe(ptr, &key_size, remaining);
      if (bytes_read < 0 || (size_t)bytes_read > remaining)
        break;
      ptr += bytes_read;
      remaining -= bytes_read;

      bytes_read = decode_varint_safe(ptr, &value_size, remaining);
      if (bytes_read < 0 || (size_t)bytes_read > remaining)
        break;
      ptr += bytes_read;
      remaining -= bytes_read;

      bytes_read = decode_varint_safe(ptr, &seq_value, remaining);
      if (bytes_read < 0 || (size_t)bytes_read > remaining)
        break;
      ptr += bytes_read;
      remaining -= bytes_read;

      uint64_t seq = seq_value;
      if (flags & TDB_KV_FLAG_DELTA_SEQ) {
        seq = prev_seq + seq_value;
      }
      prev_seq = seq;

      if (flags & TDB_KV_FLAG_HAS_TTL) {
        if (remaining < sizeof(int64_t))
          break;
        ptr += sizeof(int64_t);
        remaining -= sizeof(int64_t);
        ttl_count++;
      }

      if (flags & TDB_KV_FLAG_HAS_VLOG) {
        uint64_t vlog_offset;
        bytes_read = decode_varint_safe(ptr, &vlog_offset, remaining);
        if (bytes_read < 0 || (size_t)bytes_read > remaining)
          break;
        ptr += bytes_read;
        remaining -= bytes_read;
        vlog_count++;
      }

      if (remaining < key_size)
        break;
      ptr += key_size;
      remaining -= key_size;

      if (!(flags & TDB_KV_FLAG_HAS_VLOG) && value_size > 0) {
        if (remaining < value_size)
          break;
        ptr += value_size;
        remaining -= value_size;
      }

      total_entries++;
      if (flags & TDB_KV_FLAG_TOMBSTONE)
        tombstone_count++;

      if (seq < min_seq)
        min_seq = seq;
      if (seq > max_seq)
        max_seq = seq;

      total_key_size += key_size;
      total_value_size += value_size;
      if (key_size < min_key_size)
        min_key_size = key_size;
      if (key_size > max_key_size)
        max_key_size = key_size;
      if (value_size < min_value_size)
        min_value_size = value_size;
      if (value_size > max_value_size)
        max_value_size = value_size;
    }

    block_manager_block_release(block);
    if (block_manager_cursor_next(cursor) != 0)
      break;
  }

  printf("SSTable Statistics: %s\n", argv[1]);
  printf("  File Size: %" PRIu64 " bytes (%.2f MB)\n", file_size,
         (double)file_size / (1024 * 1024));
  printf("  Block Count: %d\n", block_count);
  printf("  Total Entries: %" PRIu64 "\n", total_entries);
  printf("  Tombstones: %" PRIu64 " (%.1f%%)\n", tombstone_count,
         total_entries > 0
             ? (double)tombstone_count * 100.0 / (double)total_entries
             : 0);
  printf("  TTL Entries: %" PRIu64 "\n", ttl_count);
  printf("  VLog References: %" PRIu64 "\n", vlog_count);
  printf("  Sequence Range: %" PRIu64 " - %" PRIu64 "\n",
         min_seq == UINT64_MAX ? 0 : min_seq, max_seq);
  printf("  Key Sizes: min=%" PRIu64 " max=%" PRIu64 " avg=%.1f\n",
         min_key_size == UINT64_MAX ? 0 : min_key_size, max_key_size,
         total_entries > 0 ? (double)total_key_size / (double)total_entries
                           : 0);
  printf("  Value Sizes: min=%" PRIu64 " max=%" PRIu64 " avg=%.1f\n",
         min_value_size == UINT64_MAX ? 0 : min_value_size, max_value_size,
         total_entries > 0 ? (double)total_value_size / (double)total_entries
                           : 0);

  block_manager_cursor_free(cursor);
  block_manager_close(bm);
  return 0;
}

static int cmd_sstable_keys(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: sstable-keys <klog_path> [limit]\n");
    return -1;
  }

  int limit = ADMINTOOL_DEFAULT_DUMP_LIMIT;
  if (argc >= 3) {
    char *endptr;
    const long parsed = strtol(argv[2], &endptr, 10);
    if (*endptr == '\0' && parsed > 0) {
      limit = (int)parsed;
    }
  }

  struct stat st;
  if (stat(argv[1], &st) == 0 && st.st_size > ADMINTOOL_LARGE_FILE_THRESHOLD) {
    printf("Warning: Large file (%" PRId64 " MB). Limiting to %d keys.\n",
           (int64_t)(st.st_size / (1024 * 1024)), limit);
  }

  block_manager_t *bm = NULL;
  if (block_manager_open(&bm, argv[1], BLOCK_MANAGER_SYNC_NONE) != 0) {
    printf("Failed to open SSTable file: %s\n", argv[1]);
    return -1;
  }

  block_manager_cursor_t *cursor = NULL;
  if (block_manager_cursor_init(&cursor, bm) != 0) {
    printf("Failed to create cursor\n");
    block_manager_close(bm);
    return -1;
  }

  if (block_manager_cursor_goto_first(cursor) != 0) {
    printf("(empty SSTable)\n");
    block_manager_cursor_free(cursor);
    block_manager_close(bm);
    return 0;
  }

  printf("SSTable Keys (limit: %d):\n", limit);
  int total_keys = 0;
  uint8_t *first_key = NULL;
  size_t first_key_size = 0;
  uint8_t *last_key = NULL;
  size_t last_key_size = 0;

  while (total_keys < limit) {
    block_manager_block_t *block = block_manager_cursor_read(cursor);
    if (!block)
      break;

    if (block->size < 4) {
      block_manager_block_release(block);
      if (block_manager_cursor_next(cursor) != 0)
        break;
      continue;
    }

    const uint8_t *ptr = (const uint8_t *)block->data;
    size_t remaining = block->size;
    uint64_t prev_seq = 0;

    while (remaining > 0 && total_keys < limit) {
      if (remaining < 1)
        break;

      uint8_t flags = *ptr++;
      remaining--;

      uint64_t key_size, value_size, seq_value;

      int bytes_read = decode_varint_safe(ptr, &key_size, remaining);
      if (bytes_read < 0 || (size_t)bytes_read > remaining)
        break;
      ptr += bytes_read;
      remaining -= bytes_read;

      bytes_read = decode_varint_safe(ptr, &value_size, remaining);
      if (bytes_read < 0 || (size_t)bytes_read > remaining)
        break;
      ptr += bytes_read;
      remaining -= bytes_read;

      bytes_read = decode_varint_safe(ptr, &seq_value, remaining);
      if (bytes_read < 0 || (size_t)bytes_read > remaining)
        break;
      ptr += bytes_read;
      remaining -= bytes_read;

      if (flags & TDB_KV_FLAG_DELTA_SEQ) {
        prev_seq = prev_seq + seq_value;
      } else {
        prev_seq = seq_value;
      }

      if (flags & TDB_KV_FLAG_HAS_TTL) {
        if (remaining < sizeof(int64_t))
          break;
        ptr += sizeof(int64_t);
        remaining -= sizeof(int64_t);
      }

      if (flags & TDB_KV_FLAG_HAS_VLOG) {
        uint64_t vlog_offset;
        bytes_read = decode_varint_safe(ptr, &vlog_offset, remaining);
        if (bytes_read < 0 || (size_t)bytes_read > remaining)
          break;
        ptr += bytes_read;
        remaining -= bytes_read;
      }

      if (remaining < key_size)
        break;
      const uint8_t *key = ptr;
      ptr += key_size;
      remaining -= key_size;

      if (!(flags & TDB_KV_FLAG_HAS_VLOG) && value_size > 0) {
        if (remaining < value_size)
          break;
        ptr += value_size;
        remaining -= value_size;
      }

      total_keys++;
      printf("%d) \"%.*s\"", total_keys, (int)key_size, (char *)key);
      if (flags & TDB_KV_FLAG_TOMBSTONE)
        printf(" [DEL]");
      printf("\n");

      if (first_key == NULL) {
        first_key = malloc(key_size);
        if (first_key) {
          memcpy(first_key, key, key_size);
          first_key_size = key_size;
        }
      }

      free(last_key);
      last_key = malloc(key_size);
      if (last_key) {
        memcpy(last_key, key, key_size);
        last_key_size = key_size;
      }
    }

    block_manager_block_release(block);
    if (block_manager_cursor_next(cursor) != 0)
      break;
  }

  printf("\n(%d keys listed)\n", total_keys);
  if (first_key && last_key) {
    printf("Key Range: \"%.*s\" to \"%.*s\"\n", (int)first_key_size,
           (char *)first_key, (int)last_key_size, (char *)last_key);
  }

  free(first_key);
  free(last_key);
  block_manager_cursor_free(cursor);
  block_manager_close(bm);
  return 0;
}

static int cmd_bloom_stats(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: bloom-stats <klog_path>\n");
    printf("Displays bloom filter statistics from an SSTable.\n");
    return -1;
  }

  block_manager_t *bm = NULL;
  if (block_manager_open(&bm, argv[1], BLOCK_MANAGER_SYNC_NONE) != 0) {
    printf("Failed to open SSTable file: %s\n", argv[1]);
    return -1;
  }

  const int block_count = block_manager_count_blocks(bm);
  if (block_count < 3) {
    printf("SSTable has insufficient blocks (need at least 3 for "
           "index/bloom/metadata)\n");
    block_manager_close(bm);
    return -1;
  }

  block_manager_cursor_t *cursor = NULL;
  if (block_manager_cursor_init(&cursor, bm) != 0) {
    printf("Failed to create cursor\n");
    block_manager_close(bm);
    return -1;
  }

  if (block_manager_cursor_goto_last(cursor) != 0) {
    printf("Failed to seek to last block\n");
    block_manager_cursor_free(cursor);
    block_manager_close(bm);
    return -1;
  }

  if (block_manager_cursor_prev(cursor) != 0) {
    printf("Failed to seek to bloom filter block\n");
    block_manager_cursor_free(cursor);
    block_manager_close(bm);
    return -1;
  }

  block_manager_block_t *bloom_block = block_manager_cursor_read(cursor);
  if (!bloom_block) {
    printf("Failed to read bloom filter block\n");
    block_manager_cursor_free(cursor);
    block_manager_close(bm);
    return -1;
  }

  if (bloom_block->size == 0) {
    printf("Bloom Filter: disabled (empty block)\n");
    block_manager_block_release(bloom_block);
    block_manager_cursor_free(cursor);
    block_manager_close(bm);
    return 0;
  }

  bloom_filter_t *bf = bloom_filter_deserialize(bloom_block->data);
  if (!bf) {
    printf(
        "Failed to deserialize bloom filter (may be disabled or corrupted)\n");
    block_manager_block_release(bloom_block);
    block_manager_cursor_free(cursor);
    block_manager_close(bm);
    return -1;
  }

  uint64_t bits_set = 0;
  for (unsigned int i = 0; i < bf->size_in_words; i++) {
    uint64_t word = bf->bitset[i];
    while (word) {
      bits_set += word & 1;
      word >>= 1;
    }
  }

  const double fill_ratio = (double)bits_set / (double)bf->m;
  const double estimated_fpr = pow(fill_ratio, (double)bf->h);

  printf("Bloom Filter Statistics: %s\n", argv[1]);
  printf("  Serialized Size: %" PRIu64 " bytes\n", bloom_block->size);
  printf("  Filter Size (m): %u bits (%.2f KB)\n", bf->m,
         (double)bf->m / 8.0 / 1024.0);
  printf("  Hash Functions (k): %u\n", bf->h);
  printf("  Storage Words: %u (uint64_t)\n", bf->size_in_words);
  printf("  Bits Set: %" PRIu64 "\n", bits_set);
  printf("  Fill Ratio: %.2f%%\n", fill_ratio * 100.0);
  printf("  Estimated FPR: %.6f (%.4f%%)\n", estimated_fpr,
         estimated_fpr * 100.0);

  if (fill_ratio > 0.5) {
    printf("  Warning: High fill ratio may increase false positives\n");
  }

  bloom_filter_free(bf);
  block_manager_block_release(bloom_block);
  block_manager_cursor_free(cursor);
  block_manager_close(bm);
  return 0;
}

static int cmd_sstable_checksum(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: sstable-checksum <klog_path>\n");
    printf("Verifies all block checksums and reports any corruption.\n");
    return -1;
  }

  const int fd = open(argv[1], O_RDONLY);
  if (fd < 0) {
    printf("Failed to open file: %s\n", argv[1]);
    return -1;
  }

  struct stat st;
  if (fstat(fd, &st) != 0) {
    printf("Failed to stat file\n");
    close(fd);
    return -1;
  }

  printf("Verifying checksums: %s\n", argv[1]);
  printf("  File Size: %ld bytes\n\n", (long)st.st_size);

  uint64_t pos = 8;
  int block_num = 0;
  int valid_blocks = 0;
  int invalid_blocks = 0;

  while (pos < (uint64_t)st.st_size) {
    uint8_t header[8];
    ssize_t nread = pread(fd, header, 8, (off_t)pos);
    if (nread != 8)
      break;

    const uint32_t block_size = decode_uint32_le(header);
    const uint32_t stored_checksum = decode_uint32_le(header + 4);

    if (block_size == 0 || block_size > 100 * 1024 * 1024) {
      printf("  Block %d @ offset %" PRIu64 ": INVALID SIZE (%u)\n", block_num,
             pos, block_size);
      invalid_blocks++;
      break;
    }

    uint8_t *data = malloc(block_size);
    if (!data) {
      printf("  Block %d: OUT OF MEMORY\n", block_num);
      break;
    }

    nread = pread(fd, data, block_size, (off_t)(pos + 8));
    if (nread != (ssize_t)block_size) {
      printf("  Block %d @ offset %" PRIu64
             ": READ ERROR (expected %u, got %zd)\n",
             block_num, pos, block_size, nread);
      free(data);
      invalid_blocks++;
      break;
    }

    const uint32_t computed_checksum = compute_block_checksum(data, block_size);

    if (computed_checksum != stored_checksum) {
      printf("  Block %d @ offset %" PRIu64 ": CHECKSUM MISMATCH\n", block_num,
             pos);
      printf("    Size: %u bytes\n", block_size);
      printf("    Stored:   0x%08X\n", stored_checksum);
      printf("    Computed: 0x%08X\n", computed_checksum);
      invalid_blocks++;
    } else {
      valid_blocks++;
    }

    free(data);

    pos += 8 + block_size + 8;
    block_num++;
  }

  printf("\nChecksum Verification Results:\n");
  printf("  Total Blocks: %d\n", block_num);
  printf("  Valid: %d\n", valid_blocks);
  printf("  Invalid: %d\n", invalid_blocks);
  printf("  Status: %s\n", invalid_blocks == 0 ? "OK" : "CORRUPTED");

  close(fd);
  return invalid_blocks > 0 ? -1 : 0;
}

static int read_vlog_value(const char *vlog_path, uint64_t vlog_offset,
                           size_t value_size, uint8_t **value_out) {
  const int fd = open(vlog_path, O_RDONLY);
  if (fd < 0)
    return -1;

  uint8_t header[8];
  ssize_t nread = pread(fd, header, 8, (off_t)vlog_offset);
  if (nread != 8) {
    close(fd);
    return -1;
  }

  const uint32_t block_size = decode_uint32_le(header);
  const uint32_t stored_checksum = decode_uint32_le(header + 4);

  if (block_size == 0 || block_size > 100 * 1024 * 1024) {
    close(fd);
    return -1;
  }

  uint8_t *data = malloc(block_size);
  if (!data) {
    close(fd);
    return -1;
  }

  nread = pread(fd, data, block_size, (off_t)(vlog_offset + 8));
  if (nread != (ssize_t)block_size) {
    free(data);
    close(fd);
    return -1;
  }

  uint32_t computed = compute_block_checksum(data, block_size);
  if (computed != stored_checksum) {
    free(data);
    close(fd);
    return -2;
  }

  *value_out = data;
  close(fd);
  return (int)block_size;
}

static int cmd_sstable_dump_full(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: sstable-dump-full <klog_path> [vlog_path] [limit]\n");
    printf(
        "Dumps SSTable entries with vlog value retrieval and checksum info.\n");
    return -1;
  }

  const char *klog_path = argv[1];
  const char *vlog_path = NULL;
  int limit = ADMINTOOL_DEFAULT_DUMP_LIMIT;

  if (argc >= 3) {
    struct stat st;
    if (stat(argv[2], &st) == 0 && S_ISREG(st.st_mode)) {
      vlog_path = argv[2];
      if (argc >= 4) {
        char *endptr;
        long parsed = strtol(argv[3], &endptr, 10);
        if (*endptr == '\0' && parsed > 0)
          limit = (int)parsed;
      }
    } else {
      char *endptr;
      long parsed = strtol(argv[2], &endptr, 10);
      if (*endptr == '\0' && parsed > 0)
        limit = (int)parsed;
    }
  }

  int fd = open(klog_path, O_RDONLY);
  if (fd < 0) {
    printf("Failed to open klog: %s\n", klog_path);
    return -1;
  }

  struct stat st;
  fstat(fd, &st);

  printf("SSTable Full Dump (limit: %d):\n", limit);
  printf("  KLog: %s\n", klog_path);
  if (vlog_path)
    printf("  VLog: %s\n", vlog_path);
  printf("\n");

  uint64_t pos = 8;
  int total_entries = 0;
  int block_num = 0;
  int checksum_errors = 0;

  while (total_entries < limit && pos < (uint64_t)st.st_size) {
    uint8_t header[8];
    ssize_t nread = pread(fd, header, 8, (off_t)pos);
    if (nread != 8)
      break;

    uint32_t block_size = decode_uint32_le(header);
    uint32_t stored_checksum = decode_uint32_le(header + 4);

    if (block_size == 0 || block_size > 100 * 1024 * 1024)
      break;

    uint8_t *block_data = malloc(block_size);
    if (!block_data)
      break;

    nread = pread(fd, block_data, block_size, (off_t)(pos + 8));
    if (nread != (ssize_t)block_size) {
      free(block_data);
      break;
    }

    uint32_t computed_checksum = compute_block_checksum(block_data, block_size);
    int checksum_ok = (computed_checksum == stored_checksum);
    if (!checksum_ok)
      checksum_errors++;

    const uint8_t *ptr = block_data;
    size_t remaining = block_size;
    uint64_t prev_seq = 0;

    while (remaining > 0 && total_entries < limit) {
      if (remaining < 1)
        break;

      uint8_t flags = *ptr++;
      remaining--;

      uint64_t key_size, value_size, seq_value;
      int bytes_read;

      bytes_read = decode_varint_safe(ptr, &key_size, remaining);
      if (bytes_read < 0 || (size_t)bytes_read > remaining)
        break;
      ptr += bytes_read;
      remaining -= bytes_read;

      bytes_read = decode_varint_safe(ptr, &value_size, remaining);
      if (bytes_read < 0 || (size_t)bytes_read > remaining)
        break;
      ptr += bytes_read;
      remaining -= bytes_read;

      bytes_read = decode_varint_safe(ptr, &seq_value, remaining);
      if (bytes_read < 0 || (size_t)bytes_read > remaining)
        break;
      ptr += bytes_read;
      remaining -= bytes_read;

      uint64_t seq = seq_value;
      if (flags & TDB_KV_FLAG_DELTA_SEQ)
        seq = prev_seq + seq_value;
      prev_seq = seq;

      int64_t ttl = 0;
      if (flags & TDB_KV_FLAG_HAS_TTL) {
        if (remaining < sizeof(int64_t))
          break;
        memcpy(&ttl, ptr, sizeof(int64_t));
        ptr += sizeof(int64_t);
        remaining -= sizeof(int64_t);
      }

      uint64_t vlog_offset = 0;
      if (flags & TDB_KV_FLAG_HAS_VLOG) {
        bytes_read = decode_varint_safe(ptr, &vlog_offset, remaining);
        if (bytes_read < 0 || (size_t)bytes_read > remaining)
          break;
        ptr += bytes_read;
        remaining -= bytes_read;
      }

      if (remaining < key_size)
        break;
      const uint8_t *key = ptr;
      ptr += key_size;
      remaining -= key_size;

      const uint8_t *value = NULL;
      uint8_t *vlog_value = NULL;
      int vlog_status = 0;

      if (flags & TDB_KV_FLAG_HAS_VLOG) {
        if (vlog_path && value_size > 0) {
          vlog_status =
              read_vlog_value(vlog_path, vlog_offset, value_size, &vlog_value);
          if (vlog_status > 0)
            value = vlog_value;
        }
      } else if (value_size > 0) {
        if (remaining < value_size)
          break;
        value = ptr;
        ptr += value_size;
        remaining -= value_size;
      }

      total_entries++;
      printf("%d) [blk:%d", total_entries, block_num);
      if (!checksum_ok)
        printf(" CHECKSUM_ERR");
      printf("] ");

      if (flags & TDB_KV_FLAG_TOMBSTONE)
        printf("[DEL] ");
      if (flags & TDB_KV_FLAG_HAS_TTL)
        printf("[TTL:%" PRId64 "] ", ttl);
      if (flags & TDB_KV_FLAG_HAS_VLOG) {
        printf("[VLOG:%" PRIu64, vlog_offset);
        if (vlog_status == -2)
          printf(" CHECKSUM_ERR");
        else if (vlog_status < 0 && vlog_path)
          printf(" READ_ERR");
        else if (!vlog_path)
          printf(" NO_VLOG_FILE");
        printf("] ");
      }

      printf("seq=%" PRIu64 " key=\"%.*s\"", seq, (int)key_size, (char *)key);

      if (value && value_size > 0) {
        if (value_size <= 64)
          printf(" value=\"%.*s\"", (int)value_size, (char *)value);
        else
          printf(" value=(%zu bytes)", (size_t)value_size);
      } else if ((flags & TDB_KV_FLAG_HAS_VLOG) && !vlog_value) {
        printf(" value=(vlog, %zu bytes, not retrieved)", (size_t)value_size);
      }
      printf("\n");

      free(vlog_value);
    }

    free(block_data);
    pos += 8 + block_size + 8;
    block_num++;
  }

  printf("\n(%d entries from %d blocks", total_entries, block_num);
  if (checksum_errors > 0)
    printf(", %d checksum errors", checksum_errors);
  printf(")\n");

  close(fd);
  return checksum_errors > 0 ? -1 : 0;
}

static int cmd_wal_list(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: wal-list <cf>\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  const tidesdb_column_family_t *cf = tidesdb_get_column_family(g_db, argv[1]);
  if (cf == NULL) {
    printf("Column family '%s' not found.\n", argv[1]);
    return -1;
  }

  char cf_path[2048];
  snprintf(cf_path, sizeof(cf_path), "%s/%s", g_db_path, argv[1]);

  DIR *dir = opendir(cf_path);
  if (dir == NULL) {
    printf("Cannot open column family directory: %s\n", strerror(errno));
    return -1;
  }

  printf("WAL files in '%s':\n", argv[1]);
  int count = 0;
  struct dirent *entry;
  while ((entry = readdir(dir)) != NULL) {
    if (strstr(entry->d_name, ".log") != NULL) {
      char full_path[4096];
      snprintf(full_path, sizeof(full_path), "%s/%s", cf_path, entry->d_name);

      struct stat st;
      if (stat(full_path, &st) == 0) {
        printf("  %s (%ld bytes)\n", entry->d_name, (long)st.st_size);
        count++;
      }
    }
  }
  closedir(dir);

  if (count == 0) {
    printf("  (no WAL files found)\n");
  } else {
    printf("(%d WAL files)\n", count);
  }

  return 0;
}

static int cmd_wal_info(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: wal-info <wal_path>\n");
    return -1;
  }

  block_manager_t *bm = NULL;
  if (block_manager_open(&bm, argv[1], BLOCK_MANAGER_SYNC_NONE) != 0) {
    printf("Failed to open WAL file: %s\n", argv[1]);
    return -1;
  }

  uint64_t file_size = 0;
  block_manager_get_size(bm, &file_size);

  const int block_count = block_manager_count_blocks(bm);

  printf("WAL: %s\n", argv[1]);
  printf("  File Size: %" PRIu64 " bytes\n", file_size);
  printf("  Block Count (entries): %d\n", block_count);
  printf("  Last Modified: %ld\n", (long)block_manager_last_modified(bm));

  block_manager_close(bm);
  return 0;
}

static int cmd_wal_dump(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: wal-dump <wal_path> [limit]\n");
    return -1;
  }

  int limit = ADMINTOOL_DEFAULT_DUMP_LIMIT;
  if (argc >= 3) {
    char *endptr;
    const long parsed = strtol(argv[2], &endptr, 10);
    if (*endptr == '\0' && parsed > 0) {
      limit = (int)parsed;
    }
  }

  struct stat st;
  if (stat(argv[1], &st) == 0 && st.st_size > ADMINTOOL_LARGE_FILE_THRESHOLD) {
    printf("Warning: Large WAL file (%" PRId64 " MB). ",
           (int64_t)(st.st_size / (1024 * 1024)));
    printf("Limiting to %d entries.\n", limit);
  }

  block_manager_t *bm = NULL;
  if (block_manager_open(&bm, argv[1], BLOCK_MANAGER_SYNC_NONE) != 0) {
    printf("Failed to open WAL file: %s\n", argv[1]);
    return -1;
  }

  block_manager_cursor_t *cursor = NULL;
  if (block_manager_cursor_init(&cursor, bm) != 0) {
    printf("Failed to create cursor\n");
    block_manager_close(bm);
    return -1;
  }

  if (block_manager_cursor_goto_first(cursor) != 0) {
    printf("(empty WAL)\n");
    block_manager_cursor_free(cursor);
    block_manager_close(bm);
    return 0;
  }

  printf("WAL Entries (limit: %d):\n", limit);
  int entry_num = 0;

  while (entry_num < limit) {
    block_manager_block_t *block = block_manager_cursor_read(cursor);
    if (!block)
      break;

    if (block->size < 1) {
      block_manager_block_release(block);
      if (block_manager_cursor_next(cursor) != 0)
        break;
      continue;
    }

    const uint8_t *ptr = (const uint8_t *)block->data;
    size_t remaining = block->size;

    if (remaining < 1) {
      block_manager_block_release(block);
      if (block_manager_cursor_next(cursor) != 0)
        break;
      continue;
    }

    const uint8_t flags = *ptr++;
    remaining--;

    uint64_t key_size, value_size, seq;

    int bytes_read = decode_varint_safe(ptr, &key_size, remaining);
    if (bytes_read < 0 || (size_t)bytes_read > remaining) {
      block_manager_block_release(block);
      if (block_manager_cursor_next(cursor) != 0)
        break;
      continue;
    }
    ptr += bytes_read;
    remaining -= bytes_read;

    bytes_read = decode_varint_safe(ptr, &value_size, remaining);
    if (bytes_read < 0 || (size_t)bytes_read > remaining) {
      block_manager_block_release(block);
      if (block_manager_cursor_next(cursor) != 0)
        break;
      continue;
    }
    ptr += bytes_read;
    remaining -= bytes_read;

    bytes_read = decode_varint_safe(ptr, &seq, remaining);
    if (bytes_read < 0 || (size_t)bytes_read > remaining) {
      block_manager_block_release(block);
      if (block_manager_cursor_next(cursor) != 0)
        break;
      continue;
    }
    ptr += bytes_read;
    remaining -= bytes_read;

    int64_t ttl = 0;
    if (flags & TDB_KV_FLAG_HAS_TTL) {
      if (remaining >= sizeof(int64_t)) {
        memcpy(&ttl, ptr, sizeof(int64_t));
        ptr += sizeof(int64_t);
        remaining -= sizeof(int64_t);
      }
    }

    if (remaining < key_size) {
      block_manager_block_release(block);
      if (block_manager_cursor_next(cursor) != 0)
        break;
      continue;
    }
    const uint8_t *key = ptr;
    ptr += key_size;
    remaining -= key_size;

    const uint8_t *value = NULL;
    if (value_size > 0 && remaining >= value_size) {
      value = ptr;
    }

    entry_num++;
    printf("%d) ", entry_num);

    if (flags & TDB_KV_FLAG_TOMBSTONE)
      printf("[DELETE] ");
    else
      printf("[PUT] ");

    if (flags & TDB_KV_FLAG_HAS_TTL)
      printf("[TTL:%" PRId64 "] ", ttl);

    printf("seq=%" PRIu64 " key=\"%.*s\"", seq, (int)key_size, (char *)key);

    if (value && value_size > 0) {
      if (value_size <= 64) {
        printf(" value=\"%.*s\"", (int)value_size, (char *)value);
      } else {
        printf(" value=(%zu bytes)", (size_t)value_size);
      }
    }
    printf("\n");

    block_manager_block_release(block);
    if (block_manager_cursor_next(cursor) != 0)
      break;
  }

  printf("\n(%d WAL entries dumped)\n", entry_num);

  block_manager_cursor_free(cursor);
  block_manager_close(bm);
  return 0;
}

static int cmd_wal_verify(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: wal-verify <wal_path>\n");
    return -1;
  }

  block_manager_t *bm = NULL;
  if (block_manager_open(&bm, argv[1], BLOCK_MANAGER_SYNC_NONE) != 0) {
    printf("Failed to open WAL file: %s\n", argv[1]);
    return -1;
  }

  uint64_t file_size = 0;
  block_manager_get_size(bm, &file_size);

  printf("Verifying WAL: %s\n", argv[1]);
  printf("  File Size: %" PRIu64 " bytes\n", file_size);

  block_manager_cursor_t *cursor = NULL;
  if (block_manager_cursor_init(&cursor, bm) != 0) {
    printf("  Status: FAILED (cannot create cursor)\n");
    block_manager_close(bm);
    return -1;
  }

  if (block_manager_cursor_goto_first(cursor) != 0) {
    printf("  Status: OK (empty WAL)\n");
    block_manager_cursor_free(cursor);
    block_manager_close(bm);
    return 0;
  }

  int valid_entries = 0;
  int corrupted_entries = 0;
  uint64_t min_seq = UINT64_MAX;
  uint64_t max_seq = 0;
  uint64_t last_valid_pos = 0;

  while (1) {
    const uint64_t current_pos = cursor->current_pos;
    block_manager_block_t *block = block_manager_cursor_read(cursor);
    if (!block) {
      corrupted_entries++;
      break;
    }

    if (block->size < 1) {
      block_manager_block_release(block);
      if (block_manager_cursor_next(cursor) != 0)
        break;
      continue;
    }

    const uint8_t *ptr = (const uint8_t *)block->data;
    size_t remaining = block->size;

    int entry_valid = 1;

    if (remaining < 1) {
      entry_valid = 0;
    } else {
      const uint8_t flags = *ptr++;
      remaining--;

      uint64_t key_size, value_size, seq;

      int bytes_read = decode_varint_safe(ptr, &key_size, remaining);
      if (bytes_read < 0 || (size_t)bytes_read > remaining) {
        entry_valid = 0;
      } else {
        ptr += bytes_read;
        remaining -= bytes_read;

        bytes_read = decode_varint_safe(ptr, &value_size, remaining);
        if (bytes_read < 0 || (size_t)bytes_read > remaining) {
          entry_valid = 0;
        } else {
          ptr += bytes_read;
          remaining -= bytes_read;

          bytes_read = decode_varint_safe(ptr, &seq, remaining);
          if (bytes_read < 0 || (size_t)bytes_read > remaining) {
            entry_valid = 0;
          } else {
            ptr += bytes_read;
            remaining -= bytes_read;

            if (flags & TDB_KV_FLAG_HAS_TTL) {
              if (remaining < sizeof(int64_t)) {
                entry_valid = 0;
              } else {
                ptr += sizeof(int64_t);
                remaining -= sizeof(int64_t);
              }
            }

            if (entry_valid && remaining < key_size) {
              entry_valid = 0;
            } else if (entry_valid) {
              ptr += key_size;
              remaining -= key_size;

              if (value_size > 0 && remaining < value_size) {
                entry_valid = 0;
              }
            }

            if (entry_valid) {
              if (seq < min_seq)
                min_seq = seq;
              if (seq > max_seq)
                max_seq = seq;
            }
          }
        }
      }
    }

    if (entry_valid) {
      valid_entries++;
      last_valid_pos = current_pos;
    } else {
      corrupted_entries++;
    }

    block_manager_block_release(block);
    if (block_manager_cursor_next(cursor) != 0)
      break;
  }

  printf("  Valid Entries: %d\n", valid_entries);
  printf("  Corrupted Entries: %d\n", corrupted_entries);
  if (valid_entries > 0) {
    printf("  Sequence Range: %" PRIu64 " - %" PRIu64 "\n",
           min_seq == UINT64_MAX ? 0 : min_seq, max_seq);
    printf("  Last Valid Position: %" PRIu64 "\n", last_valid_pos);
  }

  if (corrupted_entries == 0) {
    printf("  Status: OK\n");
  } else {
    printf("  Status: CORRUPTED (recovery possible up to position %" PRIu64
           ")\n",
           last_valid_pos);
  }

  block_manager_cursor_free(cursor);
  block_manager_close(bm);
  return corrupted_entries > 0 ? -1 : 0;
}

static int cmd_level_info(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: level-info <cf>\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  tidesdb_column_family_t *cf = tidesdb_get_column_family(g_db, argv[1]);
  if (cf == NULL) {
    printf("Column family '%s' not found.\n", argv[1]);
    return -1;
  }

  tidesdb_stats_t *stats = NULL;
  const int ret = tidesdb_get_stats(cf, &stats);
  if (ret != TDB_SUCCESS) {
    printf("Failed to get stats: %s\n", error_to_string(ret));
    return ret;
  }

  printf("Level Information for '%s':\n", argv[1]);
  printf("  Memtable Size: %zu bytes (%.2f MB)\n", stats->memtable_size,
         (double)stats->memtable_size / (1024 * 1024));
  printf("  Number of Levels: %d\n\n", stats->num_levels);

  uint64_t total_size = 0;
  int total_sstables = 0;

  for (int i = 0; i < stats->num_levels; i++) {
    printf("  Level %d:\n", i + 1);
    printf("    SSTables: %d\n", stats->level_num_sstables[i]);
    printf("    Size: %zu bytes (%.2f MB)\n", stats->level_sizes[i],
           (double)stats->level_sizes[i] / (1024 * 1024));

    total_size += stats->level_sizes[i];
    total_sstables += stats->level_num_sstables[i];
  }

  printf("\n  Total SSTables: %d\n", total_sstables);
  printf("  Total Disk Size: %" PRIu64 " bytes (%.2f MB)\n", total_size,
         (double)total_size / (1024 * 1024));

  tidesdb_free_stats(stats);
  return 0;
}

static int cmd_verify(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: verify <cf>\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  const tidesdb_column_family_t *cf = tidesdb_get_column_family(g_db, argv[1]);
  if (cf == NULL) {
    printf("Column family '%s' not found.\n", argv[1]);
    return -1;
  }

  printf("Verifying column family '%s'...\n", argv[1]);

  char cf_path[2048];
  snprintf(cf_path, sizeof(cf_path), "%s/%s", g_db_path, argv[1]);

  DIR *dir = opendir(cf_path);
  if (dir == NULL) {
    printf("  Status: FAILED (cannot open directory)\n");
    return -1;
  }

  int sstable_count = 0;
  int sstable_valid = 0;
  int sstable_invalid = 0;
  int wal_count = 0;
  int wal_valid = 0;
  int wal_invalid = 0;

  struct dirent *entry;
  while ((entry = readdir(dir)) != NULL) {
    char full_path[4096];
    snprintf(full_path, sizeof(full_path), "%s/%s", cf_path, entry->d_name);

    if (strstr(entry->d_name, ".klog") != NULL) {
      sstable_count++;
      block_manager_t *bm = NULL;
      if (block_manager_open(&bm, full_path, BLOCK_MANAGER_SYNC_NONE) == 0) {
        const int block_count = block_manager_count_blocks(bm);
        if (block_count >= 0) {
          sstable_valid++;
        } else {
          sstable_invalid++;
          printf("  Invalid SSTable: %s\n", entry->d_name);
        }
        block_manager_close(bm);
      } else {
        sstable_invalid++;
        printf("  Cannot open SSTable: %s\n", entry->d_name);
      }
    } else if (strstr(entry->d_name, ".log") != NULL) {
      wal_count++;
      block_manager_t *bm = NULL;
      if (block_manager_open(&bm, full_path, BLOCK_MANAGER_SYNC_NONE) == 0) {
        const int block_count = block_manager_count_blocks(bm);
        if (block_count >= 0) {
          wal_valid++;
        } else {
          wal_invalid++;
          printf("  Invalid WAL: %s\n", entry->d_name);
        }
        block_manager_close(bm);
      } else {
        wal_invalid++;
        printf("  Cannot open WAL: %s\n", entry->d_name);
      }
    }
  }
  closedir(dir);

  printf("\nVerification Results:\n");
  printf("  SSTables: %d total, %d valid, %d invalid\n", sstable_count,
         sstable_valid, sstable_invalid);
  printf("  WAL Files: %d total, %d valid, %d invalid\n", wal_count, wal_valid,
         wal_invalid);

  if (sstable_invalid == 0 && wal_invalid == 0) {
    printf("  Status: OK\n");
    return 0;
  } else {
    printf("  Status: ISSUES FOUND\n");
    return -1;
  }
}

static int cmd_compact(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: compact <cf>\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  tidesdb_column_family_t *cf = tidesdb_get_column_family(g_db, argv[1]);
  if (cf == NULL) {
    printf("Column family '%s' not found.\n", argv[1]);
    return -1;
  }

  const int ret = tidesdb_compact(cf);
  if (ret != TDB_SUCCESS) {
    printf("Failed to trigger compaction: %s\n", error_to_string(ret));
    return ret;
  }

  printf("Compaction triggered for '%s'\n", argv[1]);
  return 0;
}

static int cmd_flush(const int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: flush <cf>\n");
    return -1;
  }

  if (g_db == NULL) {
    printf("No database is open.\n");
    return -1;
  }

  tidesdb_column_family_t *cf = tidesdb_get_column_family(g_db, argv[1]);
  if (cf == NULL) {
    printf("Column family '%s' not found.\n", argv[1]);
    return -1;
  }

  const int ret = tidesdb_flush_memtable(cf);
  if (ret != TDB_SUCCESS) {
    printf("Failed to flush memtable: %s\n", error_to_string(ret));
    return ret;
  }

  printf("Memtable flushed for '%s'\n", argv[1]);
  return 0;
}

static int execute_command(const char *line) {
  char *argv[ADMINTOOL_MAX_ARGS];
  const int argc = parse_args((char *)line, argv);

  if (argc == 0)
    return 0;

  const char *cmd = argv[0];

  if (strcmp(cmd, "help") == 0 || strcmp(cmd, "?") == 0) {
    print_usage();
    return 0;
  }
  if (strcmp(cmd, "version") == 0) {
    printf("TidesDB version %s\n", TIDESDB_VERSION);
    return 0;
  }
  if (strcmp(cmd, "quit") == 0 || strcmp(cmd, "exit") == 0) {
    return 1;
  }
  int ret = 0;

  if (strcmp(cmd, "open") == 0) {
    ret = cmd_open(argc, argv);
  } else if (strcmp(cmd, "close") == 0) {
    ret = cmd_close(argc, argv);
  } else if (strcmp(cmd, "info") == 0) {
    ret = cmd_info(argc, argv);
  } else if (strcmp(cmd, "cf-list") == 0) {
    ret = cmd_cf_list(argc, argv);
  } else if (strcmp(cmd, "cf-create") == 0) {
    ret = cmd_cf_create(argc, argv);
  } else if (strcmp(cmd, "cf-drop") == 0) {
    ret = cmd_cf_drop(argc, argv);
  } else if (strcmp(cmd, "cf-stats") == 0) {
    ret = cmd_cf_stats(argc, argv);
  } else if (strcmp(cmd, "put") == 0) {
    ret = cmd_put(argc, argv);
  } else if (strcmp(cmd, "get") == 0) {
    ret = cmd_get(argc, argv);
  } else if (strcmp(cmd, "delete") == 0) {
    ret = cmd_delete(argc, argv);
  } else if (strcmp(cmd, "scan") == 0) {
    ret = cmd_scan(argc, argv);
  } else if (strcmp(cmd, "range") == 0) {
    ret = cmd_range(argc, argv);
  } else if (strcmp(cmd, "prefix") == 0) {
    ret = cmd_prefix(argc, argv);
  } else if (strcmp(cmd, "sstable-list") == 0) {
    ret = cmd_sstable_list(argc, argv);
  } else if (strcmp(cmd, "sstable-info") == 0) {
    ret = cmd_sstable_info(argc, argv);
  } else if (strcmp(cmd, "sstable-dump") == 0) {
    ret = cmd_sstable_dump(argc, argv);
  } else if (strcmp(cmd, "sstable-stats") == 0) {
    ret = cmd_sstable_stats(argc, argv);
  } else if (strcmp(cmd, "sstable-keys") == 0) {
    ret = cmd_sstable_keys(argc, argv);
  } else if (strcmp(cmd, "sstable-checksum") == 0) {
    ret = cmd_sstable_checksum(argc, argv);
  } else if (strcmp(cmd, "sstable-dump-full") == 0) {
    ret = cmd_sstable_dump_full(argc, argv);
  } else if (strcmp(cmd, "bloom-stats") == 0) {
    ret = cmd_bloom_stats(argc, argv);
  } else if (strcmp(cmd, "wal-list") == 0) {
    ret = cmd_wal_list(argc, argv);
  } else if (strcmp(cmd, "wal-info") == 0) {
    ret = cmd_wal_info(argc, argv);
  } else if (strcmp(cmd, "wal-dump") == 0) {
    ret = cmd_wal_dump(argc, argv);
  } else if (strcmp(cmd, "wal-verify") == 0) {
    ret = cmd_wal_verify(argc, argv);
  } else if (strcmp(cmd, "wal-checksum") == 0) {
    ret = cmd_sstable_checksum(argc, argv);
  } else if (strcmp(cmd, "level-info") == 0) {
    ret = cmd_level_info(argc, argv);
  } else if (strcmp(cmd, "verify") == 0) {
    ret = cmd_verify(argc, argv);
  } else if (strcmp(cmd, "compact") == 0) {
    ret = cmd_compact(argc, argv);
  } else if (strcmp(cmd, "flush") == 0) {
    ret = cmd_flush(argc, argv);
  } else {
    printf("Unknown command: %s. Type 'help' for available commands.\n", cmd);
    ret = -1;
  }

  return ret;
}

static void interactive_mode(void) {
  char input[ADMINTOOL_MAX_INPUT];
  printf("Type 'help' for available commands, 'quit' to exit.\n\n");

  while (1) {
    if (g_db != NULL) {
      printf(ADMINTOOL_PROMPT_DB, g_db_path);
    } else {
      printf(ADMINTOOL_PROMPT);
    }
    fflush(stdout);

    if (fgets(input, sizeof(input), stdin) == NULL) {
      printf("\n");
      break;
    }

    const char *line = trim_whitespace(input);
    if (*line == '\0')
      continue;

    if (execute_command(line) == 1) {
      break;
    }
  }

  if (g_db != NULL) {
    tidesdb_close(g_db);
    g_db = NULL;
  }
}

int main(const int argc, char **argv) {
  char *db_path = NULL;
  char *command = NULL;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      print_usage();
      return 0;
    }
    if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--version") == 0) {
      printf("TidesDB version %s\n", TIDESDB_VERSION);
      return 0;
    }
    if ((strcmp(argv[i], "-d") == 0 || strcmp(argv[i], "--directory") == 0) &&
        i + 1 < argc) {
      db_path = argv[++i];
    } else if ((strcmp(argv[i], "-c") == 0 ||
                strcmp(argv[i], "--command") == 0) &&
               i + 1 < argc) {
      command = argv[++i];
    }
  }

  if (db_path != NULL) {
    char open_cmd[1024];
    snprintf(open_cmd, sizeof(open_cmd), "open %s", db_path);
    if (execute_command(open_cmd) < 0) {
      return 1;
    }
  }

  if (command != NULL) {
    char cmd_copy[ADMINTOOL_MAX_INPUT];
    strncpy(cmd_copy, command, sizeof(cmd_copy) - 1);
    cmd_copy[sizeof(cmd_copy) - 1] = '\0';
    const int ret = execute_command(cmd_copy);

    if (g_db != NULL) {
      tidesdb_close(g_db);
    }

    return (ret < 0) ? 1 : 0;
  }

  interactive_mode();
  return 0;
}