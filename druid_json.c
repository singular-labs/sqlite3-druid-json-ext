/*
** Based on the csv.c extension (from sqlite.org)
**
******************************************************************************
**
** This file contains the implementation of an SQLite virtual table for
** reading DRUID result files.
**
** Usage:
**
**    .load ./druid_json
**    CREATE VIRTUAL TABLE temp.my_druid_result USING druid_json(filename=FILENAME);
**    SELECT * FROM csv;
**
**    CREATE VIRTUAL TABLE temp.my_druid_result USING druid_json(
**       filename = "../raw_result.json",
**       metrics = "clicks,impressions,cost"
**    );
**
** Some extra debugging features (used for testing virtual tables) are available
** if this module is compiled with -DSQLITE_TEST.
*/
#include <sqlite3ext.h>
#include "sqlite3.h"
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stdarg.h>
#include <ctype.h>
#include <stdio.h>
#include <stdbool.h>

#ifndef SQLITE_OMIT_VIRTUALTABLE

/*
** A macro to hint to the compiler that a function should not be
** inlined.
*/
#if defined(__GNUC__)
#  define DRUIDJSON_NOINLINE  __attribute__((noinline))
#elif defined(_MSC_VER) && _MSC_VER>=1310
#  define DRUIDJSON_NOINLINE  __declspec(noinline)
#else
#  define DRUIDJSON_NOINLINE
#endif


/* Max size of the error message in a DruidReader */
#define DRUIDJSON_MXERR 200

/* Size of the DruidReader input buffer */
#define DRUIDJSON_INBUFSZ 1024


// copied from json1.c
/*
** Growing our own isspace() routine this way is twice as fast as
** the library isspace() function, resulting in a 7% overall performance
** increase for the parser.  (Ubuntu14.10 gcc 4.8.4 x64 with -Os).
*/
static const char jsonIsSpace[] = {
        0, 0, 0, 0, 0, 0, 0, 0,     0, 1, 1, 0, 0, 1, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        1, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
};
#define safe_isspace(x) (jsonIsSpace[(unsigned char)x])

/*
 * also include ','(0x2c), '{'(0x7b), '['(0x5b)
 */
static const char jsonIsSpaceOrPrefix[] = {
        0, 0, 0, 0, 0, 0, 0, 0,     0, 1, 1, 0, 0, 1, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        1, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 1, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 1, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 1, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
};

// 01234567890.eE-
static const char jsonIsNumber[] = {
    0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 1, 1, 0,
    1, 1, 1, 1, 1, 1, 1, 1,     1, 1, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 1, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 1, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
};
#define safe_isnumber(x) (jsonIsNumber[(unsigned char)x])

#define safe_isspace_or_prefix(x) (jsonIsSpaceOrPrefix[(unsigned char)x])
#define JSON_STRING (1)
#define JSON_NUMBER (2)
#define JSON_OBJECT (3)
#define JSON_ARRAY  (4)
#define JSON_TRUE   (5)
#define JSON_FALSE  (6)
#define JSON_NULL   (7)

/* A context object used when read a Druid result file. */
typedef struct DruidReader DruidReader;
struct DruidReader {
  FILE *in;              /* Read the result JSON from this input stream */
  unsigned int file_off; /* offset inside JSON file */
  bool inside_event;     /* are we parsing nested {.., "event": {XXX}, ...} part? */
  char *label;           /* Accumulated text for a field */
  int label_n;           /* Number of bytes in label */
  int label_nAlloc;      /* Space allocated for label[] */
  char *value;           /* Accumulated value text for a field */
  int value_n;           /* Number of bytes in value */
  int value_type;        /* value json type (string, number, null, bool) */
  int value_nAlloc;      /* Space allocated for value_n[] */
  int nResult;             /* Current line number */
  int bNotFirst;         /* True if prior text has been seen */
  size_t iIn;            /* Next unread character in the input buffer */
  size_t nIn;            /* Number of characters in the input buffer */
  char *zIn;             /* The input buffer */
  char zErr[DRUIDJSON_MXERR];  /* Error message */
};

int count_string_reps(char *s,char c)
{
  int count=0;
  for(int i=0;s[i];i++){
    if(s[i]==c)
      count++;
  }
  return count;
}

/* Initialize a DruidReader object */
static void druid_reader_init(DruidReader *p){
  p->inside_event = false;
  p->file_off = 0;
  p->in = 0;
  p->label = 0;
  p->label_n = 0;
  p->label_nAlloc = 0;
  p->value = 0;
  p->value_n = 0;
  p->value_nAlloc = 0;
  p->nResult = 0;
  p->bNotFirst = 0;
  p->nIn = 0;
  p->zIn = 0;
  p->zErr[0] = 0;
}

/* Close and reset a DruidReader object */
static void druid_reader_reset(DruidReader *p){
  if( p->in ){
    fclose(p->in);
    sqlite3_free(p->zIn);
  }
  sqlite3_free(p->label);
  sqlite3_free(p->value);
  druid_reader_init(p);
}

/* Report an error on a DruidReader */
static void druid_errmsg(DruidReader *p, const char *zFormat, ...){
  va_list ap;
  va_start(ap, zFormat);
  sqlite3_vsnprintf(DRUIDJSON_MXERR, p->zErr, zFormat, ap);
  va_end(ap);
}

/* Open the file associated with a DruidReader
** Return the number of errors.
*/
static int druid_reader_open(
  DruidReader *p,               /* The reader to open */
  const char *zFilename      /* Read from this filename */
){
    p->zIn = sqlite3_malloc( DRUIDJSON_INBUFSZ );
    if( p->zIn==0 ){
      druid_errmsg(p, "out of memory");
      return 1;
    }
    p->in = fopen(zFilename, "rb");
    if( p->in==0 ){
      sqlite3_free(p->zIn);
      druid_reader_reset(p);
      druid_errmsg(p, "cannot open '%s' for reading", zFilename);
      return 1;
    }
  return 0;
}

/* The input buffer has overflowed.  Refill the input buffer, then
** return the next character
*/
static DRUIDJSON_NOINLINE void druid_getc_refill(DruidReader *p){
  size_t got;

  assert( p->iIn>=p->nIn );  /* Only called on an empty input buffer */
  assert( p->in!=0 );        /* Only called if reading froma file */

  got = fread(p->zIn, 1, DRUIDJSON_INBUFSZ, p->in);
  p->nIn = got;
  p->iIn = 0;
}

static void druid_advance_c(DruidReader *p){
  p->iIn++;
  p->file_off++;
  return;
}

static int druid_getc(DruidReader *p, bool advance, bool skip_whitespace, bool skip_prefix) {
  int ret = 0;
  while (true) {
    if (p->iIn >= p->nIn) {
      if (p->in != 0) {
        druid_getc_refill(p);
        if (p->nIn == 0) {
          return EOF;
        }
      } else {
        return EOF;
      }
    }
    ret = ((unsigned char *) p->zIn)[p->iIn];
    if (
        (skip_prefix && safe_isspace_or_prefix(ret)) ||
        (skip_whitespace && safe_isspace(ret))
        ) {
      druid_advance_c(p);
      continue;
    } else if (advance) {
      druid_advance_c(p);
    }
    return ret;
  }
}

/* Increase the size of p->z and append character c to the end. 
** Return 0 on success and non-zero if there is an OOM error */
static DRUIDJSON_NOINLINE int druid_resize_and_append(DruidReader *p, char c, char **z, int* nAlloc, int* n){
  char *zNew;
  int nNew;
  nNew = (*nAlloc)*2 + 100;
  zNew = sqlite3_realloc64(*z, nNew);
  if( zNew ){
    *z = zNew;
    *nAlloc = nNew;
    (*z)[(*n)++] = c;
    return 0;
  }else{
    druid_errmsg(p, "out of memory");
    return 1;
  }
}

/* Append a single character to the DruidReader.z[] array.
** Return 0 on success and non-zero if there is an OOM error */
static int druid_append(DruidReader *p, char c, bool is_value){
  int *n;
  int *nAlloc;
  char **z;
  if (is_value) {
    n = &(p->value_n);
    nAlloc = &(p->value_nAlloc);
    z = &(p->value);
  } else {
    n = &(p->label_n);
    nAlloc = &(p->label_nAlloc);
    z = &(p->label);
  }
  if (*n >= *nAlloc - 1) return druid_resize_and_append(p, c, z, nAlloc, n);
  (*z)[(*n)++] = c;
  return 0;
}

static bool read_string(DruidReader *p, bool is_value); // forward definition
static bool consume_literal(DruidReader *p, int cur_char, char* string){
  char* cur_pos = string;
  druid_append(p, cur_char, true);
    while(*++cur_pos){
        cur_char = druid_getc(p, true, false, false);
        druid_append(p, cur_char, true);
        if(cur_char != *cur_pos){
            druid_errmsg(p,
                       "consume_literal: result %d(offset %d): unexpected '%c' character (expected '%s')",
                       p->nResult,
                       p->file_off,
                       cur_char,
                       string);
            return false;
        }
    }
    return true;
}
static bool consume_number(DruidReader *p, int cur_char){
  druid_append(p, cur_char, true);
  cur_char = druid_getc(p, false, false, false);
  while (safe_isnumber(cur_char)) {
    druid_advance_c(p);
    druid_append(p, cur_char, true);
    cur_char = druid_getc(p, false, false, false);
  }
  return true;
}

static bool read_value(DruidReader *p){
    int c;
    c = druid_getc(p, true, true, false);
    switch(c){
        case '"':
            read_string(p, true);
            p->value_type = JSON_STRING;
            break;
        case 'n':
            if(!consume_literal(p, c, "null"))
              return false;
            p->value_type = JSON_NULL;
            break;
        case 't':
            if(!consume_literal(p, c, "true"))
              return false;
            p->value_type = JSON_TRUE;
            break;
        case 'f':
            if(!consume_literal(p, c, "false"))
              return false;
            p->value_type = JSON_FALSE;
            break;
        case '-':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
        case '.':
            consume_number(p, c);
            p->value_type = JSON_NUMBER;
            break;
        default:
            druid_errmsg(p, "read_value: result %d(offset %d): unexpected '%c' character\n",
                         p->nResult, p->file_off, c);
            return false;
    }
    druid_append(p, 0, true);
    return true;
}

#define	GOT_FIELD	(0)
#define	GOT_LAST_FIELD	(1)
#define	GOT_FAILURE	(-2)

/*
 * return -2 on failure
 * return -1 on EOF
 * return 0 when read field
 * return 1 when reading last field
 */
static int druid_read_one_field(DruidReader *p) {
    int c;
    p->label_n = 0;
    p->value_n = 0;
    c = druid_getc(p, true, false, true);
    if( c==EOF ){
      return EOF;
    }
    if( '"' != c){
      druid_errmsg(p, "result %d(offset %d): expected '\"' got '%c' character\n", p->nResult, p->file_off, c);
      return GOT_FAILURE;
    }
    if(!read_string(p, false)) {
      return GOT_FAILURE;
    }
    druid_append(p, 0, false); // NULL terminate label string
    c = druid_getc(p, true, true, false);
    if(':' != c){
        druid_errmsg(p, "result %d(offset %d): expected ':' got '%c' character\n",
                     p->nResult, p->file_off, c);
        return GOT_FAILURE;
    }
    if(0 == strcmp(p->label, "event")){
        p->inside_event = true;
        return druid_read_one_field(p);
    }
    if (!read_value(p)){
      return GOT_FAILURE;
    }
    c = druid_getc(p, true, true, false);
    if(!(',' == c || '}' == c)){
        druid_errmsg(p, "result %d(offset %d): expected ',' or '}' got '%c' character\n",
                     p->nResult, p->file_off, c);
        return GOT_FAILURE;
    }
    if('}' == c && p->inside_event){
        p->inside_event = false;
        c = druid_getc(p, false, true, false);
    }
    if('}' == c){
        druid_getc(p, true, true, false);
        p->nResult++;
        if(']' == druid_getc(p, false, true, false)){
          // consume last char in file
          druid_getc(p, true, true, false);
        }
        return GOT_LAST_FIELD;
    }
    return GOT_FIELD;
}

static bool read_string(DruidReader *p, bool is_value){
    int c;
    c = druid_getc(p, true, false, false);
    while('"'!=c){
        if ('\\' == c){
            switch(c){
                case '"':
                case '\\':
                case '/':
                    break;
                case 'b':
                    c = '\b';
                    break;
                case 'n':
                    c = '\n';
                    break;
                case 'r':
                    c = '\r';
                    break;
                case 't':
                    c = '\t';
                    break;
                case 'u':
                    // todo: handle escaped unicode
                    break;
                default:
                    druid_errmsg(p, "result %d(offset %d): unexpected escape char", p->nResult, p->file_off, c);
                    return false;
            }
        }
        druid_append(p, c, is_value);
        c = druid_getc(p, true, false, false);
    }
    return true;
}


/* Forward references to the various virtual table methods implemented
** in this file. */
static int druidtabCreate(sqlite3*, void*, int, const char*const*, 
                           sqlite3_vtab**,char**);
static int druidtabConnect(sqlite3*, void*, int, const char*const*, 
                           sqlite3_vtab**,char**);
static int druidtabBestIndex(sqlite3_vtab*,sqlite3_index_info*);
static int druidtabDisconnect(sqlite3_vtab*);
static int druidtabOpen(sqlite3_vtab*, sqlite3_vtab_cursor**);
static int druidtabClose(sqlite3_vtab_cursor*);
static int druidtabFilter(sqlite3_vtab_cursor*, int idxNum, const char *idxStr,
                          int argc, sqlite3_value **argv);
static int druidtabNext(sqlite3_vtab_cursor*);
static int druidtabEof(sqlite3_vtab_cursor*);
static int druidtabColumn(sqlite3_vtab_cursor*,sqlite3_context*,int);
static int druidtabRowid(sqlite3_vtab_cursor*,sqlite3_int64*);
static void rewindCur(DruidReader *p);

static void free_druid_metrics_names(int num_druid_metrics, char **druid_metric_names);

/* An instance of the Druid virtual table */
typedef struct DruidTable {
  sqlite3_vtab base;              /* Base class.  Must be first */
  char *zFilename;                /* Name of the CSV file */
  long iStart;                    /* Offset to start of data in zFilename */
  int nCol;                       /* Number of columns in the Druid response */
  bool* metricsCols;              /* Columns that should return REAL instead of TEXT */
  char **colNames;                /* Column names */
  unsigned int tstFlags;          /* Bit values used for testing */
} DruidTable;

/* Allowed values for tstFlags */
#define CSVTEST_FIDX  0x0001      /* Pretend that constrained searchs cost less*/

/* A cursor for the CSV virtual table */
typedef struct DruidCursor {
  sqlite3_vtab_cursor base;       /* Base class.  Must be first */
  DruidReader rdr;                  /* The DruidReader object */
  char **azVal;                   /* Value of the current row */
  int *aLen;                      /* Length of each entry */
  int *jsonType;                  /* Parsed JSON types of current row */
  sqlite3_int64 iRowid;           /* The current rowid.  Negative for EOF */
} DruidCursor;

/* Transfer error message text from a reader into a DruidTable */
static void druid_xfer_error(DruidTable *pTab, DruidReader *pRdr){
  sqlite3_free(pTab->base.zErrMsg);
  pTab->base.zErrMsg = sqlite3_mprintf("%s", pRdr->zErr);
}

/*
** This method is the destructor fo a DruidTable object.
*/
static int druidtabDisconnect(sqlite3_vtab *pVtab){
  DruidTable *p = (DruidTable*)pVtab;
  sqlite3_free(p->zFilename);
  sqlite3_free(p->metricsCols);
  if(p->colNames) {
      for (int i = 0; i < p->nCol; i++) {
          sqlite3_free(p->colNames[i]);
      }
      sqlite3_free(p->colNames);
  }
  sqlite3_free(p);
  return SQLITE_OK;
}

/* Skip leading whitespace.  Return a pointer to the first non-whitespace
** character, or to the zero terminator if the string has only whitespace */
static const char *druid_skip_whitespace(const char *z){
  while( isspace((unsigned char)z[0]) ) z++;
  return z;
}

/* Remove trailing whitespace from the end of string z[] */
static void druid_trim_whitespace(char *z){
  size_t n = strlen(z);
  while( n>0 && isspace((unsigned char)z[n]) ) n--;
  z[n] = 0;
}

/* Dequote the string */
static void druid_dequote(char *z){
  int j;
  char cQuote = z[0];
  size_t i, n;

  if( cQuote!='\'' && cQuote!='"' ) return;
  n = strlen(z);
  if( n<2 || z[n-1]!=z[0] ) return;
  for(i=1, j=0; i<n-1; i++){
    if( z[i]==cQuote && z[i+1]==cQuote ) i++;
    z[j++] = z[i];
  }
  z[j] = 0;
}

/* Check to see if the string is of the form:  "TAG = VALUE" with optional
** whitespace before and around tokens.  If it is, return a pointer to the
** first character of VALUE.  If it is not, return NULL.
*/
static const char *druid_parameter(const char *zTag, int nTag, const char *z){
  z = druid_skip_whitespace(z);
  if( strncmp(zTag, z, nTag)!=0 ) return 0;
  z = druid_skip_whitespace(z+nTag);
  if( z[0]!='=' ) return 0;
  return druid_skip_whitespace(z+1);
}

/* Decode a parameter that requires a dequoted string.
**
** Return 1 if the parameter is seen, or 0 if not.  1 is returned
** even if there is an error.  If an error occurs, then an error message
** is left in p->zErr.  If there are no errors, p->zErr[0]==0.
*/
static int druid_string_parameter(
  DruidReader *p,            /* Leave the error message here, if there is one */
  const char *zParam,      /* Parameter we are checking for */
  const char *zArg,        /* Raw text of the virtual table argment */
  char **pzVal             /* Write the dequoted string value here */
){
  const char *zValue;
  zValue = druid_parameter(zParam,(int)strlen(zParam),zArg);
  if( zValue==0 ) return 0;
  p->zErr[0] = 0;
  if( *pzVal ){
    druid_errmsg(p, "more than one '%s' parameter", zParam);
    return 1;
  }
  *pzVal = sqlite3_mprintf("%s", zValue);
  if( *pzVal==0 ){
    druid_errmsg(p, "out of memory");
    return 1;
  }
  druid_trim_whitespace(*pzVal);
  druid_dequote(*pzVal);
  return 1;
}

/*
** Parameters:
**    filename=FILENAME          Name of file containing CSV content
**    metrics=METRICS            Comma seperated list of metric names (changes the datatype from TEXT -> REAL)
**
** Only available if compiled with SQLITE_TEST:
**
**    testflags=N                Bitmask of test flags.  Optional
**
*/
static int druidtabConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  DruidTable *pNew = 0;        /* The DruidTable object to construct */
  int rc = SQLITE_OK;        /* Result code from this routine */
  int i, j;                  /* Loop counters */
#ifdef SQLITE_TEST
  int tstFlags = 0;          /* Value for testflags=N parameter */
#endif
  int b;                     /* Value of a boolean parameter */
  int nCol = -99;            /* Value of the columns= parameter */
  int read_field_ret;
  DruidReader sRdr;            /* A CSV file reader used to store an error
                             ** message and/or to count the number of columns */
  static const char *azParam[] = {
     "filename", "metrics",
  };
  char *azPValue[2];         /* Parameter values */
  char *schema;
  int num_druid_metrics=0;
  char **druid_metric_names;
# define DRUID_FILENAME (azPValue[0])
# define DRUID_METRICS   (azPValue[1])


  assert( sizeof(azPValue)==sizeof(azParam) );
  memset(&sRdr, 0, sizeof(sRdr));
  memset(azPValue, 0, sizeof(azPValue));
  for(i=3; i<argc; i++){
    const char *z = argv[i];
    const char *zValue;
    for(j=0; j<sizeof(azParam)/sizeof(azParam[0]); j++){
      if( druid_string_parameter(&sRdr, azParam[j], z, &azPValue[j]) ) break;
    }
    if( j<sizeof(azParam)/sizeof(azParam[0]) ){
      if( sRdr.zErr[0] ) goto csvtab_connect_error;
    }else
    {
      druid_errmsg(&sRdr, "bad parameter: '%s'", z);
      goto csvtab_connect_error;
    }
  }
  if( DRUID_FILENAME==0 ){
    druid_errmsg(&sRdr, "must specify either filename= ");
    goto csvtab_connect_error;
  }
  if(DRUID_METRICS != 0){
    num_druid_metrics = count_string_reps(DRUID_METRICS, ',') + 1;
    druid_metric_names = sqlite3_malloc(sizeof (char*) * num_druid_metrics);
    memset(druid_metric_names, 0, sizeof (char*) * num_druid_metrics);
    i=0;
    int prev_start = 0;
    int pos = 0;
    int cur_len = 0;
    for(pos=0;DRUID_METRICS[pos];pos++){
      if(DRUID_METRICS[pos]==','){
        cur_len = pos - prev_start;
        druid_metric_names[i] = sqlite3_malloc(sizeof(char) * cur_len + 1);
        memcpy(druid_metric_names[i], &DRUID_METRICS[prev_start], cur_len);
        druid_metric_names[i][cur_len] = 0;
        prev_start = pos + 1;
        i++;
      }
    }
    cur_len = pos - prev_start;
    druid_metric_names[i] = sqlite3_malloc(sizeof(char) * cur_len + 1);
    memcpy(druid_metric_names[i], &DRUID_METRICS[prev_start], cur_len);
    druid_metric_names[i][cur_len] = 0;
    assert(i+1==num_druid_metrics);
  }

  if(druid_reader_open(&sRdr, DRUID_FILENAME)){
    goto csvtab_connect_error;
  }
  pNew = sqlite3_malloc( sizeof(*pNew) );
  *ppVtab = (sqlite3_vtab*)pNew;
  if( pNew==0 ) goto csvtab_connect_oom;
  memset(pNew, 0, sizeof(*pNew));

  sqlite3_str *pStr = sqlite3_str_new(0);
  char *zSep = "";
  int iCol = 0;
  sqlite3_str_appendf(pStr, "CREATE TABLE x(");
  // count columns
  nCol = 0;
  do{
    read_field_ret = druid_read_one_field(&sRdr);
    nCol++;
  }while( GOT_FIELD == read_field_ret);
  rewindCur(&sRdr);

  pNew->colNames = sqlite3_malloc(sizeof(char*) * nCol);
  memset(pNew->colNames, 0, sizeof(char*) * nCol);
  pNew->metricsCols = sqlite3_malloc(sizeof(bool) * nCol);
  memset(pNew->metricsCols, 0, sizeof(bool) * nCol);

  // read header
  do{
    read_field_ret = druid_read_one_field(&sRdr);
    if( nCol>0 && iCol<nCol ){
      bool is_metric = 0;
      for(i=0;i<num_druid_metrics;i++){
        if(0 == strcmp(druid_metric_names[i], sRdr.label)){
          is_metric = 1;
          break;
        }
      }
      if(is_metric){
          sqlite3_str_appendf(pStr,"%s\"%w\" REAL", zSep, sRdr.label);
          pNew->metricsCols[iCol] = 1;
      }else{
          sqlite3_str_appendf(pStr,"%s\"%w\" TEXT", zSep, sRdr.label);
      }
      zSep = ",";
      pNew->colNames[iCol] = sqlite3_malloc(sizeof(char) * (strlen(sRdr.label) + 1));
      strcpy(pNew->colNames[iCol], sRdr.label);
      iCol++;
    }
  }while( GOT_FIELD == read_field_ret );
    rewindCur(&sRdr);
  pNew->nCol = nCol;
  sqlite3_str_appendf(pStr, ")");
  schema = sqlite3_str_finish(pStr);

  if( schema==0 ) goto csvtab_connect_oom;

  pNew->zFilename = DRUID_FILENAME;  DRUID_FILENAME = 0;
#ifdef SQLITE_TEST
  pNew->tstFlags = tstFlags;
#endif
  // set iStart after header
  pNew->iStart = (int)(ftell(sRdr.in) - sRdr.nIn + sRdr.iIn);
  druid_reader_reset(&sRdr);
  rc = sqlite3_declare_vtab(db, schema);
  if( rc ){
    druid_errmsg(&sRdr, "bad schema: '%s' - %s", schema, sqlite3_errmsg(db));
    goto csvtab_connect_error;
  }
  for(i=0; i<sizeof(azPValue)/sizeof(azPValue[0]); i++){
    sqlite3_free(azPValue[i]);
  }
  /* Rationale for DIRECTONLY:
  ** An attacker who controls a database schema could use this vtab
  ** to exfiltrate sensitive data from other files in the filesystem.
  ** And, recommended practice is to put all CSV virtual tables in the
  ** TEMP namespace, so they should still be usable from within TEMP
  ** views, so there shouldn't be a serious loss of functionality by
  ** prohibiting the use of this vtab from persistent triggers and views.
  */
  sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY);
  free_druid_metrics_names(num_druid_metrics, druid_metric_names);
  return SQLITE_OK;

csvtab_connect_oom:
  rc = SQLITE_NOMEM;
  druid_errmsg(&sRdr, "out of memory");

csvtab_connect_error:

  free_druid_metrics_names(num_druid_metrics, druid_metric_names);

  if( pNew ) druidtabDisconnect(&pNew->base);
  for(i=0; i<sizeof(azPValue)/sizeof(azPValue[0]); i++){
    sqlite3_free(azPValue[i]);
  }
  if( sRdr.zErr[0] ){
    sqlite3_free(*pzErr);
    *pzErr = sqlite3_mprintf("%s", sRdr.zErr);
  }
  druid_reader_reset(&sRdr);
  if( rc==SQLITE_OK ) rc = SQLITE_ERROR;
  return rc;
}

static void free_druid_metrics_names(int num_druid_metrics, char **druid_metric_names) {
  if(num_druid_metrics){
    for(int i=0;i<num_druid_metrics;i++){
      sqlite3_free(druid_metric_names[i]);
    }
    sqlite3_free(druid_metric_names);
  }
}

/*
** Reset the current row content held by a DruidCursor.
*/
static void csvtabCursorRowReset(DruidCursor *pCur){
  DruidTable *pTab = (DruidTable*)pCur->base.pVtab;
  int i;
  for(i=0; i<pTab->nCol; i++){
    sqlite3_free(pCur->azVal[i]);
    pCur->azVal[i] = 0;
    pCur->aLen[i] = 0;
    pCur->jsonType[i] = JSON_NULL;
  }
}

/*
** The xConnect and xCreate methods do the same thing, but they must be
** different so that the virtual table is not an eponymous virtual table.
*/
static int druidtabCreate(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
 return druidtabConnect(db, pAux, argc, argv, ppVtab, pzErr);
}

/*
** Destructor for a DruidCursor.
*/
static int druidtabClose(sqlite3_vtab_cursor *cur){
  DruidCursor *pCur = (DruidCursor*)cur;
  csvtabCursorRowReset(pCur);
  druid_reader_reset(&pCur->rdr);
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Constructor for a new DruidTable cursor object.
*/
static int druidtabOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  DruidTable *pTab = (DruidTable*)p;
  DruidCursor *pCur;
  size_t nByte;
  nByte = sizeof(*pCur) + (sizeof(char*)+sizeof(int)+sizeof(int))*pTab->nCol;
  pCur = sqlite3_malloc64( nByte );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, nByte);
  pCur->azVal = (char**)&pCur[1];
  pCur->aLen = (int*)&pCur->azVal[pTab->nCol];
  pCur->jsonType = (int*)&pCur->aLen[pTab->nCol];
  *ppCursor = &pCur->base;
  if(druid_reader_open(&pCur->rdr, pTab->zFilename) ){
    druid_xfer_error(pTab, &pCur->rdr);
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}


/*
** Advance a DruidCursor to its next row of input.
** Set the EOF marker if we reach the end of input.
*/
static int druidtabNext(sqlite3_vtab_cursor *cur){
  DruidCursor *pCur = (DruidCursor*)cur;
  DruidTable *pTab = (DruidTable*)cur->pVtab;
  int i = 0;
  int druid_field_ret;
  char *z;
  do{
    druid_field_ret = druid_read_one_field(&pCur->rdr);
    if( druid_field_ret < 0){
      break;
    }
    if( i<pTab->nCol ){
      if( pCur->aLen[i] < pCur->rdr.value_n + 1 ){
        char *zNew = sqlite3_realloc64(pCur->azVal[i], pCur->rdr.value_n + 1);
        if( zNew==0 ){
          druid_errmsg(&pCur->rdr, "out of memory");
          druid_xfer_error(pTab, &pCur->rdr);
          break;
        }
        pCur->azVal[i] = zNew;
        pCur->aLen[i] = pCur->rdr.value_n + 1;
      }
      memcpy(pCur->azVal[i], pCur->rdr.value, pCur->rdr.value_n + 1);
      if(0 != strcmp(pCur->rdr.label, pTab->colNames[i])){
        druid_errmsg(&pCur->rdr, "result %d(offset %d): druid json order change is not supported",
                     pCur->rdr.nResult, pCur->rdr.file_off);
        sqlite3_free(pTab->base.zErrMsg);
        pTab->base.zErrMsg = sqlite3_mprintf("%s", pCur->rdr.zErr);
        return SQLITE_ERROR;
      }
      pCur->jsonType[i] = pCur->rdr.value_type;
      i++;
    }
  }while( GOT_FIELD == druid_field_ret);
  if( GOT_FAILURE == druid_field_ret || (druid_field_ret == EOF && i<pTab->nCol) ){
    pCur->iRowid = -1;
  }else{
    pCur->iRowid++;
    while( i<pTab->nCol ){
      sqlite3_free(pCur->azVal[i]);
      pCur->azVal[i] = 0;
      pCur->aLen[i] = 0;
      pCur->jsonType[i] = JSON_NULL;
      i++;
    }
  }
  if (GOT_FAILURE == druid_field_ret) {
    sqlite3_free(pTab->base.zErrMsg);
    pTab->base.zErrMsg = sqlite3_mprintf("%s", pCur->rdr.zErr);
    return SQLITE_ERROR;
  }
  else
    return SQLITE_OK;
}

/*
** Return values of columns for the row at which the DruidCursor
** is currently pointing.
*/
static int druidtabColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  double r;
  DruidCursor *pCur = (DruidCursor*)cur;
  DruidTable *pTab = (DruidTable*)cur->pVtab;
  if( i>=0 && i<pTab->nCol && pCur->azVal[i]!=0 ){
   if(pTab->metricsCols[i]){
       switch(pCur->jsonType[i]){
         case JSON_NUMBER:
           r = strtod(pCur->azVal[i], 0);
           sqlite3_result_double(ctx, r);
           break;
         case JSON_NULL:
           sqlite3_result_null(ctx);
           break;
         default:
           druid_errmsg(&pCur->rdr,
                        "unexpected JSON value inside a metric, got %s='%s', expected JSON_NUMBER / JSON_NULL",
                        pTab->colNames[i],
                        pCur->azVal[i]
                        );
           sqlite3_free(pTab->base.zErrMsg);
           pTab->base.zErrMsg = sqlite3_mprintf("%s", pCur->rdr.zErr);
           return SQLITE_ERROR;
       }
   }else{
       sqlite3_result_text(ctx, pCur->azVal[i], -1, SQLITE_TRANSIENT);
   }


  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.
*/
static int druidtabRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  DruidCursor *pCur = (DruidCursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int druidtabEof(sqlite3_vtab_cursor *cur){
  DruidCursor *pCur = (DruidCursor*)cur;
  return pCur->iRowid<0;
}

static void rewindCur(DruidReader *p){
    fseek(p->in, 0, SEEK_SET);
    p->iIn = 0;
    p->nIn = 0;
    p->file_off = 0;
}

/*
** Only a full table scan is supported.  So xFilter simply rewinds to
** the beginning.
*/
static int druidtabFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  DruidCursor *pCur = (DruidCursor*)pVtabCursor;
  rewindCur(&(pCur->rdr));
  return druidtabNext(pVtabCursor);
}

/*
** Only a forward full table scan is supported.  xBestIndex is mostly
** a no-op.
*/
static int druidtabBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  pIdxInfo->estimatedCost = 1000000;
  return SQLITE_OK;
}


static sqlite3_module DruidJsonModule = {
  0,                       /* iVersion */
  druidtabCreate,            /* xCreate */
  druidtabConnect,           /* xConnect */
  druidtabBestIndex,         /* xBestIndex */
  druidtabDisconnect,        /* xDisconnect */
  druidtabDisconnect,        /* xDestroy */
  druidtabOpen,              /* xOpen - open a cursor */
  druidtabClose,             /* xClose - close a cursor */
  druidtabFilter,            /* xFilter - configure scan constraints */
  druidtabNext,              /* xNext - advance a cursor */
  druidtabEof,               /* xEof - check for end of scan */
  druidtabColumn,            /* xColumn - read data */
  druidtabRowid,             /* xRowid - read data */
  0,                       /* xUpdate */
  0,                       /* xBegin */
  0,                       /* xSync */
  0,                       /* xCommit */
  0,                       /* xRollback */
  0,                       /* xFindMethod */
  0,                       /* xRename */
};

#endif /* !defined(SQLITE_OMIT_VIRTUALTABLE) */


#ifdef _WIN32
__declspec(dllexport)
#endif
/* 
** This routine is called when the extension is loaded.  The new
** CSV virtual table module is registered with the calling database
** connection.
*/
int sqlite3_druidjson_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
#ifndef SQLITE_OMIT_VIRTUALTABLE
  int rc;
  SQLITE_EXTENSION_INIT2(pApi);
  rc = sqlite3_create_module(db, "druid_json", &DruidJsonModule, 0);
#ifdef SQLITE_TEST
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_module(db, "csv_wr", &DruidJsonModuleFauxWrite, 0);
  }
#endif
  return rc;
#else
  return SQLITE_OK;
#endif
}
