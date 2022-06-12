/*
 * Copyright (C) 2022, LiuYuguang <l13660020946@live.com>
 */

#ifndef _DB_H_
#define _DB_H_

#include <stddef.h>     // for size_t

typedef struct db_s db_t;

#define DB_STRINGKEY 0
#define DB_BYTESKEY  1
#define DB_INT32KEY  2
#define DB_INT64KEY  3

/**
 * @brief create dateabase file, mode default 0664
 * @param[in] path 
 * @param[in] key_type DB_STRINGKEY, DB_BYTESKEY, DB_INT32KEY, DB_INT64KEY
 * @param[in] max_key_size DB_INT32KEY: sizeof(int32_t), DB_INT64KEY: sizeof(int64_t), DB_STRINGKEY: 4 <= strlen() < 128, DB_BYTESKEY: 4 <= size <= 128
 * @return ==0 if successful, ==-1 error
*/
int db_create(char *path, int key_type, size_t max_key_size);

/**
 * @brief open dateabase file
 * @param[in] db
 * @param[in] path
 * @return ==0 if successful, ==-1 error
*/
int db_open(db_t **db, char *path);

/**
 * @brief verify if dateabase file data correct
 * @param[in] db
 * @return ==0 if successful, ==-1 error
*/
int db_verify(db_t *db);

/**
 * @brief close dateabase file
 * @param[in] db
*/
void db_close(db_t *db);

/**
 * @brief insert key
 * @param[in] db
 * @param[in] key key_size can't exceed max_key_size
 * @param[in] value
 * @param[in] value_size value_size can't too large
 * @param[in] overwrite 0 no overwrite, else overwrite
 * @return ==0 if successful, ==-1 error
*/
int db_insert(db_t* db, void* key, void *value, size_t value_size, int overwrite);

/**
 * @brief delete key
 * @param[in] db
 * @param[in] key key_size can't exceed max_key_size
 * @param[out] value, 
 * @param[in] value_size 
 * @return ==0 if successful, ==-1 error
*/
int db_delete(db_t* db, void* key, void *value, size_t value_size);

/**
 * @brief search key
 * @param[in] db
 * @param[in] key key_size can't exceed max_key_size
 * @param[out] value
 * @param[in] value_size
 * @return ==0 if successful, ==-1 error
*/
int db_search(db_t* db, void* key, void *value, size_t value_size);

#endif