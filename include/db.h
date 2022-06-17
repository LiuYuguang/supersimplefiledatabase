/*
 * Copyright (C) 2022, LiuYuguang <l13660020946@live.com>
 */

#ifndef _DB_H_
#define _DB_H_

#include <stddef.h>     // for size_t

typedef struct db_s db_t;

#define DB_STRINGKEY 0 // 4 <= max_key_size <= 128, include '\0' 包含'\0'在内
#define DB_BYTESKEY  1 // 4 <= max_key_size <= 128
#define DB_INT32KEY  2 // max_key_size = sizeof(int32_t)
#define DB_INT64KEY  3 // max_key_size = sizeof(int64_t)

/**
 * @brief create dateabase file, mode default 0664 创建数据库
 * @param[in] path 
 * @param[in] key_type DB_STRINGKEY, DB_BYTESKEY, DB_INT32KEY, DB_INT64KEY
 * @param[in] max_key_size
 * @return ==0 if successful, ==-1 error
*/
int db_create(char *path, int key_type, size_t max_key_size);

/**
 * @brief open dateabase file 打开数据库
 * @param[in] db
 * @param[in] path
 * @return ==0 if successful, ==-1 error
*/
int db_open(db_t **db, char *path);

/**
 * @brief close dateabase file 关闭数据库
 * @param[in] db
*/
void db_close(db_t *db);

/**
 * @brief insert key 插入值
 * @param[in] db
 * @param[in] key key_size can't exceed max_key_size 需要保证key类型和创建数据库时一致
 * @param[in] value
 * @param[in] value_size value_size can't too large 不能太大
 * @return ==1 if success, ==0 if key repeat, ==-1 error
*/
int db_insert(db_t* db, void* key, void *value, size_t value_size);

/**
 * @brief delete key 删除值
 * @param[in] db
 * @param[in] key key_size can't exceed max_key_size 需要保证key类型和创建数据库时一致
 * @return ==1 if success, ==0 if key no found, ==-1 error
*/
int db_delete(db_t* db, void* key);

/**
 * @brief search key 查询值
 * @param[in] db
 * @param[in] key key_size can't exceed max_key_size 需要保证key类型和创建数据库时一致
 * @param[out] value
 * @param[in] value_size 需要保证空间足够大
 * @return >=0 if success, ==-1 error
*/
int db_search(db_t* db, void* key, void *value, size_t value_size);

#endif