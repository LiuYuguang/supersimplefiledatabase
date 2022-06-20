/*
 * Copyright (C) 2022, LiuYuguang <l13660020946@live.com>
 */
/**
 * @brief 超简单的文件数据库
 */

#include <stddef.h>            // for size_t
#include <stdlib.h>            // for malloc(), free()
#include <string.h>            // for memcpy(), memmove(), strcpy(), strlen()
#include <fcntl.h>             // for open(), O_CREAT, O_RDWR, O_TRUNC
#include <sys/stat.h>          // for struct stat, fstat()
#include <unistd.h>            // for pread(), pwrite(), access(), ftruncate(), close()
#include <stdint.h>            // for int32_t
#include <errno.h>             // for E2BIG

#define DB_HEAD_SIZE  (4096UL) // head size must be pow of 2! 文件数据库的头大小
#define DB_BLOCK_SIZE (8192UL) // block size must be pow of 2! 文件数据库的数据块大小

#define BTREE_NON_LEAF 0 /** 非叶子节点 */
#define BTREE_LEAF     1 /** 叶子节点 */

/**
 * @brief 存储数据对齐方式
 */
#define DB_ALIGNMENT       16
#define db_align(d, a)     (((d) + (a - 1)) & ~(a - 1))

#define ceil(M) (((M)-1)/2)

/**
 * @brief 数据块类型
 */
#define TYPE_KEY   0
#define TYPE_VALUE 1

/**
 * @brief 创建数据库时，指定的key类型
 */
#define DB_STRINGKEY 0 /** 4 <= max_key_size <= 128, include '\0', 包含'\0'在内 */
#define DB_BYTESKEY  1 /** 4 <= max_key_size <= 128 */
#define DB_INT32KEY  2 /** max_key_size = sizeof(int32_t) */
#define DB_INT64KEY  3 /** max_key_size = sizeof(int64_t) */

typedef struct{
    off_t value;
    off_t child;
    unsigned char key[0];
}btree_key;

typedef struct{
    size_t size;
    unsigned char value[0];
}btree_value;

/**
 * @brief 文件数据库的数据块的头
 */
typedef struct{
    off_t self;       /** 数据块位置 */
    size_t num;       /** 当前数据块作为btree_key时，表示btree节点的关键字数；当前数据块作为btree_value时，表示块的引用数 */
    off_t free;       /** 空闲链表节点 */
    uint32_t use:1;   /** 当前数据块是否被使用 */
    uint32_t type:1;  /** 当前数据块作为btree_key或btree_value */
    uint32_t leaf:1;  /** 当前数据块作为btree_key时，表示节点为叶子节点或非叶子节点 */
    uint32_t last:29; /** 当前数据块作为btree_value时，表示数据块未分配的空间 */
}btree_node;

#define btree_key_ptr(db,node,n) ((btree_key*)((char *)(node) + sizeof(*node) + (db->key_align) * (n)))
#define btree_value_ptr(node,n) ((btree_value*)((char *)(node) + (n)))

/**
 * @brief 文件数据库的头，即是句柄
 */
typedef struct db_s{
    int fd;                             /** 文件句柄 */
    int key_type;                       /** key类型，必须在创建文件数据库时指定 */
    size_t key_size;                    /** key的最大长度 */
    size_t key_align;                   /** 对齐，值 = db_align(sizeof(btree_key) + key_size, DB_ALIGNMENT) */
    size_t M;                           /** Btree 节点child的最大值 */
    size_t key_total;                   /** 已存储的key总数 */
    size_t key_use_block;               /** 数据块为btree_key类型的总数 */
    size_t value_use_block;             /** 数据块为btree_value类型的总数 */ 
    off_t free;                         /** 空闲链表的头 */
    off_t current;                      /** 当前作为btree_value的数据块，未用完分配空间 */
    int (*key_cmp)(void*,void*,size_t); /** key比较方式 */
}db_t;

static int cmp_string(void *a, void *b, size_t n){
    return strncmp((char*)a, (char*)b, n);
}

static int cmp_bytes(void *a, void *b, size_t n){
    return memcmp(a, b, n);
}

static int cmp_int32(void *a, void *b, size_t n){
    return *(int32_t*)a - *(int32_t*)b;
}

static int cmp_int64(void *a, void *b, size_t n){
    return *(int64_t*)a - *(int64_t*)b;
}

/** 
 * @brief 读出文件数据库的头，即是数据库句柄
*/
inline static ssize_t head_seek(db_t *db){
    int fd = db->fd;
    int (*key_cmp)(void*,void*,size_t) = db->key_cmp;
    ssize_t rc = pread(fd,db,DB_HEAD_SIZE,0);
    db->fd = fd;
    db->key_cmp = key_cmp;
    return rc;
}

/** 
 * @brief 写入文件数据库的头，即是数据库句柄
*/
inline static ssize_t head_flush(db_t *db){
    return pwrite(db->fd,db,DB_HEAD_SIZE,0);
}

/** 
 * @brief 读出文件数据库的数据块
*/
inline static ssize_t node_seek(db_t *db, btree_node* node, off_t offset){
    return pread(db->fd,node,DB_BLOCK_SIZE,offset);
}

/** 
 * @brief 写入文件数据库的数据块
*/
inline static ssize_t node_flush(db_t *db, btree_node *node){
    return pwrite(db->fd,node,DB_BLOCK_SIZE,node->self);
}

/** 
 * @brief 分配文件数据库的数据块
*/
inline static int node_create(db_t *db, btree_node* node, int leaf, int type){
    if(db->free != 0L){
        // 空闲链表不为空
        node_seek(db,node,db->free);
        db->free = node->free;
    }else{
        // 空闲链表为空，在文件尾追加
        struct stat stat;
        fstat(db->fd,&stat);
        memset(node,0,DB_BLOCK_SIZE);
        node->self = stat.st_size;
        if(node_flush(db,node) != DB_BLOCK_SIZE){
            // 存储空间不够时，只会写入部分数据
            ftruncate(db->fd, stat.st_size);
            errno = ENOMEM;
            return -1;
        }
    }
    if(type == TYPE_KEY){
        db->key_use_block++;
    }else{
        db->value_use_block++;
        db->current = node->self;
    }
    head_flush(db);

    node->num = 0;
    node->free = 0;
    node->leaf = leaf;
    node->use = 1;
    node->type = type;
    node->last = sizeof(btree_node);
    memset((char*)node + sizeof(btree_node), 0, DB_BLOCK_SIZE-sizeof(btree_node));
    return node_flush(db,node);
}

/** 
 * @brief 释放文件数据库的数据块
*/
inline static void node_destroy(db_t *db, btree_node *node){
    node->free = db->free;
    db->free = node->self;// 加入空闲链表中
    node->num = 0;
    node->use = 0;
    if(node->type == TYPE_KEY){
        db->key_use_block--;
    }else{
        db->value_use_block--;
    }
    head_flush(db);
    node_flush(db,node);
    return;
}

/**
 * @brief create dateabase file, mode default 0664 创建数据库
 * @param[in] path 数据库文件路径
 * @param[in] key_type DB_STRINGKEY, DB_BYTESKEY, DB_INT32KEY, DB_INT64KEY
 * @param[in] max_key_size key长度最大值
 * @return ==0 if successful, ==-1 error
*/
int db_create(char *path, int key_type, size_t max_key_size){
    switch (key_type)
    {
    case DB_STRINGKEY:
    case DB_BYTESKEY:
        if(max_key_size < 4UL || max_key_size > 128UL){
            errno = EINVAL;
            return -1;
        }
        break;
    case DB_INT32KEY:
        if(max_key_size != sizeof(int32_t)){
            errno = EINVAL;
            return -1;
        }
        break;
    case DB_INT64KEY:
        if(max_key_size != sizeof(int64_t)){
            errno = EINVAL;
            return -1;
        }
        break;
    default:
        errno = EINVAL;
        return -1;
        break;
    }

    size_t key_align = db_align(sizeof(btree_key) + max_key_size,DB_ALIGNMENT);

    if(DB_BLOCK_SIZE < sizeof(btree_node) + key_align){
        errno = EINVAL;
        return -1;
    }

    // 需要预留一个位置，比如M=5（关键字个数是4），分裂后变成左2右1，右插入变成左2右2，合并后变成M=6（关键字个数变成了5）
    size_t M = (DB_BLOCK_SIZE - sizeof(btree_node))/key_align - 1;
    if(M < 3){
        errno = EINVAL;
        return -1;
    }

    if(access(path,F_OK) == 0){
        errno = EEXIST;
        return -1;
    }

    int fd = open(path,O_CREAT|O_RDWR|O_TRUNC,0664);
    if(fd == -1){
        return -1;
    }

    char *buf = calloc(DB_HEAD_SIZE + DB_BLOCK_SIZE, sizeof(char));
    if(buf == NULL){
        close(fd);
        return -1;
    }

    db_t *db = (db_t *)buf;
    db->fd = fd;
    db->key_type = key_type;
    db->key_size = max_key_size;
    db->key_align = key_align;
    db->M = M;
    db->key_total = 0;
    db->key_use_block = 1;// Btree根的数据块，绝对不会被释放
    db->value_use_block = 0;
    db->free = 0;
    db->current = 0;

    if(head_flush(db) != DB_HEAD_SIZE){
        close(fd);
        free(buf);
        return -1;
    }

    btree_node *root = (btree_node *)(buf + DB_HEAD_SIZE);
    root->self = DB_HEAD_SIZE;
    root->leaf = BTREE_LEAF;
    root->use = 1;// Btree根的数据块，绝对不会被释放
    if(node_flush(db, root) != DB_BLOCK_SIZE){
        close(fd);
        free(buf);
        return -1;
    }

    close(fd);
    free(buf);
    return 0;
}

/**
 * @brief 校验数据库的一致性
 * @param[in] db 数据库句柄
 * @return ==0 if successful, ==-1 error
*/
int db_checker(db_t *db){
    struct stat stat;
    if(fstat(db->fd, &stat) == -1){
        return -1;
    }

    if(stat.st_size < DB_HEAD_SIZE + DB_BLOCK_SIZE || (stat.st_size-DB_HEAD_SIZE)%DB_BLOCK_SIZE != 0){
        return -1;
    }
    
    switch (db->key_type)
    {
    case DB_STRINGKEY:
    case DB_BYTESKEY:
        if(db->key_size < 4UL || db->key_size > 128UL){
            return -1;
        }
        break;
    case DB_INT32KEY:
        if(db->key_size != sizeof(int32_t)){
            return -1;
        }
        break;
    case DB_INT64KEY:
        if(db->key_size != sizeof(int64_t)){
            return -1;
        }
        break;
    default:
        return -1;
        break;
    }

    if(db->key_align != db_align(sizeof(btree_key) + db->key_size,DB_ALIGNMENT)){
        return -1;
    }

    if(db->M != (DB_BLOCK_SIZE - sizeof(btree_node))/db->key_align - 1){
        return -1;
    }

    // 校验每个数据块的数目是否一致
    btree_node *node = (btree_node *)((char*)db + DB_HEAD_SIZE + DB_BLOCK_SIZE * 0);
    off_t i;
    size_t key_total=0,value_total=0,key_use_block=0,value_use_block=0;
    for(i=DB_HEAD_SIZE;i<stat.st_size;i+=DB_BLOCK_SIZE){
        node_seek(db,node,i);
        if(node->self != i){
            return -1;
        }
        if(node->use){
            if(node->type == TYPE_KEY){
                key_total += node->num;
                key_use_block++;
            }else{
                value_total += node->num;
                value_use_block++;
            }
        }
    }
    if(key_total != value_total 
        || key_total != db->key_total
        || key_use_block != db->key_use_block
        || value_use_block != db->value_use_block
    ){
        return -1;
    }
    return 0;
}

/**
 * @brief open dateabase file 打开数据库
 * @param[out] db 数据库句柄
 * @param[in] path 数据库文件路径
 * @return ==0 if success, ==-1 error
*/
int db_open(db_t **db, char *path){
    int fd = open(path, O_RDWR);
    if(fd == -1){
        return -1;
    }

    *db = malloc(DB_HEAD_SIZE + DB_BLOCK_SIZE * 5);
    if(*db == NULL){
        close(fd);
        return -1;
    }
    
    (*db)->fd = fd;

    head_seek(*db);
    // 校验数据库
    if(db_checker(*db) == -1){
        close(fd);
        free(*db);
        return -1;
    }

    switch ((*db)->key_type)
    {
    case DB_STRINGKEY:
        (*db)->key_cmp = cmp_string;
        break;
    case DB_BYTESKEY:
        (*db)->key_cmp = cmp_bytes;
        break;
    case DB_INT32KEY:
        (*db)->key_cmp = cmp_int32;
        break;
    case DB_INT64KEY:
        (*db)->key_cmp = cmp_int64;
        break;
    default:
        (*db)->key_cmp = NULL;
        break;
    }

    return 0;
}

/**
 * @brief close dateabase file 关闭数据库
 * @param[in] db 数据库句柄
*/
void db_close(db_t *db){
    close(db->fd);
    free(db);
}

inline static int key_binary_search(db_t *db, btree_node *node, void* target)
{
    int low = 0, high = node->num - 1, mid, rc;
    while (low <= high) {
        mid = low + (high - low) / 2;
        rc = db->key_cmp(target, btree_key_ptr(db,node,mid)->key, db->key_size);
        if(rc == 0){
            return mid;
        }else if(rc > 0){
            low = mid + 1;
        }else{
            high = mid - 1;
        }
    }
    return -low-1;
}

#define keycpy(db,dest,src,n) memmove(dest,src,(db)->key_align * ((n)+1));// 需要包括 src[n]->child

/**
 * @brief 分裂Btree节点
 * 将sub_x分裂，一半给sub_y，中间上升到node[position]
 * @param db 
 * @param node 
 * @param position 
 * @param sub_x node->child[position] = sub_x
 * @param sub_y node->child[position+1] = sub_y
 */
inline static void btree_split_child(db_t* db, btree_node *node, int position, btree_node *sub_x, btree_node *sub_y){
    size_t n = ceil(db->M);

    keycpy(db, btree_key_ptr(db, sub_y, 0), btree_key_ptr(db, sub_x, n+1), sub_x->num-n-1);
    sub_y->num = sub_x->num - n - 1;
    sub_x->num = n;

    keycpy(db, btree_key_ptr(db, node, position+1), btree_key_ptr(db, node, position), node->num - position);
    memcpy(btree_key_ptr(db, node, position), btree_key_ptr(db, sub_x, n), db->key_align);
    btree_key_ptr(db, node, position)->child = sub_x->self;
    btree_key_ptr(db, node, position+1)->child = sub_y->self;
    node->num++;

    node_flush(db, node);
    node_flush(db, sub_x);
    node_flush(db, sub_y);
}

/**
 * @brief 合并Btree节点
 * 将sub_x和sub_y和node[position]合并
 * @param db 
 * @param node 
 * @param position 
 * @param sub_x node->child[position] = sub_x
 * @param sub_y node->child[position+1] = sub_y
 */
inline static int btree_merge(db_t* db, btree_node *node, int position, btree_node *sub_x, btree_node *sub_y){
    memcpy(btree_key_ptr(db, sub_x, sub_x->num)->key, btree_key_ptr(db, node, position)->key, db->key_align - sizeof(btree_key));
    btree_key_ptr(db, sub_x, sub_x->num)->value = btree_key_ptr(db, node, position)->value;

    keycpy(db, btree_key_ptr(db, sub_x, sub_x->num+1), btree_key_ptr(db, sub_y, 0), sub_y->num);    
    sub_x->num += ( 1 + sub_y->num );

    keycpy(db, btree_key_ptr(db, node, position), btree_key_ptr(db, node, position+1), node->num - position - 1);
    btree_key_ptr(db, node, position)->child = sub_x->self;
    node->num--;

    node_destroy(db, sub_y);

    if(node->num != 0){
        node_flush(db, sub_x);
        node_flush(db, node);
        return 0;
    }else{
        // must be root
        node->num = sub_x->num;
        node->leaf = sub_x->leaf;
        memcpy((char*)node + sizeof(btree_node),(char*)sub_x + sizeof(btree_node),DB_BLOCK_SIZE - sizeof(btree_node));
        node_destroy(db, sub_x);
        node_flush(db, node);
        return 1;
    }
}

/**
 * @brief insert key 插入值
 * @param[in] db 数据库句柄
 * @param[in] key key_size can't exceed max_key_size 需要保证key类型和创建数据库时一致
 * @param[in] value
 * @param[in] value_size value_size can't too large 不能太大
 * @return ==1 if success, ==0 if key repeat, ==-1 error
*/
int db_insert(db_t* db, void* key, void *value, size_t value_size){
    if(db->key_type == DB_STRINGKEY && strlen((char*)key) >= db->key_size){
        errno = EINVAL;
        return -1;
    }

    if(sizeof(btree_node) + db_align(sizeof(btree_value) + value_size, DB_ALIGNMENT) > DB_BLOCK_SIZE){
        errno = E2BIG;
        return -1;
    }

    int i,rc;
    btree_node *node = (btree_node *)((char*)db + DB_HEAD_SIZE + DB_BLOCK_SIZE * 0);
    btree_node *sub_x = (btree_node *)((char*)db + DB_HEAD_SIZE + DB_BLOCK_SIZE * 1);
    btree_node *sub_y = (btree_node *)((char*)db + DB_HEAD_SIZE + DB_BLOCK_SIZE * 2);
    btree_node *valnode = (btree_node *)((char*)db + DB_HEAD_SIZE + DB_BLOCK_SIZE * 3);
    // 关键字只会插入到叶子节点，在由上往下的遍历中，需要将已满的节点分裂
    /*     node       */
    /*    /    \      */
    /*  sub_x  sub_y  */
    
    node_seek(db, node, DB_HEAD_SIZE);// root 读取根节点

    if(node->num >= db->M-1){
        // root is full 根节点已满
        
        if(node_create(db, sub_x, node->leaf, TYPE_KEY) == -1 || node_create(db, sub_y, node->leaf, TYPE_KEY) == -1){
            return -1;
        }

        sub_x->num = node->num;
        memcpy((char*)sub_x + sizeof(btree_node), (char*)node + sizeof(btree_node), DB_BLOCK_SIZE - sizeof(btree_node));

        node->num = 0;
        node->leaf = BTREE_NON_LEAF;
        btree_key_ptr(db,node,0)->child = sub_x->self;

        btree_split_child(db, node, 0, sub_x, sub_y);
    }
    
    while(node->leaf == BTREE_NON_LEAF){
        i = key_binary_search(db, node, key);
        if(i >= 0){
            // 关键字已存在
            return 0;
        }

        i = -(i+1);
        
        // 需要判断子节点是否已满
        node_seek(db, sub_x, btree_key_ptr(db,node,i)->child);

        if(sub_x->num < db->M-1){
            // child is no full 子节点未满
            memcpy(node, sub_x, DB_BLOCK_SIZE);
            continue;
        }

        // child is full 子节点已满，开始分裂
        if(node_create(db, sub_y, sub_x->leaf, TYPE_KEY) == -1){
            return -1;
        }
        btree_split_child(db, node, i, sub_x, sub_y);

        // 判断上升的关键字
        rc = db->key_cmp(key, btree_key_ptr(db,node,i)->key, db->key_size);
        if(rc == 0){
            // 上升的关键字相同
            return 0;
        }else if(rc > 0){
            // 上升的关键字更大
            memcpy(node, sub_y, DB_BLOCK_SIZE);
        }else{
            // 上升的关键字更小
            memcpy(node, sub_x, DB_BLOCK_SIZE);
        }
    }

    i = key_binary_search(db, node, key);
    if(i >= 0){
        // 关键字已存在
        return 0;
    }

    i = -(i+1);

    // 寻找合适的btree_value数据块
    if(db->current != 0L){
        node_seek(db, valnode, db->current);
        if(valnode->last + db_align(sizeof(btree_value) + value_size, DB_ALIGNMENT) > DB_BLOCK_SIZE){
            // 当前的btree_value数据块不够空间分配
            db->current = 0L;
            head_flush(db);
        }
    }
    if(db->current == 0L && node_create(db, valnode, 0, TYPE_VALUE) == -1){
        return -1;
    }

    // 先存储值
    btree_value_ptr(valnode,valnode->last)->size = value_size;
    memcpy(btree_value_ptr(valnode,valnode->last)->value, value, value_size);
    size_t last = valnode->last;
    valnode->last += db_align(sizeof(btree_value) + value_size, DB_ALIGNMENT);
    valnode->num++;
    node_flush(db, valnode);

    // 再存储关键字
    // leaf node right shift one position 叶子节点右移腾出一个空位
    keycpy(db, btree_key_ptr(db,node,i+1), btree_key_ptr(db,node,i), node->num-i);
    switch (db->key_type)
    {
    case DB_STRINGKEY:
        strcpy((char*)(btree_key_ptr(db,node,i)->key), key);
        break;
    case DB_BYTESKEY:
    case DB_INT32KEY:
    case DB_INT64KEY:
        memcpy(btree_key_ptr(db,node,i)->key, key, db->key_size);
        break;
    }
    btree_key_ptr(db, node, i)->value = valnode->self + last; // 记录value所在数据块的位置 + 偏移
    node->num++;
    node_flush(db, node);
    db->key_total++;
    head_flush(db);
    return 1;
}

/**
 * @brief delete key 删除值
 * @param[in] db 数据库句柄
 * @param[in] key key_size can't exceed max_key_size 需要保证key类型和创建数据库时一致
 * @return ==1 if success, ==0 if key no found, ==-1 error
*/
int db_delete(db_t* db, void* key){
    if(db->key_type == DB_STRINGKEY && strlen((char*)key) >= db->key_size){
        errno = EINVAL;
        return -1;
    }

    #define LESS 1
    #define MORE 2
    int i,i_match=-1,flag = 0;
    btree_node *node = (btree_node *)((char*)db + DB_HEAD_SIZE + DB_BLOCK_SIZE * 0);
    btree_node *node_match = (btree_node *)((char*)db + DB_HEAD_SIZE + DB_BLOCK_SIZE * 1);
    btree_node *sub_x = (btree_node *)((char*)db + DB_HEAD_SIZE + DB_BLOCK_SIZE * 2);
    btree_node *sub_y = (btree_node *)((char*)db + DB_HEAD_SIZE + DB_BLOCK_SIZE * 3);
    btree_node *sub_w = (btree_node *)((char*)db + DB_HEAD_SIZE + DB_BLOCK_SIZE * 4);
    /* 删除只会发生在叶子节点，在由上往下的遍历中，需要保证叶子节点有足够的关键字数（大于ceil(M)） */
    /*       __  node       */
    /*     /    /    \      */
    /*  sub_w  sub_x sub_y  */

    node_seek(db, node, DB_HEAD_SIZE);// root 读取根节点

    while(node->leaf == BTREE_NON_LEAF){
        switch (flag)
        {
        case LESS:
            i = -0-1;
            break;
        case MORE:
            i = -node->num-1;
            break;
        default:
            i = key_binary_search(db, node, key);
            break;
        }

        // match when in internal 在非叶子节点中匹配到，需要找到在前缀或后缀关键字（该关键字只会在叶子结点中）
        if(i >= 0){
            // 判断左子树是否方便删除前缀关键字（左子树关键字个数大于ceil(M)）
            node_seek(db, sub_x, btree_key_ptr(db,node,i)->child);
            if(sub_x->num > ceil(db->M)){
                // 寻找前缀关键字，即是寻找左子树的最大关键字
                flag = MORE;
                i_match = i;
                memcpy(node_match, node, DB_BLOCK_SIZE);
                memcpy(node, sub_x, DB_BLOCK_SIZE);
            }else{
                // 判断右子树是否方便删除后缀关键字（右子树关键字个数大于ceil(M)）
                node_seek(db,sub_y,btree_key_ptr(db,node,i+1)->child);
                if(sub_y->num > ceil(db->M)){
                    // 寻找后缀关键字，即是寻找右子树的最小关键字
                    flag = LESS;
                    i_match = i;
                    memcpy(node_match, node, DB_BLOCK_SIZE);
                    memcpy(node, sub_y, DB_BLOCK_SIZE);
                }else{
                    // 左右子树都不方便，则合并
                    if(!btree_merge(db, node, i, sub_x, sub_y)){
                        memcpy(node, sub_x, DB_BLOCK_SIZE);
                    }
                }
            }
            continue;
        }
        
        i = -(i+1);

        // need prepare , make sure child have enough key 自上而下调整子树，保证最后在叶子节点处方便删除关键字（自上而下，确保子树关键字个数大于ceil(M)）
        node_seek(db, sub_x, btree_key_ptr(db,node,i)->child);

        if(sub_x->num > ceil(db->M)){
            // already enough
            memcpy(node, sub_x, DB_BLOCK_SIZE);
            continue;
        }

        if(i+1<=node->num){
            node_seek(db, sub_y, btree_key_ptr(db,node,i+1)->child);
        }

        if(i-1>=0 && ((i+1>node->num) || sub_y->num<=ceil(db->M))){
            node_seek(db, sub_w, btree_key_ptr(db,node,i-1)->child);
        }
        
        if(i+1<=node->num && sub_y->num>ceil(db->M)){
            // borrow from right 从子树的右兄弟借
            memcpy(btree_key_ptr(db,sub_x,sub_x->num)->key, btree_key_ptr(db,node,i)->key, db->key_align - sizeof(btree_key));
            btree_key_ptr(db,sub_x,sub_x->num)->value = btree_key_ptr(db,node,i)->value;
            btree_key_ptr(db,sub_x,sub_x->num+1)->child = btree_key_ptr(db,sub_y,0)->child;
            sub_x->num++;

            memcpy(btree_key_ptr(db,node,i)->key, btree_key_ptr(db,sub_y,0)->key, db->key_align - sizeof(btree_key));
            btree_key_ptr(db,node,i)->value = btree_key_ptr(db,sub_y,0)->value;
            keycpy(db, btree_key_ptr(db,sub_y,0),btree_key_ptr(db,sub_y,1), sub_y->num-1);
            sub_y->num--;

            node_flush(db,node);
            node_flush(db,sub_x);
            node_flush(db,sub_y);
            memcpy(node,sub_x,DB_BLOCK_SIZE);
        }else if(i-1>=0 && sub_w->num>ceil(db->M)){
            // borrow from left 从子树的左兄弟借
            keycpy(db, btree_key_ptr(db,sub_x,1),btree_key_ptr(db,sub_x,0), sub_x->num);
            memcpy(btree_key_ptr(db,sub_x,0)->key, btree_key_ptr(db,node,i-1)->key, db->key_align - sizeof(btree_key));
            btree_key_ptr(db,sub_x,0)->value = btree_key_ptr(db,node,i-1)->value;
            btree_key_ptr(db,sub_x,0)->child = btree_key_ptr(db,sub_w,sub_w->num)->child;
            sub_x->num++;
            
            memcpy(btree_key_ptr(db,node,i-1)->key, btree_key_ptr(db,sub_w,sub_w->num-1)->key, db->key_align - sizeof(btree_key));
            btree_key_ptr(db,node,i-1)->value = btree_key_ptr(db,sub_w,sub_w->num-1)->value;
            sub_w->num--;

            node_flush(db,node);
            node_flush(db,sub_x);
            node_flush(db,sub_w);
            memcpy(node,sub_x,DB_BLOCK_SIZE);
        }else{
            if(i+1<=node->num){
                // merge with right
                if(!btree_merge(db,node,i,sub_x,sub_y)){
                    memcpy(node,sub_x,DB_BLOCK_SIZE);
                }
            }else{
                // merge with left
                if(!btree_merge(db,node,i-1,sub_w,sub_x)){
                    memcpy(node,sub_w,DB_BLOCK_SIZE);
                }
            }
        }
    }

    off_t offset = 0;
    if(flag == LESS){
        // 找到后缀关键字，即是右子树的最小关键字
        offset = btree_key_ptr(db,node_match,i_match)->value;

        memcpy(btree_key_ptr(db,node_match,i_match)->key, btree_key_ptr(db,node,0)->key, db->key_align - sizeof(btree_key));
        btree_key_ptr(db,node_match,i_match)->value = btree_key_ptr(db,node,0)->value;
        keycpy(db, btree_key_ptr(db,node,0), btree_key_ptr(db,node,1), node->num-1);
        node->num--;
        node_flush(db,node_match);
        node_flush(db,node);
    }else if(flag == MORE){
        // 找到前缀关键字，即是左子树的最大关键字
        offset = btree_key_ptr(db,node_match,i_match)->value;

        memcpy(btree_key_ptr(db,node_match,i_match)->key, btree_key_ptr(db,node,node->num-1)->key, db->key_align - sizeof(btree_key));
        btree_key_ptr(db,node_match,i_match)->value = btree_key_ptr(db,node,node->num-1)->value;
        node->num--;
        node_flush(db,node_match);
        node_flush(db,node);
    }else{
        i = key_binary_search(db,node,key);
        if(i < 0){
            return 0;
        }

        // 关键字刚好在叶子节点
        offset = btree_key_ptr(db,node,i)->value;
        keycpy(db, btree_key_ptr(db,node,i),btree_key_ptr(db,node,i+1), node->num - i - 1);
        node->num--;
        node_flush(db,node);
    }

    // release value block 释放关键字对应的value
    node_seek(db, node, DB_HEAD_SIZE + ((offset - DB_HEAD_SIZE)& ~(DB_BLOCK_SIZE-1)));
    node->num--;
    // btree_value的数据块，引用数为0时，释放该数据块
    if(node->num == 0){
        if(node->self == db->current){
            db->current = 0;
            // head_flush(d);// node_destroy will flush again
        }
        node_destroy(db, node);
    }else{
        node_flush(db, node);
    }
    db->key_total--;
    head_flush(db);
    return 1;
}

/**
 * @brief search key 查询值
 * @param[in] db 数据库句柄
 * @param[in] key key_size can't exceed max_key_size 需要保证key类型和创建数据库时一致
 * @param[out] value
 * @param[in] value_size 需要保证空间足够大
 * @return >=0 if success, ==-1 error
*/
int db_search(db_t* db, void* key, void *value, size_t value_size){
    if(db->key_type == DB_STRINGKEY && strlen((char*)key) >= db->key_size){
        errno = EINVAL;
        return -1;
    }

    btree_node *node = (btree_node *)((char*)db + DB_HEAD_SIZE + DB_BLOCK_SIZE * 0);
    int i;
    off_t offset = DB_HEAD_SIZE;

    do{
        node_seek(db, node, offset);
        i = key_binary_search(db, node, key);
        if(i >= 0){
            offset = btree_key_ptr(db, node, i)->value;
            node_seek(db, node, DB_HEAD_SIZE + ((offset - DB_HEAD_SIZE)& ~(DB_BLOCK_SIZE-1)));
            btree_value *pval = btree_value_ptr(node, offset-node->self);
            if(pval->size > value_size){
                errno = E2BIG;
                return -1;
            }
            memcpy(value,pval->value, pval->size);
            return pval->size;
        }
        i = -(i+1);
        offset = btree_key_ptr(db, node, i)->child;
    }while(offset != 0);

    errno = ENOMSG;
    return -1;
}

/***************************************/
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>

#define COUNT 100000
#define PATH "./test.db"

int main(){
    db_t* db;
    int i,rc;
    char value[128];

    // 创建数据库
    unlink(PATH);
    assert(db_create(PATH,DB_INT32KEY,sizeof(int)) == 0);

    // 打开数据库
    assert(db_open(&db,PATH) == 0);

    // 插入操作
    for(i=0;i<COUNT;i++){
        sprintf(value,"%d",i);
        assert(db_insert(db,&i,value,strlen(value)) == 1);
    }
    printf("insert key from %d to %d\n",0,COUNT);

    //查询操作
    i = 0;
    bzero(value,sizeof(value));
    rc = db_search(db,&i,value,sizeof(value));
    assert(rc >= 0);
    printf("search key: %d value: %.*s\n",i,rc,value);

    // 删除操作
    for(i=0;i<COUNT;i++){
        assert(db_delete(db,&i) == 1);
    }
    printf("delete key from %d to %d\n",0,COUNT);

    // 关闭数据库
    db_close(db);

    return 0;
}