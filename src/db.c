/*
 * Copyright (C) 2022, LiuYuguang <l13660020946@live.com>
 */

#include "db.h"
#include <stdlib.h>            // for malloc(), free()
#include <string.h>            // for memcpy(), memmove(), strcpy(), strlen()
#include <fcntl.h>             // for open(), O_CREAT, O_RDWR, O_TRUNC
#include <sys/stat.h>          // for struct stat, fstat()
#include <unistd.h>            // for pread(), pwrite(), access(), ftruncate(), close()
#include <stdint.h>            // for int32_t
#include <errno.h>             // for E2BIG

#define DB_HEAD_SIZE  (4096UL) // head size must be pow of 2! 
#define DB_BLOCK_SIZE (8192UL) // block size must be pow of 2!

#define BTREE_NON_LEAF 0
#define BTREE_LEAF     1

#define DB_ALIGNMENT       16
#define db_align(d, a)     (((d) + (a - 1)) & ~(a - 1))

#define ceil(M) (((M)-1)/2)

#define TYPE_KEY   0
#define TYPE_VALUE 1

typedef struct{
    off_t value;
    off_t child;
    unsigned char key[0];
}btree_key;

typedef struct{
    size_t size;
    unsigned char value[0];
}btree_value;

typedef struct{
    off_t self;       // block position 文件位置
    size_t num;       // num of btree_key or btree_value
    off_t free;       // when block free, it will join free list
    uint32_t leaf:1;  // whether BTREE_NON_LEAF or BTREE_LEAF
    uint32_t use:1;   // 
    uint32_t type:1;  // whether TYPE_KEY or TYPE_VALUE
    uint32_t last:29; // when type is TYPE_VALUE, last mean rest unuse space, [ sizeof(btree_node) : DB_BLOCK_SIZE )
}btree_node;

#define btree_key_ptr(db,node,n) ((btree_key*)((char *)(node) + sizeof(*node) + (db->key_align) * (n)))
#define btree_value_ptr(node,n) ((btree_value*)((char *)(node) + (n)))

typedef struct db_s{
    int fd;
    int key_type;
    size_t key_size;
    size_t key_align;                  // btree_key对齐
    size_t M;
    size_t key_total;
    size_t key_use_block;
    size_t value_use_block;
    off_t free;                         // free list 空闲链表
    off_t current;                      // current block for TYPE_VALUE
    int (*key_cmp)(void*,void*,size_t);
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

inline static ssize_t head_seek(db_t *db){
    int fd = db->fd;
    int (*key_cmp)(void*,void*,size_t) = db->key_cmp;
    ssize_t rc = pread(fd,db,DB_HEAD_SIZE,0);
    db->fd = fd;
    db->key_cmp = key_cmp;
    return rc;
}

inline static ssize_t head_flush(db_t *db){
    return pwrite(db->fd,db,DB_HEAD_SIZE,0);
}

inline static ssize_t node_seek(db_t *db, btree_node* node, off_t offset){
    return pread(db->fd,node,DB_BLOCK_SIZE,offset);
}

inline static ssize_t node_flush(db_t *db, btree_node *node){
    return pwrite(db->fd,node,DB_BLOCK_SIZE,node->self);
}

inline static int node_create(db_t *db, btree_node* node, int leaf, int type){
    if(db->free != 0L){
        node_seek(db,node,db->free);
        db->free = node->free;
    }else{
        struct stat stat;
        fstat(db->fd,&stat);
        memset(node,0,DB_BLOCK_SIZE);
        node->self = stat.st_size;
        if(node_flush(db,node) != DB_BLOCK_SIZE){
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

inline static void node_destroy(db_t *db, btree_node *node){
    node->free = db->free;
    db->free = node->self;
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
    db->key_use_block = 1;// root block never be free
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
    root->use = 1;
    if(node_flush(db, root) != DB_BLOCK_SIZE){
        close(fd);
        free(buf);
        return -1;
    }

    close(fd);
    free(buf);
    return 0;
}

int db_checker(db_t *db){
    struct stat stat;
    fstat(db->fd,&stat);
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
    // printf("\n\nkey_total=%lu,value_total=%lu,db->key_total=%lu\n"
    //     "key_use_block=%lu,db->key_use_block=%lu\n"
    //     "value_use_block=%lu,db->value_use_block=%lu\n",
    //     key_total,value_total,db->key_total,
    //     key_use_block, db->key_use_block,
    //     value_use_block, db->value_use_block
    // );
    if(key_total != value_total 
        || key_total != db->key_total
        || key_use_block != db->key_use_block
        || value_use_block != db->value_use_block
    ){
        return -1;
    }
    return 0;
}

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

#define keycpy(db,dest,src,n) memmove(dest,src,db->key_align * (n+1));// include src[n]->child

inline static void btree_split_child(db_t* db, btree_node *node, int position, btree_node *sub_x, btree_node *sub_y){
    // node->child[position] = sub_x
    // node->child[position+1] = sub_y
    // 将sub_x拆分，一半给sub_y，中间上升到node
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

inline static int btree_merge(db_t* db, btree_node *node, int position, btree_node *sub_x, btree_node *sub_y){
    // node->child[position] = sub_x
    // node->child[position+1] = sub_y
    // 将sub_x和sub_y合并，node[position]下降
    memcpy(btree_key_ptr(db, sub_x, sub_x->num)->key, btree_key_ptr(db, node, position)->key, db->key_align - sizeof(btree_key));
    btree_key_ptr(db, sub_x, sub_x->num)->value = btree_key_ptr(db, node, position)->value;

    keycpy(db, btree_key_ptr(db, sub_x, sub_x->num+1), btree_key_ptr(db, sub_y, 0), sub_y->num);    
    sub_x->num += ( 1 + sub_y->num );

    keycpy(db, btree_key_ptr(db, node, position), btree_key_ptr(db, node, position+1), node->num - position - 1);
    btree_key_ptr(db, node, position)->child = sub_x->self;
    node->num--;

    node_destroy(db, sub_y);

    if(node->num == 0){// must be root
        node->num = sub_x->num;
        node->leaf = sub_x->leaf;
        memcpy((char*)node + sizeof(btree_node),(char*)sub_x + sizeof(btree_node),DB_BLOCK_SIZE - sizeof(btree_node));
        node_destroy(db, sub_x);
        node_flush(db, node);
        return 1;
    }else{
        node_flush(db, sub_x);
        node_flush(db, node);
        return 0;
    }
}

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
        
        if(node_create(db, sub_x, node->leaf, TYPE_KEY) == -1 || node_create(db, sub_y, sub_x->leaf, TYPE_KEY) == -1){
            return -1;
        }

        sub_x->num = node->num;
        memcpy((char*)sub_x + sizeof(btree_node),(char*)node + sizeof(btree_node),DB_BLOCK_SIZE - sizeof(btree_node));

        node->num = 0;
        node->leaf = BTREE_NON_LEAF;
        btree_key_ptr(db,node,0)->child = sub_x->self;

        btree_split_child(db, node, 0, sub_x, sub_y);
    }
    
    while(node->leaf == BTREE_NON_LEAF){
        i = key_binary_search(db, node, key);
        if(i >= 0){
            return 0;
        }

        i = -(i+1);
        
        node_seek(db, sub_x, btree_key_ptr(db,node,i)->child);

        if(sub_x->num < db->M-1){
            // child is no full
            memcpy(node, sub_x, DB_BLOCK_SIZE);
            continue;
        }

        // child is full 子节点已满
        if(node_create(db, sub_y, sub_x->leaf, TYPE_KEY) == -1){
            return -1;
        }
        btree_split_child(db, node, i, sub_x, sub_y);
        rc = db->key_cmp(key, btree_key_ptr(db,node,i)->key, db->key_size);
        if(rc == 0){
            // 上升的关键字相同
            return 0;
        }else if(rc > 0){
            // 上升的关键字更大
            memcpy(node, sub_y, DB_BLOCK_SIZE);
        }else{
            memcpy(node, sub_x, DB_BLOCK_SIZE);
        }
    }

    i = key_binary_search(db, node, key);
    if(i >= 0){
        return 0;
    }

    i = -(i+1);

    // 分配合适的btree_value文件块
    if(db->current != 0L){
        node_seek(db, valnode, db->current);
        if(valnode->last + db_align(sizeof(btree_value) + value_size, DB_ALIGNMENT) > DB_BLOCK_SIZE){
            db->current = 0L;
            head_flush(db);
        }
    }
    if(db->current == 0L){
        if(node_create(db, valnode, 0, TYPE_VALUE) == -1){
            return -1;
        }
    }

    btree_value_ptr(valnode,valnode->last)->size = value_size;
    memcpy(btree_value_ptr(valnode,valnode->last)->value, value, value_size);
    size_t last = valnode->last;
    valnode->last += db_align(sizeof(btree_value) + value_size, DB_ALIGNMENT);
    valnode->num++;
    node_flush(db, valnode);

    // leaf node right shift one position 叶子节点右移1个空位
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
    btree_key_ptr(db, node, i)->value = valnode->self + last;
    node->num++;
    node_flush(db, node);
    db->key_total++;
    head_flush(db);
    return 1;
}

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
    // 删除只会发生在叶子节点，在由上往下的遍历中，需要保证叶子节点有足够的关键字数，大于ceil(M)
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

        // match when in internal 在非叶子节点中匹配到，需要找到在前缀/后缀关键字（只会在叶子结点）
        if(i >= 0){
            node_seek(db, sub_x, btree_key_ptr(db,node,i)->child);
            if(sub_x->num > ceil(db->M)){
                // 寻找左子树的最大关键字
                flag = MORE;
                i_match = i;
                memcpy(node_match, node, DB_BLOCK_SIZE);
                memcpy(node, sub_x, DB_BLOCK_SIZE);
            }else{
                node_seek(db,sub_y,btree_key_ptr(db,node,i+1)->child);
                if(sub_y->num > ceil(db->M)){
                    // 寻找右子树的最小关键字
                    flag = LESS;
                    i_match = i;
                    memcpy(node_match, node, DB_BLOCK_SIZE);
                    memcpy(node, sub_y, DB_BLOCK_SIZE);
                }else{
                    if(!btree_merge(db, node, i, sub_x, sub_y)){
                        memcpy(node, sub_x, DB_BLOCK_SIZE);
                    }
                }
            }
            continue;
        }
        
        i = -(i+1);

        // need prepare , make sure child have enough key 需要保证叶子节点有足够的关键字数
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
            // borrow from right 从右兄弟借一个关键字
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
            // borrow from left 从左兄弟借一个关键字
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
        // 后缀关键字是右子树的最小关键字
        offset = btree_key_ptr(db,node_match,i_match)->value;

        memcpy(btree_key_ptr(db,node_match,i_match)->key, btree_key_ptr(db,node,0)->key, db->key_align - sizeof(btree_key));
        btree_key_ptr(db,node_match,i_match)->value = btree_key_ptr(db,node,0)->value;
        keycpy(db, btree_key_ptr(db,node,0), btree_key_ptr(db,node,1), node->num-1);
        node->num--;
        node_flush(db,node_match);
        node_flush(db,node);
    }else if(flag == MORE){
        // 前缀关键字是左子树的最大关键字
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