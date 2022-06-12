#include <stdio.h>
#include <assert.h>
#include <math.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <uuid/uuid.h>
#include "db.h"

#define COUNT 100000L
#define PATH "./db_bytes"
int main(){
    db_t* db= NULL;
    char value[1024];
    uuid_t uuid;
    char **uuid_set;
    int i,rc;
    clock_t t;

    uuid_set = malloc(sizeof(char*) * COUNT);
    for(i=0;i<COUNT;i++){
        uuid_set[i] = malloc(sizeof(char) * 64);
        uuid_generate(uuid);
        memcpy(uuid_set[i],&uuid,16);
    }

    unlink(PATH);
    db = NULL;
    assert(db_create(PATH,DB_BYTESKEY,64) == 0);
    assert(db_open(&db,PATH) ==0);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start insert %s\n", uuid_set[i]);
        memcpy(value,uuid_set[i],16);
        rc = db_insert(db, uuid_set[i], value, 16, 0);
        assert(rc);
    }
    t = clock() - t;
    printf("insert %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start search %s\n", uuid_set[i]);
        memset(value, 0, sizeof(value));
        rc = db_search(db, uuid_set[i], value, sizeof(value));
        assert(rc && memcmp(uuid_set[i], value, 16) == 0);
        // printf("search success %s %s\n",uuid_set[i],value);
    }
    t = clock() - t;
    printf("search %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start insert %s\n", uuid_set[i]);
        memcpy(value,uuid_set[i],16);
        value[0] = ' ';
        rc = db_insert(db, uuid_set[i], value, 16, 1);
        assert(rc);
    }
    t = clock() - t;
    printf("insert overwrite %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start search %s\n", uuid_set[i]);
        memset(value, 0, sizeof(value));
        rc = db_search(db, uuid_set[i], value, sizeof(value));
        assert(rc && value[0] == ' ' && memcmp(&uuid_set[i][1], &value[1], 15) == 0);
        // printf("search success %s %s\n", uuid_set[i], value);
    }
    t = clock() - t;
    printf("search overwrite %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start delete %s\n", uuid_set[i]);
        memset(value,0,sizeof(value));
        rc = db_delete(db,uuid_set[i],value,sizeof(value));
        assert(rc && value[0] == ' ' && memcmp(&uuid_set[i][1], &value[1], 15) == 0);
    }
    t = clock() - t;
    printf("delete %ld use %ldus, per %lfus\n",COUNT,t,t/(double)COUNT);


    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start search %s\n", uuid_set[i]);
        memset(value,0,sizeof(value));
        rc = db_search(db,uuid_set[i],value,sizeof(value));
        assert(rc == -1);
    }
    t = clock() - t;
    printf("search %ld use %ldus, per %lfus\n",COUNT,t,t/(double)COUNT);

    return 0;
}
