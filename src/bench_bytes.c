#include <stdio.h>
#include <assert.h>
#include <math.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <uuid/uuid.h>
#include <errno.h>
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
        memcpy(uuid_set[i],&uuid,sizeof(uuid));
    }

    unlink(PATH);

    assert(db_create(PATH,DB_BYTESKEY,sizeof(uuid)) == 0);

    // bench insert
    assert(db_open(&db,PATH) ==0);

    t = clock();
    for(i=0;i<COUNT;i++){
        memcpy(value,uuid_set[i],sizeof(uuid));
        rc = db_insert(db, uuid_set[i], value, sizeof(uuid));
        assert(rc);
    }
    t = clock() - t;
    printf("insert %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);
    db_close(db);
    // ------------------------------------

    // bench search
    assert(db_open(&db,PATH) ==0);

    t = clock();
    for(i=0;i<COUNT;i++){
        memset(value, 0, sizeof(value));
        rc = db_search(db, uuid_set[i], value, sizeof(value));
        assert(rc && memcmp(uuid_set[i], value, sizeof(uuid)) == 0);
    }
    t = clock() - t;
    printf("search %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);
    db_close(db);
    // ------------------------------------

    // bench delete
    assert(db_open(&db,PATH) ==0);

    t = clock();
    for(i=0;i<COUNT;i++){
        memset(value,0,sizeof(value));
        rc = db_delete(db, uuid_set[i]);
        assert(rc);
    }
    t = clock() - t;
    printf("delete %ld use %ldus, per %lfus\n",COUNT,t,t/(double)COUNT);
    db_close(db);
    // ------------------------------------

    // bench search
    assert(db_open(&db,PATH) ==0);

    t = clock();
    for(i=0;i<COUNT;i++){
        memset(value, 0, sizeof(value));
        rc = db_search(db,uuid_set[i],value,sizeof(value));
        assert(rc == -1 && errno == ENOMSG);
    }
    t = clock() - t;
    printf("search %ld use %ldus, per %lfus\n",COUNT,t,t/(double)COUNT);

    db_close(db);
    // ------------------------------------
    
    for(i=0;i<COUNT;i++){
        free(uuid_set[i]);
    }
    free(uuid_set);
    
    return 0;
}
