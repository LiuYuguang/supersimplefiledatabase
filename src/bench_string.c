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
#define PATH "./db_string"
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
        sprintf(uuid_set[i],"%02X%02X%02X%02X-%02X%02X-%02X%02X-%02X%02X-%02X%02X%02X%02X%02X%02X",
            uuid[0],uuid[1],uuid[2],uuid[3],
            uuid[4],uuid[5],
            uuid[6],uuid[7],
            uuid[8],uuid[9],
            uuid[10],uuid[11],uuid[12],uuid[13],uuid[14],uuid[15]
        );
    }

    unlink(PATH);
    db = NULL;
    assert(db_create(PATH,DB_STRINGKEY,64) == 0);
    assert(db_open(&db,PATH) ==0);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start insert %s\n", uuid_set[i]);
        strcpy(value,uuid_set[i]);
        rc = db_insert(db, uuid_set[i], value, strlen(value), 0);
        assert(rc);
    }
    t = clock() - t;
    printf("insert %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start search %s\n", uuid_set[i]);
        memset(value, 0, sizeof(value));
        rc = db_search(db, uuid_set[i], value, sizeof(value));
        assert(rc && strcmp(uuid_set[i], value) == 0);
        // printf("search success %s %s\n",uuid_set[i],value);
    }
    t = clock() - t;
    printf("search %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start insert %s\n", uuid_set[i]);
        strcpy(value,uuid_set[i]);
        value[0] = ' ';
        rc = db_insert(db, uuid_set[i], value, strlen(value), 1);
        assert(rc);
    }
    t = clock() - t;
    printf("insert overwrite %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start search %s\n", uuid_set[i]);
        memset(value, 0, sizeof(value));
        rc = db_search(db, uuid_set[i], value, sizeof(value));
        assert(rc && value[0] == ' ' && strcmp(&uuid_set[i][1], &value[1]) == 0);
        // printf("search success %s %s\n", uuid_set[i], value);
    }
    t = clock() - t;
    printf("search overwrite %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start delete %s\n", uuid_set[i]);
        memset(value,0,sizeof(value));
        rc = db_delete(db,uuid_set[i],value,sizeof(value));
        assert(rc && value[0] == ' ' && strcmp(&uuid_set[i][1], &value[1]) == 0);
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
