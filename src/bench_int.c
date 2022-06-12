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
#define PATH "./db_int"

int main(){
    db_t* db= NULL;
    char value[1024];
    int nums[COUNT];
    int i,j,k,rc;
    clock_t t;

    srand(time(NULL) ^ getpid());
    for(i=0;i<COUNT;i++){
        nums[i] = i;
    }
    for(i=COUNT; i>0; i--){
        j = rand()%i;
        k = nums[j];
        nums[j] = nums[i-1];
        nums[i-1] = k;
    }

    unlink(PATH);
    db = NULL;
    assert(db_create(PATH,DB_INT32KEY,sizeof(int))==0);
    assert(db_open(&db,PATH)==0);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start insert %d\n", nums[i]);
        sprintf(value,"%d",nums[i]);
        rc = db_insert(db, &nums[i], value, strlen(value), 0);
        assert(rc);
    }
    t = clock() - t;
    printf("insert %ld use %ldus, per %lfus\n",COUNT, t, t/(double)COUNT);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start search %d\n", nums[i]);
        memset(value,0,sizeof(value));
        rc = db_search(db,&nums[i],value,sizeof(value));
        assert(rc && nums[i] == atoi(value));
        // printf("search success %d %s\n", nums[i], value);
    }
    t = clock() - t;
    printf("search %ld use %ldus, per %lfus\n",COUNT,t,t/(double)COUNT);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start insert %d\n", nums[i]);
        value[0] = ' ';
        sprintf(&value[1],"%d",nums[i]);
        rc = db_insert(db, &nums[i], value, strlen(value), 1);
        assert(rc);
    }
    t = clock() - t;
    printf("insert overwrite %ld use %ldus, per %lfus\n",COUNT, t, t/(double)COUNT);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start search %d\n", nums[i]);
        memset(value,0,sizeof(value));
        rc = db_search(db,&nums[i],value,sizeof(value));
        assert(rc && value[0] == ' ' && nums[i] == atoi(&value[1]));
        // printf("search success %d %s\n", nums[i], value);
    }
    t = clock() - t;
    printf("search overwrite %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);

    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start delete %d\n", nums[i]);
        memset(value,0,sizeof(value));
        rc = db_delete(db, &nums[i], value, sizeof(value));
        assert(rc && value[0] == ' ' && nums[i] == atoi(&value[1]));
    }
    t = clock() - t;
    printf("delete %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);


    t = clock();
    for(i=0;i<COUNT;i++){
        // printf("start search %d\n", nums[i]);
        memset(value, 0, sizeof(value));
        rc = db_search(db, &nums[i], value, sizeof(value));
        assert(rc == -1);
    }
    t = clock() - t;
    printf("search %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);

    return 0;
}
