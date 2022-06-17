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
#define PATH "./db_int"

int main(){
    db_t* db= NULL;
    char value[1024];
    int *nums;
    int i,j,k,rc;
    clock_t t;

    srand(time(NULL) ^ getpid());

    nums = malloc(sizeof(int) * COUNT);

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
    
    assert(db_create(PATH,DB_INT32KEY,sizeof(int))==0);

    // bench insert
    assert(db_open(&db,PATH)==0);

    t = clock();
    for(i=0;i<COUNT;i++){
        sprintf(value,"%d",nums[i]);
        rc = db_insert(db, &nums[i], value, strlen(value));
        assert(rc);
    }
    t = clock() - t;
    printf("insert %ld use %ldus, per %lfus\n",COUNT, t, t/(double)COUNT);
    db_close(db);
    // ------------------------------------

    // bench search
    assert(db_open(&db,PATH) ==0);
    t = clock();
    for(i=0;i<COUNT;i++){
        memset(value,0,sizeof(value));
        rc = db_search(db,&nums[i],value,sizeof(value));
        assert(rc && nums[i] == atoi(value));
    }
    t = clock() - t;
    printf("search %ld use %ldus, per %lfus\n",COUNT,t,t/(double)COUNT);
    db_close(db);
    // ------------------------------------

    // bench delete
    assert(db_open(&db,PATH) ==0);

    t = clock();
    for(i=0;i<COUNT;i++){
        memset(value,0,sizeof(value));
        rc = db_delete(db, &nums[i]);
        assert(rc);
    }
    t = clock() - t;
    printf("delete %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);
    db_close(db);
    // ------------------------------------

    // bench search
    assert(db_open(&db,PATH) ==0);

    t = clock();
    for(i=0;i<COUNT;i++){
        memset(value, 0, sizeof(value));
        rc = db_search(db, &nums[i], value, sizeof(value));
        assert(rc == -1 && errno == ENOMSG);
    }
    t = clock() - t;
    printf("search %ld use %ldus, per %lfus\n", COUNT, t, t/(double)COUNT);

    db_close(db);
    // ------------------------------------

    free(nums);
    
    return 0;
}
