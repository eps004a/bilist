#include <stdlib.h>
#include <string.h>
#include <time.h>

#define REDISMODULE_EXPERIMENTAL_API

#include "../redis/src/redismodule.h"

#include "skiplist.h"
#include "prand.h"

#define BILIST_MAX_COUNTER_INCREMENT 0X4c

#define BILIST_TIMER_PERIOD 1000
#define BILIST_PRUNE_SIZE 20
#define BILIST_PRUNE_TRESHOLD 5

static RedisModuleType *bilist_type;

struct binode
{
    RedisModuleString *key1;
    RedisModuleString *key2;
    RedisModuleString *value;
    long long expire_time;
    struct binode *next;
    struct binode *prev;
};

struct bilist
{
    struct s_list * primary_slist;
    struct s_list * secondary_slist;

    u_int32_t counter;
    u_int8_t increment;
    u_int8_t timer_active;

    unsigned long items;

    struct prand prand;

    struct binode *first;

    struct binode *next_prune;

    RedisModuleTimerID timer_id;

};

struct bilist *bilist_create()
{
    struct bilist *bilist;

    bilist = RedisModule_Alloc(sizeof(struct bilist));

    bilist->primary_slist = slist_create();
    bilist->secondary_slist = slist_create();

    pseed(&(bilist->prand), time(NULL));

    bilist->counter = prand32(&(bilist->prand));
    bilist->increment = prand32(&(bilist->prand)) % BILIST_MAX_COUNTER_INCREMENT;

    bilist->items = 0;
    bilist->first = NULL;
    bilist->next_prune = NULL;

    bilist->timer_id = 0;
    bilist->timer_active = 0;

    return bilist;
}

void bilist_data_free(struct binode *datanode)
{
    if (datanode == NULL)
        return;
    RedisModule_FreeString(NULL, datanode->key1);
    RedisModule_FreeString(NULL, datanode->key2);
    RedisModule_FreeString(NULL, datanode->value);
    RedisModule_Free(datanode);
}

void bilist_release(struct bilist *bilist)
{
    struct binode *node;

    if (bilist == NULL)
        return;
    slist_free(bilist->primary_slist);
    slist_free(bilist->secondary_slist);

    for (node = bilist->first; node; ) {
        struct binode *tmp = node->next;
        bilist_data_free(node);
        node = tmp;
    }
    FREE(bilist);
}

struct bilist *bilist_get_from_key(RedisModuleCtx *ctx, RedisModuleString *keyname)
{
    struct bilist *bilist;
    RedisModuleKey *key;
    int type;

    key = RedisModule_OpenKey(ctx, keyname, REDISMODULE_READ | REDISMODULE_WRITE);

    type = RedisModule_KeyType(key);

    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != bilist_type) {
        return NULL;
    }

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        bilist = bilist_create();
        RedisModule_ModuleTypeSetValue(key, bilist_type, bilist);
    } else {
        bilist = RedisModule_ModuleTypeGetValue(key);
    }
    RedisModule_CloseKey(key);

    return bilist;
}

void bilist_remove_node(struct bilist *bilist, struct binode *node)
{
    if (bilist->next_prune == node) {
        bilist->next_prune = node->next;
        if (bilist->next_prune == NULL) {
            bilist->next_prune = bilist->first;
        }
    }
    if (node->prev) {
        node->prev->next = node->next;
    }
    if (node->next) {
        node->next->prev = node->prev;
    }
    if (bilist->first == node)
        bilist->first = node->next;

    bilist_data_free(node);
}

struct binode * bilist_create_node(struct bilist *bilist, RedisModuleString *key1, RedisModuleString *key2, RedisModuleString *value, long expire)
{
    struct binode *binode;

    binode = MALLOC(sizeof(struct binode));
    binode->key1 = key1;
    binode->key2 = key2;
    binode->value = value;
    binode->expire_time = expire;
    binode->next = NULL;
    binode->prev = NULL;

    if (bilist->first) {
        bilist->first->prev = binode;
        binode->next = bilist->first;
        bilist->first = binode;
    } else {
        bilist->first = binode;
        bilist->next_prune = binode;
    }
    return binode;
}

int bilist_node_expired(struct binode *binode)
{
    if (binode->expire_time == 0)
        return 0;
    
    return binode->expire_time < RedisModule_Milliseconds();
}

int bilist_test_prune(struct bilist *bilist, long count)
{
    struct binode *binode;
    struct binode *tmpnode;

    size_t size;
    int pruned;

    binode = bilist->next_prune;

    pruned = 0;
    while (count) {
        if (binode == NULL) {
            bilist->next_prune = bilist->first;
            return pruned;
        }
        tmpnode = binode->next;
        if (bilist_node_expired(binode)) {
            slist_delete(bilist->primary_slist, RedisModule_StringPtrLen(binode->key1, &size), RedisModule_StringPtrLen(binode->key2, & size));
            slist_delete(bilist->secondary_slist, RedisModule_StringPtrLen(binode->key2, &size), RedisModule_StringPtrLen(binode->key1, &size));
            bilist_remove_node(bilist, binode);
            bilist->items--;            
            pruned++;
        }
        binode = tmpnode;
        count--;
    }
    bilist->next_prune = binode;
    return pruned;
}

void bilist_timer_handler(RedisModuleCtx *ctx, void *data)
{

    struct bilist *bilist = data;
    int pruned;

    do {
        pruned = bilist_test_prune(bilist, BILIST_PRUNE_SIZE);
    } while (pruned > BILIST_PRUNE_TRESHOLD);

    bilist->timer_id = RedisModule_CreateTimer(ctx, BILIST_TIMER_PERIOD, bilist_timer_handler, bilist);  // Refresh timer
}

/* ========================= "bilist" type commands ======================= */

static char key_chars[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-";

#define KEY_CHARS_ELEMENTS (sizeof(key_chars)/sizeof(key_chars[0])-1)

int bilist_ckey_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    long long count;
    long long i;

    char *buffer;

    char suffix[10];

    struct bilist *bilist;

    RedisModule_AutoMemory(ctx);

    if (argc != 3)
        return RedisModule_WrongArity(ctx);

    bilist = bilist_get_from_key(ctx, argv[1]);
    if (bilist == NULL) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    if (RedisModule_StringToLongLong(argv[2],&count) != REDISMODULE_OK || count < 0) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid count parameter");
    }

    buffer = RedisModule_PoolAlloc(ctx, count+8+1);

    char *ptr = buffer;

    for (i=0; i < count; i++) {
        *ptr++ = key_chars[prand(&(bilist->prand)) % KEY_CHARS_ELEMENTS]; 
    }
    *ptr = '\0';

    sprintf(suffix,"%08x", bilist->counter);
    strcat(ptr, suffix);

    bilist->counter += prand(&(bilist->prand)) % bilist->increment +1;

    return RedisModule_ReplyWithSimpleString(ctx, buffer);
}

int bilist_set_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    struct bilist *bilist;
    size_t size;
    RedisModuleString *data;
    RedisModuleString *stringkey1;
    RedisModuleString * stringkey2;

    struct binode *binode;
    struct binode *oldnode;

    long long expire;

    RedisModule_AutoMemory(ctx);

    if (argc != 6)
        return RedisModule_WrongArity(ctx);

    bilist = bilist_get_from_key(ctx, argv[1]);

    if (bilist == NULL) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    if (RedisModule_StringToLongLong(argv[5], & expire) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, "ERR Invalid expire time");
    }
    
    stringkey1 = RedisModule_CreateStringFromString(NULL, argv[2]);
    stringkey2 = RedisModule_CreateStringFromString(NULL, argv[3]);
    data = RedisModule_CreateStringFromString(NULL, argv[4]);

    if (expire) {
        expire *= 1000;
        expire += RedisModule_Milliseconds();
    }

    if (!bilist->timer_active) {
        bilist->timer_id = RedisModule_CreateTimer(ctx, BILIST_TIMER_PERIOD, bilist_timer_handler, bilist);
        bilist->timer_active = 1;
    }

    binode = bilist_create_node(bilist, stringkey1, stringkey2, data, expire);

    oldnode = slist_insert(bilist->primary_slist, RedisModule_StringPtrLen(stringkey1, &size), RedisModule_StringPtrLen(stringkey2, &size), binode);
    
    slist_insert(bilist->secondary_slist, RedisModule_StringPtrLen(stringkey2, &size), RedisModule_StringPtrLen(stringkey1, &size), binode);

    if (oldnode) {
        bilist_remove_node(bilist, oldnode);
    } else {
        bilist->items++;
    }

    RedisModuleString *guardian = RedisModule_CreateStringPrintf(ctx, "::bilist-guardian::", argv[1]);

    RedisModule_Call(ctx, "INCR", "s", guardian);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int bilist_get_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    struct bilist *bilist;
    size_t size;

    struct s_node *node;

    struct binode *binode;

    RedisModule_AutoMemory(ctx);

    if (argc != 4)
        return RedisModule_WrongArity(ctx);

    bilist = bilist_get_from_key(ctx, argv[1]);

    if (bilist == NULL) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
 
    node = slist_find(bilist->primary_slist, RedisModule_StringPtrLen(argv[2], &size), RedisModule_StringPtrLen(argv[3], &size));

    if (node == NULL) {
        return RedisModule_ReplyWithNull(ctx);
    }

    binode = node->data;

    if (bilist_node_expired(binode)) {
        slist_delete(bilist->primary_slist, RedisModule_StringPtrLen(argv[2], &size), RedisModule_StringPtrLen(argv[3], & size));
        slist_delete(bilist->secondary_slist, RedisModule_StringPtrLen(argv[3], &size), RedisModule_StringPtrLen(argv[2], &size));

        bilist_remove_node(bilist, binode);

        bilist->items--;
        return RedisModule_ReplyWithNull(ctx);
    }

    return RedisModule_ReplyWithString(ctx, binode->value);
}

int bilist_get1_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    struct bilist *bilist;
    const char *key1;
    size_t size;

    struct s_node *node;
    struct s_node *tmpnode;

    long elements;

    struct binode *binode;

    RedisModule_AutoMemory(ctx);

    if (argc != 3)
        return RedisModule_WrongArity(ctx);

    bilist = bilist_get_from_key(ctx, argv[1]);

    if (bilist == NULL) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    key1 = RedisModule_StringPtrLen(argv[2], &size);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    elements = 0;

    node = slist_find_first(bilist->primary_slist, key1);

    while (node && strcmp(node->primary_key, key1) == 0) {
        tmpnode = node->next_n[0];
        binode = node->data;

        if (bilist_node_expired(binode)) {
            slist_delete(bilist->primary_slist, RedisModule_StringPtrLen(binode->key1, &size), RedisModule_StringPtrLen(binode->key2, & size));
            slist_delete(bilist->secondary_slist, RedisModule_StringPtrLen(binode->key2, &size), RedisModule_StringPtrLen(binode->key1, &size));
            bilist_remove_node(bilist, binode);
            bilist->items--;
        } else {
            RedisModule_ReplyWithArray(ctx, 2);
            RedisModule_ReplyWithString(ctx, binode->key2);
            RedisModule_ReplyWithString(ctx, binode->value);
            elements++;
        }
        node = tmpnode;
    }
    RedisModule_ReplySetArrayLength(ctx, elements);
    return REDISMODULE_OK;
}

int bilist_get2_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    struct bilist *bilist;
    const char *key1;
    size_t size;

    struct s_node *node;
    struct s_node *tmpnode;

    long elements;

    struct binode *binode;

    RedisModule_AutoMemory(ctx);

    if (argc != 3)
        return RedisModule_WrongArity(ctx);

    bilist = bilist_get_from_key(ctx, argv[1]);

    if (bilist == NULL) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    key1 = RedisModule_StringPtrLen(argv[2], &size);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    elements = 0;

    node = slist_find_first(bilist->secondary_slist, key1);

    while (node && strcmp(node->primary_key, key1) == 0) {
        tmpnode = node->next_n[0];
        binode = node->data;

        if (bilist_node_expired(binode)) {
            slist_delete(bilist->primary_slist, RedisModule_StringPtrLen(binode->key1, &size), RedisModule_StringPtrLen(binode->key2, & size));
            slist_delete(bilist->secondary_slist, RedisModule_StringPtrLen(binode->key2, &size), RedisModule_StringPtrLen(binode->key1, &size));
            bilist_remove_node(bilist, binode);
            bilist->items--;
        } else {
            RedisModule_ReplyWithArray(ctx, 2);
            RedisModule_ReplyWithString(ctx, binode->key1);
            RedisModule_ReplyWithString(ctx, binode->value);
            elements++;
        }
        node = tmpnode;
    }
    RedisModule_ReplySetArrayLength(ctx, elements);
    return REDISMODULE_OK;
}

int bilist_del_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    struct bilist *bilist;
    const char *key1;
    const char *key2;
    size_t size;

    struct binode * binode;

    RedisModule_AutoMemory(ctx);

    if (argc != 4)
        return RedisModule_WrongArity(ctx);

    bilist = bilist_get_from_key(ctx, argv[1]);

    if (bilist == NULL) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    key1 = RedisModule_StringPtrLen(argv[2], &size);
    key2 = RedisModule_StringPtrLen(argv[3], &size);

    binode = slist_delete(bilist->primary_slist, key1, key2);

    if (binode) {
        slist_delete(bilist->secondary_slist, key2, key1);
        bilist_remove_node(bilist, binode);
        bilist->items--;
    }
    return RedisModule_ReplyWithLongLong(ctx, binode?1:0);
}

int bilist_count_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    struct bilist *bilist;

    RedisModule_AutoMemory(ctx);

    if (argc != 2)
        return RedisModule_WrongArity(ctx);

    bilist = bilist_get_from_key(ctx, argv[1]);

    if (bilist == NULL) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    return RedisModule_ReplyWithLongLong(ctx, bilist->items);
}

int bilist_all_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    struct bilist *bilist;
    struct binode *binode;
    long elements;

    size_t size;

    RedisModule_AutoMemory(ctx);

    if (argc != 2)
        return RedisModule_WrongArity(ctx);

    bilist = bilist_get_from_key(ctx, argv[1]);

    if (bilist == NULL) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    elements = 0;

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    for (binode = bilist->first; binode; binode = binode->next) {
        if (bilist_node_expired(binode)) {
            slist_delete(bilist->primary_slist, RedisModule_StringPtrLen(binode->key1, &size), RedisModule_StringPtrLen(binode->key2, & size));
            slist_delete(bilist->secondary_slist, RedisModule_StringPtrLen(binode->key2, &size), RedisModule_StringPtrLen(binode->key1, &size));
            bilist_remove_node(bilist, binode);
            bilist->items--;
        } else {
            RedisModule_ReplyWithArray(ctx, 4);
            RedisModule_ReplyWithString(ctx, binode->key1);
            RedisModule_ReplyWithString(ctx, binode->key2);
            RedisModule_ReplyWithString(ctx, binode->value);
            RedisModule_ReplyWithLongLong(ctx, binode->expire_time?(binode->expire_time-RedisModule_Milliseconds())/1000:-1);
            elements++;
        }
    }
    RedisModule_ReplySetArrayLength(ctx, elements);
    return REDISMODULE_OK;
}

/* ========================== "bilist" type methods ======================= */

void *bilistRdbLoad(RedisModuleIO *rdb, int encver)
{
    if (encver != 0) {
        /* RedisModule_Log("warning","Can't load data with version %d", encver);*/
        return NULL;
    }

    unsigned long i;
    unsigned long elements;

    struct bilist *bilist;
    struct binode *binode;
    struct binode *prev;

    size_t size;

    bilist = bilist_create();

    bilist->counter = RedisModule_LoadUnsigned(rdb);
    bilist->increment = RedisModule_LoadUnsigned(rdb);
    bilist->items = RedisModule_LoadUnsigned(rdb);
    bilist->prand.state.a = RedisModule_LoadUnsigned(rdb);

    prev = NULL;
    elements = 0;

    for (i=0; i < bilist->items; i++) {

        binode = MALLOC(sizeof (struct binode));

        binode->key1 = RedisModule_LoadString(rdb);
        binode->key2 = RedisModule_LoadString(rdb);
        binode->value = RedisModule_LoadString(rdb);
        binode->expire_time = RedisModule_LoadSigned(rdb);
        binode->next = NULL;
        binode->prev = NULL;

        if (bilist_node_expired(binode)) {
            bilist_data_free(binode);
        } else {
            elements++;
            if (prev) {
                prev->next = binode;
                binode->prev = prev;
            } else {
                bilist->first = binode;
                bilist->next_prune = binode;
            }
            slist_insert(bilist->primary_slist, RedisModule_StringPtrLen(binode->key1, &size), RedisModule_StringPtrLen(binode->key2, &size), NULL);
            slist_insert(bilist->secondary_slist, RedisModule_StringPtrLen(binode->key2, &size), RedisModule_StringPtrLen(binode->key1, &size), binode);

            prev = binode;
        }
    }

    bilist->items = elements;

    return bilist;
}

void bilistRdbSave(RedisModuleIO *rdb, void *value) {
    struct bilist *bilist = (struct bilist *)value;
    struct binode *node;

    RedisModule_SaveUnsigned(rdb, bilist->counter);
    RedisModule_SaveUnsigned(rdb, bilist->increment);
    RedisModule_SaveUnsigned(rdb, bilist->items);
    RedisModule_SaveUnsigned(rdb, bilist->prand.state.a);

    for (node = bilist->first;node;node = node->next) {
        RedisModule_SaveString(rdb, node->key1);
        RedisModule_SaveString(rdb, node->key2);
        RedisModule_SaveString(rdb, node->value);
        RedisModule_SaveSigned(rdb, node->expire_time);
    }
}

void bilistAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value)
{
    REDISMODULE_NOT_USED(aof);
    REDISMODULE_NOT_USED(key);
    REDISMODULE_NOT_USED(value);

}

/* The goal of this function is to return the amount of memory used by
 * the bilist value. */
size_t bilistMemUsage(const void *value)
{
    const struct bilist *bilist = (struct bilist *)value;

    return sizeof(struct bilist)+2*sizeof(struct s_list)+(bilist->items)*(sizeof(struct binode)+2*sizeof(struct s_node));
}

void bilistFree(void *value)
{
    bilist_release(value);
}

void bilistDigest(RedisModuleDigest *md, void *value)
{
        REDISMODULE_NOT_USED(md);
        REDISMODULE_NOT_USED(value);
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx,"bilist-jt",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = bilistRdbLoad,
        .rdb_save = bilistRdbSave,
        .aof_rewrite = bilistAofRewrite,
        .mem_usage = bilistMemUsage,
        .free = bilistFree,
        .digest = bilistDigest
    };

    bilist_type = RedisModule_CreateDataType(ctx,"bilist-jt",0,&tm);
    if (bilist_type == NULL) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"bilist.ckey", bilist_ckey_RedisCommand, "write deny-oom random",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"bilist.set", bilist_set_RedisCommand, "write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"bilist.get", bilist_get_RedisCommand, "write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"bilist.get1", bilist_get1_RedisCommand, "write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"bilist.get2", bilist_get2_RedisCommand, "write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"bilist.del", bilist_del_RedisCommand, "write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"bilist.count", bilist_count_RedisCommand, "readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"bilist.all", bilist_all_RedisCommand, "write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // if (RedisModule_CreateCommand(ctx,"bilist.add",
    //     bilist_add_RedisCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;

    // if (RedisModule_CreateCommand(ctx,"bilist.get",
    //     bilist_get_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;

    // if (RedisModule_CreateCommand(ctx,"bilist.getl",
    //     bilist_getl_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
