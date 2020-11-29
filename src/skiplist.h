#pragma once

#include <sys/types.h>
#include <memory.h>
#include <stdlib.h>

#include "../redis/src/redismodule.h"

#include "prand.h"

#define MALLOC(S) RedisModule_Alloc(S)
#define CALLOC(N,S) RedisModule_Calloc(N, S) 
#define REALLOC(P,S) RedisModule_Realloc(P, S)
#define FREE(P) RedisModule_Free(P)
#define FREESTRING(P) RedisModule_FreeString(NULL,P)

#define STRLEN(S) strlen((char *)S)
#define STRCMP(S1,S2) strcmp((char *)S1, (char *)S2)
#define STRCPY(TO, FROM) strcpy((char *)TO, (const char *)FROM)

#define STRDUP(S) RedisModule_Strdup(S)

#define TIMESTAMP() RedisModule_Milliseconds()

/** **/

#define S_HEIGHT 32

#define S_REVERSE 1
#define S_STRAIGHT 0

#define S_NOTFOUND 0
#define S_CREATED 1
#define S_REPLACED 2

// struct s_data {
//     int64_t value;
//     const char *key1;
//     struct s_node *node1;
//     const char *key2;
//     struct s_node *node2;
//     void *data;
// };

struct s_node {
    const char * primary_key;
    const char * secondary_key;
    struct s_node *next_n[S_HEIGHT];
    struct s_node *prev_n[S_HEIGHT];
    void *data;
};

struct s_list {
    struct s_node *first_n[S_HEIGHT];
    u_int64_t elements;

    struct prand pseed;
};

// struct s_list *slist_create(int reverse, size_t *size);
// struct s_data * slist_find(struct s_list *list, const char *firstkey, const char *secondkey);
// struct s_node *slist_find_first(struct s_list *list, const char *key);
// struct s_data * slist_insert(struct s_list *list, struct s_data *data);
// struct s_data * slist_delete(struct s_list *list, const char *key1, const char *key2);
// void slist_free(struct s_list *list, void (*freenode)(struct s_data *data));

inline static struct s_list *slist_create()
{
    int i;

    struct s_list *result = (struct s_list *)MALLOC(sizeof(struct s_list));
    struct s_node *first_node = (struct s_node *)MALLOC(sizeof(struct s_node));
    memset(result, 0, sizeof(struct s_list));
    memset(first_node, 0, sizeof(struct s_node));

    for (i=0; i < S_HEIGHT; i++) {
        result->first_n[i] = first_node;
    }

    pseed(&(result->pseed), time(NULL));

    return result;
}

// inline static struct s_data *slist_datanode_create(void *data, const char *key1, const char *key2, long value)
// {
//     struct s_data *result = MALLOC(sizeof(struct s_data));

//     result->data = data;
//     result->key1 = key1;
//     result->key2 = key2;
//     result->value = value;
//     result->node1 = NULL;
//     result->node2 = NULL;
//     return result;
// }

inline static int keycmp(const char *key1, const char *key2, const char *compare1, const char *compare2)
{
    unsigned char *ptr, *ptr2;
    int phase = 0;

    if (key1 == NULL)
        return -1;
    if (compare1 == NULL)
        return 1;
    
    ptr = (unsigned char *)key1;
    ptr2 = (unsigned char *)compare1;

    for (;;) {
        if (*ptr != *ptr2) {
            return *ptr - *ptr2;
        }
        if (*ptr == '\0' && phase == 0) {
            if (key2 == NULL || compare2 == NULL)
                return 0;
            ptr = (unsigned char *)key2;
            ptr2 = (unsigned char *)compare2;
            phase = 1;
        } else if (*ptr == '\0' && phase == 1) {
            return 0;
        }
        ptr++;
        ptr2++;
    }
    return 0;
}

inline static struct s_node * slist_path(struct s_list *list, const char *key1, const char *key2, struct s_node **path)
{
    int height = S_HEIGHT;

    int i;

    struct s_node *node;

    node = list->first_n[0];

    for (i=height-1; i >= 0; --i) {
        path[i] = node;
        node = node->next_n[i];
        while (node) {
            int cmp = keycmp(node->primary_key, node->secondary_key, key1, key2);
            if (cmp == 0)
                return node;
            if (cmp > 0) {
                node = node->prev_n[i];
                break;
            }
            path[i] = node;
            node = node->next_n[i];
        }
        node = path[i];
    }
    return NULL;
}

inline static struct s_node * slist_find(struct s_list *list, const char *firstkey, const char *secondkey)
{

    struct s_node *node;

    struct s_node *path[S_HEIGHT];

    node = slist_path(list, firstkey, secondkey, path);

    return node;
}

inline static void * slist_find_first(struct s_list *list, const char *key)
{
    struct s_node *node;
    struct s_node *prev;

    struct s_node *path[S_HEIGHT];

    const char *key1 = key;
    const char *key2 = NULL;

    node = slist_path(list, key1, key2, path);

    if (node == NULL)
        return NULL;
    
    // We need a faster algorithm - JT
    for (prev = node->prev_n[0]; keycmp(prev->primary_key, NULL, key, NULL) == 0; node = prev, prev = node->prev_n[0]);
    return node;
}

inline static void * slist_insert(struct s_list *list, const char *key1, const char *key2, void *datanode)
{

    struct s_node *node;
    struct s_node *path[S_HEIGHT];
    void *olddata;

    int i;
    unsigned int rtest;

    node = slist_path(list, key1, key2, path);

    if (node) {
        olddata = node->data;
        node->data = datanode;
        return olddata;
    }

    node = (struct s_node *)MALLOC(sizeof(struct s_node));
    memset(node, 0, sizeof(struct s_node));
    node->primary_key = STRDUP(key1);
    node->secondary_key = STRDUP(key2);
    node->data = datanode;
    
    for (i = 0; i < S_HEIGHT; i++) {
        rtest = prand(&(list->pseed)) % 2; 
        if (i > 0 && !rtest)
            return NULL; // NULL => Did not replace old data
        node->prev_n[i] = path[i];
        node->next_n[i] = path[i]->next_n[i];
        if (node->next_n[i])
            node->next_n[i]->prev_n[0] = node;
        path[i]->next_n[i] = node;
    }
    return NULL;
}


// inline static struct s_data * slist_delete(struct s_list *list, const char *key1, const char *key2)
// {
//     int i;
//     int height;

//     struct s_data *result;

//     struct s_node *next;
//     struct s_node *start;
//     // struct s_node *node;

//     if (list->reverse) {
//         const char *tmp = key1;
//         key1 = key2;
//         key2 = tmp;
//     }

//     struct s_node *prevs[S_HEIGHT];

//     memset(prevs,0,sizeof(prevs));

//     height = list->height;

//     start = list->first_n[height-1];

//     int cmpvalue;

//     cmpvalue = -1;
    
//     next = NULL;
//     for (i = height-1; i >= 0; --i) {
//         for (next = start; next && (cmpvalue = keycmp(next->primary_key, next->secondary_key, key1, key2)) < 0; prevs[i] = next, next = next->next_n[i]);
//         if (cmpvalue == 0) {
//             break;
//         }
//         start = prevs[i];       
//     }

//     if (next && cmpvalue == 0) {
//         for (i=0; i < height; i++) {
//             if (prevs[i] && prevs[i]->next_n[i] == next) {
//                 if (prevs[i] == NULL) {
//                     list->first_n[i] = next->next_n[i];
//                 } else {
//                     prevs[i]->next_n[i] = next->next_n[i];
//                 }
//             }
//         }
//         result = next->data;
//         FREE(next);
//         return result;  // Unique node, node deleted
//     }
//     return NULL; // Not found
// }

inline static void slist_free(struct s_list *list)
{
    struct s_node *node;
    struct s_node *tmp;

    for (node = list->first_n[0]; node;) {
        tmp = node;
        node = node->next_n[0];
        FREE(tmp);
    }
    FREE(list);
}

inline static void * slist_delete(struct s_list *list, const char *key1, const char *key2)
{
    int i;
    int height;
    struct s_node *node;
    void *result;

    struct s_node *path[S_HEIGHT];

    memset(path,0,sizeof(path));


    height = S_HEIGHT;

    node = slist_path(list, key1, key2, path);
    result = NULL;

    if (node) {
        result = node->data;
        for (i=0; i<height; i++) {
            if (node->prev_n[i])
                node->prev_n[i]->next_n[i] = node->next_n[i];
            if (node->next_n[i]) {
                node->next_n[i]->prev_n[i] = node->prev_n[i];
            }
        }
        FREE(node);
    }
    return result;
}
