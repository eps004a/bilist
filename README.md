# bilist - two-directional index module for redis

This is a two-way index module for redis.

Note: You will need redismodule.h from the [redis distribution](https://github.com/redis/redis) in order to compile the add-on.  Please include it in the redis/src directory 

## Usage:

You may add the compiled module to redis.  Please see the [redis documentation](https://redis.io/topics/modules-intro) for more info.

The following commands are available once the module is loaded:

- bilist.ckey list-name length - create an atomic key of length (length + 8) 
- bilist.set list-name key1 key2 value expire-time - set value to index pair (key1,key2)
- bilist.get list-name key1 key2 - get value from index pair (key1, key2)
- bilist.get1 list-name key1 - get value based on first key
- bilist.get2 list-name key2 - get value based on second key
- bilist.del list-name key1 key2 - delete value based on (key1,key2)-pair
- bilist.count list-name - get the number of elements in a bilist
- bilist.all list-name - get all keys from a bilist

Bilist uses an internal [skip list](https://en.wikipedia.org/wiki/Skip_list) data structure
