package main

import (
	"fmt"
	"sync"
)

// To represent an item stored in cache
type CacheItem struct {
	Key   string
	Value string
}

// It represents the sharded part of the cache
type Shard struct {
	Items map[string]CacheItem
	Lock  sync.RWMutex
}

// It represents the distributed cache system
type Cache struct {
	Shards    []*Shard
	NumShards int
}

func hash(key string) int {
	return len(key)
}

// Initialize a new cache with sharding and replication
func NewCache(numShards int) *Cache {
	cache := &Cache{
		Shards:    make([]*Shard, numShards),
		NumShards: numShards,
	}

	for i := 0; i < numShards; i++ {
		cache.Shards[i] = &Shard{
			Items: make(map[string]CacheItem),
		}
	}

	return cache
}

// Implementing a shard selection bases on key hash
func (c *Cache) GetShard(key string) *Shard {
	shardIndex := hash(key) % c.NumShards
	return c.Shards[shardIndex]
}

// Operation for caching to Set, Get and Delete
// Get
func (c *Cache) Get(key string) (string, bool) {
	shard := c.GetShard(key)
	shard.Lock.RLock()
	defer shard.Lock.RUnlock()

	item, ok := shard.Items[key]
	if !ok {
		return "", false
	}
	return item.Value, true
}

// Set
func (c *Cache) Set(key, value string) {
	shard := c.GetShard(key)
	shard.Lock.Lock()
	defer shard.Lock.Unlock()
	shard.Items[key] = CacheItem{Key: key, Value: value}
}

// Delete
func (c *Cache) Delete(key string) {
	shard := c.GetShard(key)
	shard.Lock.Lock()
	defer shard.Lock.Unlock()
	delete(shard.Items, key)
}

func main() {
	cache := NewCache(4)

	cache.Set("Name", "Nikhil")
	cache.Set("Age", "27")
	cache.Set("Sex", "Male")
	cache.Set("Blood", "O+")
	cache.Set("Profession", "SDE")

	val1, found := cache.Get("Name")
	if found {
		fmt.Println("Value for key1:", val1)
	} else {
		fmt.Println("Key1 not found in cache")
	}

	val2, found := cache.Get("Profession")
	if found {
		fmt.Println("Value for key5:", val2)
	} else {
		fmt.Println("Key not found in cache")
	}

	cache.Delete("Age")

	for i := 0; i < len(cache.Shards); i++ {
		fmt.Println(cache.Shards[i].Items)
	}
}
