CassandraCache is an implementation of .net 4.0's [ObjectCache](http://msdn.microsoft.com/en-us/library/system.runtime.caching.objectcache.aspx) that saves to an instance of a [Cassandra Db](http://cassandra.apache.org/).

Cassandra is scalable, easily replicated database that runs on any platform that can run Java. 

## Installation

It's easiest to install via NuGet. 

Run the following command in the Package Manager Console

    PM> Install-Package LuckyAssetManager

## Example Usage

### Setup Cassandra

Create a new keyspace for CassandraCache to use. The name will be passed into Cassandra when instantiated. 

Next, create a ColumnFamily with the name "Default" that has the "Super" column type. If you want to uses a different family (or use multiple families to store different information), this can be overriden by passing a different string in the "region" optional parameter with all caching mechanisms. 


### Code

    // create new instance of the cache
    var cache = new CassandraCache<string>(keySpace);
    // add string value to the cache with a expiration of 10 minutes from now
    cache.Set("key", "value", DateTimeOffset.Now.AddMinutes(10) /*, regionName: "NotDefault" */);
    // get our value from the cache
    var value = cache.Get("key" /*, regionName: "NotDefault" */);
    
For serialization purposes, you'll notice that CassandraCache is a generic type. Any non-primitive type object must have the `[Serializable]` attribute. Internally, CassandraCache uses the serialization mechanism of [Cassandraemon](http://cassandraemon.codeplex.com). Refer to their documentation to learn more. 

## Dependencies 

Internally, CassandraCache uses [Cassandraemon](http://cassandraemon.codeplex.com) to accesses Cassandra Db. If installed from NuGet, this package will also be installed.  

## Limitations

Currently, cache items can only be evicted with the SlidingExpirations and the AbsoluteExpirations policy types. Furthermore, these expirations are only checked on a `Get(...)` request. If you have a large number of cache items that are inserted but never retrieved, these records can remain on disk, growing in size. You may want to clear the cache on AppStart with the following function: 

    // create new instance of the cache
    var cache = new CassandraCache<string>(keySpace);
    cache.CleanCache(/* regionName: "NotDefault" */)