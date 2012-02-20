using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Caching;
using System.Xml.Serialization;
using Apache.Cassandra;
using Cassandraemon;

namespace Lucky.CassandraCache {

    public class CassandraCache<T> : ObjectCache {
        private const string DefaultFamilyName = "Default";
        private readonly string _keyspace;
        private readonly string _host;
        private readonly int _port;

        public CassandraCache(string keyspace, string host = "localhost", int port = 9160) {
            if (string.IsNullOrWhiteSpace(host)) {
                throw new ArgumentNullException("host");
            }
            if (string.IsNullOrWhiteSpace(keyspace)) {
                throw new ArgumentNullException("keyspace");   
            }

            _keyspace = keyspace;
            _host = host;
            _port = port;
        }

        public void CleanCache(string regionName = null, DateTimeOffset? maxAge = null) {
            using (var db = CreateCassandraContext()) {
                var familyName = regionName ?? DefaultFamilyName;
                if (maxAge.HasValue) {
                    // todo: keep recently created cache items 
                    // but omg! how?!
                } else {
                    db.Column.DeleteOnSubmit(c => c.ColumnFamily == familyName);
                    db.SubmitChanges();
                }
            }
        }

        private CassandraContext CreateCassandraContext() {
            return new CassandraContext(_host, _port, _keyspace);
        }

        public override string Name {
            get { return "CassandraCache"; }
        }

        public override DefaultCacheCapabilities DefaultCacheCapabilities {
            get {
                return   DefaultCacheCapabilities.OutOfProcessProvider
                       | DefaultCacheCapabilities.AbsoluteExpirations
                       | DefaultCacheCapabilities.SlidingExpirations
                       | DefaultCacheCapabilities.CacheRegions;
            }
        }

        public override object AddOrGetExisting(string key, object value, CacheItemPolicy policy, string regionName = null) {
            return AddOrGetExisting(new CacheItem(key, value, regionName), policy).Value;
        }

        public override object AddOrGetExisting(string key, object value, DateTimeOffset absoluteExpiration, string regionName = null) {
            return AddOrGetExisting(new CacheItem(key, value, regionName), new CacheItemPolicy { AbsoluteExpiration = absoluteExpiration }).Value;
        }

        public override CacheItem AddOrGetExisting(CacheItem value, CacheItemPolicy policy) {
            var data = Get(value.Key, value.RegionName);
            if (data != null) {
                return new CacheItem(value.Key, data, value.RegionName);
            }

            Set(value, policy);
            return value;
        }

        public override object Get(string key, string regionName = null) {
            var familyName = regionName ?? DefaultFamilyName;
            using (var db = CreateCassandraContext()) {
                var family = db.GetColumnFamily<UTF8Type, BytesType>(familyName);
                dynamic column = family.Get(key).FirstOrDefault();
                if (column == null) {
                    return null;
                }

                if (column.Policy != null) {
                    if (column.Policy.SlidingExpiration is long) {
                        var slidingExpiration = new TimeSpan((long)column.Policy.SlidingExpiration);
                        if (slidingExpiration > TimeSpan.Zero) {
                            var lastAccessedDynamic = column.Item.LastAccessed ?? column.Item.Added;
                            var lastAccessed = (DateTimeOffset) lastAccessedDynamic;
                            if (DateTimeOffset.Now - lastAccessed > slidingExpiration) {
                                family.RemoveKey(key);
                                return null;
                            } else {
                                column.Item.LastAccessed = DateTimeOffset.Now;
                                db.SaveChanges();
                            }
                        }
                    }
                    if (column.Policy.AbsoluteExpiration is DateTimeOffset) {
                        var absoluteExpiration = (DateTimeOffset) column.Policy.AbsoluteExpiration;
                        if (absoluteExpiration != DateTimeOffset.MinValue && absoluteExpiration < DateTimeOffset.Now) {
                            family.RemoveKey(key);
                            return null;
                        }
                    }
                }

                return Deserialize(column.Item.Value);
            }
        }

        public override bool Contains(string key, string regionName = null) {
            return Get(key, regionName) != null;
        }

        public override CacheItem GetCacheItem(string key, string regionName = null) {
            var data = Get(key, regionName);
            if (data != null) {
                return new CacheItem(key, data, regionName); 
            }

            return null;
        }

        public override object Remove(string key, string regionName = null) {
            var data = Get(key, regionName);
            if (data != null) {

                var familyName = regionName ?? DefaultFamilyName;
                using (var db = CreateCassandraContext()) {
                    db.Column.DeleteOnSubmit(c => c.ColumnFamily == familyName && c.Key == key);
                    db.SubmitChanges();
                }

                return data;
            }

            return null;
        }

        public override void Set(string key, object value, CacheItemPolicy policy, string regionName = null) {
            Set(new CacheItem(key, value, regionName), policy);
        }

        public override void Set(string key, object value, DateTimeOffset absoluteExpiration, string regionName = null) {
            Set(new CacheItem(key, value, regionName), new CacheItemPolicy { AbsoluteExpiration = absoluteExpiration });
        }

        public override void Set(CacheItem item, CacheItemPolicy policy) {
            if (item == null) throw new ArgumentNullException("item");
            if (item.Value == null) return;

            var familyName = item.RegionName ?? DefaultFamilyName;
            using (var db = CreateCassandraContext()) {

                var itemColumn = new List<Column>();

                itemColumn.Add("Added", DateTimeOffset.Now);
                itemColumn.Add("Value", item.Value);

                var policyColumn = new List<Column>();

                policyColumn.Add("SlidingExpiration", policy.SlidingExpiration.Ticks);
                policyColumn.Add("AbsoluteExpiration", policy.AbsoluteExpiration);
                
                var columnList = new List<SuperColumn>();
                columnList.Add("Item", itemColumn);
                columnList.Add("Policy", policyColumn);

                db.InsertOnSubmit(familyName, item.Key, columnList);
            }
            
        }

        private static string Serialize(object item) {
            var serializer = new XmlSerializer(typeof(T));
            var stream = new StringWriter();
            serializer.Serialize(stream, item);
            return stream.ToString();
        }

        private static object Deserialize(string serializedItem) {
            if (string.IsNullOrWhiteSpace(serializedItem)) {
                return null;
            }
            var serializer = new XmlSerializer(typeof(T));
            var stream = new StringReader(serializedItem);
            return serializer.Deserialize(stream);
        }

        public override object this[string key] {
            get { return Get(key); }
            set { throw new NotImplementedException(); }
        }
        
        


        public override long GetCount(string regionName = null) {
            throw new NotSupportedException();
        }

        protected override IEnumerator<KeyValuePair<string, object>> GetEnumerator() {
            throw new NotSupportedException();
        }

        public override IDictionary<string, object> GetValues(IEnumerable<string> keys, string regionName = null) {
            throw new NotSupportedException();
        }

        public override CacheEntryChangeMonitor CreateCacheEntryChangeMonitor(IEnumerable<string> keys, string regionName = null) {
            throw new NotSupportedException();
        }

}
