using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Caching;
using Apache.Cassandra;
using Cassandraemon;

namespace Lucky.Cassandra {

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
                    var spec = db.DescribeKeySpace(_keyspace);
                    var familySpec = spec.Cf_defs.Single(cf => cf.Name == familyName);
                    db.SystemDropColumnFamily(familyName);
                    db.SystemAddColumnFamily(familySpec);
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
                return DefaultCacheCapabilities.OutOfProcessProvider
                       | DefaultCacheCapabilities.AbsoluteExpirations
                       | DefaultCacheCapabilities.SlidingExpirations
                       | DefaultCacheCapabilities.CacheEntryChangeMonitors
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
                var columnList = db.SuperColumnList.Where(c => c.Key == key && c.ColumnFamily == familyName).ToList();
                if (!columnList.Any() || columnList.Count != 1 || !columnList.Single().Data.Any()) {
                    return null;
                }

                CacheItemColumn item = columnList.Single().ToObjectDictionary<string, CacheItemColumn>()
                    .Single(c => c.Key == "Item").Value;
                CacheItemPolicyColumn policy = columnList.Single().ToObjectDictionary<string, CacheItemPolicyColumn>()
                    .Single(c => c.Key == "Policy").Value;

                if (policy != null) {
                    if (policy.SlidingExpiration > TimeSpan.Zero) {
                        if (DateTimeOffset.Now - item.LastAccessed > policy.SlidingExpiration) {
                            db.Column.DeleteOnSubmit(c => c.ColumnFamily == familyName && c.Key == key);
                            db.SubmitChanges();
                            return null;
                        } else {
                            item.LastAccessed = DateTimeOffset.Now;
                            Set(key, familyName, item, policy);
                        }
                    }
                    if (policy.AbsoluteExpiration != DateTimeOffset.MinValue && policy.AbsoluteExpiration < DateTimeOffset.Now) {
                        db.Column.DeleteOnSubmit(c => c.ColumnFamily == familyName && c.Key == key);
                        db.SubmitChanges();
                        return null;
                    }
                }

                return item.Value;
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

            var itemColumn = new CacheItemColumn {
                Added = DateTimeOffset.Now, 
                Value = (T)item.Value
            };
            itemColumn.LastAccessed = itemColumn.Added;
                
            var policyColumn = new CacheItemPolicyColumn {
                SlidingExpiration = policy.SlidingExpiration,
                AbsoluteExpiration = policy.AbsoluteExpiration
            };

            if (policy.ChangeMonitors.Any()) {
                foreach (var monitor in policy.ChangeMonitors) {
                    monitor.NotifyOnChanged(state => Remove(item.Key, familyName));
                }
            }

            Set(item.Key, familyName, itemColumn, policyColumn);
        }

        private void OnChangedCallback(object state) {
            throw new NotImplementedException();
        }

        private void Set(string key, string familyName, CacheItemColumn item, CacheItemPolicyColumn policy) {
            using (var db = CreateCassandraContext()) {
                var columnList = new List<SuperColumn>()
                    .Add("Item", item)
                    .Add("Policy", policy);

                var entity = new CassandraEntity<List<SuperColumn>>()
                    .SetKey(key)
                    .SetColumnFamily(familyName)
                    .SetData(columnList);

                db.SuperColumnList.InsertOnSubmit(entity);
                db.SubmitChanges();
            }
        }

        public class CacheItemColumn {
            public DateTimeOffset Added { get; set; }
            public T Value { get; set; }
            public DateTimeOffset LastAccessed { get; set; }
        }

        public class CacheItemPolicyColumn {
            public TimeSpan SlidingExpiration { get; set; }
            public DateTimeOffset AbsoluteExpiration { get; set; }
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
}
