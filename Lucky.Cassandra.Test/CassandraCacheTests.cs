using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Mime;
using System.Reflection;
using System.Runtime.Caching;
using System.Threading;
using Apache.Cassandra;
using Cassandraemon;
using NUnit.Framework;

namespace Lucky.Cassandra.Test {
    // ReSharper disable InconsistentNaming

    [TestFixture]
    public class CassandraCacheTests {
       
        [SetUp]
        public virtual void SetUp() {
            InitializeKeyspace();
        }

        [TearDown]
        public virtual void TearDown() {
            DropKeyspace();
        }
        
        private const string _Host = "localhost";
        private const int _Port = 9160;
        private const string _KeySpace = "LuckyCassandraTest";
        private const string _ColumnFamily = "Default";

        private static void InitializeKeyspace(string keyspace = null) {
            if (keyspace == null) keyspace = _KeySpace;
            DropKeyspace(keyspace);
            try {
                using (var ctx = new CassandraContext(_Host, _Port, "system")) {
                    var ksdef = new KsDef {
                        Name = keyspace,
                        Replication_factor = 1,
                        Strategy_class = "org.apache.cassandra.locator.SimpleStrategy",
                        Cf_defs = new List<CfDef> {
                            new CfDef {
                                Keyspace = keyspace,
                                Name = _ColumnFamily,
                                Column_type = "Super"
                            }
                        }
                    };

                    ctx.SystemAddKeyspace(ksdef);
                }
            } catch {
                Assert.Inconclusive("Cannot initialize keyspace. Cannot proceed with tests.");
            }
        }

        private static void DropKeyspace(string keyspace = null) {
            if (keyspace == null) keyspace = _KeySpace;

            using (var ctx = new CassandraContext(_Host, _Port, "system")) {
                var rgksd = ctx.DescribeKeySpaces();
                if (rgksd.Any(ksd => ksd.Name == keyspace)) {
                    ctx.SystemDropKeyspace(keyspace);
                }
            }
        }

        private static CassandraContext GetContext() {
            return new CassandraContext(_Host, _Port, _KeySpace);
        }

        [Test]
        public void General_CanConnectToDatabase() {
            var cache = new CassandraCache<string>(_KeySpace);
            cache.Get("testKey");
        }

        #region Common Tasks

        private IQueryable<CassandraEntity<List<SuperColumn>>> GetSuperColumnList(CassandraContext db, string key) {
            var list = db.SuperColumnList.Where(c => c.ColumnFamily == _ColumnFamily && c.Key == key);
            Assert.That(list, Is.Not.Null);
            Assert.That(list.Any());
            Assert.That(list.Count(), Is.EqualTo(1));
            return list;
        }

        private void InsertCacheItem<T>(string key, T value, 
                DateTimeOffset? added = null, DateTimeOffset? lastAccessed = null,
                TimeSpan? slidingExpiration = null, DateTimeOffset? absoluteExpiration = null) where T : class {
            if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException("key");
            if (value == null) throw new ArgumentNullException("value");
            if (!added.HasValue) added = DateTimeOffset.Now;
            if (!lastAccessed.HasValue) lastAccessed = DateTimeOffset.Now;
            if (!slidingExpiration.HasValue) slidingExpiration = TimeSpan.Zero;
            if (!absoluteExpiration.HasValue) absoluteExpiration = DateTimeOffset.Now.AddMinutes(5);

            var item = new CassandraCache<T>.CacheItemColumn {
                Added = added.Value,
                Value = value,
                LastAccessed = lastAccessed.Value
            };

            var policy = new CassandraCache<T>.CacheItemPolicyColumn {
                SlidingExpiration = slidingExpiration.Value,
                AbsoluteExpiration = absoluteExpiration.Value
            };

            using (var db = GetContext()) {
                var columnList = new List<SuperColumn>()
                    .Add("Item", item)
                    .Add("Policy", policy);

                var entity = new CassandraEntity<List<SuperColumn>>()
                    .SetKey(key)
                    .SetColumnFamily(_ColumnFamily)
                    .SetData(columnList);

                db.SuperColumnList.InsertOnSubmit(entity);
                db.SubmitChanges();
            }
        }

        #endregion Common Tasks

        #region CleanCache

        [Test]
        public void CleanCache_NoParams_DeletesItAll() {
            const string testKey = "SetString1";
            const string testValue = "TestString1";
            var cache = new CassandraCache<string>(_KeySpace);

            cache.Set(testKey, testValue, DateTimeOffset.Now.AddMinutes(1));

            using (var db = GetContext()) {
                var list = GetSuperColumnList(db, testKey);
                var itemCount = list.Single().ToObjectDictionary<string, CassandraCache<string>.CacheItemColumn>()
                    .Count(c => c.Key == "Item");
                Assert.That(itemCount, Is.EqualTo(1));
            }

            cache.CleanCache();

            using (var db = GetContext()) {
                var list = GetSuperColumnList(db, testKey);
                var itemCount = list.Single().ToObjectDictionary<string, CassandraCache<string>.CacheItemColumn>()
                    .Count(c => c.Key == "Item");
                Assert.That(itemCount, Is.EqualTo(0));
            }
        }

        #endregion CleanCache

        #region Set Tests

        [Test]
        public void Set_String_SavesValue() {
            const string testKey = "SetString1";
            const string testValue = "TestString1";
            var cache = new CassandraCache<string>(_KeySpace);

            cache.Set(testKey, testValue, DateTimeOffset.Now.AddMinutes(1));

            using (var db = GetContext()) {
                var list = GetSuperColumnList(db, testKey);
                var item = list.Single().ToObjectDictionary<string, CassandraCache<string>.CacheItemColumn>()
                    .SingleOrDefault(c => c.Key == "Item").Value;
                Assert.That(item, Is.Not.Null);
                Assert.That(item.Value, Is.EqualTo(testValue));
            }
        }

        [Test]
        public void Set_DateTime_SavesValue() {
            const string testKey = "SetDateTime1";
            var testValue = DateTime.UtcNow;
            var cache = new CassandraCache<DateTime>(_KeySpace);

            cache.Set(testKey, testValue, DateTimeOffset.Now.AddMinutes(1));

            using (var db = GetContext()) {
                var list = GetSuperColumnList(db, testKey);
                var item = list.Single().ToObjectDictionary<string, CassandraCache<DateTime>.CacheItemColumn>()
                    .SingleOrDefault(c => c.Key == "Item").Value;
                Assert.That(item, Is.Not.Null);
                Assert.That(item.Value, Is.EqualTo(testValue));
            }
        }

        [Test]
        public void Set_Double_SavesValue() {
            const string testKey = "SetDouble1";
            const double testValue = double.MaxValue;
            var cache = new CassandraCache<double>(_KeySpace);

            cache.Set(testKey, testValue, DateTimeOffset.Now.AddMinutes(1));

            using (var db = GetContext()) {
                var list = GetSuperColumnList(db, testKey);
                var item = list.Single().ToObjectDictionary<string, CassandraCache<double>.CacheItemColumn>()
                    .SingleOrDefault(c => c.Key == "Item").Value;
                Assert.That(item, Is.Not.Null);
                Assert.That(item.Value, Is.EqualTo(testValue));
            }
        }

        [Serializable]
        public class ClassWithPrimitives {
            public string Test1 { get; set; }
            public double Test2 { get; set; }
            public Guid Test3 { get; set; }
        }

        [Test]
        public void Set_ClassWithPrimitives_SavesValue() {
            const string testKey = "SetClassWithPrimitives1";
            var testValue = new ClassWithPrimitives { Test1 = "testVal1", Test2 = 5.2, Test3 = new Guid() };
            var cache = new CassandraCache<ClassWithPrimitives>(_KeySpace);

            cache.Set(testKey, testValue, DateTimeOffset.Now.AddMinutes(1));

            using (var db = GetContext()) {
                var list = GetSuperColumnList(db, testKey);
                var item = list.Single().ToObjectDictionary<string, CassandraCache<ClassWithPrimitives>.CacheItemColumn>()
                    .SingleOrDefault(c => c.Key == "Item").Value;
                Assert.That(item, Is.Not.Null);
                Assert.That(item.Value.Test1, Is.EqualTo(testValue.Test1));
                Assert.That(item.Value.Test2, Is.EqualTo(testValue.Test2));
                Assert.That(item.Value.Test3, Is.EqualTo(testValue.Test3));
            }
        }

        [Test]
        public void Set_AddedDateTimeOffset_IsSet() {
            const string testKey = "SetString1";
            const string testValue = "TestString1";
            var cache = new CassandraCache<string>(_KeySpace);

            cache.Set(testKey, testValue, DateTimeOffset.Now.AddMinutes(1));

            using (var db = GetContext()) {
                var list = GetSuperColumnList(db, testKey);
                var item = list.Single().ToObjectDictionary<string, CassandraCache<string>.CacheItemColumn>()
                    .SingleOrDefault(c => c.Key == "Item").Value;
                Assert.That(item, Is.Not.Null);
                // we don't know when exactly the code ran so just make sure it's set to a reasonable value
                Assert.That(item.Added, Is.GreaterThan(DateTimeOffset.Now.AddSeconds(-30)));
                Assert.That(item.Added, Is.LessThan(DateTimeOffset.Now));
            }
        }

        [Test]
        public void Set_SlidingExpirationPolicy_SavesPolicy() {
            const string testKey = "SetString1";
            var testValue = TimeSpan.FromMinutes(20);
            var cache = new CassandraCache<string>(_KeySpace);

            cache.Set(testKey, "anything", new CacheItemPolicy { SlidingExpiration = testValue });

            using (var db = GetContext()) {
                var list = GetSuperColumnList(db, testKey);
                var policy = list.Single().ToObjectDictionary<string, CassandraCache<string>.CacheItemPolicyColumn>()
                    .SingleOrDefault(c => c.Key == "Policy").Value;
                Assert.That(policy, Is.Not.Null);
                Assert.That(policy.SlidingExpiration, Is.EqualTo(testValue));
            }
        }

        [Test]
        public void Set_AbsoluteExpirationPolicy_SavesPolicy() {
            const string testKey = "SetString1";
            var testValue = DateTimeOffset.Now.AddMinutes(5);
            var cache = new CassandraCache<string>(_KeySpace);

            cache.Set(testKey, "anything", new CacheItemPolicy { AbsoluteExpiration = testValue });

            using (var db = GetContext()) {
                var list = GetSuperColumnList(db, testKey);
                var policy = list.Single().ToObjectDictionary<string, CassandraCache<string>.CacheItemPolicyColumn>()
                    .SingleOrDefault(c => c.Key == "Policy").Value;
                Assert.That(policy, Is.Not.Null);
                Assert.That(policy.AbsoluteExpiration, Is.EqualTo(testValue));
            }
        }

        #endregion Set Tests

        #region Get Tests

        [Test]
        public void Get_String_ReturnsCachedValue() {
            const string testKey = "Get_String_ReturnsCachedValue";
            const string testValue = "TestString1";
            var cache = new CassandraCache<string>(_KeySpace);

            cache.Set(testKey, testValue, DateTimeOffset.Now.AddMinutes(1));

            var value = cache.Get(testKey);
            Assert.That(value, Is.Not.Null);
            Assert.That(value, Is.TypeOf<string>());
            Assert.That(value as string, Is.EqualTo(testValue));
        }

        [Test]
        public void Get_SlidingExpiredItemNeverAccessed_ReturnsNull() {
            const string testKey = "Get_SlidingExpiredItemNeverAccessed_ReturnsNull";
            InsertCacheItem(testKey, "TestString1",
                                    slidingExpiration: TimeSpan.FromMinutes(5),
                                    lastAccessed: DateTimeOffset.Now.AddMinutes(-6),
                                    added: DateTimeOffset.Now.AddMinutes(-6)
                );

            var cache = new CassandraCache<string>(_KeySpace);

            var value = cache.Get(testKey);
            Assert.That(value, Is.Null);
        }

        [Test]
        public void Get_SlidingValidItemNeverAccessed_ReturnsItem() {
            const string testKey = "Get_SlidingValidItemNeverAccessed_ReturnsItem";
            const string testValue = "TestString1";
            InsertCacheItem(testKey, testValue,
                                    slidingExpiration: TimeSpan.FromMinutes(5),
                                    lastAccessed: DateTimeOffset.Now.AddMinutes(-2),
                                    added: DateTimeOffset.Now.AddMinutes(-2)
                );

            var cache = new CassandraCache<string>(_KeySpace);

            var value = cache.Get(testKey);
            Assert.That(value, Is.Not.Null);
            Assert.That(value, Is.TypeOf<string>());
            Assert.That(value, Is.EqualTo(testValue));
        }

        [Test]
        public void Get_SlidingExpiredItem_ReturnsNull() {
            const string testKey = "Get_SlidingExpiredItem_ReturnsNull";
            InsertCacheItem(testKey, "TestString1",
                                    slidingExpiration: TimeSpan.FromMinutes(5),
                                    lastAccessed: DateTimeOffset.Now.AddMinutes(-6),
                                    added: DateTimeOffset.Now.AddMinutes(-10)
                );

            var cache = new CassandraCache<string>(_KeySpace);

            var value = cache.Get(testKey);
            Assert.That(value, Is.Null);
        }

        [Test]
        public void Get_SlidingValidItemAccessedRecently_ReturnsItem() {
            const string testKey = "Get_SlidingValidItemAccessedRecently_ReturnsItem";
            const string testValue = "TestString1";
            InsertCacheItem(testKey, testValue,
                                    slidingExpiration: TimeSpan.FromMinutes(5),
                                    lastAccessed: DateTimeOffset.Now.AddMinutes(-2),
                                    added: DateTimeOffset.Now.AddMinutes(-10)
                );

            var cache = new CassandraCache<string>(_KeySpace);

            var value = cache.Get(testKey);
            Assert.That(value, Is.Not.Null);
            Assert.That(value, Is.TypeOf<string>());
            Assert.That(value, Is.EqualTo(testValue));
        }

        [Test]
        public void Get_AbsoluteExpiredItem_ReturnsNull() {
            const string testKey = "Get_AbsoluteExpiredItem_ReturnsNull";
            const string testValue = "TestString1";
            InsertCacheItem(testKey, testValue,
                                    absoluteExpiration: DateTimeOffset.Now.AddMinutes(-2),
                                    added: DateTimeOffset.Now.AddMinutes(-10)
                );
            var cache = new CassandraCache<string>(_KeySpace);

            var value = cache.Get(testKey);
            Assert.That(value, Is.Null);
        }

        [Test]
        public void Get_AbsoluteValidItem_ReturnsItem() {
            const string testKey = "Get_AbsoluteExpiredItem_ReturnsNull";
            const string testValue = "TestString1";
            InsertCacheItem(testKey, testValue,
                                    absoluteExpiration: DateTimeOffset.Now.AddMinutes(2),
                                    added: DateTimeOffset.Now.AddMinutes(-10)
                );
            var cache = new CassandraCache<string>(_KeySpace);

            var value = cache.Get(testKey);
            Assert.That(value, Is.Not.Null);
            Assert.That(value, Is.TypeOf<string>());
            Assert.That(value, Is.EqualTo(testValue));
        }

        private readonly List<string> filePaths = new List<string>();
        private void CreateTestFiles() {
            var path = Path.GetDirectoryName(AppDomain.CurrentDomain.BaseDirectory);
            //throw new Exception(dir);
            for (int i = 1; i < 4; i++) {
                var newPath = Path.Combine(path, "TestFile" + i);
                filePaths.Add(newPath);
                using (FileStream fs = File.Create(newPath)) {
                    for (byte b = 0; b < 100; b++) {
                        fs.WriteByte(b);
                    }
                }
            }
        }

        private void ModifyFile(int index) {
            using (FileStream fs = File.Create(filePaths[index])) {
                for (byte b = 100; b < 200; b++) {
                    fs.WriteByte(b);
                }
            }
        }

        private void DestroyTestFiles() {
            foreach (var path in filePaths.Where(File.Exists)) {
                File.Delete(path);
            }
        }

        [Test]
        public void Get_SingleChangeMonitor_EvictsItem() {
            const string testKey = "Get_SingleChangeMonitor_EvictsItem";
            const string testValue = "TestString1";
            var cache = new CassandraCache<string>(_KeySpace);
            try {
                CreateTestFiles();
                var policy = new CacheItemPolicy();
                var paths = new List<string> { filePaths[0] };
                policy.ChangeMonitors.Add(new HostFileChangeMonitor(paths));

                cache.Set(testKey, testValue, policy);
                var value = cache.Get(testKey);

                Assert.That(value, Is.Not.Null);
                Assert.That(value, Is.TypeOf<string>());
                Assert.That(value, Is.EqualTo(testValue));

                ModifyFile(0);

                Thread.Sleep(300); // give it time to update and execute callback

                value = cache.Get(testKey);

                Assert.That(value, Is.Null);
            } finally {
                DestroyTestFiles();
            }
        }

        [Test]
        public void Get_MultipleChangeMonitor_EvictsItem() {
            const string testKey = "Get_MultipleChangeMonitor_EvictsItem";
            const string testValue = "TestString1";
            var cache = new CassandraCache<string>(_KeySpace);
            try {
                CreateTestFiles();
                var policy = new CacheItemPolicy();
                var paths = new List<string> { filePaths[0], filePaths[1], filePaths[2] };
                policy.ChangeMonitors.Add(new HostFileChangeMonitor(paths));

                cache.Set(testKey, testValue, policy);
                var value = cache.Get(testKey);

                Assert.That(value, Is.Not.Null);
                Assert.That(value, Is.TypeOf<string>());
                Assert.That(value, Is.EqualTo(testValue));

                ModifyFile(2);

                Thread.Sleep(300); // give it time to update and execute callback

                value = cache.Get(testKey);

                Assert.That(value, Is.Null);
            } finally {
                DestroyTestFiles();
            }
        }

        [Test]
        public void Get_SingleChangeMonitor_EvictsProperItem() {
            const string testKey1 = "Get_SingleChangeMonitor_EvictsProperItem1";
            const string testKey2 = "Get_SingleChangeMonitor_EvictsProperItem2";
            const string testValue = "TestString1";
            var cache = new CassandraCache<string>(_KeySpace);
            try {
                CreateTestFiles();
                var policy1 = new CacheItemPolicy();
                var paths1 = new List<string> { filePaths[1] };
                policy1.ChangeMonitors.Add(new HostFileChangeMonitor(paths1));

                cache.Set(testKey1, testValue, policy1);
                var value1 = cache.Get(testKey1);

                Assert.That(value1, Is.Not.Null);
                Assert.That(value1, Is.TypeOf<string>());
                Assert.That(value1, Is.EqualTo(testValue));

                var policy2 = new CacheItemPolicy();
                var paths2 = new List<string> { filePaths[2] };
                policy2.ChangeMonitors.Add(new HostFileChangeMonitor(paths2));

                cache.Set(testKey2, testValue, policy2);
                var value2 = cache.Get(testKey1);

                Assert.That(value2, Is.Not.Null);
                Assert.That(value2, Is.TypeOf<string>());
                Assert.That(value2, Is.EqualTo(testValue));

                ModifyFile(2);

                Thread.Sleep(300); // give it time to update and execute callback

                value1 = cache.Get(testKey1);
                value2 = cache.Get(testKey2);

                Assert.That(value1, Is.Not.Null);
                Assert.That(value1, Is.TypeOf<string>());
                Assert.That(value1, Is.EqualTo(testValue));
                Assert.That(value2, Is.Null);
            } finally {
                DestroyTestFiles();
            }
        }

        [Test]
        public void Get_MultipleChangeMonitor_EvictsMultipleItems() {
            const string testKey1 = "Get_SingleChangeMonitor_EvictsProperItem1";
            const string testKey2 = "Get_SingleChangeMonitor_EvictsProperItem2";
            const string testValue = "TestString1";
            var cache = new CassandraCache<string>(_KeySpace);
            try {
                CreateTestFiles();
                var policy1 = new CacheItemPolicy();
                var paths1 = new List<string> { filePaths[1] };
                policy1.ChangeMonitors.Add(new HostFileChangeMonitor(paths1));

                cache.Set(testKey1, testValue, policy1);
                var value1 = cache.Get(testKey1);

                Assert.That(value1, Is.Not.Null);
                Assert.That(value1, Is.TypeOf<string>());
                Assert.That(value1, Is.EqualTo(testValue));

                var policy2 = new CacheItemPolicy();
                var paths2 = new List<string> { filePaths[2] };
                policy2.ChangeMonitors.Add(new HostFileChangeMonitor(paths2));

                cache.Set(testKey2, testValue, policy2);
                var value2 = cache.Get(testKey1);

                Assert.That(value2, Is.Not.Null);
                Assert.That(value2, Is.TypeOf<string>());
                Assert.That(value2, Is.EqualTo(testValue));

                ModifyFile(1);
                ModifyFile(2);

                Thread.Sleep(300); // give it time to update and execute callback

                value1 = cache.Get(testKey1);
                value2 = cache.Get(testKey2);

                Assert.That(value1, Is.Null);
                Assert.That(value2, Is.Null);
            } finally {
                DestroyTestFiles();
            }
        }

        #endregion Get Tests
    }

    // ReSharper restore InconsistentNaming
}