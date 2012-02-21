using System;
using System.Collections.Generic;
using System.Linq;
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

        #region Set Tests

        [Test]
        public void Set_String_SavesValue() {
            const string testKey = "SetString1";
            var testValue = "TestString1";
            var cache = new CassandraCache<string>(_KeySpace);

            cache.Set(testKey, testValue, DateTimeOffset.Now.AddMinutes(1));

            using (var db = GetContext()) {
                var list = db.SuperColumnList.Where(c => c.ColumnFamily == _ColumnFamily && c.Key == testKey);
                Assert.That(list, Is.Not.Null);
                Assert.That(list.Any());
                Assert.That(list.Count(), Is.EqualTo(1));
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
                var list = db.SuperColumnList.Where(c => c.ColumnFamily == _ColumnFamily && c.Key == testKey);
                Assert.That(list, Is.Not.Null);
                Assert.That(list.Any());
                Assert.That(list.Count(), Is.EqualTo(1));
                var item = list.Single().ToObjectDictionary<string, CassandraCache<DateTime>.CacheItemColumn>()
                    .SingleOrDefault(c => c.Key == "Item").Value;
                Assert.That(item, Is.Not.Null);
                Assert.That(item.Value, Is.EqualTo(testValue));
            }
        }

        [Test]
        public void Set_Double_SavesValue() {
            const string testKey = "SetDouble1";
            var testValue = double.MaxValue;
            var cache = new CassandraCache<double>(_KeySpace);

            cache.Set(testKey, testValue, DateTimeOffset.Now.AddMinutes(1));

            using (var db = GetContext()) {
                var list = db.SuperColumnList.Where(c => c.ColumnFamily == _ColumnFamily && c.Key == testKey);
                Assert.That(list, Is.Not.Null);
                Assert.That(list.Any());
                Assert.That(list.Count(), Is.EqualTo(1));
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
                var list = db.SuperColumnList.Where(c => c.ColumnFamily == _ColumnFamily && c.Key == testKey);
                Assert.That(list, Is.Not.Null);
                Assert.That(list.Any());
                Assert.That(list.Count(), Is.EqualTo(1));
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
            var testValue = "TestString1";
            var cache = new CassandraCache<string>(_KeySpace);

            cache.Set(testKey, testValue, DateTimeOffset.Now.AddMinutes(1));

            using (var db = GetContext()) {
                var list = db.SuperColumnList.Where(c => c.ColumnFamily == _ColumnFamily && c.Key == testKey);
                Assert.That(list, Is.Not.Null);
                Assert.That(list.Any());
                Assert.That(list.Count(), Is.EqualTo(1));
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
                var list = db.SuperColumnList.Where(c => c.ColumnFamily == _ColumnFamily && c.Key == testKey);
                Assert.That(list, Is.Not.Null);
                Assert.That(list.Any());
                Assert.That(list.Count(), Is.EqualTo(1));
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
                var list = db.SuperColumnList.Where(c => c.ColumnFamily == _ColumnFamily && c.Key == testKey);
                Assert.That(list, Is.Not.Null);
                Assert.That(list.Any());
                Assert.That(list.Count(), Is.EqualTo(1));
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
            const string testKey = "SetString1";
            var testValue = "TestString1";
            var cache = new CassandraCache<string>(_KeySpace);

            cache.Set(testKey, testValue, DateTimeOffset.Now.AddMinutes(1));

            var value = cache.Get(testKey);
            Assert.That(value, Is.Not.Null);
            Assert.That(value, Is.TypeOf<string>());
            Assert.That(value as string, Is.EqualTo(testValue));
        }

        #endregion Get Tests
    }

    // ReSharper restore InconsistentNaming
}