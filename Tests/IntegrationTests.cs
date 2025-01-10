using Azure.Data.Tables;
using AzureTableGenerics;
using Shouldly;

namespace Tests
{
    public class SharedFixture : IDisposable
    {
        private static TableClient? client;

        public SharedFixture()
        {
            if (client != null)
                return;
            client = new TableClient("UseDevelopmentStorage=true", $"UnitTest{DateTimeOffset.UtcNow:yyMMddHHmmss}");
            client.CreateIfNotExists();
        }

        public void Dispose()
        {
            if (client == null)
                return;
            client.Delete();
            client = null;
        }

        public TableClient TableClient => client == null ? throw new NullReferenceException() : client;
    }

    [CollectionDefinition(nameof(SharedFixtureCollection))]
    public class SharedFixtureCollection : ICollectionFixture<SharedFixture> { }

    [Trait("Category", "Local")]
    [Collection(nameof(SharedFixtureCollection))]
    public class IntegrationTests : IClassFixture<SharedFixture>
    {
        private readonly SharedFixture fixture;
        private TableClient TableClient => fixture.TableClient;

        public IntegrationTests(SharedFixture fixture)
        {
            this.fixture = fixture;
        }

        [Fact]
        public void BaseLine()
        {
            var entity = new TableEntity("x", DateTimeOffset.UtcNow.ToString());
            entity["DateTimeOffsetNull"] = (DateTimeOffset?)null;
            entity["DateTimeOffsetNonNull"] = (DateTimeOffset?)DateTimeOffset.UtcNow;
            TableClient.AddEntity(entity);

            var retrieved = (TableClient.GetEntity<TableEntity>(entity.PartitionKey, entity.RowKey)).Value;
            retrieved["DateTimeOffsetNull"].ShouldBe(null);
            retrieved["DateTimeOffsetNonNull"].GetType().ShouldBe(typeof(DateTimeOffset));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ExpandableTableEntityConverter_NullableDates(bool useNulls)
        {
            var converter = new ExpandableTableEntityConverter<PocoNullableDates>(val => new TableFilter("none", val.DateOffset.HasValue ? $"{val.DateOffset.Value:yymmddhhmmss}" : "NA"));
            var repo = new AutoConvertTableEntityRepository<PocoNullableDates>(TableClient, converter, new TableFilter("none"));

            var poco = new PocoNullableDates { 
                DateOffset = useNulls ? null : DateTimeOffset.UtcNow,
                Date = useNulls ? null : DateTime.UtcNow,
            };
            await repo.Add(poco);

            var converted = converter.FromPoco(poco);
            var retrievedRaw = (TableClient.GetEntity<TableEntity>(converted.PartitionKey, converted.RowKey)).Value;
            var dateOffsetRaw = (string)retrievedRaw[nameof(PocoNullableDates.DateOffset)];
            if (useNulls)
                dateOffsetRaw.ShouldBe("null");
            else
                dateOffsetRaw.StartsWith("\"").ShouldBeTrue();

            // Hm, when using this from a different project, "prop.SetValue(poco, val)" generates an ArgumentException. Does it depend on library versions?
            var retrieved = await repo.Get((string)converted["RowKey"]);

            retrieved.DateOffset.ShouldBe(poco.DateOffset);
        }
    }
}
