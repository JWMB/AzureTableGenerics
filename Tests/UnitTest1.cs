using AzureTableGenerics;
using Shouldly;

namespace Tests
{
    public class UnitTest1
    {
        [Theory]
        [InlineData("2024-12-08 20:24:04 +00:00")]
        [InlineData(null)]
        public void Test1(string? date)
        {
            var sut = new ExpandableTableEntityConverter<Poco>(val => new TableFilter("none", "1"));

            var poco = new Poco
            { 
                DateOffset = date == null ? null : DateTimeOffset.Parse(date),
                Date = date == null ? null : DateTime.Parse(date),
            };
            var entity = sut.FromPoco(poco);
            var retrieved = sut.ToPoco(entity);

            retrieved.DateOffset.ShouldBe(poco.DateOffset);
            retrieved.Date.ShouldBe(poco.Date);
        }

        public class Poco
        {
            public DateTimeOffset? DateOffset { get; set; }
            public DateTime? Date { get; set; }
        }
    }
}
