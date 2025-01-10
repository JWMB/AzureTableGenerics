using AzureTableGenerics;
using Shouldly;

namespace Tests
{
    public class ExpandableTableEntityConverterTests
    {
        [Theory]
        [InlineData("2024-12-08 20:24:04 +00:00")]
        [InlineData(null)]
        public void NullableDates(string? date)
        {
            var sut = new ExpandableTableEntityConverter<PocoNullableDates>(val => new TableFilter("none", "1"));

            var poco = new PocoNullableDates
            { 
                DateOffset = date == null ? null : DateTimeOffset.Parse(date),
                Date = date == null ? null : DateTime.Parse(date),
            };
            var entity = sut.FromPoco(poco);
            var retrieved = sut.ToPoco(entity);

            retrieved.DateOffset.ShouldBe(poco.DateOffset);
            retrieved.Date.ShouldBe(poco.Date);
        }

        [Fact]
        public void NonNullableDates()
        {
            var sut = new ExpandableTableEntityConverter<PocoNonNullableDates>(val => new TableFilter("none", "1"));

            var poco = new PocoNonNullableDates
            {
                DateOffset = DateTimeOffset.UtcNow,
                Date = DateTime.UtcNow,
            };
            var entity = sut.FromPoco(poco);
            var retrieved = sut.ToPoco(entity);

            retrieved.DateOffset.ShouldBe(poco.DateOffset);
            retrieved.Date.ShouldBe(poco.Date);
        }

        [Fact]
        public void NonNullableDates_TempFixForStrangeError()
        {
            var sut = new ExpandableTableEntityConverter<PocoNonNullableDates>(val => new TableFilter("none", "1"));

            var poco = new PocoNonNullableDates
            {
                DateOffset = DateTimeOffset.UtcNow,
                Date = DateTime.UtcNow,
            };
            var entity = sut.FromPoco(poco);
            entity["DateOffset"] = $"\"{entity["DateOffset"]}\""; // Azure adds quotes?!

            Should.NotThrow(() => sut.ToPoco(entity));
            //Should.Throw<ArgumentException>(() => sut.ToPoco(entity));
            //try { sut.ToPoco(entity); }
            //catch (ArgumentException ex) { ex.Message.ShouldContain(entity["DateOffset"].ToString()); }
        }

        [Fact]
        public void Deserialize_Error()
        {
            var sut = new ExpandableTableEntityConverter<PocoNullableDates>(val => new TableFilter("none", "1"));
            var poco = new PocoNullableDates
            {
                DateOffset = DateTimeOffset.UtcNow,
                Date = DateTime.UtcNow,
            };
            var entity = sut.FromPoco(poco);
            entity["DateOffset"] = "2024-13-08 20:24:04 +00:00";

            Should.Throw<Newtonsoft.Json.JsonReaderException>(() => sut.ToPoco(entity));
        }
    }
}
