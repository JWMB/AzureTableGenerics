namespace Tests
{
    public class PocoNullableDates
    {
        public DateTimeOffset? DateOffset { get; set; }
        public DateTime? Date { get; set; }
    }
    public class PocoNonNullableDates
    {
        public DateTimeOffset DateOffset { get; set; }
        public DateTime Date { get; set; }
    }
}
