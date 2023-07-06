using Azure.Data.Tables;
//using ProblemSource.Services.Storage.AzureTables.TableEntities;
//using ProblemSourceModule.Services.Storage;

namespace AzureTableGenerics
{
    public class TableFilter
    {
        public TableFilter(string? partition, string? row = null)
        {
            Partition = partition;
            Row = row;
        }

        public string? Partition { get; private set; }
        public string? Row { get; private set; }

        public string? Render() =>
            RenderPartitionOnly()
            + (string.IsNullOrEmpty(Row) ? "" : $" and {nameof(ITableEntity.RowKey)} eq '{Row}'");

        public string? RenderPartitionOnly() => Partition == null ? null : $"{nameof(ITableEntity.PartitionKey)} eq '{Partition}'";
    }
}
