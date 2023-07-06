using Azure.Data.Tables;
//using ProblemSource.Services.Storage.AzureTables.TableEntities;
//using ProblemSourceModule.Services.Storage;

namespace AzureTableGenerics
{
    public class AutoConvertTableEntityRepository<T> : TableEntityRepository<T, TableEntity> where T : class, new()
    {
        public AutoConvertTableEntityRepository(TableClient tableClient, ExpandableTableEntityConverter<T> converter, TableFilter keyFilter)
            : base(tableClient, converter.ToPoco, converter.FromPoco, keyFilter)
        {
        }
    }
}
