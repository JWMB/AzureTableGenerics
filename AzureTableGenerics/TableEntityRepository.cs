using Azure;
using Azure.Data.Tables;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AzureTableGenerics
{
    public class TableEntityRepository<T, TTableEntity> where TTableEntity : class, ITableEntity, new() //: IRepository<T, string>, IBatchRepository<T> 
    {
        protected readonly TableClient tableClient;
        private readonly Func<TTableEntity, T> toBusinessObject;
        private readonly Func<T, TTableEntity> toTableEntity;
        private readonly TableFilter keyForFilter;

        public TableEntityRepository(TableClient tableClient, Func<TTableEntity, T> toBusinessObject, Func<T, TTableEntity> toTableEntity, TableFilter keyForFilter)
        {
            this.tableClient = tableClient;
            this.toBusinessObject = toBusinessObject;
            this.toTableEntity = toTableEntity;
            this.keyForFilter = keyForFilter;
        }

        private async Task<List<Response>> UpsertBatch(IEnumerable<ITableEntity> entities)
        {
            // TODO (low): use SubmitTransactionsBatched instead
            //Note: UpsertMerge keeps old columns
            var batch = new List<TableTransactionAction>(entities.Select(f => new TableTransactionAction(TableTransactionActionType.UpsertReplace, f)));

            var result = await tableClient.SubmitTransactionsBatched(batch);

            //var result = new List<Response>();
            //foreach (var chunk in batch.Chunk(100))
            //{
            //    var response = await tableClient.SubmitTransactionAsync(chunk); // Not supported type System.Collections.Generic.Dictionary`2[System.String,System.Int32]
            //    if (response.Value.Any(o => o.IsError))
            //        throw new Exception($"SubmitTransaction errors: {string.Join("\n", response.Value.Where(o => o.IsError).Select(o => o.ReasonPhrase))}");
            //    result.AddRange(response.Value);
            //}

            return result;
        }

        // https://learn.microsoft.com/en-us/rest/api/storageservices/Understanding-the-Table-Service-Data-Model#characters-disallowed-in-key-fields
        public static Regex InvalidKeyRegex = new Regex(@"\/|\\|#|\?");

        public async Task<(IEnumerable<T> Added, IEnumerable<T> Updated)> Upsert(IEnumerable<T> items)
        {
            if (!items.Any())
                return (new List<T>(), new List<T>());

            var tableEntities = items.Select(toTableEntity).ToList();

            var duplicates = tableEntities.GroupBy(o => $"{o.PartitionKey}__{o.RowKey}").Where(o => o.Count() > 1).ToList();
            if (duplicates.Any())
            {
                throw new Exception($"{typeof(T).Name}: Duplicate entries: {string.Join(",", duplicates.Select(o => $"{o.First().PartitionKey}/{o.First().RowKey}"))}");
            }

            var invalidEntries = tableEntities.Where(o => InvalidKeyRegex.IsMatch(o.RowKey) || InvalidKeyRegex.IsMatch(o.PartitionKey));
            if (invalidEntries.Any())
            {
                throw new Exception($"{typeof(T).Name}: Invalid key(s) for {string.Join(",", invalidEntries.Select(o => $"{o.PartitionKey}/{o.RowKey}"))}");
            }

            try
            {
                var responses = await UpsertBatch(tableEntities);

                // TODO (low): do we always get 204 with TableTransactionActionType.UpsertMerge ?
                // would UpsertReplace always get 201? Goddamn Azure Tables...
                var itemAndStatus = items.Select((o, i) => new { Item = o, responses[i].Status });
                return (itemAndStatus.Where(o => o.Status == 201).Select(o => o.Item), itemAndStatus.Where(o => o.Status != 201).Select(o => o.Item));
            }
            catch (Exception ex) when (ex is TableTransactionFailedException || ex is RequestFailedException) //(TableTransactionFailedException ttfEx)
            {
                string lengthsInfo = "N/A";
                try
                {
                    lengthsInfo = string.Join(", ", tableEntities.Select(o => $"{o.PartitionKey}/{o.RowKey}: {JsonConvert.SerializeObject(o).Length}\n{JsonConvert.SerializeObject(o)}"));
                }
                catch { }

                var code = ex is TableTransactionFailedException ttfEx ? ttfEx.ErrorCode :
                    (ex is RequestFailedException rfEx ? rfEx.ErrorCode : null);
                throw new Exception($"{typeof(T).Name} code:{code} stored:{lengthsInfo}", ex);
            }
        }

        public async Task<Dictionary<string, T>> GetByRowKeys(IEnumerable<string> rowKeys, string? partitionKey = null)
        {
            var results = new List<KeyValuePair<string, T>>();
            var result = new Dictionary<string, T>();
            foreach (var chunk in rowKeys.Chunk(50))
            {
                var tasks = chunk.Select(o => tableClient.GetEntityIfExistsAsync<TTableEntity>(partitionKey ?? keyForFilter.Partition, o));
                var partial = await Task.WhenAll(tasks);
                var kvs = partial.Select((o, i) => new KeyValuePair<string, T>(chunk[i], o.HasValue ? toBusinessObject(o.Value) : default(T)));  //KeyValuePair<string, T?>.Create(i, o.HasValue ? toBusinessObject(o.Value) : null));
                results.AddRange(kvs);
            }
            return results.ToDictionary(o => o.Key, o => o.Value);
        }

        public async Task<IEnumerable<T>> GetAll()
        {
            return await GetWithFilter(keyForFilter.Render());
        }

        private async Task<IEnumerable<T>> GetWithFilter(string? filter)
        {
            //System.Globalization.CultureInfo.CurrentCulture = System.Globalization.CultureInfo.InvariantCulture;
            var query = tableClient.QueryAsync<TTableEntity>(filter);
            var result = new List<T>();
            await foreach (var entity in query)
            {
                result.Add(toBusinessObject(entity));
            }
            return result;
        }

        public async Task<T> Get(string id)
        {
            var response = await tableClient.GetEntityIfExistsAsync<TTableEntity>(keyForFilter.Partition, id);
            return response.HasValue ? toBusinessObject(response.Value) : default;
        }

        public async Task<string> Add(T item)
        {
            var entity = toTableEntity(item);
            await tableClient.AddEntityAsync(entity);
            return entity.RowKey;
        }

        public async Task<string> Upsert(T item)
        {
            var entity = toTableEntity(item);
            await tableClient.UpsertEntityAsync(entity);
            return entity.RowKey;
        }

        public async Task Update(T item)
        {
            var entity = toTableEntity(item);
            await tableClient.UpdateEntityAsync(entity, ETag.All, TableUpdateMode.Replace);
        }

        public async Task Remove(T item)
        {
            var entity = toTableEntity(item);
            await tableClient.DeleteEntityAsync(entity.PartitionKey, entity.RowKey);
        }

        public static Exception ReduceException(Exception exception)
        {
            if (exception is AggregateException aEx && aEx.InnerExceptions.Count == 1)
            {
                return aEx.InnerExceptions.First();
            }
            return exception;
        }

        public async Task<int> RemoveAll()
        {
            var all = await GetAll();
            foreach (var item in all.Select(o => toTableEntity(o)))
                await tableClient.DeleteEntityAsync(item.PartitionKey, item.RowKey);
            return all.Count();
        }

        //public class TableEntityId
        //{
        //    public string PartitionKey { get; set; } = string.Empty;
        //    public string RowKey { get; set; } = string.Empty;
        //}
    }
}
