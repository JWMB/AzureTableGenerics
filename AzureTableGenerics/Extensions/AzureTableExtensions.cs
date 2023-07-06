using Azure.Data.Tables;
using Azure;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace AzureTableGenerics
{
    public static class AzureTableExtensions
    {
        private static readonly int MaxBatchSize = 100;
        public static async Task<List<Response>> SubmitTransactionsBatched(this TableClient tableClient, IEnumerable<TableTransactionAction> transactions)
        {
            // TODO: Come on Microsoft - the 100 limit makes it no longer a transaction >:(
            var result = new List<Response>();
            foreach (var chunk in transactions.Chunk(MaxBatchSize))
            {
                var response = await tableClient.SubmitTransactionAsync(chunk); // Not supported type System.Collections.Generic.Dictionary`2[System.String,System.Int32]
                if (response.Value.Any(o => o.IsError))
                    throw new Exception($"SubmitTransaction errors: {string.Join("\n", response.Value.Where(o => o.IsError).Select(o => o.ReasonPhrase))}");
                result.AddRange(response.Value);
            }
            return result;
        }

        public static async Task IterateOverRows(this TableClient client, string filter, Func<TableEntity, TableTransactionAction> createTransaction, Func<string, IEnumerable<TableTransactionAction>, Task> executeTransaction)
        {
            var rows = client.QueryAsync<TableEntity>(filter, MaxBatchSize);
            var cnt = 0;
            await foreach (var page in rows.AsPages())
            {
                var transactions = page.Values.Select(createTransaction).ToList();
                var byPartition = transactions.GroupBy(o => o.Entity.PartitionKey);
                foreach (var grp in byPartition)
                {
                    await executeTransaction(grp.Key, grp);
                }
                var lastCnt = cnt;
                cnt += page.Values.Count;
            }
        }
    }
}

