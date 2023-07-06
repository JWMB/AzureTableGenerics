using System;
using System.Collections.Generic;
using System.Linq;

namespace AzureTableGenerics
{
    public static class IEnumerableExtensions
    {
        public static IEnumerable<T> DistinctBy<T, TDistinct>(this IEnumerable<T> values, Func<T, TDistinct> predicate)
        {
            var byPredicate = values.GroupBy(predicate);
            return byPredicate.Select(o => o.First());
        }

        public static IEnumerable<List<TValue>> Chunk<TValue>(
            this IEnumerable<TValue> values,
            int chunkSize)
        {
            using (var enumerator = values.GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    yield return GetChunk(enumerator, chunkSize).ToList();
                }
            }
        }

        private static IEnumerable<T> GetChunk<T>(
                         IEnumerator<T> enumerator,
                         int chunkSize)
        {
            do
            {
                yield return enumerator.Current;
            } while (--chunkSize > 0 && enumerator.MoveNext());
        }
    }
}
