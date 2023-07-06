using System;
using System.Collections.Generic;

namespace AzureTableGenerics
{
    public static class StringExtensions
    {
        public static IEnumerable<string> SplitByLength(this string value, int maxLength)
        {
            for (int index = 0; index < value.Length; index += maxLength)
            {
                yield return value.Substring(index, Math.Min(maxLength, value.Length - index));
            }
        }
    }
}
