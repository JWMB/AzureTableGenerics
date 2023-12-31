﻿using Azure.Data.Tables;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace AzureTableGenerics
{
    public class ExpandableTableEntityConverter<T> where T : class, new()
    {
        private static readonly int maxLength = 32 * 1024;
        private static readonly string expandedColumnListPropertyName = "__ExpandedColumns";

        private readonly Func<T, TableFilter> idFunc;

        public ExpandableTableEntityConverter(Func<T, TableFilter> idFunc)
        {
            this.idFunc = idFunc;
        }

        public T ToPoco(TableEntity entity) => ToPocoStatic(entity);
        public TableEntity FromPoco(T obj) => FromPoco(obj, idFunc);


        public static T ToPocoStatic(TableEntity entity)
        {
            var poco = new T();
            var expandedRaw = entity[expandedColumnListPropertyName];
            var expanded = expandedRaw == null ? null : JsonConvert.DeserializeObject<Dictionary<string, int>>(expandedRaw.ToString() ?? "");

            foreach (var prop in GetProps())
            {
                object? val;
                if (expanded?.TryGetValue(prop.Name, out var count) == true)
                {
                    val = string.Join("", Enumerable.Range(0, count).Select(index => entity[GetExpandedName(prop.Name, index)]));
                }
                else
                {
                    val = entity[prop.Name];
                }

                if (val == null)
                {
                    continue;
                }

                if (IsNativelySupportedType(prop.PropertyType) == false)
                {
                    if (val is string str)
                    {
                        val = JsonConvert.DeserializeObject(str, prop.PropertyType);
                    }
                    else
                    {
                        throw new Exception($"Unhandled type ({prop.Name}/{prop.PropertyType.Name}): {val?.GetType().Name}");
                    }
                }

                prop.SetValue(poco, val);
            }

            return poco;
        }

        public static TableEntity FromPoco(T obj, Func<T, TableFilter> idFunc)
        {
            var entity = new TableEntity();

            var expandedProps = new Dictionary<string, int>();
            foreach (var prop in GetProps())
            {
                var value = prop.GetValue(obj);
                if (IsNativelySupportedType(prop.PropertyType) == false)
                {
                    var serialized = JsonConvert.SerializeObject(value);
                    if (serialized.Length > maxLength)
                    {
                        // TODO: this could just as well apply to e.g. a string! Splitting should be common to both codepaths
                        var pairs = serialized.SplitByLength(maxLength)
                            .Select((o, i) => new { Key = GetExpandedName(prop.Name, i), Value = o });
                        foreach (var pair in pairs)
                            entity[pair.Key] = pair.Value;

                        expandedProps.Add(prop.Name, pairs.Count());
                        continue;
                    }
                    else
                        value = serialized;
                }
                else
                {
                    if (prop.PropertyType == typeof(DateTimeOffset))
                    {
                        // TODO: probably DateTime as well
                        // TODO: min value cannot be before
                    }
                }
                entity[prop.Name] = value;
            }

            if (expandedProps.Any())
                entity[expandedColumnListPropertyName] = JsonConvert.SerializeObject(expandedProps);

            var id = idFunc(obj);
            entity.RowKey = id.Row;
            entity.PartitionKey = id.Partition;

            return entity;
        }

        private static List<PropertyInfo> GetProps()
        {
            // TODO (low): cache, or emit code instead (performance)?
            return typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanWrite && p.CanRead)
                .ToList();
        }

        private static string GetExpandedName(string propertyName, int index) => $"{propertyName}__{index}";

        private static bool IsNativelySupportedType(Type type)
        {
            return new[]
                { typeof(byte), typeof(BinaryData), typeof(DateTime), typeof(DateTimeOffset), typeof(double), typeof(Guid), typeof(int), typeof(long), typeof(string) }
            .Contains(type);
        }

        //private static bool IsBuiltIn(Type t)
        //{
        //    if (t.IsPrimitive || t == typeof(string) || t == typeof(DateTimeOffset) || t == typeof(DateTime))
        //        return true;

        //    if (typeof(System.Collections.IEnumerable).IsAssignableFrom(t))
        //    {
        //        if (t.IsGenericType)
        //            return t.GenericTypeArguments.All(x => IsBuiltIn(x));
        //    }

        //    return false;
        //}
    }
}
