﻿using System.Collections.Generic;
using System.Linq;
using DataMasker.Models;

namespace DataMasker
{
    /// <summary>
    /// Extensions
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// Gets the select columns.
        /// </summary>
        /// <param name="columns">The columns.</param>
        /// <param name="primaryKeyColumn">The primary key column.</param>
        /// <returns></returns>
        public static string GetSelectColumns(
            this IList<ColumnConfig> columns,
            string primaryKeyColumn,Config config)
        {
            IList<string> columnNames = new List<string>();
            switch (config.DataSource.Type)
            {
                case DataSourceType.InMemoryFake:
                    columnNames = new List<string>(columns.Select(x => $"[{x.Name}]"));
                    break;
                case DataSourceType.SqlServer:
                    columnNames = new List<string>(columns.Select(x => $"[{x.Name}]"));
                    break;
                case DataSourceType.OracleServer:
                    columnNames = new List<string>(columns.Select(x => $"{x.Name}"));
                    break;
                case DataSourceType.SpreadSheet:
                    columnNames = new List<string>(columns.Select(x => $"[{x.Name}]"));
                    break;
                case DataSourceType.PostgresServer:
                    columnNames = new List<string>(columns.Select(x => $"{x.Name.AddDoubleQuotes()}"));
                    break;
                case DataSourceType.MySqlServer:
                    columnNames = new List<string>(columns.Select(x => $"`{x.Name}`"));
                    break;
                default:
                    columnNames = new List<string>(columns.Select(x => $"[{x.Name}]"));
                    break;
            }
           
            columnNames.Insert(0, primaryKeyColumn);
            return string.Join(", ", columnNames);
        }
        public static string AddDoubleQuotes(this string value)
        {
            return "\"" + value + "\"";
        }
        /// <summary>
        /// Gets the update columns.
        /// </summary>
        /// 
        /// <param name="paramPrefix">The parameter prefix.</param>
        /// <returns></returns>
        public static string GetUpdateColumns(
            this IList<ColumnConfig> columns, Config config,
            string paramPrefix = null)
        {
            string o = "";
            switch (config.DataSource.Type)
            {
                case DataSourceType.InMemoryFake:
                    o = string.Join(
                             ", ",
                             columns.Where(x => !x.Ignore)
                                    .Select(x => $"[{x.Name}] = @{paramPrefix}{x.Name}"));
                    break;
                case DataSourceType.SqlServer:
                    o = string.Join(
                               ", ",
                               columns.Where(x => !x.Ignore)
                                      .Select(x => $"[{x.Name}] = @{paramPrefix}{x.Name}"));
                    break;
                case DataSourceType.OracleServer:
                    o = string.Join(
                               ", ",
                               columns.Where(x => !x.Ignore)
                                      .Select(x => $"[{x.Name}] = :{paramPrefix}{x.Name}"));
                    break;
                case DataSourceType.MySqlServer:
                    o = string.Join(
                               ", ",
                               columns.Where(x => !x.Ignore)
                                      .Select(x => $"`{x.Name}` = @{paramPrefix}{x.Name}"));
                    break;
                case DataSourceType.SpreadSheet:
                    o = string.Join(
                             ", ",
                             columns.Where(x => !x.Ignore)
                                    .Select(x => $"[{x.Name}] = @{paramPrefix}{x.Name}"));
                    break;
                case DataSourceType.PostgresServer:
                    o = string.Join(
                             ", ",
                             columns.Where(x => !x.Ignore)
                                    .Select(x => $"{x.Name.AddDoubleQuotes()} = :{paramPrefix}{x.Name}"));
                    break;
                default:
                    o = string.Join(
                             ", ",
                             columns.Where(x => !x.Ignore)
                                    .Select(x => $"[{x.Name}] = @{paramPrefix}{x.Name}"));
                    break;
            }
            return o;
        }
    }
}
