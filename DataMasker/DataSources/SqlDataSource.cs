using System;
using Dapper;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using DataMasker.Interfaces;
using DataMasker.Models;
using DataMasker.Utils;
using System.Collections;

namespace DataMasker.DataSources
{
    /// <summary>
    /// SqlDataSource
    /// </summary>
    /// <seealso cref="IDataSource"/>
    public class SqlDataSource : IDataSource
    {
        private readonly DataSourceConfig _sourceConfig;

        private readonly string _connectionString;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlDataSource"/> class.
        /// </summary>
        /// <param name="sourceConfig"></param>
        public SqlDataSource(
            DataSourceConfig sourceConfig)
        {
            _sourceConfig = sourceConfig;
            if (sourceConfig.Config.connectionString!=null && !string.IsNullOrWhiteSpace(sourceConfig.Config.connectionString.ToString()))
            {
                _connectionString = sourceConfig.Config.connectionString;
            }
            else
            {
                _connectionString =
                    $"User ID={sourceConfig.Config.userName};Password={sourceConfig.Config.password};Data Source={sourceConfig.Config.server};Initial Catalog={sourceConfig.Config.name};Persist Security Info=False;";
            }

        }


        /// <summary>
        /// Gets the data.
        /// </summary>
        /// <param name="tableConfig">The table configuration.</param>
        /// <returns></returns>
        /// <inheritdoc/>
        public IEnumerable<IDictionary<string, object>> GetData(
            TableConfig tableConfig)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                return (IEnumerable<IDictionary<string, object>>)connection.Query(BuildSelectSql(tableConfig));
            }
        }

        /// <summary>
        /// Updates the row.
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="tableConfig">The table configuration.</param>
        /// <inheritdoc/>
        public void UpdateRow(
            IDictionary<string, object> row,
            TableConfig tableConfig)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                connection.Execute(BuildUpdateSql(tableConfig), row, null, commandType: CommandType.Text);
            }
        }


        /// <inheritdoc/>
        public void UpdateRows(
            IEnumerable<IDictionary<string, object>> rows,
            TableConfig config,
            Action<int> updatedCallback)
        {
            int? batchSize = _sourceConfig.UpdateBatchSize;
            if (batchSize == null ||
                batchSize <= 0)
            {
                batchSize = rows.Count();
            }

            IEnumerable<Batch<IDictionary<string, object>>> batches = Batch<IDictionary<string, object>>.BatchItems(
                rows.ToArray(),
                (
                    objects,
                    enumerable) => enumerable.Count() < batchSize);

            int totalUpdated = 0;
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                foreach (Batch<IDictionary<string, object>> batch in batches)
                {
                    SqlTransaction sqlTransaction = connection.BeginTransaction();


                    string sql = BuildUpdateSql(config);
                    var res = connection.Execute(sql, batch.Items, sqlTransaction, null, CommandType.Text);

                    if (_sourceConfig.DryRun)
                    {
                        sqlTransaction.Rollback();
                    }
                    else
                    {
                        sqlTransaction.Commit();
                    }


                    if (updatedCallback != null)
                    {
                        totalUpdated += batch.Items.Count;
                        updatedCallback.Invoke(totalUpdated);
                    }
                }
            }
        }

        /// <summary>
        /// Builds the update SQL.
        /// </summary>
        /// <param name="tableConfig">The table configuration.</param>
        /// <returns></returns>
        private string BuildUpdateSql(
            TableConfig tableConfig)
        {
            string sql = $"UPDATE [{tableConfig.Name}] SET ";

            sql += tableConfig.Columns.GetUpdateColumns();
            sql += $" WHERE [{tableConfig.PrimaryKeyColumn}] = @{tableConfig.PrimaryKeyColumn}";
            return sql;
        }


        /// <summary>
        /// Builds the select SQL.
        /// </summary>
        /// <param name="tableConfig">The table configuration.</param>
        /// <returns></returns>
        private string BuildSelectSql(
            TableConfig tableConfig)
        {
            return $"SELECT  {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn)} FROM [{tableConfig.Name}]";
        }
        public object shuffle(string table, string column, object existingValue, bool retainnull, DataTable dataTable = null)
        {
            //ArrayList list = new ArrayList();
            Random rnd = new Random();
            string sql = "SELECT " + column + " FROM " + " " + table;
            using (var connection = new Oracle.DataAccess.Client.OracleConnection(_connectionString))
            {
                connection.Open();
                var result = (IEnumerable<IDictionary<string, object>>)connection.Query(sql);
                var values = result.Select(n => n.Values).SelectMany(x => x).ToArray();
                object value = values[rnd.Next(values.Count())];
                while (value.Equals(existingValue))
                {
                    value = values[rnd.Next(values.Count())];
                }



                return value;

            }
        }

        public object GetData(string column, string table)
        {
            throw new NotImplementedException();
        }

        public DataTable DataTableFromCsv(string csvPath)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IDictionary<string, object>> CreateObject(DataTable dataTable)
        {
            throw new NotImplementedException();
        }

        public DataTable SpreadSheetTable(IEnumerable<IDictionary<string, object>> parents, TableConfig config)
        {
            throw new NotImplementedException();
        }

        public DataTable CreateTable(IEnumerable<IDictionary<string, object>> obj)
        {
            var table = new DataTable();

            //excuse the meaningless variable names

            var c = obj.FirstOrDefault(x => x.Values
                                                 .OfType<IEnumerable<IDictionary<string, object>>>()
                                                 .Any());
            var p = c ?? obj.FirstOrDefault();
            if (p == null)
                return table;

            var headers = p.Where(x => x.Value is string)
                           .Select(x => x.Key)
                           .Concat(c == null ?
                                   Enumerable.Empty<string>() :
                                   c.Values
                                    .OfType<IEnumerable<IDictionary<string, object>>>()
                                    .First()
                                    .SelectMany(x => x.Keys))
                           .Select(x => new DataColumn(x))
                           .ToArray();
            table.Columns.AddRange(headers);

            foreach (var parent in obj)
            {
                var children = parent.Values
                                     .OfType<IEnumerable<IDictionary<string, object>>>()
                                     .ToArray();

                var length = children.Any() ? children.Length : 1;

                var parentEntries = parent.Where(x => x.Value is string)
                                          .Repeat(length)
                                          .ToLookup(x => x.Key, x => x.Value);
                var childEntries = children.SelectMany(x => x.First())
                                           .ToLookup(x => x.Key, x => x.Value);

                var allEntries = parentEntries.Concat(childEntries)
                                              .ToDictionary(x => x.Key, x => x.ToArray());

                var addedRows = Enumerable.Range(0, length)
                                          .Select(x => new
                                          {
                                              relativeIndex = x,
                                              actualIndex = table.Rows.IndexOf(table.Rows.Add())
                                          })
                                          .ToArray();

                foreach (DataColumn col in table.Columns)
                {
                    object[] columnRows;
                    if (!allEntries.TryGetValue(col.ColumnName, out columnRows))
                        continue;

                    foreach (var row in addedRows)
                        table.Rows[row.actualIndex][col] = columnRows[row.relativeIndex];
                }
            }

            return table;
        }

        public IEnumerable<IDictionary<string, object>> RawData(IEnumerable<IDictionary<string, object>> PrdData)
        {
            throw new NotImplementedException();
        }
    }
}
