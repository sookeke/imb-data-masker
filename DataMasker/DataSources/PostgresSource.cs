using Dapper;
using DataMasker.Interfaces;
using DataMasker.Models;
using DataMasker.Utils;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;

namespace DataMasker.DataSources
{
    class PostgresSource : IDataSource
    {
        private readonly DataSourceConfig _sourceConfig;
        //global::Dapper.SqlMapper.AddTypeHandler(typeof(DbGeography), new GeographyMapper());
        private static string _exceptionpath = Directory.GetCurrentDirectory() + $@"\Output\" + ConfigurationManager.AppSettings["APP_NAME"] + "_exception.txt";
        private static string _successfulCommit = Directory.GetCurrentDirectory() + $@"\Output\" + ConfigurationManager.AppSettings["APP_NAME"] + "_successfulCommit.txt";


        private readonly string _connectionString;

        public PostgresSource(
           DataSourceConfig sourceConfig)
        {
            _sourceConfig = sourceConfig;
            if (sourceConfig.Config.connectionString != null && !string.IsNullOrWhiteSpace(sourceConfig.Config.connectionString.ToString()))
            {
                _connectionString = sourceConfig.Config.connectionString;
            }
            else
            {
                _connectionString =
                    $"User ID={sourceConfig.Config.userName};Password={sourceConfig.Config.password};Data Source={sourceConfig.Config.server};Initial Catalog={sourceConfig.Config.name};Persist Security Info=False;";
            }

        }
        public IEnumerable<IDictionary<string, object>> GetData(TableConfig tableConfig)
        {
            string _connectionStringGet = ConfigurationManager.AppSettings["ConnectionStringPrd"];
            using (var connection = new NpgsqlConnection(_connectionStringGet))
            {
                connection.Open();
                //var retu = connection.Query(BuildSelectSql(tableConfig));
                return (IEnumerable<IDictionary<string, object>>)connection.Query(BuildSelectSql(tableConfig));
            }
        }

        public void UpdateRow(IDictionary<string, object> row, TableConfig tableConfig)
        {
            using (var connection = new NpgsqlConnection(_connectionString))
            {
                connection.Open();
                connection.Execute(BuildUpdateSql(tableConfig), row, null, commandType: System.Data.CommandType.Text);
            }
        }

        public void UpdateRows(IEnumerable<IDictionary<string, object>> rows, TableConfig config, Action<int> updatedCallback = null)
        {
            SqlMapper.AddTypeHandler(new GeographyMapper());
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

            if (!(File.Exists(_successfulCommit) && File.Exists(_exceptionpath)))
            {

                //write to the file
                File.Create(_successfulCommit).Close();

                //write to the file
                File.Create(_exceptionpath).Close();



            }
            using (System.IO.StreamWriter sw = System.IO.File.AppendText(_exceptionpath))
            {
                if (new FileInfo(_exceptionpath).Length == 0)
                {
                    sw.WriteLine("exceptions for " + ConfigurationManager.AppSettings["APP_NAME"] + ".........." + Environment.NewLine + Environment.NewLine);
                    //  File.WriteAllText(_exceptionpath, "exceptions for " + ConfigurationManager.AppSettings["APP_NAME"] + ".........." + Environment.NewLine + Environment.NewLine);
                }
                // sw.WriteLine(""); 
            }
            using (System.IO.StreamWriter sw = System.IO.File.AppendText(_successfulCommit))
            {
                //write my text 
                if (new FileInfo(_successfulCommit).Length == 0)
                {
                    // File.WriteAllText(_successfulCommit, "Successful Commits for " + ConfigurationManager.AppSettings["APP_NAME"] + ".........." + Environment.NewLine + Environment.NewLine);

                    sw.WriteLine("Successful Commits for " + ConfigurationManager.AppSettings["APP_NAME"] + ".........." + Environment.NewLine + Environment.NewLine);
                }
            }





            using (NpgsqlConnection connection = new NpgsqlConnection(_connectionString))
            {
                connection.Open();



                foreach (Batch<IDictionary<string, object>> batch in batches)
                {
                    using (IDbTransaction sqlTransaction = connection.BeginTransaction())
                    {
                        //OracleBulkCopy oracleBulkCopy = new OracleBulkCopy(connection, OracleBulkCopyOptions.UseInternalTransaction);


                        string sql = BuildUpdateSql(config);


                        try
                        {
                            //File.AppendAllText(_successfulCommit, "Successful Commit on table " + config.Name + Environment.NewLine + Environment.NewLine);

                            connection.Execute(sql, batch.Items, sqlTransaction);

                            if (_sourceConfig.DryRun)
                            {
                                sqlTransaction.Rollback();
                            }
                            else
                            {
                                sqlTransaction.Commit();
                                File.AppendAllText(_successfulCommit, "Successful Commit on table " + config.Name + Environment.NewLine + Environment.NewLine);
                            }


                            if (updatedCallback != null)
                            {
                                totalUpdated += batch.Items.Count;
                                updatedCallback.Invoke(totalUpdated);
                            }
                        }
                        catch (Exception ex)
                        {

                            Console.WriteLine(ex.Message);
                            File.AppendAllText(_exceptionpath, ex.Message + " on table " + config.Name + Environment.NewLine + Environment.NewLine);

                        }


                    }
                }
            }
        }
        public string BuildUpdateSql(
           TableConfig tableConfig)
        {
            var charsToRemove = new string[] { "[", "]" };
            string sql = $"UPDATE [{tableConfig.Name}] SET ";

            sql += tableConfig.Columns.GetUpdateColumns();
            sql += $" WHERE [{tableConfig.PrimaryKeyColumn}] = @{tableConfig.PrimaryKeyColumn}";
            //thisis oracle replace @ WITH :
            var sqltOrc = new string[] { "@" };
            foreach (var c in charsToRemove)
            {
                sql = sql.Replace(c, string.Empty);
            }
            foreach (var c in sqltOrc)
            {
                sql = sql.Replace(c, ":");
            }
            return sql;
        }
        private string BuildSelectSql(
           TableConfig tableConfig)
        {
            //var clumns = tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn)
            string sql = $"SELECT  {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn)} FROM {tableConfig.Name}";
            if (sql.Contains("[") || sql.Contains("]"))
            {
                var charsToRemove = new string[] { "[", "]" };
                foreach (var c in charsToRemove)
                {
                    sql = sql.Replace(c, string.Empty);
                }
            }
            return sql;
        }
        public object shuffle(string table, string column, object existingValue, DataTable dataTable = null)
        {
            //ArrayList list = new ArrayList();
            Random rnd = new Random();
            string sql = "SELECT " + column + " FROM " + " " + table;
            using (var connection = new NpgsqlConnection(_connectionString))
            {
                connection.Open();
                var result = (IEnumerable<IDictionary<string, object>>)connection.Query(sql);
                //Randomizer randomizer = new Randomizer();

                var values = result.Select(n => n.Values).SelectMany(x => x).ToList().Where(n => n != null).Distinct().ToArray();
                //var find = randomizer.Shuffle(values);
                object value = values[rnd.Next(values.Count())];
                if (values.Count() <= 1)
                {
                    File.AppendAllText(_exceptionpath, "Cannot generate unique shuffle value" + " on table " + table + "for column " + column + Environment.NewLine + Environment.NewLine);
                    return value;
                }
                while (value.Equals(existingValue))
                {

                    value = values[rnd.Next(0, values.Count())];
                }

                return value;

            }

            //return list;
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

        public DataTable SpreadSheetTable(IEnumerable<IDictionary<string, object>> parents)
        {
            var table = new DataTable();

            foreach (var parent in parents)
            {
                var children = parent.Values
                                     .OfType<IEnumerable<IDictionary<string, object>>>()
                                     .ToArray();

                var length = children.Any() ? children.Length : 1;

                var parentEntries = parent.Where(x => x.Value is object)
                                          .Repeat(length)
                                          .ToLookup(x => x.Key, x => x.Value);
                var childEntries = children.SelectMany(x => x.First())
                                           .ToLookup(x => x.Key, x => x.Value);

                var allEntries = parentEntries.Concat(childEntries)
                                              .ToDictionary(x => x.Key, x => x.ToArray());

                var headers = allEntries.Select(x => x.Key)
                                        .Except(table.Columns
                                                     .Cast<DataColumn>()
                                                     .Select(x => x.ColumnName))
                                        .Select(x => new DataColumn(x))
                                        .ToArray();
                table.Columns.AddRange(headers);

                var addedRows = new int[length];
                for (int i = 0; i < length; i++)
                    addedRows[i] = table.Rows.IndexOf(table.Rows.Add());

                foreach (DataColumn col in table.Columns)
                {
                    object[] columnRows;
                    if (!allEntries.TryGetValue(col.ColumnName, out columnRows))
                        continue;

                    for (int i = 0; i < addedRows.Length; i++)
                        table.Rows[addedRows[i]][col] = columnRows[i];
                }
            }

            return table;
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
    }
}
