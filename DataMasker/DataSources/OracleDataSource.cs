using DataMasker.Interfaces;
using DataMasker.Models;
using Dapper;
using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using DataMasker.Utils;
using System.Data;
using System.IO;
using System.Configuration;
//using ChoETL;
using DataMasker.DataLang;
using KellermanSoftware.CompareNetObjects;

namespace DataMasker.DataSources
{
    public class OracleDataSource : IDataSource
    {
        private readonly DataSourceConfig _sourceConfig;
        //global::Dapper.SqlMapper.AddTypeHandler(typeof(DbGeography), new GeographyMapper());
        private static readonly string _exceptionpath = Directory.GetCurrentDirectory() +  ConfigurationManager.AppSettings["_exceptionpath"];
        private static readonly string _successfulCommit = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_successfulCommit"];
       
        //private IEnumerable<IDictionary<string, object>> getData { get;  set; }
        public object[] Values { get; private set; }
        public int o = 0;

        private static List<IDictionary<string, object>> rawData = new List<IDictionary<string, object>>();
        private static Dictionary<string, string> exceptionBuilder = new Dictionary<string, string>();

        private readonly string _connectionString;
        private readonly string _connectionStringPrd;

        public OracleDataSource(
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
            if (sourceConfig.Config.connectionStringPrd != null && !string.IsNullOrWhiteSpace(sourceConfig.Config.connectionStringPrd.ToString()))
            {
                _connectionStringPrd = sourceConfig.Config.connectionStringPrd;
            }

        }
        public IEnumerable<IDictionary<string, object>> GetData(TableConfig tableConfig)
        {
            //string _connectionStringGet = ConfigurationManager.AppSettings["ConnectionStringPrd"];
             var connection = new Oracle.DataAccess.Client.OracleConnection(_connectionStringPrd);
            {
                connection.Open();
                string query = "";
                IDictionary<string, object> idict = new Dictionary<string, object>();
                IEnumerable<IDictionary<string, object>> row = new List<IDictionary<string, object>>();
                List<IDictionary<string, object>> rows = new List<IDictionary<string, object>>();
                rawData = new List<IDictionary<string, object>>();
                var rowCount = GetCount(tableConfig);



                if (rowCount != 0 && rowCount > 50000 && tableConfig.Columns.Where(n => n.Type == DataType.Blob).Count() > 0)
                {
                    query = BuildSelectSql(tableConfig);
                    using (OracleCommand cmd = new OracleCommand(query, connection))
                    {
                        cmd.InitialLOBFetchSize = 1;


                        using (OracleDataReader reader = cmd.ExecuteReader())
                        {
                            reader.FetchSize = cmd.RowSize * 1000;
                            var start_time = DateTime.Now;
                            if (reader.HasRows)
                            {


                                while (reader.Read())
                                {
                                   
                                    
                                   
                                    var o = Enumerable.Range(0, reader.FieldCount).ToDictionary(reader.GetName, reader.GetValue);

                                    


                                    rows.Add(o);

                                    
                                    rawData.Add(new Dictionary<string,object>(o));
                                    //Console.WriteLine(it);
                                    // }

                                }
                                var end_time = DateTime.Now;
                                var ts = end_time - start_time;
                                var ts2 = Math.Round(ts.TotalSeconds, 3);
                                Console.WriteLine("Fetch Size = 100: " + ts2 + " seconds");
                                Console.WriteLine();
                                row = rows;
                                GC.Collect();
                                GC.WaitForPendingFinalizers();
                            }
                        }
                    }
                    return row;
                }
                else
                {
                    query = BuildSelectSql(tableConfig);
                    //var retu = connection.Query(BuildSelectSql(tableConfig));
                    rawData = new List<IDictionary<string, object>>();
                    var _prdData = (IEnumerable<IDictionary<string, object>>)connection.Query(query, buffered: false);
                    rawData.AddRange(new List<IDictionary<string, object>>(_prdData));
                    
                   

                    return _prdData;
                }
                
                
            }
        }
        public static string ByteArrayToString(byte[] ba)
        {

            return BitConverter.ToString(ba).Replace("-", "");
        }
        private string BuildCountSql(
           TableConfig tableConfig)
        {
            return $"SELECT COUNT(*) FROM {tableConfig.Schema}.{tableConfig.Name}";
        }
        public void UpdateRow(IDictionary<string, object> row, TableConfig tableConfig)
        {
            using (var connection = new OracleConnection(_connectionString))
            {
                connection.Open();
                connection.Execute(BuildUpdateSql(tableConfig), row, null,commandType: System.Data.CommandType.Text);
            }
        }

        public void UpdateRows(IEnumerable<IDictionary<string, object>> rows,int rowCount, TableConfig config, Action<int> updatedCallback = null)
        {
            SqlMapper.AddTypeHandler(new GeographyMapper());
            int? batchSize = _sourceConfig.UpdateBatchSize;
            if (batchSize == null ||
                batchSize <= 0)
            {
                batchSize = rowCount;
            }

            IEnumerable<Batch<IDictionary<string, object>>> batches = Batch<IDictionary<string, object>>.BatchItems(
                rows,
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
            
           
            
           
           
            using (OracleConnection connection = new OracleConnection(_connectionString))
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
            string sql = "";
            if (int.TryParse(tableConfig.RowCount, out int n))
            {
                sql = $"SELECT  {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn)} FROM {tableConfig.Name} WHERE rownum <=" + n;
            }
            else
                sql = $"SELECT  {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn)} FROM {tableConfig.Name}";

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
        public object Shuffle(string table, string column, object existingValue, bool retainNull,DataTable dataTable = null)
        {
            CompareLogic compareLogic = new CompareLogic();
            //string _connectionStringGet = ConfigurationManager.AppSettings["ConnectionStringPrd"];
            Random rnd = new Random();
            string sql = "SELECT " + column + " FROM " + " " + table;
            using (var connection = new Oracle.DataAccess.Client.OracleConnection(_connectionStringPrd))
            {
                connection.Open();
                var result = (IEnumerable<IDictionary<string, object>>)connection.Query(sql);
                //var values = Array();
                //Randomizer randomizer = new Randomizer();
                if (retainNull)
                {
                    Values = result.Select(n => n.Values).SelectMany(x => x).ToList().Where(n => n != null).Distinct().ToArray();
                }
                else
                    Values = result.Select(n => n.Values).SelectMany(x => x).ToList().Distinct().ToArray();


                //var find = values.Count();
                object value = Values[rnd.Next(Values.Count())];         
                if (Values.Count() <= 1)
                {
                    o = o + 1;
                    if (o == 1)
                    {
                        File.WriteAllText(_exceptionpath, "");
                    }
                   
                    if (!(exceptionBuilder.ContainsKey(table) && exceptionBuilder.ContainsValue(column)))
                    {
                        exceptionBuilder.Add(table, column);
                        File.AppendAllText(_exceptionpath, "Cannot generate unique shuffle value" + " on table " + table + " for column " + column + Environment.NewLine + Environment.NewLine);
                    }
                    //o = o + 1;
                    //File.AppendAllText(_exceptionpath, "Cannot generate unique shuffle value" + " on table " + table + " for column " + column + Environment.NewLine + Environment.NewLine);
                    return value;
                }
                if (compareLogic.Compare(value, null).AreEqual && retainNull)
                {
                    //var nt = values.Where(n => n != null).Select(n => n).ToArray()[rnd.Next(0, values.Where(n => n != null).ToArray().Count())];
                    return Values.Where(n => n != null).Select(n => n).ToArray()[rnd.Next(0, Values.Where(n => n != null).ToArray().Count())];
                }
                while (compareLogic.Compare(value, existingValue).AreEqual)
                {

                    value = Values[rnd.Next(0,Values.Count())];
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

        public DataTable SpreadSheetTable(IEnumerable<IDictionary<string, object>> parents, TableConfig config)
        {
            var table = new DataTable();



            var c = parents.FirstOrDefault(x => x.Values
                                           .OfType<IEnumerable<IDictionary<string, object>>>()
                                           .Any());
            var p = c ?? parents.FirstOrDefault();
            if (p == null)
                return table;

            //var ccc = p.Where(x => x.Value is object)
            //               .Select(x => x.Key);



            //var headers1 = p.Where(x => x.Value is object)
            //               .Select(x => x.Key)
            //               .Concat(c == null ?
            //                       Enumerable.Empty<object>() :
            //                       c.Values
            //                        .OfType<IEnumerable<IDictionary<string, object>>>()
            //                        .First()
            //                        .SelectMany(x => x.Keys)).ToArray();





            foreach (var parent in parents)
            {
                var children = parent.Values
                                     .OfType<IEnumerable<IDictionary<string, object>>>()
                                     .ToArray();

                var length = children.Any() ? children.Length : 1;
                var parentEntries1 = parent.Where(x => x.Value is object).ToLookup(x => x.Key, x => x.Value);


                var parentEntries = parent
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
                foreach (var header in headers)
                {
                    if (config.Columns.Where(n=>n.Name.Equals(header.ColumnName)).Count() != 0)
                    {
                        foreach (ColumnConfig columnConfig in config.Columns.Where(n => n.Name.Equals(header.ColumnName)))
                        {
                            if (columnConfig.Type == DataType.Geometry || columnConfig.Type == DataType.Shufflegeometry)
                            {
                                header.DataType = typeof(SdoGeometry);
                            }
                            else if (columnConfig.Type == DataType.Blob)
                            {
                                header.DataType = typeof(byte[]);
                            }
                            
                        }
                    }

                    
                  
                    table.Columns.Add(header);
                }
                

                var addedRows = new int[length];
                //var xxx = table.Rows.Add();
                for (int i = 0; i < length; i++)
                    addedRows[i] = table.Rows.IndexOf(table.Rows.Add());

                foreach (DataColumn col in table.Columns)
                {
                    if (!allEntries.TryGetValue(col.ColumnName, out object[] columnRows))
                        continue;

                    for (int i = 0; i < addedRows.Length; i++)
                    {
                        if (columnRows[i] is SdoGeometry)
                        {
                            table.Rows[addedRows[i]][col] = (SdoGeometry)columnRows[i];
                        }
                        else if (columnRows[i] is byte[])
                        {
                            table.Rows[addedRows[i]][col] = (byte[])columnRows[i];
                        }
                        else
                        {
                            table.Rows[addedRows[i]][col] = columnRows[i];
                        }
                    }
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
                    if (!allEntries.TryGetValue(col.ColumnName, out object[] columnRows))
                        continue;

                    foreach (var row in addedRows)
                        table.Rows[row.actualIndex][col] = columnRows[row.relativeIndex];
                }
            }

            return table;
        }

        public IEnumerable<IDictionary<string, object>> RawData(IEnumerable<IDictionary<string, object>> PrdData)
        {
            //rawData = getData; 
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
            return rawData;
        }

        public int GetCount(TableConfig config)
        {
            using (OracleConnection connection = new OracleConnection(_connectionStringPrd))
            {
                connection.Open();
                //var tb = BuildCountSql(config);
                var count = connection.ExecuteScalar(BuildCountSql(config));
                return Convert.ToInt32(count);
            }
        }
    }
}
