using DataMasker.Interfaces;
using DataMasker.Models;
using Dapper;
using Oracle.DataAccess.Client;
//using Oracle.ManagedDataAccess.Client;
//using Oracle.ManagedDataAccess.Types;
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
using System.Globalization;
using Bogus;

namespace DataMasker.DataSources
{
    public class OracleDataSource : IDataSource
    {
        private readonly DataSourceConfig _sourceConfig;
        //global::Dapper.SqlMapper.AddTypeHandler(typeof(DbGeography), new GeographyMapper());
        private static readonly string _exceptionpath = Directory.GetCurrentDirectory() +  ConfigurationManager.AppSettings["_exceptionpath"];
        private static readonly string _successfulCommit = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_successfulCommit"];
        private static readonly DateTime DEFAULT_MIN_DATE = new DateTime(1900, 1, 1, 0, 0, 0, 0);
        //private static readonly string _exceptionpath = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
        private static readonly DateTime DEFAULT_MAX_DATE = DateTime.Now;
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
        public IEnumerable<IDictionary<string, object>> GetData(TableConfig tableConfig, Config config)
        {
            //string _connectionStringGet = ConfigurationManager.AppSettings["ConnectionStringPrd"];
            using (var connection = new OracleConnection(_connectionStringPrd))
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
                    query = BuildSelectSql(tableConfig, config);
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


                                    rawData.Add(new Dictionary<string, object>(o));
                                    //Console.WriteLine(it);
                                    // }

                                }

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
                    query = BuildSelectSql(tableConfig, config);
                    //var retu = connection.Query(BuildSelectSql(tableConfig));
                    rawData = new List<IDictionary<string, object>>();
                    var _prdData = (IEnumerable<IDictionary<string, object>>)connection.Query(query, buffered: true);
                    foreach (IDictionary<string, object> prd in _prdData)
                    {

                        rawData.Add(new Dictionary<string, object>(prd));
                    }
                    //rawData.AddRange(new List<IDictionary<string, object>>(_prdData));
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
        public void UpdateRow(IDictionary<string, object> row, TableConfig tableConfig, Config config)
        {
            using (var connection = new OracleConnection(_connectionString))
            {
                connection.Open();
                connection.Execute(BuildUpdateSql(tableConfig, config), row, null,commandType: System.Data.CommandType.Text);
            }
        }

        public void UpdateRows(IEnumerable<IDictionary<string, object>> rows,int rowCount, TableConfig tableConfig, Config config, Action<int> updatedCallback = null)
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
                    sw.WriteLine("exceptions for " + ConfigurationManager.AppSettings["DatabaseName"] + ".........." + Environment.NewLine + Environment.NewLine);
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

                    sw.WriteLine("Successful Commits for " + ConfigurationManager.AppSettings["DatabaseName"] + ".........." + Environment.NewLine + Environment.NewLine);
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


                        string sql = BuildUpdateSql(tableConfig, config);
                       
                        
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
                                File.AppendAllText(_successfulCommit, $"Successful Commit on table  {tableConfig.Schema}.{tableConfig.Name}" + Environment.NewLine + Environment.NewLine);
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
                            File.AppendAllText(_exceptionpath, ex.Message + $" on table {tableConfig.Schema}.{tableConfig.Name}" + Environment.NewLine + Environment.NewLine);
                           
                        }

                        
                    }
                }
            }
        }
        public string BuildUpdateSql(
           TableConfig tableConfig, Config config)
        {
            var charsToRemove = new string[] { "[", "]" };
            string sql = $"UPDATE {tableConfig.Schema}.{tableConfig.Name} SET ";

            sql += tableConfig.Columns.GetUpdateColumns(config);
            sql += $" WHERE {tableConfig.PrimaryKeyColumn} = @{tableConfig.PrimaryKeyColumn}";
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
           TableConfig tableConfig, Config config)
        {
            //var clumns = tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn)
            string sql = "";
            if (int.TryParse(tableConfig.RowCount, out int n))
            {
                sql = $"SELECT  {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn, config)} FROM {tableConfig.Schema}.{tableConfig.Name} WHERE rownum <=" + n;
            }
            else
                sql = $"SELECT  {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn, config)} FROM {tableConfig.Schema}.{tableConfig.Name}";

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
        public object Shuffle(string schema, string table, string column, object existingValue, bool retainNull, IEnumerable<IDictionary<string,object>> dataTable)
        {
            CompareLogic compareLogic = new CompareLogic();
            //string _connectionStringGet = ConfigurationManager.AppSettings["ConnectionStringPrd"];
            Random rnd = new Random();
            //string sql = $"SELECT {column} FROM {schema}.{table}";
            //using (var connection = new OracleConnection(_connectionStringPrd))
            //{
            //    connection.Open();
            //    var result = (IEnumerable<IDictionary<string, object>>)connection.Query(sql);
                //var values = Array();
                //Randomizer randomizer = new Randomizer();
                try
                {


                    if (retainNull)
                    {
                        Values = dataTable.Select(n => n.Values).SelectMany(x => x).ToList().Where(n => n != null).Distinct().ToArray();
                    }
                    else
                        Values = dataTable.Select(n => n.Values).SelectMany(x => x).ToList().Distinct().ToArray();


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
                        return Values.Where(n => n != null).Select(n => n).ToArray()[rnd.Next(0, Values.Where(n => n != null).ToArray().Count())];
                    }
                    while (compareLogic.Compare(value, existingValue).AreEqual)
                    {

                        value = Values[rnd.Next(0, Values.Count())];
                    }
                    if (value is SdoGeometry)
                    {
                        return (SdoGeometry)value;
                    }
                    return value;
                }
                catch (Exception ex)
                {

                    Console.WriteLine(ex.ToString());
                    File.AppendAllText(_exceptionpath, ex.ToString() + Environment.NewLine);
                    return null;
                }
               

            //}

            //return list;
        }
      

        public object GetData(string column, string table)
        {
            throw new NotImplementedException();
        }

        public DataTableCollection DataTableFromCsv(string csvPath, TableConfig tableConfig)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IDictionary<string, object>> CreateObject(DataTable dataTable)
        {
            List<Dictionary<string, object>> _sheetObject = new List<Dictionary<string, object>>();
            foreach (DataRow row in dataTable.Rows)
            {

                var dictionary = row.Table.Columns
                                .Cast<DataColumn>()
                                .ToDictionary(col => col.ColumnName, col => row.Field<object>(col.ColumnName));
                _sheetObject.Add(dictionary);

            }
            return _sheetObject;
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

            string[] formats = {"M/d/yyyy h:mm:ss tt", "M/d/yyyy h:mm tt",
                     "MM/dd/yyyy hh:mm:ss", "M/d/yyyy h:mm:ss",
                     "M/d/yyyy hh:mm tt", "M/d/yyyy hh tt",
                     "M/d/yyyy h:mm", "M/d/yyyy h:mm",
                     "MM/dd/yyyy hh:mm", "M/dd/yyyy hh:mm",

                     "M-d-yyyy h:mm:ss tt", "M-d-yyyy h:mm tt",
                     "MM-dd-yyyy hh:mm:ss", "M-d-yyyy h:mm:ss",
                     "M-d-yyyy hh:mm tt", "M-d-yyyy hh tt",
                     "M-d-yyyy h:mm", "M-d-yyyy h:mm",
                     "MM-dd-yyyy hh:mm", "M-dd-yyyy hh:mm",

                     "yyyy-d-M h:mm:ss tt", "yyyy-d-M h:mm tt",
                     "yyyy-dd-MM hh:mm:ss", "yyyy-d-M h:mm:ss",
                     "yyyy-d-M hh:mm tt", "yyyy-d-M hh tt",
                     "yyyy-d-M h:mm", "yyyy-d-M h:mm",
                     "yyyy-dd-MM hh:mm", "yyyy-dd-M hh:mm",

                     "yyyy-M-d h:mm:ss tt", "yyyy-M-d h:mm tt",
                     "yyyy-MM-dd hh:mm:ss", "yyyy-M-d h:mm:ss",
                     "yyyy-M-d hh:mm tt", "yyyy-M-d hh tt",
                     "yyyy-M-d h:mm", "yyyy-M-d h:mm",
                     "yyyy-MM-dd hh:mm", "yyyy-M-dd hh:mm",
                            "yyyyMMdd", "yyyyMdd hh:mm","yyyy",


            };
            Faker faker = new Faker();


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
                            if (columnConfig.Type == DataType.Geometry || columnConfig.Type == DataType.Shufflegeometry || columnConfig.Type == DataType.ShufflePolygon)
                            {
                                header.DataType = typeof(SdoGeometry);
                            }
                            else if (columnConfig.Type == DataType.Blob)
                            {
                                header.DataType = typeof(byte[]);
                            }
                            else if (columnConfig.Type == DataType.DateOfBirth)
                            {
                                header.DataType = typeof(DateTime);
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
                       else if (col.DataType == typeof(DateTime))
                        {
                            if (columnRows[i] is string && string.IsNullOrWhiteSpace(columnRows[i].ToString()))
                            {
                                
                                columnRows[i] = RemoveWhitespace(columnRows[i].ToString());
                                //columnRows[i] = DateTime.Parse(columnRows[i].ToString());
                                //Clear nullspace date record;
                                columnRows[i] = DateTime.TryParse(columnRows[i].ToString(), out DateTime temp) ? temp : faker.Date.Between(DEFAULT_MIN_DATE,  DEFAULT_MAX_DATE);

                                table.Rows[addedRows[i]][col] = columnRows[i];
                            }
                            else
                                 table.Rows[addedRows[i]][col] = DateTime.TryParseExact(columnRows[i].ToString(), formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime temp) ? temp : DateTime.Now;

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
        public string RemoveWhitespace(string str)
        {
            return string.Join("", str.Split(default(string[]), StringSplitOptions.RemoveEmptyEntries));
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

        public IEnumerable<T> CreateObjecttst<T>(DataTable dataTable)
        {
            throw new NotImplementedException();
        }
        class HazSqlGeo
        {
            //public int Id { get; set; }
            public SdoDimArray Geo { get; set; }
            
        }
        public DataTable GetDataTable(string table, string schema, string connection)
        {
            DataTable dataTable = new DataTable();
            List<object> geoInfoList = new List<object>();
            using (OracleConnection oracleConnection = new OracleConnection(connection))
            {
                string squery = "";
                if (schema == "MDSYS")
                {
                    var view = "USER_SDO_GEOM_METADATA";
                    squery = $"Select * from {schema}.{view} where TABLE_NAME = {table.AddSingleQuotes()}";
                }
                else
                    squery = $"Select * from {schema}.{table}";
                oracleConnection.Open();
                using (OracleDataAdapter oda = new OracleDataAdapter(squery, oracleConnection))
                {
                    try
                    {
                        //Fill the data table with select statement's query results:
                        int recordsAffectedSubscriber = 0;

                        recordsAffectedSubscriber = oda.Fill(dataTable);

                    }
                    catch (Exception ex)
                    {

                        Console.WriteLine(ex.Message);
                    }
                }
            }
            return dataTable;
        }
    }
}
