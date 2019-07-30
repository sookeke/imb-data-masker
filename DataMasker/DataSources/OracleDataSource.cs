using DataMasker.Interfaces;
using DataMasker.Models;
using Dapper;
using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataMasker.Utils;
using System.Data;
using Z.Dapper.Plus;
using System.Collections;
using System.Data.Entity.Spatial;
using System.IO;
using System.Configuration;

namespace DataMasker.DataSources
{
    public class OracleDataSource : IDataSource
    {
        private readonly DataSourceConfig _sourceConfig;
        //global::Dapper.SqlMapper.AddTypeHandler(typeof(DbGeography), new GeographyMapper());
        private static string _exceptionpath = Directory.GetCurrentDirectory() + $@"\Output\"+ ConfigurationManager.AppSettings["APP_NAME"] + "_exception.txt";
        private static string _successfulCommit = Directory.GetCurrentDirectory() + $@"\Output\"+ ConfigurationManager.AppSettings["APP_NAME"] +"_successfulCommit.txt";


        private readonly string _connectionString;

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

        }
        public IEnumerable<IDictionary<string, object>> GetData(TableConfig tableConfig)
        {
            string _connectionStringGet = ConfigurationManager.AppSettings["ConnectionStringPrd"];
            using (var connection = new Oracle.DataAccess.Client.OracleConnection(_connectionStringGet))
            {
                connection.Open();
                //var retu = connection.Query(BuildSelectSql(tableConfig));
                return (IEnumerable<IDictionary<string, object>>)connection.Query(BuildSelectSql(tableConfig));
            }
        }

        public void UpdateRow(IDictionary<string, object> row, TableConfig tableConfig)
        {
            using (var connection = new OracleConnection(_connectionString))
            {
                connection.Open();
                connection.Execute(BuildUpdateSql(tableConfig), row, null,commandType: System.Data.CommandType.Text);
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
                            //Console.WriteLine(ex.Message);
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
        public object shuffle(string table, string column, object existingValue)
        {
            //ArrayList list = new ArrayList();
            Random rnd = new Random();
            string sql = "SELECT " + column + " FROM " + " " + table;
            using (var connection = new Oracle.DataAccess.Client.OracleConnection(_connectionString))
            {
                connection.Open();
                var result = (IEnumerable<IDictionary<string, object>>)connection.Query(sql);
                var values = result.Select(n => n.Values).SelectMany(x => x).ToList().Where(n => n != null).Distinct().ToArray();
                object value = values[rnd.Next(values.Count())];
                object ss = 1248200;
                var sect = result.Select(n => n.Values).Where(n => n == ss);
                if (values.Count() <= 1)
                {
                    File.AppendAllText(_exceptionpath, "Cannot generate unique shuffle value" + " on table " + table + "for column " + column + Environment.NewLine + Environment.NewLine);
                    //var sect1 = result.Select(n => n.Values).SelectMany(x => x).Where(n => n == ss);
                    return value;
                }
                //if (value.Equals(existingValue))
                //{
                //    Console.WriteLine("MATCH");
                //}
                while (value.Equals(existingValue))
                {
                    var xxx = values.Count();
                    value = values[rnd.Next(values.Count())];
                }
                 
                
              
                return value;

            }

            //return list;
        }
      

        public object GetData(string column, string table)
        {
            throw new NotImplementedException();
        }
    }
}
