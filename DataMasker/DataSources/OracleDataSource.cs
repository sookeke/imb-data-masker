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

namespace DataMasker.DataSources
{
    public class OracleDataSource : IDataSource
    {
        private readonly DataSourceConfig _sourceConfig;

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
            using (var connection = new Oracle.DataAccess.Client.OracleConnection(_connectionString))
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
                            connection.Execute(sql, batch.Items, sqlTransaction);

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
                        catch (Exception ex)
                        {

                            Console.WriteLine(ex.Message);
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
        public object shuffle(string table, string column)
        {
            //ArrayList list = new ArrayList();
            Random rnd = new Random();
            string sql = "SELECT " + table + " FROM " + " " + column;
            using (var connection = new Oracle.DataAccess.Client.OracleConnection(_connectionString))
            {
                connection.Open();
                var result = (IEnumerable<IDictionary<string, object>>)connection.Query(sql);
                var values = result.Select(n => n.Values).SelectMany(x => x).ToArray();

                return values[rnd.Next(values.Count())];

            }

            //return list;
        }

        
    }
}
