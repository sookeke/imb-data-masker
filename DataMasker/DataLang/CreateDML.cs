using DataMasker.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMasker.DataLang
{

    public static class SqlDML
    {
        #region Public Methods

        /// <summary>
        /// Generates an insert statement for a data table
        /// </summary>
        /// <param name="table">The table.</param>
        /// <param name="removeFields">a list of fields to be left out of the insert statement</param>
        /// <returns></returns>
        public static string GenerateInsert(DataTable table, string[] removeFields, string fieldToReplace, string replacementValue, string writePath, Config config)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }
            if (string.IsNullOrEmpty(table.TableName) || table.TableName.Trim() == "")
            {
                throw new ArgumentException("tablename must be set on table");
            }

            var excludeNames = new SortedList<string, string>();
            if (removeFields != null)
            {
                foreach (string removeField in removeFields)
                {
                    excludeNames.Add(removeField.ToUpper(), removeField.ToUpper());
                }
            }

            var names = new List<string>();
            foreach (DataColumn col in table.Columns)
            {
                if (config.DataSource.Type == DataSourceType.SqlServer)
                {
                    if (!excludeNames.ContainsKey(col.ColumnName.ToUpper()))
                    {
                        names.Add("[" + col.ColumnName + "]");
                    }
                }
                else if (config.DataSource.Type == DataSourceType.OracleServer)
                {
                    if (!excludeNames.ContainsKey(col.ColumnName.ToUpper()))
                    {
                        names.Add(col.ColumnName);
                    }
                }
                else if (config.DataSource.Type == DataSourceType.PostgresServer)
                {
                    if (!excludeNames.ContainsKey(col.ColumnName.ToUpper()))
                    {
                        names.Add("\"" + col.ColumnName + "\"");
                    }
                }
                else
                {
                    if (!excludeNames.ContainsKey(col.ColumnName.ToUpper()))
                    {
                        names.Add("[" + col.ColumnName + "]");
                    }
                }
               
            }

            var output = new StringBuilder();

            if (config.DataSource.Type == DataSourceType.OracleServer)
            {

            }
            else if (config.DataSource.Type == DataSourceType.PostgresServer)
            {
                output.AppendFormat("INSERT INTO "+ "\"{0}\""+"\n\t({1})\nVALUES ", table.TableName, string.Join(", ", names.ToArray()));
            }
            else
            {
                output.AppendFormat("INSERT INTO [{0}]\n\t({1})\nVALUES ", table.TableName, string.Join(", ", names.ToArray()));
            }

           

            bool firstRow = true;
            int i = 1;
            foreach (DataRow rw in table.Rows)
            {
                

                if (firstRow)
                {
                    firstRow = false;
                    output.AppendLine("");
                }
                else
                {
                    // there was a previous item, so add a comma
                    output.AppendLine(",");
                }

                output.Append("\t(");

                output.Append(GetInsertColumnValues(table, rw, excludeNames, fieldToReplace, replacementValue,config));

                output.Append(")");
                i = i + 1;
               // var xuux = string.Join("", config.Tables.Select(n => n.Columns.ToArray().Select(x => x.Type + " ,").ToArray()));
                if ( i == table.Rows.Count + 1)
                {
                    var _allmaskType = string.Join("", config.Tables.Select(n => n.Columns.Select(x => x.Type + " ,")).ToArray()[0].ToArray());
                    var _commentOut = _allmaskType.Remove(_allmaskType.Length - 1).Insert(0, "--").Replace("Bogus", "Fake Data");
                    output.Append(Environment.NewLine);
                    output.Append(_commentOut);
                }

            }
            
            using (var tw = new StreamWriter(writePath, false))
            {
                tw.WriteLine(output.ToString());
                tw.Close();
                Console.WriteLine("{0}{1}", table.TableName + "DML" + Environment.NewLine, output.ToString());
            }

            return output.ToString();
        }

        /// <summary>
        /// Gets the column values list for an insert statement
        /// </summary>
        /// <param name="table">The table</param>
        /// <param name="row">a data row</param>
        /// <param name="excludeNames">A list of fields to be excluded</param>
        /// <returns></returns>
        public static string GetInsertColumnValues(DataTable table, DataRow row, SortedList<string, string> excludeNames, string fieldToReplace, string replacementValue, Config config)
        {
            var output = new StringBuilder();

            bool firstColumn = true;

            foreach (DataColumn col in table.Columns)
            {
                if (!excludeNames.ContainsKey(col.ColumnName.ToUpper()))
                {
                    if (firstColumn)
                    {
                        firstColumn = false;
                    }
                    else
                    {
                        output.Append(", ");
                    }

                    if (fieldToReplace != null && col.ColumnName.Equals(fieldToReplace, StringComparison.InvariantCultureIgnoreCase))
                    {
                        if (replacementValue == null)
                        {
                            output.Append("NULL");
                        }
                        else
                        {
                            output.Append(replacementValue);
                        }
                    }
                    else
                    {
                        output.Append(GetInsertColumnValue(row, col,config));
                        //output.
                    }
                }
            }

            return output.ToString();
        }

        /// <summary>
        /// Gets the insert column value, adding quotes and handling special formats
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="column">The column</param>
        /// <returns></returns>
        public static string GetInsertColumnValue(DataRow row, DataColumn column, Config config)
        {
            string output = "";

            if (row[column.ColumnName] == DBNull.Value)
            {
                output = "NULL";
            }
            else
            {
                if (column.DataType == typeof(bool))
                {
                    output = (bool)row[column.ColumnName] ? "1" : "0";
                }
                else
                {
                    bool addQuotes = false;
                    addQuotes = addQuotes || (column.DataType == typeof(string));
                    addQuotes = addQuotes || (column.DataType == typeof(DateTime));

                    if (addQuotes)
                    {
                        if (config.DataSource.Type == DataSourceType.PostgresServer)
                        {
                            if (row[column.ColumnName].ToString().Contains("'"))
                            {
                                output = "'" + row[column.ColumnName].ToString().Replace("'", "''") + "'";
                            }
                            else
                            {
                                output = "'" + row[column.ColumnName].ToString() + "'";
                            }
                        }
                        else
                        {
                            output = "'" + row[column.ColumnName].ToString() + "'";
                        }
                    }
                    else
                    {
                        output = row[column.ColumnName].ToString();
                    }
                }
            }

            return output;
        }
    }

    #endregion Public Methods

}
