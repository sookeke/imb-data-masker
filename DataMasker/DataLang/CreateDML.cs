using DataMasker.Models;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

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
        public static string GenerateInsert(DataTable table, string[] removeFields, string fieldToReplace, string replacementValue, string writePath, Config config, TableConfig tableConfig)
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
            if (table.Rows.Count == 0 && table.Columns.Count == 0 && config.DataSource.Type != DataSourceType.OracleServer)
            {
                


                if (config.DataSource.Type == DataSourceType.SqlServer)
                {
                    names.Add("[" + tableConfig.Name + "]");
                    foreach (ColumnConfig col in tableConfig.Columns)
                    {
                        if (!excludeNames.ContainsKey(col.Name.ToUpper()))
                        {
                            names.Add("[" + col.Name + "]");
                        }
                    }
                }
                if (config.DataSource.Type == DataSourceType.MySqlServer)
                {
                    names.Add("`" + tableConfig.Name + "`");
                    foreach (ColumnConfig col in tableConfig.Columns)
                    {
                        if (!excludeNames.ContainsKey(col.Name.ToUpper()))
                        {
                            names.Add("`" + col.Name + "`");
                        }
                    }
                }
                
            }
            else
            {
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
                    else if (config.DataSource.Type == DataSourceType.MySqlServer)
                    {
                        if (!excludeNames.ContainsKey(col.ColumnName.ToUpper()))
                        {
                            names.Add("`" + col.ColumnName + "`");
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
            }
          

            var output = new StringBuilder();

            if (config.DataSource.Type == DataSourceType.OracleServer)
            {

                output.AppendFormat("REM INSERTING into {0}\n", table.TableName);
                output.AppendFormat("SET DEFINE OFF\n\t");
            }
            else if (config.DataSource.Type == DataSourceType.PostgresServer)
            {
               
                    output.AppendFormat("INSERT INTO " + "\"{0}\"" + "\n\t({1})\nVALUES ", $"{tableConfig.Schema}" + @""".""" + $"{tableConfig.Name}", string.Join(", ", names.ToArray()));

            }
            else if (config.DataSource.Type == DataSourceType.SqlServer && table.Rows.Count > 1000)
            {
                
                    output.AppendFormat("SET ANSI_NULLS ON\n GO\n");
                    output.AppendFormat("SET QUOTED_IDENTIFIER ON\n GO\n");


              
            }
            else if (config.DataSource.Type == DataSourceType.MySqlServer)
            {
                //output.AppendFormat("INSERT INTO `{0}`\n\t({1})\nVALUES ", table.TableName, string.Join(", ", names.ToArray()));
                output.AppendFormat("INSERT INTO {0}\n\t({1})\nVALUES ", $"`{tableConfig.Schema}`." + $"`{tableConfig.Name}`", string.Join(", ", names.ToArray()));

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
                    if (config.DataSource.Type == DataSourceType.OracleServer)
                    {
                        output.AppendLine(";");
                    }
                    else if (config.DataSource.Type == DataSourceType.SqlServer && table.Rows.Count > 1000)
                    {
                        output.AppendLine(";");
                    }
                    else
                        output.AppendLine(",");

                }

              

                //oracle does not allow multi insert
                if (config.DataSource.Type == DataSourceType.OracleServer)
                {
                    output.AppendFormat("INSERT INTO {0}({1}) VALUES ", table.TableName, string.Join(", ", names.ToArray()));
                    output.Append("(");
                    output.Append(GetInsertColumnValues(table, rw, excludeNames, fieldToReplace, replacementValue, config));

                    output.Append(")");
                }
                else if (config.DataSource.Type == DataSourceType.SqlServer && table.Rows.Count > 1000)
                {
                    output.AppendFormat("INSERT INTO [{0}]({1}) VALUES ", table.TableName, string.Join(", ", names.ToArray()));
                    output.Append("(");
                    output.Append(GetInsertColumnValues(table, rw, excludeNames, fieldToReplace, replacementValue, config));
                    output.Append(")");
                }
                else
                {
                    output.Append("\t(");
                    output.Append(GetInsertColumnValues(table, rw, excludeNames, fieldToReplace, replacementValue, config));

                    output.Append(")");
                }
               
                i = i + 1;
               // var xuux = string.Join("", config.Tables.Select(n => n.Columns.ToArray().Select(x => x.Type + " ,").ToArray()));
                if ( i == table.Rows.Count + 1)
                {
                    var _allmaskType = string.Join("", tableConfig.Columns.Select(n => n.Type + " ,").ToArray());
                        //string.Join("", config.Tables.Select(n => n.Columns.Select(x => x.Type + " ,")).ToArray()[0].ToArray());
                    var _commentOut = _allmaskType.Remove(_allmaskType.Length - 1).Insert(0, "-- No Masking PK, ").Replace("Bogus", "Fake Data");
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
        private static bool CheckDate(string date)
        {
            try
            {
                if (DateTime.TryParse(date, out DateTime dateTime))
                {
                    return true;
                }
                else
                    return false;
               
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Gets the insert column value, adding quotes and handling special formats
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="column">The column</param>
        /// <returns></returns>
        /// 
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
                    bool hasGeometry = false;
                    bool hasByte = false;
                    addQuotes = addQuotes || (column.DataType == typeof(string));
                    addQuotes = addQuotes || (column.DataType == typeof(DateTime));
                    hasByte = hasByte || (column.DataType == typeof(byte[]));
                    hasGeometry = hasGeometry || (column.DataType == typeof(SdoGeometry));

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
                        else if (config.DataSource.Type == DataSourceType.OracleServer)
                        {
                            if (column.DataType == typeof(DateTime))
                            {
                                var data = DateTime.ParseExact(row[column.ColumnName].ToString(), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                                output = "To_DATE(" + "'" + data.ToString() + "," + "'YYYY-MM-DD HH:MI:SS'";
                            }
                            else if(CheckDate(row[column.ColumnName].ToString()))
                            {
                                DateTime dt = DateTime.Parse(row[column.ColumnName].ToString());
                                output = "TO_DATE(" + "'" + dt.ToString("yyyy-MM-dd HH:mm:ss") + "'" + ", " + "'YYYY-MM-DD HH24:MI:SS')";
                            }
                            else
                                output = "'" + row[column.ColumnName].ToString().Replace("'", "''") + "'";
                        }
                        else if (config.DataSource.Type == DataSourceType.MySqlServer)
                        {
                            if (column.DataType == typeof(DateTime))
                            {
                                var data = DateTime.ParseExact(row[column.ColumnName].ToString(), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                                output = "'" + data.ToString("yyyy-MM-dd HH:mm:ss") + "'" ;
                            }
                            else if (CheckDate(row[column.ColumnName].ToString()))
                            {
                                DateTime dt = DateTime.Parse(row[column.ColumnName].ToString());
                                output = "'" + dt.ToString("yyyy-MM-dd HH:mm:ss") + "'";
                            }
                            else
                                output = "'" + row[column.ColumnName].ToString().Replace("'", "''") + "'";

                        }
                        else
                        {
                            output = "'" + row[column.ColumnName].ToString().Replace("'", "''") + "'";
                        }
                    }
                    else if (hasGeometry)
                    {
                        switch (config.DataSource.Type)
                        {
                            case DataSourceType.InMemoryFake:
                                break;
                            case DataSourceType.SqlServer:
                                break;
                            case DataSourceType.OracleServer:
                                var value = (SdoGeometry)row[column.ColumnName];
                                var Z = "NULL"; var Y = "NULL"; var X = "NULL";
                                string arry_tostring = "NULL";
                                string info = "NULL";
                                string SDO_POINT = "NULL";
                                string INFO_ARRAY = "NULL";
                                string ORDINATE_ARRAY = "NULL";
                                if (value.OrdinatesArray != null)
                                {
                                    arry_tostring = string.Join(", ", value.OrdinatesArray);
                                    ORDINATE_ARRAY = string.Format("MDSYS.SDO_ORDINATE_ARRAY({0})", arry_tostring);
                                }
                               if(value.ElemArray != null)
                                {
                                    info = string.Join(",", value.ElemArray);
                                    INFO_ARRAY = string.Format("MDSYS.SDO_ELEM_INFO_ARRAY({0})", info);
                                }
                                if (value.Point != null)
                                {
                                    X = value.Point.X.ToString(); if (string.IsNullOrEmpty(X)) { X = "NULL"; };
                                    Y = value.Point.Y.ToString(); if (string.IsNullOrEmpty(Y)) { Y = "NULL"; };
                                    Z = value.Point.Z.ToString();if (string.IsNullOrEmpty(Z)) { Z = "NULL"; }; 
                                    SDO_POINT = string.Format("MDSYS.SDO_POINT_TYPE({0}, {1}, {2})", X, Y, Z);
                                }
                                

                                //var sdo_geometry_command_text = "MDSYS.SDO_GEOMETRY(" + value.Sdo_Gtype + "," + value.Sdo_Srid + ",NULL,MDSYS.SDO_ELEM_INFO_ARRAY(" + info + "),MDSYS.SDO_ORDINATE_ARRAY(" + arry_tostring + "))";
                                var sdo_geometry_command_text = string.Format("MDSYS.SDO_GEOMETRY({0},{1},{2},{3},{4})", value.Sdo_Gtype, value.Sdo_Srid, SDO_POINT, INFO_ARRAY, ORDINATE_ARRAY);
                                output = sdo_geometry_command_text;
                               
                                break;
                            case DataSourceType.SpreadSheet:
                                break;
                            case DataSourceType.PostgresServer:
                                break;
                            //default:
                            //    throw new NotImplementedException(nameof(config.DataSource.Type), config.DataSource.Type, "Unrecongnized Implemetation type");
                            //    break;


                        }
                        //throw new ArgumentOutOfRangeException(nameof(config.DataSource.Type), config.DataSource.Type, null);


                    }
                    else if (hasByte)
                    {
                        var o = (byte[])row[column.ColumnName];
                        var t = ByteArrayToString(o);
                        var h = t.TruncateLongString(1000);
                        switch (config.DataSource.Type)
                        {
                            case DataSourceType.InMemoryFake:
                                break;
                            case DataSourceType.SqlServer:
                                break;
                            case DataSourceType.OracleServer:
                                output = "hextoraw('" + h +"')";
                                break;
                            case DataSourceType.SpreadSheet:
                                break;
                            case DataSourceType.PostgresServer:
                                break;
                            default:
                                break;  
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
        public static string ByteArrayToString(byte[] ba)
        {
            return BitConverter.ToString(ba).Replace("-", "");
        }
        public static string TruncateLongString(this string str, int maxLength)
        {
            if (string.IsNullOrEmpty(str))
                return str;
            return str.Substring(0, Math.Min(str.Length, maxLength));
        }
        public static void DataTableToExcelSheet(DataTable dataTable, string path, TableConfig tableConfig)
        {
            //HttpContext.Current.Response.Clear();
            if (dataTable.Columns.Count == 0 && tableConfig.Columns.Count() != 0)
            {
                foreach (var col in tableConfig.Columns)
                {
                    dataTable.Columns.Add(col.Name);
                }
            }
            var format = new ExcelTextFormat
            {
                EOL = "\r\n"
            };
            for (int i = 0; i < dataTable.Columns.Count; i++)
            {



                if (dataTable.Columns[i].DataType == typeof(SdoGeometry))
                {
                    DataColumn dcolColumn = new DataColumn("GeometryToString".ToUpper(), typeof(string));
                    dataTable.Columns.Add(dcolColumn);
                    //dataTable.AcceptChanges();
                    //dataTable.Columns["GeometryToString"].DataType = typeof(SdoGeometry);
                }
            }
            foreach (DataColumn dataColumn in dataTable.Columns)
            {


                foreach (DataRow dataRow in dataTable.Rows)
                {
                    if (dataColumn.DataType == typeof(SdoGeometry))
                    {
                        //dataColumn.DataType = typeof(string);
                        var value = (SdoGeometry)dataRow[dataColumn.ColumnName];
                        var Z = "NULL"; var Y = "NULL"; var X = "NULL";
                        string arry_tostring = "NULL";
                        string info = "NULL";
                        string SDO_POINT = "NULL";
                        string INFO_ARRAY = "NULL";
                        string ORDINATE_ARRAY = "NULL";
                        if (value.OrdinatesArray != null)
                        {
                            arry_tostring = string.Join(", ", value.OrdinatesArray);
                            ORDINATE_ARRAY = string.Format("MDSYS.SDO_ORDINATE_ARRAY({0})", arry_tostring);
                        }
                        if (value.ElemArray != null)
                        {
                            info = string.Join(",", value.ElemArray);
                            INFO_ARRAY = string.Format("MDSYS.SDO_ELEM_INFO_ARRAY({0})", info);
                        }
                        if (value.Point != null)
                        {
                            X = value.Point.X.ToString(); if (string.IsNullOrEmpty(X)) { X = "NULL"; };
                            Y = value.Point.Y.ToString(); if (string.IsNullOrEmpty(Y)) { Y = "NULL"; };
                            Z = value.Point.Z.ToString(); if (string.IsNullOrEmpty(Z)) { Z = "NULL"; };
                            SDO_POINT = string.Format("MDSYS.SDO_POINT_TYPE({0}, {1}, {2})", X, Y, Z);
                        }


                        //var sdo_geometry_command_text = "MDSYS.SDO_GEOMETRY(" + value.Sdo_Gtype + "," + value.Sdo_Srid + ",NULL,MDSYS.SDO_ELEM_INFO_ARRAY(" + info + "),MDSYS.SDO_ORDINATE_ARRAY(" + arry_tostring + "))";
                        var sdo_geometry_command_text = string.Format("MDSYS.SDO_GEOMETRY({0},{1},{2},{3},{4})", value.Sdo_Gtype, value.Sdo_Srid, SDO_POINT, INFO_ARRAY, ORDINATE_ARRAY);
                        string output = sdo_geometry_command_text;
                        dataRow["GeometryToString".ToUpper()] = output;
                    }
                }
            }

            using (ExcelPackage pack = new ExcelPackage())
            {
                ExcelWorksheet ws = pack.Workbook.Worksheets.Add(dataTable.TableName);
                ws.Cells["A1"].LoadFromDataTable(dataTable,true,OfficeOpenXml.Table.TableStyles.Medium28);
                var ms = new System.IO.MemoryStream();
                pack.SaveAs(new FileInfo(path));
                
            }

           
        }
        public static DataTable RemoveBlob(DataTable dataTable)
        {
            foreach (DataColumn dataColumn in dataTable.Columns)
            {
                if (dataColumn.DataType == typeof(byte[]))
                {
                    dataTable.Columns.Remove(dataColumn);
                }
            }

            return dataTable;
       
        }
    }

    #endregion Public Methods

}
