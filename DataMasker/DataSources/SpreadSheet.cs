using DataMasker.Interfaces;
using DataMasker.Models;
using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DataMasker.DataSources
{
   public class SpreadSheet : IDataSource
    {
        //create object = > SpreadSheetTable = > mask = >
        private static TextFieldParser cvsReader;
        private readonly DataSourceConfig _sourceConfig;
        private readonly string _connectionString;
        public SpreadSheet(
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

        public DataTable DataTableFromCsv(string csvPath)
        {
            

       
            DataTable dataTable = new DataTable();
            dataTable.Columns.Clear();
            dataTable.Rows.Clear();
            dataTable.Clear();

            List<string> allEmails = new List<string>();


            using (cvsReader = new TextFieldParser(csvPath))
            {
                cvsReader.SetDelimiters(new string[] { "," });

                //cvsReader.HasFieldsEnclosedInQuotes = true;
                //read column
                string[] colfield = cvsReader.ReadFields();
                //colfield
                //specila chra string
                string specialChar = @"\|!#$%&/()=?»«@£§€{}.-;'<>,";
                string repclace = @"_";
                repclace.ToCharArray();
                foreach (string column in colfield)
                {
                    foreach (var item in specialChar)
                    {
                        if (column.Contains(item))
                        {
                            column.Replace(item, repclace[0]);

                        }
                    }
                    DataColumn datacolumn = new DataColumn(column);
                    datacolumn.AllowDBNull = true;
                    var dcol = Regex.Replace(datacolumn.ColumnName, @"[^a-zA-Z0-9_.]+", "_");
                    dataTable.Columns.Add(dcol);


                }

                while (!cvsReader.EndOfData)
                {

                    try
                    {
                        string[] fieldData = cvsReader.ReadFields();
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }


                        }



                        dataTable.Rows.Add(fieldData);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        return null;
                    }




                }
            }
            return dataTable;
        }

        public IEnumerable<IDictionary<string, object>> GetData(TableConfig tableConfig)
        {
            throw new NotImplementedException();
        }

        public object GetData(string column, string table)
        {
            throw new NotImplementedException();
        }

        public object shuffle(string table, string column, object existingValue, DataTable _dataTable)
        {
            //ArrayList list = new ArrayList();
            Random rnd = new Random();
            
                var result = new DataView(_dataTable).ToTable(false, new string[] { column}).AsEnumerable().Select(n => n[0]).ToList();
                //Randomizer randomizer = new Randomizer();

                var values = result.Where(n => n != null).Distinct().ToArray();
                //var find = randomizer.Shuffle(values);
                object value = values[rnd.Next(values.Count())];
                if (values.Count() <= 1)
                {
                    Console.WriteLine("Cannot generate unique shuffle value" + " on table " + table + "for column " + column + Environment.NewLine + Environment.NewLine);
                    return value;
                }
                while (value.Equals(existingValue))
                {

                    value = values[rnd.Next(0, values.Count())];
                }

                return value;

            
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

                var parentEntries = parent.Where(x => x.Value is string)
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

        public void UpdateRow(IDictionary<string, object> row, TableConfig tableConfig)
        {
            throw new NotImplementedException();
        }

        public void UpdateRows(IEnumerable<IDictionary<string, object>> rows, TableConfig config, Action<int> updatedCallback = null)
        {
            throw new NotImplementedException();
        }
    }
}
