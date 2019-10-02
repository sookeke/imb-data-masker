using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using DataMasker.Interfaces;
using DataMasker.Models;

namespace DataMasker.DataSources
{
    public class InMemoryFakeDataSource : IDataSource
    {
        private readonly IDictionary<string, IList<IDictionary<string, object>>> tables;

        private IDictionary<string, IList<IDictionary<string, object>>> tableData
            => new Dictionary<string, IList<IDictionary<string, object>>>
            {
                {
                    "Users",
                    new List<IDictionary<string, object>>
                    {
                        new Dictionary<string, object>
                        {
                            {"UserId", 1},
                            {"  FirstName", "Steve"},
                            {"LastName", "Smith"},
                            {"Password", "SecurePassword!!!11"},
                            {"DOB", DateTime.Parse("1974-09-23")},
                            {"Gender", "M"},
                            {"Address", "55 Blue Street, Blue Town, Blueberry, BLue Kingdom, Blue Universe"},
                            {"ContactNumber", "+1 555-555-555-555-555-55-55"}
                        },
                        new Dictionary<string, object>
                        {
                            {"UserId", 2},
                            {"FirstName", "John "},
                            {"LastName", "Lucas"},
                            {"Password", "123SecurePassword!!!11"},
                            {"DOB", DateTime.Parse("1972-04-13")},
                            {"Gender", "M"},
                            {"Address", "56 Blue Street, Blue Town, Blueberry, BLue Kingdom, Blue Universe"},
                            {"ContactNumber", "+1 555-555-555-555-555-55-56"}
                        },
                        new Dictionary<string, object>
                        {
                            {"UserId", 3},
                            {"FirstName", "Jane"},
                            {"LastName", "Smith"},
                            {"Password", "Se123cureP33assword!!!11"},
                            {"DOB", DateTime.Parse("1938-03-21")},
                            {"Gender", "F"},
                            {"Address", "57 Blue Street, Blue Town, Blueberry, BLue Kingdom, Blue Universe"},
                            {"ContactNumber", "+1 555-555-555-555-555-55-57"}
                        }
                    }
                }
            };

        public InMemoryFakeDataSource()
        {
            tables = tableData;
        }

        /// <inheritdoc/>
        public IEnumerable<IDictionary<string, object>> GetData(
            TableConfig tableConfig, Config config)
        {
            return tableData[tableConfig.Name];
        }

        /// <inheritdoc/>
        public void UpdateRow(
            IDictionary<string, object> row,
            TableConfig tableConfig, Config config)
        {

            int index = tables[tableConfig.Name]
               .IndexOf(
                    tables[tableConfig.Name]
                       .Single(
                            x =>
                            {
                                bool e = x["UserId"]
                                   .Equals(row["UserId"]);

                                return e;
                            }));

            tables[tableConfig.Name][index] = row;
        }

        /// <inheritdoc/>
        public void UpdateRows(
            IEnumerable<IDictionary<string, object>> rows,
            int rowCount,
            TableConfig tableConfig, Config config,
            Action<int> updatedCallback)
        {

            foreach (IDictionary<string, object> dictionary in rows)
            {
                UpdateRow(dictionary, tableConfig, config);
            }
        }

        public object Shuffle(string table, string column, object existingValue, bool retainnull, DataTable dataTable)
        {
            throw new NotImplementedException();
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

        public int GetCount(TableConfig config)
        {
            return tableData.Count;
        }
    }
}
