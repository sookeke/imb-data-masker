using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using DataMasker.Models;

namespace DataMasker.Interfaces
{
    /// <summary>
    /// IDataSource
    /// </summary>
    public interface IDataSource
    {
        /// <summary>
        /// Gets the data.
        /// </summary>
        /// <param name="tableConfig">The table configuration.</param>
        /// <returns></returns>
        IEnumerable<IDictionary<string, object>> GetData(
            TableConfig tableConfig, Config config);
        IEnumerable<IDictionary<string, object>> RawData(
           IEnumerable<IDictionary<string, object>> PrdData);

        //For spreadshet
        DataTableCollection DataTableFromCsv(string csvPath);
        IEnumerable<IDictionary<string, object>> CreateObject(DataTable dataTable);

        IEnumerable<T> CreateObjecttst<T>(DataTable dataTable);

        DataTable CreateTable(IEnumerable<IDictionary<string, object>> obj);
        DataTable SpreadSheetTable(IEnumerable<IDictionary<string, object>> parents, TableConfig tableConfig);


        /// <summary>
        /// Updates the row.
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="tableConfig">The table configuration.</param>
        void UpdateRow(
            IDictionary<string, object> row,
            TableConfig tableConfig, Config config);
        object Shuffle(
            string table, string column,
            object existingValue, bool retainnull,
            DataTable dataTable);

        /// <summary>
        /// Updates the rows.
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <param name="config">The configuration.</param>
        /// <param name="updatedCallback">
        /// Called when a number of items have been updated, the value passed is the total items
        /// updated
        /// </param>
        void UpdateRows(
            IEnumerable<IDictionary<string, object>> rows,
            int rowCount,
            TableConfig tableConfig, Config config,
            Action<int> updatedCallback = null);

        int GetCount(TableConfig config);
        object GetData(string column, string table);
    }
}
