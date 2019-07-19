﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Bogus.DataSets;
using DataMasker.Interfaces;
using DataMasker.Models;
using System.Drawing;

namespace DataMasker
{
    /// <summary>
    /// DataMasker
    /// </summary>
    /// <seealso cref="DataMasker.Interfaces.IDataMasker"/>
    public class DataMasker : IDataMasker
    {
        /// <summary>
        /// The maximum iterations allowed when attempting to retrieve a unique value per column
        /// </summary>
        private const int MAX_UNIQUE_VALUE_ITERATIONS = 50000;
        /// <summary>
        /// The data generator
        /// </summary>
        private readonly IDataGenerator _dataGenerator;
        //private static readonly DataSourceProvider dataSourceProvider;

        /// <summary>
        /// A dictionary key'd by {tableName}.{columnName} containing a <see cref="HashSet{T}"/> of values which have been previously used for this table/column
        /// </summary>
        private readonly ConcurrentDictionary<string, HashSet<object>> _uniqueValues = new ConcurrentDictionary<string, HashSet<object>>();
        private readonly IDataSource _dataSource;
       


        /// <summary>
        /// Initializes a new instance of the <see cref="DataMasker"/> class.
        /// </summary>
        /// <param name="dataGenerator">The data generator.</param>
        public DataMasker(
            IDataGenerator dataGenerator)
        {
            _dataGenerator = dataGenerator;
        }
        //public DataSources()

        /// <summary>
        /// Masks the specified object with new data
        /// </summary>
        /// <param name="obj">The object to mask</param>
        /// <param name="tableConfig">The table configuration.</param>
        /// <returns></returns>
        public IDictionary<string, object> Mask(
            IDictionary<string, object> obj,
            TableConfig tableConfig, IDataSource dataSource)
        {

            foreach (ColumnConfig columnConfig in tableConfig.Columns.Where(x => !x.Ignore && x.Type != DataType.Computed))
            {
                object existingValue = obj[columnConfig.Name];

                Name.Gender? gender = null;
                if (!string.IsNullOrEmpty(columnConfig.UseGenderColumn))
                {
                  object g = obj[columnConfig.UseGenderColumn];
                  gender = Utils.Utils.TryParseGender(g?.ToString());
                }

                if (columnConfig.Unique)
                {
                  existingValue = GetUniqueValue(tableConfig.Name, columnConfig, existingValue, gender);
                }
                if (columnConfig.Type.ToString() == "Shuffle")
                {
                    existingValue = _dataGenerator.GetValueShuffle(columnConfig, tableConfig.Name, columnConfig.Name, dataSource, existingValue, gender);
                }
                //else if (columnConfig.Type.ToString() == "Blob" && !string.IsNullOrEmpty(columnConfig.StringFormatPattern))
                //{
                //   // existingValue = _dataGenerator.GetBlobValue(columnConfig, tableConfig.Name, columnConfig.Name, dataSource, existingValue, columnConfig.StringFormatPattern, gender)
                //}
                else
                {
                  existingValue = _dataGenerator.GetValue(columnConfig, existingValue, gender);
                }
                //replace the original value
                obj[columnConfig.Name] = existingValue;
            }

          foreach (ColumnConfig columnConfig in tableConfig.Columns.Where(x => !x.Ignore && x.Type == DataType.Computed))
          {
            var separator = columnConfig.Separator ?? " ";
            StringBuilder colValue = new StringBuilder();
            bool first = true;
            foreach (var sourceColumn in columnConfig.SourceColumns)
            {
              if (!obj.ContainsKey(sourceColumn))
              {
                throw new Exception($"Source column {sourceColumn} could not be found.");
              }

              if (first)
              {
                first = false;
              }
              else
              {
                colValue.Append(separator);
              }
              colValue.Append(obj[sourceColumn] ?? String.Empty);
             }
            obj[columnConfig.Name] = colValue.ToString();
          }
          return obj;
        }
        public IDictionary<string, object> MaskBLOB(IDictionary<string, object> obj,
            TableConfig tableConfig, IDataSource dataSource, string fileExtension)
        {
            foreach (ColumnConfig columnConfig in tableConfig.Columns.Where(x => !x.Ignore && x.Type != DataType.Computed))
            {
                object existingValue = obj[columnConfig.Name];

                Name.Gender? gender = null;
                if (!string.IsNullOrEmpty(columnConfig.UseGenderColumn))
                {
                    object g = obj[columnConfig.UseGenderColumn];
                    gender = Utils.Utils.TryParseGender(g?.ToString());
                }

                if (columnConfig.Unique)
                {
                    existingValue = GetUniqueValue(tableConfig.Name, columnConfig, existingValue, gender);
                }
                else if (columnConfig.Type.ToString() == "Filename" && !string.IsNullOrEmpty(columnConfig.StringFormatPattern))
                {
                    //   // existingValue = _dataGenerator.GetBlobValue(columnConfig, tableConfig.Name, columnConfig.Name, dataSource, existingValue, columnConfig.StringFormatPattern, gender)
                    existingValue = _dataGenerator.GetBlobValue(columnConfig, dataSource, existingValue, fileExtension, gender);
                }
                else
                {
                    existingValue = _dataGenerator.GetBlobValue(columnConfig, dataSource, existingValue, fileExtension, gender);
                }
                //replace the original value
                obj[columnConfig.Name] = existingValue;
            }
            return obj;
        }

        private object GetUniqueValue(string tableName,
            ColumnConfig columnConfig,
            object existingValue,
            Name.Gender? gender)
        {
            //create a unique key
            string uniqueCacheKey = $"{tableName}.{columnConfig.Name}";

            //if this table/column combination hasn't been seen before add an empty hash set
            if (!_uniqueValues.ContainsKey(uniqueCacheKey))
            {
                _uniqueValues.AddOrUpdate(uniqueCacheKey, new HashSet<object>(), (a, b) => b);
            }
            //grab the hash set for this table/column 
            HashSet<object> uniqueValues = _uniqueValues[uniqueCacheKey];

            int totalIterations = 0;
            do
            {

                existingValue = _dataGenerator.GetValue(columnConfig, existingValue, gender);
                totalIterations++;
                if (totalIterations >= MAX_UNIQUE_VALUE_ITERATIONS)
                {
                    throw new Exception($"Unable to generate unique value for {uniqueCacheKey}, attempt to resolve value {totalIterations} times");
                }
            }
            while (uniqueValues.Contains(existingValue));

            uniqueValues.Add(existingValue);
            return existingValue;
        }


    }
}
