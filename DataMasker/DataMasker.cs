using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Bogus.DataSets;
using DataMasker.Interfaces;
using DataMasker.Models;
using System.Drawing;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using System.Data;

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
            TableConfig tableConfig, IDataSource dataSource, DataTable dataTable)
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
                //else if (columnConfig.Name == "VALUE" || columnConfig.Name == "KEY" || columnConfig.Name == "ACCESS_TYPE_DESCRIPTION")
                //{

                //}
                if (columnConfig.Type == DataType.Shuffle)
                {
                    existingValue = _dataGenerator.GetValueShuffle(columnConfig, tableConfig.Name, columnConfig.Name, dataSource,dataTable, existingValue, gender);
                }          
                else if (columnConfig.Type == DataType.File)
                {
                    if (existingValue.ToString().Contains("."))
                    {
                        columnConfig.StringFormatPattern = existingValue.ToString().Substring(existingValue.ToString().LastIndexOf('.') + 1);
                    }
                    else
                    {
                        //columnConfig.StringFormatPattern = "{{SYSTEM.FILENAME}}";
                        columnConfig.Type = DataType.Filename;
                    }
                    
                    existingValue = _dataGenerator.GetValue(columnConfig, existingValue, gender);

                    // existingValue = _dataGenerator.get(columnConfig, tableConfig.Name, columnConfig.Name, dataSource, existingValue, columnConfig.StringFormatPattern, gender)
                }
                else if (columnConfig.Type == DataType.Ignore)
                {
                    obj[columnConfig.Name] = existingValue;
                }
                else if (columnConfig.Type == DataType.exception)
                {
                    var cc = existingValue.ToString().Length;
                    if (existingValue.ToString().Length > Convert.ToInt32(columnConfig.StringFormatPattern))
                    {
                        columnConfig.Ignore = false;
                        existingValue = _dataGenerator.GetValue(columnConfig, existingValue, gender);
                    }
                    else
                    {
                        //columnConfig.Ignore = true;
                        //existingValue = existingValue;
                    }
                }
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
            TableConfig tableConfig, IDataSource dataSource,string filename, string fileExtension)
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
                else if (columnConfig.Unmask == true)
                {
                    obj[columnConfig.Name] = existingValue;
                }
                else if (columnConfig.Type == DataType.Filename && !string.IsNullOrEmpty(columnConfig.StringFormatPattern))
                {
                    //   // existingValue = _dataGenerator.GetBlobValue(columnConfig, tableConfig.Name, columnConfig.Name, dataSource, existingValue, columnConfig.StringFormatPattern, gender)
                    existingValue = _dataGenerator.GetBlobValue(columnConfig, dataSource, existingValue,filename, fileExtension, gender);
                }
                else if (columnConfig.Type == DataType.Blob)
                {
                    existingValue = _dataGenerator.GetBlobValue(columnConfig, dataSource, existingValue, filename, fileExtension, gender);
                }
                else
                {
                    //existingValue = _dataGenerator.GetBlobValue(columnConfig, dataSource, existingValue, filename, fileExtension, gender);
                    existingValue = _dataGenerator.GetValue(columnConfig, existingValue, gender);
                }
                //replace the original value
                obj[columnConfig.Name] = existingValue;
            }
            return obj;
        }
        private int GetObjectSize(object TestObject)
        {
            BinaryFormatter bf = new BinaryFormatter();
            MemoryStream ms = new MemoryStream();
            byte[] Array;
            bf.Serialize(ms, TestObject);
            Array = ms.ToArray();
            return Array.Count();
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
