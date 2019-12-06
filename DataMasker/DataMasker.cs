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
using KellermanSoftware.CompareNetObjects;
using System.Xml;
using System.Xml.Serialization;
using System.Configuration;
using ChoETL;
using Bogus;

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
        private const int MAX_UNIQUE_VALUE_ITERATIONS = 5000;
        /// <summary>
        /// The data generator
        /// </summary>
        private readonly IDataGenerator _dataGenerator;
        private static readonly string _exceptionpath = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
        //private static readonly DataSourceProvider dataSourceProvider;
        private static Randomizer _randomizer;
        /// <summary>
        /// A dictionary key'd by {tableName}.{columnName} containing a <see cref="HashSet{T}"/> of values which have been previously used for this table/column
        /// </summary>
        private readonly ConcurrentDictionary<string, HashSet<object>> _uniqueValues = new ConcurrentDictionary<string, HashSet<object>>();
        private readonly DataTable _location = new DataTable() { Columns = { "Country", "States", "Province", "City", "Address"} };
        //private readonly IDataSource _dataSource;



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
            TableConfig tableConfig, IDataSource dataSource,int rowCount, DataTable dataTable)
        {
            var addr = new DataTable();
            _location.Rows.Clear();
            foreach (ColumnConfig columnConfig in tableConfig.Columns.Where(x => !x.Ignore && x.Type != DataType.Computed))
            {
                //CompareLogic compareLogic = new CompareLogic();
                object existingValue = obj[columnConfig.Name];
                string uniqueCacheKey = $"{tableConfig.Name}.{columnConfig.Name}";


                Name.Gender? gender = null;
                if (!string.IsNullOrEmpty(columnConfig.UseGenderColumn) && obj[columnConfig.UseGenderColumn] != null)
                {
                  object g = obj[columnConfig.UseGenderColumn];
                  gender = Utils.Utils.TryParseGender(g?.ToString());
                }

                if (columnConfig.Unique)
                {
                  existingValue = GetUniqueValue(tableConfig.Name, columnConfig, existingValue, gender);
                }
                else if (columnConfig.Type == DataType.Shuffle || columnConfig.Type == DataType.Shufflegeometry)
                {
                    if (string.IsNullOrEmpty(tableConfig.Schema))
                    {
                        existingValue = _dataGenerator.GetValueShuffle(columnConfig, $"{tableConfig.Name}", columnConfig.Name, dataSource, dataTable, existingValue, gender);
                    }
                    else
                         existingValue = _dataGenerator.GetValueShuffle(columnConfig, $"{tableConfig.Schema}.{tableConfig.Name}", columnConfig.Name, dataSource,dataTable, existingValue, gender);
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
                    
                    existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableConfig.Name, gender);

                    // existingValue = _dataGenerator.get(columnConfig, tableConfig.Name, columnConfig.Name, dataSource, existingValue, columnConfig.StringFormatPattern, gender)
                }
                else if (columnConfig.Type == DataType.math)
                {
                    try
                    {
                        List<object> source = new List<object>();
                        if (string.IsNullOrEmpty(columnConfig.StringFormatPattern) && columnConfig.Max  == null && columnConfig.StringFormatPattern.Contains(","))
                        {
                            throw new InvalidOperationException("StringFormatPattern and Max Cannot be empty");
                        }
                        //check position of stringformat pattern objects
                        var columnPosition = tableConfig.Columns.Select(n=>n.Name).ToList();
                        foreach (var item in columnConfig.StringFormatPattern.Split(','))
                        {
                            //column A should have been masked alongside column B to apply operation: Col A + Col B = Col C
                            if (!(columnPosition.IndexOf(columnConfig.Name) > columnPosition.IndexOf(item)))
                            {
                                throw new InvalidOperationException(columnConfig.Name + " Index must be Greater than " + item);
                            }
                            else
                            {
                                //only number objects
                                if (IsNumeric(obj[item]))
                                {
                                    source.Add(obj[item]);
                                }
                                else
                                    throw new InvalidOperationException(item + " must be Numeric for " + columnConfig.Operator);

                            }
                            
                        }
                        existingValue = _dataGenerator.MathOperation(columnConfig, existingValue, source.ToArray(), columnConfig.Operator, Convert.ToInt32(columnConfig.Max));
                        
                    }
                    catch (Exception ex)
                    {
                        File.WriteAllText(_exceptionpath, "Masking Operation InvalidOperationException: " + ex.Message  + Environment.NewLine);
                       // throw;
                    }                                     
                }
                else if (columnConfig.Type == DataType.MaskingOut)
                {
                    if (columnConfig.StringFormatPattern.ToUpper().Contains("MaskingRight".ToUpper()))
                    {
                        existingValue = MaskingRight(existingValue, Convert.ToInt32(columnConfig.Max), columnConfig);
                    }
                   else if (columnConfig.StringFormatPattern.ToUpper().Contains("MaskingLeft".ToUpper()))
                    {
                        existingValue = MaskingLeft(existingValue, Convert.ToInt32(columnConfig.Max), columnConfig);
                    }
                   else if (columnConfig.StringFormatPattern.ToUpper().Contains("MaskingMiddle".ToUpper()))
                    {
                        existingValue = MaskingMiddle(existingValue, Convert.ToInt32(columnConfig.Max), columnConfig);
                    }
                    else
                        throw new ArgumentException("Invalid MaskingOut Operation", columnConfig.StringFormatPattern);
                }
                else if (columnConfig.Type == DataType.Scramble)
                {
                    existingValue = DataScramble(existingValue, columnConfig);
                }
                else if (columnConfig.Type == DataType.exception)
                {
                    var cc = existingValue.ToString().Length;
                    if (existingValue.ToString().Length > Convert.ToInt32(columnConfig.StringFormatPattern))
                    {
                        columnConfig.Ignore = false;
                        existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableConfig.Name, gender);
                    }
                    else
                    {
                        //columnConfig.Ignore = true;
                        //existingValue = existingValue;col
                    }
                }
                else if (_location.Columns.Cast<DataColumn>().Where(s=>columnConfig.Name.ToUpper().Contains(s.ColumnName.ToUpper())).Count() == 1 || _location.Columns.Cast<DataColumn>().Where(s => columnConfig.Type.ToString().ToUpper().Contains(s.ColumnName.ToUpper())).Count() == 1)
                {
                    var cname = _location.Columns.Cast<DataColumn>().Where(s => columnConfig.Name.ToUpper().Contains(s.ColumnName.ToUpper())).ToList().FirstOrDefault();
                    try
                    {
                        if (_location.Rows.Count == 0)
                        {
                            addr = (DataTable)_dataGenerator.GetAddress(columnConfig, existingValue, _location);
                        }
                        if (_location.Rows.Count > 0)
                        {
                            existingValue = _location.Rows[0][cname.ColumnName];
                        }
                        else
                            existingValue = null;
                    }
                    catch (Exception)
                    {
                        existingValue = null;
                    }

                }
                else if (_location.Columns.Contains(columnConfig.Name) || _location.Columns.Contains(columnConfig.Type.ToString()))
                {
                    try
                    {
                        if (_location.Rows.Count == 0)
                        {
                            addr = (DataTable)_dataGenerator.GetAddress(columnConfig, existingValue, _location);
                        }
                        if (_location.Rows.Count > 0)
                        {
                            existingValue = _location.Rows[0][columnConfig.Name]; 
                        }
                        else
                            existingValue = null;
                    }
                    catch (Exception)
                    {
                        existingValue = null;
                    }
                   
                }
                else
                {
                   
                    existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableConfig.Name, gender);               
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

        public static bool IsNumeric(object Expression)
        {
            //double retNum;
            if (Expression == null)
            {
                Expression = 0;
            }
            bool isNum = Double.TryParse(Convert.ToString(Expression), System.Globalization.NumberStyles.Any, System.Globalization.NumberFormatInfo.InvariantInfo, out double retNum);
            return isNum;
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
               else if (columnConfig.Type == DataType.Shuffle || columnConfig.Type == DataType.Shufflegeometry)
                {
                    existingValue = _dataGenerator.GetValueShuffle(columnConfig, tableConfig.Name, columnConfig.Name, dataSource, null, existingValue, gender);
                }
                else
                {
                    //existingValue = _dataGenerator.GetBlobValue(columnConfig, dataSource, existingValue, filename, fileExtension, gender);
                    existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableConfig.Name, gender);
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
        private int GetObjectSize(object TestObject)
        {
            BinaryFormatter bf = new BinaryFormatter();
            MemoryStream ms = new MemoryStream();
            byte[] Array;
            bf.Serialize(ms, TestObject);
            Array = ms.ToArray();
            return Array.Count();
        }
        private object DataScramble(object o, ColumnConfig columnConfig)
        {
            _randomizer = new Randomizer();
            if (columnConfig.RetainNullValues &&
              o == null)
            {
                return null;
            }
            if (columnConfig.RetainEmptyStringValues &&
               (o is string && string.IsNullOrWhiteSpace((string)o)))
            {
                return o;
            }
            if (o is string)
            {
               
                return string.Join("", _randomizer.Shuffle(Convert.ToString(o)).ToArray());
            }
            else if (o is decimal)
            {
                //_randomizer = new Randomizer();
                return Convert.ToDecimal(string.Join("", _randomizer.Shuffle(Convert.ToString(o)).ToArray()));
            }
            else if (o is double)
            {
               // _randomizer = new Randomizer();
                return Convert.ToDouble(string.Join("", _randomizer.Shuffle(Convert.ToString(o)).ToArray()));
            }
            else if (o is int)
            {
                //_randomizer = new Randomizer();
                return Convert.ToInt32(string.Join("", _randomizer.Shuffle(Convert.ToString(o)).ToArray()));
            }
            else if (o is long)
            {
                //_randomizer = new Randomizer();
                return Convert.ToInt64(string.Join("", _randomizer.Shuffle(Convert.ToString(o)).ToArray()));
            }
            else
                throw new ArgumentException(columnConfig.Type.ToString() + " does not apply to " +  o.GetType().ToString());
            return null;
        }
        private object MaskingLeft(object o, int position, ColumnConfig columnConfig)
        {
            if (columnConfig.RetainNullValues &&
              o == null)
            {
                return null;
            }
            if (columnConfig.RetainEmptyStringValues &&
               (o is string && string.IsNullOrWhiteSpace((string)o)))
            {
                return o;
            }
            List<string> slist = new List<string>();
            if (o is string && !string.IsNullOrWhiteSpace((string)o))
            {
                int i = 0;
                var s = Convert.ToString(o).Batch(position).Select(r => new String(r.ToArray())).ToList();
                s.ToList().ForEach(u =>
                {
                    if (i == 0)
                    {
                        var tu = string.Join("", u.Select(n => n.ToString().Replace(n, '*')));                     
                        slist.Add(tu);
                    }
                    else
                        slist.Add(u);
                    i = i + 1;
                });
                return string.Join("", slist.ToArray());
            }
            return string.Empty;
        }
        private object MaskingRight(object o, int position, ColumnConfig columnConfig)
        {
            if (columnConfig.RetainNullValues &&
           o == null)
            {
                return null;
            }
            if (columnConfig.RetainEmptyStringValues &&
               (o is string && string.IsNullOrWhiteSpace((string)o)))
            {
                return o;
            }
            List<string> slist = new List<string>();
            if (o is string && !string.IsNullOrWhiteSpace((string)o))
            {
                int i = 0;
                var s = Convert.ToString(o).Batch(position).Select(r => new String(r.ToArray())).ToList();
                s.ToList().ForEach(u =>
                {
                    if (i == s.Count - 1)
                    {
                        var tu = string.Join("", u.Select(n => n.ToString().Replace(n, '*')));                     
                        slist.Add(tu);
                    }
                    else
                        slist.Add(u);
                    i = i + 1;
                });
                return string.Join("", slist.ToArray());
            }
            return string.Empty;
        }
        private object  MaskingMiddle(object o, int position, ColumnConfig columnConfig)
        {
            List<string> slist = new List<string>();
            if (columnConfig.RetainNullValues &&
           o == null)
            {
                return null;
            }
            if (columnConfig.RetainEmptyStringValues &&
               (o is string && string.IsNullOrWhiteSpace((string)o)))
            {
                return o;
            }
            if (o is string && !string.IsNullOrWhiteSpace((string)o))
            {
                int i = 0;
                var s = Convert.ToString(o).Batch(position).Select(r => new String(r.ToArray())).ToList();
                s.ToList().ForEach(u =>
                {
                    if ( i == 0 || i == s.Count - 1)
                    {
                        slist.Add(u);
                    }
                    else
                    {
                        var tu = string.Join("", u.Select(n => n.ToString().Replace(n, '*')));
                        slist.Add(tu);                        
                    }
                    i = i + 1;
                });
                return string.Join("", slist.ToArray());
            }
            return string.Empty;
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

                existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableName, gender);
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
        private string Serialize<T>(T value)
        {
            if (value == null)
            {
                return string.Empty;
            }
            try
            {
                System.Xml.Serialization.XmlSerializer xmlserializer = new XmlSerializer(typeof(T));
                StringWriter stringWriter = new StringWriter();
                XmlWriter writer = XmlWriter.Create(stringWriter);
                xmlserializer.Serialize(writer, value);
                string serializeXml = stringWriter.ToString();
                writer.Close();
                return serializeXml;
            }
            catch (Exception)
            {
                return string.Empty;
            }
        }
    }


}

