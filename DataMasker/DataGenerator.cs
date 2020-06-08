using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Bogus;
using Bogus.DataSets;
using DataMasker.Interfaces;
using DataMasker.Models;
using GeoAPI.Geometries;
using NetTopologySuite.Geometries;
using System.Linq;
using Dapper;
using System.Configuration;
using System.Data;
using System.Net.Http;
using CountryData;
using System.Globalization;
using WaffleGenerator;
using KellermanSoftware.CompareNetObjects;

namespace DataMasker
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="IDataGenerator"/>
    public class DataGenerator : IDataGenerator
    {
        private static readonly DateTime DEFAULT_MIN_DATE = new DateTime(1900, 1, 1, 0, 0, 0, 0);
        private static readonly string _exceptionpath = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
        private static readonly DateTime DEFAULT_MAX_DATE = DateTime.Now;
        public static object[] Values { get; private set; }
        private static Dictionary<string, string> exceptionBuilder = new Dictionary<string, string>();
        private static readonly List<string> shuffleList = new List<string>();
        private static readonly List<KeyValuePair<object, object>> uniquevalue = new List<KeyValuePair<object, object>>();

        private const int DEFAULT_LOREM_MIN = 5;

        private const int DEFAULT_LOREM_MAX = 30;
        private const int MAX_UNIQUE_VALUE_ITERATIONS = 5000;

        private const int DEFAULT_RANT_MAX = 25;



        /// <summary>
        /// The data generation configuration
        /// </summary>
        private readonly DataGenerationConfig _dataGenerationConfig;
        //private static DataSourceProvider datasource;


        /// <summary>
        /// The faker
        /// </summary>
        private static Faker _faker;
        private readonly Fare.Xeger _xeger = new Fare.Xeger("[A-Za-z][0-9][A-Za-z] [0-9][A-Za-z][0-9]", new Random());

        /// <summary>
        /// The randomizer
        /// </summary>
        private readonly Randomizer _randomizer;


        /// <summary>
        /// The global value mappings
        /// </summary>
        private readonly IDictionary<string, IDictionary<object, object>> _globalValueMappings;

        /// <summary>
        /// Initializes a new instance of the <see cref="DataGenerator"/> class.
        /// </summary>
        /// <param name="dataGenerationConfig">The data generation configuration.</param>
        public DataGenerator(
            DataGenerationConfig dataGenerationConfig)
        {
            _dataGenerationConfig = dataGenerationConfig;
            _faker = new Faker(dataGenerationConfig.Locale ?? "en");
            _randomizer = new Randomizer();
            _globalValueMappings = new Dictionary<string, IDictionary<object, object>>();
        }


        /// <summary>
        /// Gets the value.
        /// </summary>
        /// <param name="columnConfig">The column configuration.</param>
        /// <param name="existingValue">The existing value.</param>
        /// <param name="gender">The gender.</param>
        /// <returns></returns>
        public object GetValue(
            ColumnConfig columnConfig,
            object existingValue,
            string tableName,
            Name.Gender? gender)
        {
            object getValue = null;
            int totalIterations = 0;
            string uniqueCacheKey = $"{tableName}.{columnConfig.Name}";
            if (columnConfig.ValueMappings == null)
            {
                columnConfig.ValueMappings = new Dictionary<object, object>();
            }

            if (!string.IsNullOrEmpty(columnConfig.UseValue))
            {
                return ConvertValue(columnConfig.Type, columnConfig.UseValue);
            }


            if (columnConfig.RetainNullValues &&
                existingValue == null)
            {
                return null;
            }
            if (columnConfig.RetainEmptyStringValues &&
               (existingValue is string && string.IsNullOrWhiteSpace((string)existingValue)))
            {
                return existingValue;
            }
            if (existingValue == null)
            {
                getValue = GetValue(columnConfig, gender);
                while (Convert.ToString(getValue).Equals(Convert.ToString(existingValue)))
                {
                    totalIterations++;
                    if (totalIterations >= MAX_UNIQUE_VALUE_ITERATIONS)
                    {
                        if (!(uniquevalue.Where(n=>n.Key.Equals(tableName)).Count() > 0 && uniquevalue.Where(n => n.Value.Equals(columnConfig.Name)).Count() > 0))
                        {
                            File.AppendAllText(_exceptionpath, $"Unable to generate unique value for {uniqueCacheKey}, attempt to resolve value {totalIterations} times" + Environment.NewLine + Environment.NewLine);
                            uniquevalue.Add(new KeyValuePair<object, object>(tableName,columnConfig.Name));
                        }
                        break;
                    }
                    getValue = GetValue(columnConfig, gender);
                }
                return getValue;
            }

            if (HasValueMapping(columnConfig, existingValue))
            {
                return GetValueMapping(columnConfig, existingValue);
            }
            if (columnConfig.Type == DataType.Geometry)
            {               
                Random random = new Random();              
                var exist = (SdoGeometry)existingValue;
                var obj = new HazSqlGeo {
                Geo = new SdoGeometry()
                {
                    Sdo_Gtype = exist.Sdo_Gtype,
                    Sdo_Srid = exist.Sdo_Srid
                }
                };

                if (exist.Point != null)
                {
                    obj.Geo.Point.X = exist.Point.Y; obj.Geo.Point.Y = exist.Point.Z; obj.Geo.Point.Z = exist.Point.X;
                }
                else {
                    
                    obj.Geo.Point = exist.Point;
                }


                if (exist.OrdinatesArray != null)
                {
                    decimal[] cood = new decimal[exist.OrdinatesArray.Count()];
                    for (int i = 0; i < exist.OrdinatesArray.Count(); i++)
                    {
                        //coords[i] = new Coordinate(center.X + random.NextDouble(-4291402.04717672, 16144349.4032217), center.Y + random.NextDouble(-4291402.04717672, 16144349.4032217));
                        cood[i] = new decimal(random.NextDouble(-4291402.04717672, 16144349.4032217));
                    }
                    obj.Geo.OrdinatesArray = cood;
                }
                else { obj.Geo.OrdinatesArray = exist.OrdinatesArray; }

                if (exist.ElemArray != null && exist.OrdinatesArray != null)
                {
                    obj.Geo.ElemArray = exist.ElemArray;
                    
                }
                else if (exist.ElemArray != null && exist.OrdinatesArray == null)
                {
                    obj.Geo.ElemArray = _randomizer.Shuffle(exist.ElemArray).ToArray();
                }
                else { obj.Geo.ElemArray = exist.ElemArray; }


                return obj.Geo;
            }

            object newValue = GetValue(columnConfig,gender);
          
            while (Convert.ToString(newValue).Equals(Convert.ToString(existingValue)))
            {
                totalIterations++;
                if (totalIterations >= MAX_UNIQUE_VALUE_ITERATIONS)
                {
                    if (!(uniquevalue.Where(n => n.Key.Equals(tableName)).Count() > 0 && uniquevalue.Where(n => n.Value.Equals(columnConfig.Name)).Count() > 0))
                    {
                        File.AppendAllText(_exceptionpath, $"Unable to generate unique value for {uniqueCacheKey}, attempt to resolve value {totalIterations} times" + Environment.NewLine + Environment.NewLine);
                        uniquevalue.Add(new KeyValuePair<object, object>(tableName, columnConfig.Name));
                        //break;
                    }
                    //File.AppendAllText(_exceptionpath, $"Unable to generate unique value for {uniqueCacheKey}, attempt to resolve value {totalIterations} times" + Environment.NewLine + Environment.NewLine);
                    break;
                }
                newValue = GetValue(columnConfig,gender);
            }
           
            if (columnConfig.UseGlobalValueMappings ||
                columnConfig.UseLocalValueMappings)
            {
                AddValueMapping(columnConfig, existingValue, newValue);
            }
            
            return newValue;
        }

        /// <summary>
        /// Determines whether [has value mapping] [the specified column configuration].
        /// </summary>
        /// <param name="columnConfig">The column configuration.</param>
        /// <param name="existingValue">The existing value.</param>
        /// <returns>
        /// <c>true</c> if [has value mapping] [the specified column configuration]; otherwise, <c>false</c>.
        /// </returns>
        private bool HasValueMapping(
            ColumnConfig columnConfig,
            object existingValue)
        {
            if (columnConfig.UseGlobalValueMappings)
            {
                return _globalValueMappings.ContainsKey(columnConfig.Name) &&
                       _globalValueMappings[columnConfig.Name]
                          .ContainsKey(existingValue);
            }

            return columnConfig.UseLocalValueMappings && columnConfig.ValueMappings.ContainsKey(existingValue);
        }

        /// <summary>
        /// Gets the value mapping.
        /// </summary>
        /// <param name="columnConfig">The column configuration.</param>
        /// <param name="existingValue">The existing value.</param>
        /// <returns></returns>
        private object GetValueMapping(
            ColumnConfig columnConfig,
            object existingValue)
        {
            if (columnConfig.UseGlobalValueMappings)
            {
                return _globalValueMappings[columnConfig.Name][existingValue];
            }

            return columnConfig.ValueMappings[existingValue];
        }

        /// <summary>
        /// Adds the value mapping.
        /// </summary>
        /// <param name="columnConfig">The column configuration.</param>
        /// <param name="existingValue">The existing value.</param>
        /// <param name="newValue">The new value.</param>
        private void AddValueMapping(
            ColumnConfig columnConfig,
            object existingValue,
            object newValue)
        {
            if (columnConfig.UseGlobalValueMappings)
            {
                if (_globalValueMappings.ContainsKey(columnConfig.Name))
                {
                    _globalValueMappings[columnConfig.Name]
                       .Add(existingValue, newValue);
                }
                else
                {
                    _globalValueMappings.Add(columnConfig.Name, new Dictionary<object, object> { { existingValue, newValue } });
                }
            }
            else if (columnConfig.UseLocalValueMappings)
            {
                columnConfig.ValueMappings.Add(existingValue, newValue);
            }
        }

        /// <summary>
        /// Gets the value.
        /// </summary>
        /// <param name="columnConfig">The column configuration.</param>
        /// <param name="gender">The gender.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException">Type - null</exception>
        /// 
        public static T ToEnum<T>(string value, T defaultValue) where T : struct
        {
            if (string.IsNullOrEmpty(value))
            {
                return defaultValue;
            }

            try
            {
                if (!Enum.TryParse(value, true, out T enumValue))
                {
                    return defaultValue;
                }
                return enumValue;
            }
            catch (Exception)
            {
                return defaultValue;
            }
        }
        private object GetValue(
            ColumnConfig columnConfig,
            Name.Gender? gender = null)
        {
            _faker = new Faker();
            switch (columnConfig.Type)
            {
                case DataType.FirstName:
                    return _faker.Name.FirstName(gender);
                case DataType.LastName:
                    return _faker.Name.LastName(gender);
                case DataType.FullName:
                    return _faker.Name.FullName(gender);
                case DataType.DateOfBirth:
                    return _faker.Date.Between(
                        ParseMinMaxValue(columnConfig, MinMax.Min, DEFAULT_MIN_DATE),
                        ParseMinMaxValue(columnConfig, MinMax.Max, DEFAULT_MAX_DATE));
                case DataType.Longitude:
                    return _faker.Address.Longitude();
                case DataType.Latitude:
                    return _faker.Address.Latitude();
                case DataType.TimeSpan:
                    DateTime start_time = DateTime.Today.AddHours(rnd.Next(3600));
                    DateTime span = start_time.AddMinutes(rnd.Next(241));
                    return span.ToString(columnConfig.StringFormatPattern);
                case DataType.Rant:
                    var rant = WaffleEngine.Text(rnd, ToInt32(columnConfig.Min), false);
                    int lenght = rant.Length;
                    if (!string.IsNullOrEmpty(columnConfig.Max) && rant.Length > ToInt32(columnConfig.Max))
                    {
                        var rantSub = rant.Substring(0, ToInt32(columnConfig.Max));
                        return rantSub;
                    }
                    return rant; // return _faker.Rant.Review(columnConfig.StringFormatPattern);
                case DataType.Lorem:
                    return _faker.Lorem.Sentence(ToInt32(columnConfig.Min), ToInt32(columnConfig.Max));
                case DataType.StringFormat:
                    return _randomizer.Replace(columnConfig.StringFormatPattern);
                case DataType.FullAddress:
                    return _faker.Address.FullAddress();
                case DataType.StreetAddress:
                    return _faker.Address.StreetAddress(false);
                case DataType.File:
                    if (columnConfig.Max.Contains("."))
                    {
                        //return decimal
                        columnConfig.Max = Math.Round(ToDecimal(columnConfig.Max)).ToString();
                    }
                    var f = _faker.System.FileName(columnConfig.StringFormatPattern);
                    //Console.WriteLine(f);
                    if (!string.IsNullOrEmpty(columnConfig.Max) && f.Length > ToInt32(columnConfig.Max))
                    {
                        var _shortnum = f.Substring(f.Length - ToInt32(columnConfig.Max), ToInt32(columnConfig.Max));
                        return _shortnum;
                    }
                    return f;
                case DataType.Filename:
                    var file = _faker.System.FileName("");
                    if (!string.IsNullOrEmpty(columnConfig.Max) && file.Length > ToInt32(columnConfig.Max))
                    {
                        var _shortnum = file.Substring(file.Length - ToInt32(columnConfig.Max), ToInt32(columnConfig.Max));
                        return _shortnum.Remove(_shortnum.Length - 1 );
                    }
                    return file.Remove(file.Length - 1);
                case DataType.SecondaryAddress:
                    return _faker.Address.SecondaryAddress();
                case DataType.Vehicle:
                    switch (ToEnum(columnConfig.StringFormatPattern, Vehicles.None))
                    {
                        case Vehicles.Manufacturer:
                            return _faker.Vehicle.Manufacturer();
                        case Vehicles.Model:
                            return _faker.Vehicle.Model();
                        case Vehicles.Type:
                            return _faker.Vehicle.Type();
                        case Vehicles.Vin:
                            return _faker.Vehicle.Vin();
                        case Vehicles.Fuel:
                            return _faker.Vehicle.Fuel();
                        default:
                            break;
                    }

                    throw new ArgumentOutOfRangeException(nameof(columnConfig.StringFormatPattern),columnConfig.StringFormatPattern,"Invalid Vehicle String Format value: " + columnConfig.StringFormatPattern.AddDoubleQuotes()); ;
                case DataType.State:
                    //rnd = new Random();
                    var state = CountryLoader.LoadCanadaLocationData().States.OrderBy(x => rnd.Next()).First().Name;
                    return state;
                case DataType.City:
                    //rnd = new Random();
                    
                    var cities = CountryLoader.LoadCanadaLocationData().States.OrderBy(x => rnd.Next()).First().Provinces;
                    var states = CountryLoader.LoadCanadaLocationData().States.Where(n => n.Provinces.Count > 1 ).Select(n=>n).ToArray();
                    var provinces = states[rnd.Next(0, states.Count())].Provinces.Where(n => n.Name != null).Select(n=>n).ToArray();                    
                    var city = provinces[rnd.Next(0, provinces.Count())];
                    return city.Name;
                case DataType.Blob:
                    var fileUrl = _faker.Image.PicsumUrl();
                    string someUrl = fileUrl;
                    using (var WebClient = new WebClient())
                    {

                        byte[] imageBytes = WebClient.DownloadData(someUrl);
                        return imageBytes;
                    }
                case DataType.Clob:
                    var randomString = _faker.Lorem.Text();
                    byte[] newvalue = System.Text.Encoding.Unicode.GetBytes(randomString);
                    var bs64 = Convert.ToBase64String(newvalue);
                    return bs64;
                case DataType.Ignore:
                    return null;
                case DataType.Money:
                    var money = _faker.Parse(columnConfig.StringFormatPattern);
                    return ToDecimal(money);
                case DataType.RandomYear:
                    return _faker.Date.Between(ParseMinMaxValue(columnConfig, MinMax.Min, DEFAULT_MIN_DATE),
                        ParseMinMaxValue(columnConfig, MinMax.Max, DEFAULT_MAX_DATE)).ToString("yyyy");
                case DataType.RandomMonth:
                    if (columnConfig.StringFormatPattern.Contains("string"))
                    {
                        return _faker.Date.Month();
                    }
                    DateTime startM = new DateTime(1999, 1, 1);
                    var ranMonth = _faker.Date.Between(startM, DateTime.Now).ToString("MM");
                    return ranMonth;
                case DataType.RandomSeason:
                    DateTime _start = new DateTime(1995, 1, 1);
                    Random _genD = new Random();
                    int _range = ((TimeSpan)(DateTime.Today - _start)).Days;
                    var _randomYear = _start.AddDays(_genD.Next(_range));
                    return _randomYear.Year + "/" + _randomYear.AddYears(1).ToString("yy");             
                case DataType.RandomInt:
                    var min = columnConfig.Min;
                    var max = columnConfig.Max;
                    if (min.Contains(".") || max.Contains("."))
                    {
                        return Math.Round(_faker.Random.Decimal(ToDecimal(min), ToDecimal(max)));
                    }
                    return _faker.Random.Int(ToInt32(min), ToInt32(max));
                case DataType.CompanyPersonName:
                    var _compName = new Faker().Company.CompanyName();
                    var _personName = _faker.Person.FullName;
                    string[] _array = new string[] { _compName, _personName };
                    return _faker.PickRandom(_array);
                case DataType.PostalCode:
                    return _xeger.Generate().ToUpper().Replace(" ", string.Empty);
                case DataType.Company:
                    var company = new Faker();
                    var _genCompany = company.Company.CompanyName(columnConfig.StringFormatPattern); ;
                    if (!string.IsNullOrEmpty(columnConfig.Max) && _genCompany.Length > ToInt32(columnConfig.Max))
                    {
                        var _shortComp = _genCompany.Substring(0, ToInt32(columnConfig.Max));
                        return _shortComp;
                    }
                    return _genCompany;
                case DataType.RandomString2:
                    var rand = _faker.Random.String2(ToInt32(columnConfig.Max), columnConfig.StringFormatPattern);
                    return _faker.Random.String2(ToInt32(columnConfig.Max), columnConfig.StringFormatPattern);
                case DataType.PhoneNumber:
                    var _number = _faker.Phone.PhoneNumber(columnConfig.StringFormatPattern);
                    if (!string.IsNullOrEmpty(columnConfig.Max) && _number.Length > ToInt32(columnConfig.Max))
                    {
                        var _shortnum = _number.Substring(0, ToInt32(columnConfig.Max));
                        return _shortnum;
                    }
                    return _number;
                case DataType.StringConcat:
                    var _string = _faker.Phone.PhoneNumber(columnConfig.StringFormatPattern);
                    if (!string.IsNullOrEmpty(columnConfig.Max) && _string.Length > ToInt32(columnConfig.Max))
                    {
                        var _shortString = _string.Substring(0, ToInt32(columnConfig.Max));
                        return _shortString;
                    }
                    return _string;
                case DataType.exception:
                    var fileexception = _faker.System.FileName("");
                    return fileexception.Remove(fileexception.Length - 1);
                case DataType.PhoneNumberInt:
                    var _phone = Convert.ToInt64(_faker.Phone.PhoneNumber(columnConfig.StringFormatPattern));
                    return _phone;
                case DataType.RandomDec:
                    var value = _faker.Random.Decimal(ToDecimal(columnConfig.Min), ToDecimal(columnConfig.Max));
                    return value;
                case DataType.PickRandom:
                    var stringarray = columnConfig.StringFormatPattern.Split(',');
                    return _faker.PickRandom(stringarray);
                case DataType.RandomHexa:
                    return _faker.Random.Hexadecimal(ToInt32(columnConfig.StringFormatPattern));
                case DataType.Bogus:
                    if (string.IsNullOrEmpty(columnConfig.StringFormatPattern))
                    {
                        
                        throw new ArgumentException(nameof(columnConfig.Type) + " must have a StringFormatPattern", columnConfig.StringFormatPattern);
                        
                    }
                    var _gen = _faker.Parse(columnConfig.StringFormatPattern);
                    if (columnConfig.Min.Contains(".") || columnConfig.Max.Contains("."))
                    {
                        if (!string.IsNullOrEmpty(columnConfig.Max) && _gen.Length > ToDecimal(columnConfig.Max))
                        {
                            var _short = _gen.Substring(0, ToInt32(columnConfig.Max));
                            return _short;
                        }
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(columnConfig.Max) && _gen.Length > ToInt32(columnConfig.Max))
                        {
                            var _short = _gen.Substring(0, ToInt32(columnConfig.Max));
                            return _short;
                        }
                    }
                    return _gen;
                case DataType.RandomUsername:
                    var ussername = new Faker();
                    return ussername.Person.UserName;
                case DataType.Computed:
                    return null;
            }
            throw new ArgumentOutOfRangeException(nameof(columnConfig.Type), columnConfig.Type, null);
        }

        /// <summary>
        /// Converts the value.
        /// </summary>
        /// <param name="dataType">Type of the data.</param>
        /// <param name="val">The value.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException">dataType - null</exception>
        private object ConvertValue(
            DataType dataType,
            string val)
        {
            if (val.ToUpper() == ("NULL"))
            {
                return null;
            }
            switch (dataType)
            {
                case DataType.FirstName:
                case DataType.LastName:
                case DataType.Rant:
                case DataType.Lorem:
                case DataType.StringFormat:
                case DataType.Company:
                case DataType.Vehicle:
                case DataType.FullName:
                case DataType.CompanyPersonName:
                case DataType.PostalCode:
                case DataType.RandomUsername:
                case DataType.RandomYear:
                case DataType.RandomSeason:                
                case DataType.PickRandom:
                case DataType.Shuffle:
                case DataType.FullAddress:
                case DataType.State:
                case DataType.SecondaryAddress:
                case DataType.City:
                case DataType.Bogus:
                case DataType.StringConcat:
                case DataType.PhoneNumber:
                case DataType.None:
                    return val;
                case DataType.DateOfBirth:
                    return DateTime.Parse(val);
                case DataType.RandomInt:
                    return Convert.ToInt32(val);
                case DataType.RandomDec:
                    return Convert.ToDecimal(val);
            }

            throw new ArgumentOutOfRangeException(nameof(dataType) + " not implemented for UseValue", dataType, null);
        }

        class HazSqlGeo
        {
            //public int Id { get; set; }
            public SdoGeometry Geo { get; set; }
            public int[] Point { get; set; }
        }

        private static Random rnd = new Random();
        private static int o;

        private dynamic ParseMinMaxValue(
            ColumnConfig columnConfig,
            MinMax minMax,
            dynamic defaultValue = null)
        {
            string unparsedValue = minMax == MinMax.Max ? columnConfig.Max : columnConfig.Min;
            if (string.IsNullOrEmpty(unparsedValue))
            {
                return defaultValue;
            }

            switch (columnConfig.Type)
            {
                case DataType.Rant:
                case DataType.Lorem:
                    return int.Parse(unparsedValue);
                case DataType.RandomYear:
                    return DateTime.TryParseExact(unparsedValue, "yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateTime) ? dateTime : defaultValue;
                case DataType.DateOfBirth:
                    return DateTime.Parse(unparsedValue);
            }

            throw new ArgumentOutOfRangeException(nameof(columnConfig.Type), columnConfig.Type, null);
        }
        public object MathFuction(ColumnConfig columnConfig, string columnA, string columnB, object operator1)
        {
            return null;
        }

        public object GetValueShuffle(ColumnConfig columnConfig, string schema, string table, string column, IDataSource dataSources, IEnumerable<IDictionary<string,object>> dataTable,
            object existingValue, Name.Gender? gender = null)
        {
            if (!string.IsNullOrEmpty(columnConfig.UseValue))
            {
                return ConvertValue(columnConfig.Type, columnConfig.UseValue);
            }
            if (columnConfig.RetainNullValues &&
               existingValue == null)
            {
                return null;
            }
            else if (columnConfig.Type == DataType.Shufflegeometry)
            {
                var exist = (SdoGeometry)existingValue;
                var obj = new HazSqlGeo();

                if (obj.Geo.Point != null)
                {
                    obj.Geo.Point.X = exist.Point.Y; obj.Geo.Point.Y = exist.Point.Z; obj.Geo.Point.Z = exist.Point.X;
                }
                else { obj.Geo.Point = exist.Point; }


                if (obj.Geo.OrdinatesArray != null)
                {
                    obj.Geo.OrdinatesArray = _randomizer.Shuffle(exist.OrdinatesArray).ToArray();
                }
                else { obj.Geo.OrdinatesArray = exist.OrdinatesArray; }

                if (obj.Geo.ElemArray != null)
                {
                    obj.Geo.ElemArray = _randomizer.Shuffle(exist.ElemArray).ToArray();
                }
                else { obj.Geo.ElemArray = exist.ElemArray; }
                while (obj.Geo.Equals(existingValue))
                {
                    obj.Geo.OrdinatesArray = _randomizer.Shuffle(exist.OrdinatesArray).ToArray();


                }

                return obj.Geo;
            }
            else
            {
                switch (columnConfig.Type)
                {
                    case DataType.Shuffle:
                        var random = new Random();
                        var shuffle = ShuffleData(table, column, existingValue, columnConfig.RetainNullValues, dataTable);
                        return shuffle;
                    case DataType.ShufflePolygon:
                        var rand = new Random();
                        var shufflePoly = dataSources.Shuffle(schema, table, column, existingValue, columnConfig.RetainNullValues, dataTable);
                        return shufflePoly;
                }
            }
            throw new ArgumentOutOfRangeException(nameof(columnConfig.Type), columnConfig.Type, null);
        }

        public static object ShuffleData(string table, string column, object existingValue, bool RetainNull, IEnumerable<IDictionary<string,object>> dataTable)
        {
            try
            {

                CompareLogic compareLogic = new CompareLogic();
                if (RetainNull)
                {
                    Values = dataTable.Select(n => n.Values).SelectMany(x => x).ToList().Where(n => n != null).Distinct().ToArray();
                }
                else
                    Values = dataTable.Select(n => n.Values).SelectMany(x => x).ToList().Distinct().ToArray();


                //var find = values.Count();
                object value = Values[rnd.Next(Values.Count())];
                if (Values.Count() <= 1)
                {
                    o = o + 1;
                    if (o == 1)
                    {
                        File.WriteAllText(_exceptionpath, "");
                    }

                    if (!exceptionBuilder.ContainsKey(table))
                    {
                        if (!exceptionBuilder.ContainsValue(column))
                        {
                            exceptionBuilder.Add(table, column);
                            File.AppendAllText(_exceptionpath, "Cannot generate unique shuffle value" + " on table " + table + " for column " + column + Environment.NewLine + Environment.NewLine);

                        }
                    }
                    //o = o + 1;
                    //File.AppendAllText(_exceptionpath, "Cannot generate unique shuffle value" + " on table " + table + " for column " + column + Environment.NewLine + Environment.NewLine);
                    return value;
                }
                if (compareLogic.Compare(value, null).AreEqual && RetainNull)
                {
                    return Values.Where(n => n != null).Select(n => n).ToArray()[rnd.Next(0, Values.Where(n => n != null).ToArray().Count())];
                }
                while (compareLogic.Compare(value, existingValue).AreEqual)
                {

                    value = Values[rnd.Next(0, Values.Count())];
                }
                if (value is SdoGeometry)
                {
                    return (SdoGeometry)value;
                }
                return value;
            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.ToString());
                File.AppendAllText(_exceptionpath, ex.ToString() + Environment.NewLine);
                return null;
            }


        }
        public object GetBlobValue(ColumnConfig columnConfig, IDataSource dataSource, object existingValue,
            string filename, FileTypes fileExtension, string blobLocation, Name.Gender? gender = null)
        {
            if (columnConfig.RetainNullValues &&
               existingValue == null)
            {
                return null;
            }
            var fileName = filename.ReplaceInvalidChars();
            switch (columnConfig.Type)
            {
                case DataType.Blob:
                    IFileType fileType = new FileType();
                    switch (fileExtension)
                    {
                        case FileTypes.PDF:
                            //generate pdf
                            //@"output\" + confi + @"\BinaryFiles\" + tableConfig.Name
                            fileName = fileType.GeneratePDF("\\"+blobLocation + fileName, "").ToString();
                            byte[] byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            //delete the file from location
                           // File.Delete(fileName);
                            return byteArray;
                        case FileTypes.TXT:
                            fileName = fileType.GenerateTXT(Environment.CurrentDirectory + "\\" +  blobLocation + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                           // File.Delete(fileName);
                            return byteArray;

                        case FileTypes.DOCX:
                            fileName = fileType.GenerateDOCX(Environment.CurrentDirectory + "\\" + blobLocation + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            //File.Delete(fileName);
                            return byteArray;
                        // return fileType.GenerateDOCX(@"\", "");
                        case FileTypes.DOC:
                            fileName = fileType.GenerateDOCX(Environment.CurrentDirectory + "\\" + blobLocation + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            //File.Delete(fileName);
                            return byteArray;
                        case FileTypes.RTF:
                            fileName = fileType.GenerateRTF(Environment.CurrentDirectory + "\\" + blobLocation + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            //File.Delete(fileName);
                            return byteArray;
                        //return fileType.GenerateRTF(@"\", "");
                        case FileTypes.JPG:
                            var fileUrl = _faker.Image.PicsumUrl();
                            string someUrl = fileUrl;
                            //check if URL is valid
                            var _validUrl = IsValidUri(new Uri(someUrl));
                            if (_validUrl)
                            {
                                using (var WebClient = new WebClient())
                                {

                                    byte[] imageBytes = WebClient.DownloadData(someUrl);
                                    File.WriteAllBytes(Environment.CurrentDirectory + "\\" + blobLocation + fileName, imageBytes);
                                    return imageBytes;
                                    
                                }
                            }
                            else
                            {
                                fileName = fileType.GenerateJPEG(Environment.CurrentDirectory + "\\" + blobLocation + fileName, "").ToString();
                                byteArray = null;

                                using (FileStream fs = new FileStream
                                    (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                                {

                                    byteArray = new byte[fs.Length];

                                    int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                                }
                               // File.Delete(fileName);
                                return byteArray;
                            }
                        case FileTypes.PNG:
                            fileUrl = _faker.Image.PicsumUrl();
                            someUrl = fileUrl;
                            //check if URL is valid
                            _validUrl = IsValidUri(new Uri(someUrl));
                            if (_validUrl)
                            {
                                using (var WebClient = new WebClient())
                                {

                                    byte[] imageBytes = WebClient.DownloadData(someUrl);
                                    File.WriteAllBytes(Environment.CurrentDirectory + "\\" + blobLocation + fileName, imageBytes);
                                    return imageBytes;

                                }
                            }
                            else
                            {
                                fileName = fileType.GenerateJPEG(Environment.CurrentDirectory + "\\" + blobLocation + fileName, "").ToString();
                                byteArray = null;

                                using (FileStream fs = new FileStream
                                    (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                                {

                                    byteArray = new byte[fs.Length];

                                    int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                                }
                                // File.Delete(fileName);
                                return byteArray;
                            }
                        case FileTypes.MSG:
                            //generate pdf
                            fileName = fileType.GenerateMSG("\\" +  blobLocation + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            //File.Delete(fileName);
                            return byteArray;
                        //return fileType.GenerateMSG(@"\", "");
                        case FileTypes.HTM:
                            fileName = fileType.GenerateHTML(Environment.CurrentDirectory + "\\" + blobLocation + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            //File.Delete(fileName);
                            return byteArray;
                        // return fileType.GenerateHTML(@"\" "");
                        case FileTypes.TIF:
                            fileName = fileType.GenerateTIF(Environment.CurrentDirectory + "\\" + blobLocation + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            //File.Delete(fileName);
                            return byteArray;
                        case FileTypes.HTML:
                            fileName = fileType.GenerateHTML(Environment.CurrentDirectory + "\\" + blobLocation + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            //File.Delete(fileName);
                            return byteArray;
                        case FileTypes.TIFF:
                            fileName = fileType.GenerateTIF(Environment.CurrentDirectory + "\\" + blobLocation + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            //File.Delete(fileName);
                            return byteArray;
                        case FileTypes.XLSX:
                            fileName = fileType.GenerateXLSX(Environment.CurrentDirectory + "\\" + blobLocation + fileName, columnConfig.Name).ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            //File.Delete(fileName);
                            return byteArray;
                        default:
                            {
                                fileName = fileType.GenerateRandom(Environment.CurrentDirectory + "\\" + blobLocation + fileName).ToString();
                                byteArray = null;

                                using (FileStream fs = new FileStream
                                    (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                                {

                                    byteArray = new byte[fs.Length];

                                    int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                                }
                                //File.Delete(fileName);
                                return byteArray;
                                //return fileType.GenerateRandom(@"\");
                                //break;
                            }
                    }
                case DataType.Filename:
                       return _faker.System.FileName(fileExtension.ToString());
            }
            throw new ArgumentOutOfRangeException(nameof(columnConfig.Type), columnConfig.Type, "not implemented");
        }

        private enum MinMax
        {
            Min = 0,

            Max = 1
        }
        private enum Vehicles
        {
            Manufacturer,
            Model,
            Type,
            Vin,
            Fuel,
            None
        }
        public static int ToInt32(object value)
        {
            if (null == value)
                return 0;

            try
            {
                return Convert.ToInt32(value, CultureInfo.InvariantCulture);
            }
            catch (FormatException)
            {
                return 0;
            }
        }
      
        public static decimal ToDecimal(object value)
        {
            if (null == value)
                return 0.00m;

            try
            {
                return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            }
            catch (FormatException)
            {
                return 0.00m;
            }
        }
        //private enum FileTypes{
        //    PDF,
        //    XLSX,
        //    DOC,
        //    DOCX,
        //    TIFF,
        //    TIF,
        //    HTML,
        //    HTM,
        //    JPG,
        //    JPEG,
        //    TXT,
        //    MSG,
        //    RTF,
        //    PNG,
        //    None
        //}

        public bool IsValidUri(Uri uri)
        {

            using (HttpClient Client = new HttpClient())
            {
                try
                {
                    HttpResponseMessage result = Client.GetAsync(uri).Result;
                    HttpStatusCode StatusCode = result.StatusCode;

                    switch (StatusCode)
                    {

                        case HttpStatusCode.Accepted:
                            return true;
                        case HttpStatusCode.OK:
                            return true;
                        default:
                            return false;
                    }
                }
                catch (Exception)
                {

                    return false;
                }
               
            }
        }

        public object MathOperation(ColumnConfig columnConfig, object existingValue, object[] source, string operation, int factor)
        {
            double _value = 0;
            if (columnConfig.RetainNullValues &&
              existingValue == null)
            {
                return null;
            }
            if (columnConfig.RetainEmptyStringValues &&
          (existingValue is string && string.IsNullOrWhiteSpace((string)existingValue)))
            {
                return existingValue;
            }
            switch (columnConfig.Type)
            {
                case DataType.math:
                    switch (ToEnum(operation, Operation.None))
                    {
                        case Operation.addition:
                            return source.Sum(n=>Convert.ToDouble(n));
                        case Operation.substraction:
                            for (int i = 0; i < columnConfig.StringFormatPattern.Split(',').Count(); i++)
                            {
                                _value -= Convert.ToDouble(source[i]);
                            }
                            return _value;
                        case Operation.percentage:
                            _value = factor / 100 * (Convert.ToDouble(source[0]));
                            return _value;
                        case Operation.randomPercentage:
                            Random random = new Random();
                            _value = (double)random.Next(factor,100) / 100 * (Convert.ToDouble(source[0]));
                            return _value;
                        case Operation.avarage:
                            return source.Sum(n=>Convert.ToDouble(n))/source.Count();
                        case Operation.division:
                            var k =  Convert.ToDouble(source.FirstOrDefault()) / factor;
                            return k;
                        default:
                            break;
                    }
                    throw new ArgumentOutOfRangeException(nameof(columnConfig.Type), columnConfig.Type, "Invalid Math Operation");
                    //return null;
            }
            throw new ArgumentOutOfRangeException(nameof(columnConfig.Type), columnConfig.Type, null);
        }

        public object GetAddress(ColumnConfig columnConfig, object existingValue, DataTable dataTable, bool isFulladdress)
        {
            var loader = CountryLoader.LoadCanadaLocationData();
            var address = "";
            if (columnConfig.RetainNullValues &&
              existingValue == null)
            {
                return null;
            }
            switch (ToEnum(columnConfig.UseGenderColumn, CountryLoad.None))
            {
                case CountryLoad.Canada:
                    loader = CountryLoader.LoadCanadaLocationData();
                    break;         
                case CountryLoad.UnitedStates:
                    loader = CountryLoader.LoadUnitedStatesLocationData();
                    break;
                case CountryLoad.Australia:
                    loader = CountryLoader.LoadAustraliaLocationData();
                    break;
                case CountryLoad.France:
                    loader = CountryLoader.LoadFranceLocationData();
                    break;
                default:
                    break;
            }
            var states = loader.States.Where(n => n.Provinces.Count > 1 && n.Name != null && !n.Name.ToString().Equals(existingValue.ToString())).Select(n => n).ToArray();
            var provinces = states[rnd.Next(0, states.Count())].Provinces.Where(n => n.Name != null && !n.Name.ToString().Equals(existingValue.ToString())).Where(x => !x.Name.ToString().Equals(existingValue)).Select(n => n).ToArray();
            var city = provinces[rnd.Next(0, provinces.Count())];
            if (isFulladdress)
            {
                if (columnConfig.Name.ToUpper().Contains("ADDRESS"))
                {

                    if (columnConfig.Name.ToUpper().Contains("STREET") && (columnConfig.Name.ToUpper().Contains("2") || columnConfig.Name.ToUpper().Contains("3")))
                    {
                        address = _faker.Address.SecondaryAddress();
                    }
                    else
                        address = _faker.Parse("{{ADDRESS.BUILDINGNUMBER}} {{ADDRESS.STREETNAME}}");
                }
                else
                    address = _faker.Parse("{{ADDRESS.BUILDINGNUMBER}} {{ADDRESS.STREETNAME}}");
            }
            else
            {
                if (columnConfig.Name.ToUpper().Contains("ADDRESS"))
                {

                    if (columnConfig.Name.ToUpper().Contains("STREET") && (columnConfig.Name.ToUpper().Contains("2") || columnConfig.Name.ToUpper().Contains("3")))
                    {
                        address = _faker.Address.SecondaryAddress();
                    }
                    else
                        address = _faker.Parse("{{ADDRESS.BUILDINGNUMBER}} {{ADDRESS.STREETNAME}}");                    
                }
                else
                    address = _faker.Parse("{{ADDRESS.BUILDINGNUMBER}} {{ADDRESS.STREETNAME}}") + " " + city.Name + ", " + city.State.Name;
            }
            dataTable.Rows.Add(CountryLoader.LoadCanadaLocationData().Name, city.State.Name, city.State.Name, city.Name, address);      
            return dataTable;
        }

        public enum Operation
        {
            addition,
            avarage,
            substraction,
            multiplication,
            division,
            percentage,
            randomPercentage,
            None
        }
        public enum CountryLoad
        {
            Canada,
            UnitedStates,
            UnitedKingdom,
            Australia,
            France,
            None
        }
    }
    public static class RandomExtensions
    {
        // Return a random value between 0 inclusive and max exclusive.
        public static double NextDouble(this Random rand, double max)
        {
            return rand.NextDouble() * max;
        }

        // Return a random value between min inclusive and max exclusive.
        public static double NextDouble(this Random rand,
            double min, double max)
        {
            return min + (rand.NextDouble() * (max - min));
        }
        public static bool NextBool(this Random r, int truePercentage = 50)
        {
            return r.NextDouble() < truePercentage / 100.0;
        }
    }
}
