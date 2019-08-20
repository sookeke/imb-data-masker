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

namespace DataMasker
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="IDataGenerator"/>
    public class DataGenerator : IDataGenerator
    {
        private static readonly DateTime DEFAULT_MIN_DATE = new DateTime(1900, 1, 1, 0, 0, 0, 0);

        private static readonly DateTime DEFAULT_MAX_DATE = DateTime.Now;
        private readonly IDataSource dataSources1;
        private static List<string> shuffleList = new List<string>();

        private const int DEFAULT_LOREM_MIN = 5;

        private const int DEFAULT_LOREM_MAX = 30;

        private const int DEFAULT_RANT_MAX = 25;
        
       

        /// <summary>
        /// The data generation configuration
        /// </summary>
        private readonly DataGenerationConfig _dataGenerationConfig;
        //private static DataSourceProvider datasource;
     

        /// <summary>
        /// The faker
        /// </summary>
        private readonly Faker _faker;
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
            Name.Gender? gender)
        {
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

            if (existingValue == null)
            {
                return GetValue(columnConfig, gender);
            }

            if (HasValueMapping(columnConfig, existingValue))
            {
                return GetValueMapping(columnConfig, existingValue);
            }


            object newValue = GetValue(columnConfig, gender);
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

        private object GetValue(
            ColumnConfig columnConfig,
            Name.Gender? gender = null)
        {
            //global::Dapper.SqlMapper.AddTypeHandler(typeof(DbGeography), new GeographyMapper());
            switch (columnConfig.Type)
            {
                case DataType.FirstName:
                    return _faker.Name.FirstName(gender);
                case DataType.LastName:
                    return _faker.Name.LastName(gender);
                case DataType.DateOfBirth:
                    return _faker.Date.Between(
                        ParseMinMaxValue(columnConfig, MinMax.Min, DEFAULT_MIN_DATE),
                        ParseMinMaxValue(columnConfig, MinMax.Max, DEFAULT_MAX_DATE));
                case DataType.Rant:
                    var rant = _faker.Rant.Review(columnConfig.StringFormatPattern);
                    int lenght = rant.Length;
                    
                    if (!string.IsNullOrEmpty(columnConfig.Max) && rant.Length> Convert.ToInt32(columnConfig.Max))
                    {
                       var rantSub = rant.Substring(0, Convert.ToInt32(columnConfig.Max));
                       return rantSub;
                    }
                    //var rant = _faker.Rant.Reviews(lines: ParseMinMaxValue(columnConfig, MinMax.Max, DEFAULT_RANT_MAX))[0];

                    return rant; // return _faker.Rant.Review(columnConfig.StringFormatPattern);
                case DataType.Lorem:
                    return _faker.Lorem.Sentence(Convert.ToInt32(columnConfig.Min),Convert.ToInt32(columnConfig.Max))
                        ;
                case DataType.StringFormat:
                    return _randomizer.Replace(columnConfig.StringFormatPattern);
                case DataType.FullAddress:
                    return _faker.Address.FullAddress();
                case DataType.File:
                    return _faker.System.FileName(columnConfig.StringFormatPattern);
                case DataType.Filename:
                    var file = _faker.System.FileName("");
                    return file.Remove(file.Length - 1);
                case DataType.Blob:
                   
                    var fileUrl = _faker.Image.LoremPixelUrl();
                    //_faker.System.
                    //FileStream stream = new FileStream(fileUrl, FileMode.Open, FileAccess.Read);
                    ///BinaryReader reader = new BinaryReader(stream);
                    string someUrl = fileUrl;
                    using (var WebClient = new WebClient())
                    {

                        byte[] imageBytes = WebClient.DownloadData(someUrl);
                        return imageBytes;
                    }
                case DataType.Clob:
                    var randomString = _faker.Lorem.Text();
                    byte[] newvalue = System.Text.Encoding.Unicode.GetBytes(randomString);
                    var bs64 = System.Convert.ToBase64String(newvalue);
                    //var base64EncodedBytes = System.Convert.FromBase64String(newvalue);
                    return bs64;
                case DataType.Ignore:
                    return null;
                case DataType.RandomYear:
                    DateTime start = new DateTime(1999, 1, 1);
                    Random gen = new Random();
                    int range = ((TimeSpan)(DateTime.Today - start)).Days;
                    var randomYear = start.AddDays(gen.Next(range)).ToString("yyyy");
                    return randomYear;
                case DataType.RandomSeason:
                    DateTime _start = new DateTime(1995, 1, 1);
                    Random _genD = new Random();
                    int _range = ((TimeSpan)(DateTime.Today - _start)).Days;
                    var _randomYear = _start.AddDays(_genD.Next(_range));
                    var season = _randomYear.Year + "/" + _randomYear.AddYears(1).ToString("yy");
                    return season;
                case DataType.Geometry:
                    //generate 20 SIZE LINESTRING random polygon coordinate with same precision model using GEOAPI and NETOPOLOGY SUITE
                    Random random = new Random();
                    SdoGeometry sdoGeometry = new SdoGeometry();
                    
                    var SDO_GTYPElist = new List<string> { "2003", "2006", "2002" };
                    int index = random.Next(SDO_GTYPElist.Count);
                    string SDO_GTYPE = SDO_GTYPElist[index];
                    var gf = new GeometryFactory(new PrecisionModel(), 3857);
                    //Identify the centre of the polygon
                    Coordinate center = new Coordinate((random.NextDouble() * 360) - 180, (random.NextDouble() * 180) - 90);
                    //decimal center1 = new decimal((random.NextDouble() * 360) - 180, (random.NextDouble() * 180) - 90);
                    Coordinate[] coords = new Coordinate[20];
                    decimal[] cood = new decimal[20];
                    for (int i = 0; i < 20; i++)
                    {
                        coords[i] = new Coordinate(center.X + random.NextDouble(-4291402.04717672, 16144349.4032217), center.Y + random.NextDouble(-4291402.04717672, 16144349.4032217));
                        cood[i] = new decimal(random.NextDouble(-4291402.04717672, 16144349.4032217));
                    }
                    //creates a new polygon from the coordinate array
                    coords[19] = new Coordinate(coords[0].X, coords[0].Y);

                    var obj = new HazSqlGeo
                    {

                        Geo = new SdoGeometry()
                        {
                            ElemArray = new decimal[3] { 1, 2, 1 },
                            OrdinatesArray = cood,
                            Sdo_Gtype = Convert.ToInt32(_faker.PickRandom(SDO_GTYPElist)),
                            Sdo_Srid = Convert.ToDecimal(ConfigurationManager.AppSettings["SRID"])
                        }
                    };

                    var pCoordinate = gf.CreateMultiPointFromCoords(coords);
                    //Geo = new SdoGeometry();
                   
                    var xx = pCoordinate.ToString().Replace(",", string.Empty).Replace(" ", ",").Split(new string[] { "LINESTRING," }, StringSplitOptions.None)[1];
                    var sdo_geometry_command_text = "MDSYS.SDO_GEOMETRY(" + SDO_GTYPE + "," + 5255 + ",NULL,MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1),MDSYS.SDO_ORDINATE_ARRAY" + pCoordinate + ")";
                    var values = "ST_GeomFromText(" + pCoordinate +")";
                    //GeographyMapper geographyMapper = new GeographyMapper();
                    SqlMapper.AddTypeHandler(new GeographyMapper());
                    //geographyMapper.SetValue("@GEO", obj.Geo);

                    return obj.Geo;

                case DataType.RandomInt:
                    var min = columnConfig.Min;
                    var max = columnConfig.Max;
                    if (min.Contains(".") || max.Contains("."))
                    {
                        //return decimal
                        return Math.Round(_faker.Random.Decimal(Convert.ToDecimal(min), Convert.ToDecimal(max)), 2);
                    }
                    return _faker.Random.Int(Convert.ToInt32(min),Convert.ToInt32(max));
                //return _faker.Random.Int(Convert.ToInt32(columnConfig.Min), Convert.ToInt32(columnConfig.Max));
                case DataType.CompanyPersonName:
                    var _compName = new Faker().Company.CompanyName();
                    var _personName = _faker.Person.FullName;
                    string[] _array = new string[] { _compName,_personName};

                    return _faker.PickRandom(_array);
                case DataType.PostalCode:
                    //var xx = _xeger.Generate().ToUpper();
                    return _xeger.Generate().ToUpper().Replace(" ",string.Empty);
                case DataType.Company:
                    var company = new Faker();
                    var _genCompany = company.Company.CompanyName(columnConfig.StringFormatPattern); ;
                    if (!string.IsNullOrEmpty(columnConfig.Max) && _genCompany.Length > Convert.ToInt32(columnConfig.Max))
                    {
                        var _shortComp = _genCompany.Substring(0, Convert.ToInt32(columnConfig.Max));
                        return _shortComp;
                    }
                    return _genCompany;
                case DataType.RandomString2:
                    var rand = _faker.Random.String2(Convert.ToInt32(columnConfig.Max), columnConfig.StringFormatPattern);
                    return _faker.Random.String2(Convert.ToInt32(columnConfig.Max), columnConfig.StringFormatPattern);
                case DataType.PhoneNumber:
                    var _number = _faker.Phone.PhoneNumber(columnConfig.StringFormatPattern);
                    if (!string.IsNullOrEmpty(columnConfig.Max) && _number.Length > Convert.ToInt32(columnConfig.Max))
                    {
                        var _shortnum = _number.Substring(0, Convert.ToInt32(columnConfig.Max));
                        return _shortnum;
                    }
                    return _number;
                case DataType.exception:
                    var fileexception = _faker.System.FileName("");
                    return fileexception.Remove(fileexception.Length - 1);
                case DataType.PhoneNumberInt:
                    var _phone = Convert.ToInt64(_faker.Phone.PhoneNumber(columnConfig.StringFormatPattern));
                    var numeric = "TONUMERIC(" + _phone + ")";
                    return _phone;
                case DataType.RandomDec:
                    var value = _faker.Random.Decimal(Convert.ToInt32(columnConfig.Min), Convert.ToInt32(columnConfig.Max));
                    return value;
                case DataType.PickRandom:
                    var stringarray = columnConfig.StringFormatPattern.Split(',');
                    return _faker.PickRandom(stringarray);
                case DataType.RandomHexa:
                    return _faker.Random.Hexadecimal(Convert.ToInt32(columnConfig.StringFormatPattern));
                case DataType.Bogus:
                    var _gen = _faker.Parse(columnConfig.StringFormatPattern);
                    if (!string.IsNullOrEmpty(columnConfig.Max) && _gen.Length > Convert.ToInt32(columnConfig.Max))
                    {
                        var _short = _gen.Substring(0, Convert.ToInt32(columnConfig.Max));
                        return _short;
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
            switch (dataType)
            {
                case DataType.FirstName:
                case DataType.LastName:
                case DataType.Rant:
                case DataType.Lorem:
                case DataType.StringFormat:
                case DataType.FullAddress:
                case DataType.PhoneNumber:
                case DataType.None:
                    return val;
                case DataType.DateOfBirth:
                    return DateTime.Parse(val);
            }

            throw new ArgumentOutOfRangeException(nameof(dataType), dataType, null);
        }

        class HazSqlGeo
        {
            //public int Id { get; set; }
            public SdoGeometry Geo { get; set; }
        }
        

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

                case DataType.DateOfBirth:
                    return DateTime.Parse(unparsedValue);
            }

            throw new ArgumentOutOfRangeException(nameof(columnConfig.Type), columnConfig.Type, null);
        }

        public object GetValueShuffle(ColumnConfig columnConfig , string table, string column,IDataSource dataSources, DataTable dataTable,
            object existingValue, Name.Gender? gender = null)
        {
            if (columnConfig.RetainNullValues &&
               existingValue == null)
            {
                return null;
            }
            else if (columnConfig.Type == DataType.Shufflegeometry)
            {
                var exist = (SdoGeometry)existingValue;
                var obj = new HazSqlGeo
                {

                    Geo = new SdoGeometry()
                    {
                        ElemArray = exist.ElemArray,
                        OrdinatesArray = _randomizer.Shuffle(exist.OrdinatesArray).ToArray(),
                        Sdo_Gtype = exist.Sdo_Gtype,
                        Sdo_Srid = exist.Sdo_Srid,
                        Point = exist.Point
                    }
                };
              
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
                        var shuffle = dataSources.shuffle(table, column, existingValue,dataTable);
                        return shuffle;
                }
            }
            throw new ArgumentOutOfRangeException(nameof(columnConfig.Type), columnConfig.Type, null);
        }

        public object GetBlobValue(ColumnConfig columnConfig, IDataSource dataSource, object existingValue,
            string fileName, string fileExtension, Name.Gender? gender = null )
        {
            if (columnConfig.RetainNullValues &&
               existingValue == null)
            {
                return null;
            }
            switch (columnConfig.Type)
            {
                case DataType.Blob:
                    IFileType fileType =  new FileType();
                    //fileType.GenerateDOCX("", "");
                    switch (fileExtension.ToUpper())
                    {
                        case "PDF":
                            //generate pdf
                           fileName =  fileType.GeneratePDF(@"\output\"+ fileName, "").ToString();
                            byte[] byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            //delete the file from location
                            File.Delete(fileName);
                            return byteArray;
                        case "TXT":
                            fileName = fileType.GenerateTXT(Environment.CurrentDirectory + @"\output\" + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            File.Delete(fileName);
                            return byteArray;
                          
                        case "DOCX":
                            fileName = fileType.GenerateDOCX(Environment.CurrentDirectory + @"\output\" + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            File.Delete(fileName);
                            return byteArray;
                        // return fileType.GenerateDOCX(@"\", "");
                        case "DOC":
                            fileName = fileType.GenerateDOCX(Environment.CurrentDirectory + @"\output\" + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            File.Delete(fileName);
                            return byteArray;
                        case "RTF":
                            fileName = fileType.GenerateRTF(Environment.CurrentDirectory + @"\output\" + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            File.Delete(fileName);
                            return byteArray;
                            //return fileType.GenerateRTF(@"\", "");
                        case "JPG":
                            var fileUrl = _faker.Image.LoremPixelUrl();
                            string someUrl = fileUrl;
                            using (var WebClient = new WebClient())
                            {

                                byte[] imageBytes = WebClient.DownloadData(someUrl);
                                return imageBytes;
                            }
                        case "MSG":
                            //generate pdf
                            fileName = fileType.GenerateMSG(@"\output\" + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            File.Delete(fileName);
                            return byteArray;
                            //return fileType.GenerateMSG(@"\", "");
                        case "HTM":
                            fileName = fileType.GenerateHTML(Environment.CurrentDirectory + @"\output\" + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            File.Delete(fileName);
                            return byteArray;
                           // return fileType.GenerateHTML(@"\", "");
                        case "TIF":
                            fileName = fileType.GenerateTIF(Environment.CurrentDirectory + @"\output\" + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            File.Delete(fileName);
                            return byteArray;
                        case "HTML":
                            fileName = fileType.GenerateHTML(Environment.CurrentDirectory + @"\output\" + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            File.Delete(fileName);
                            return byteArray;
                        case "TIFF":
                            fileName = fileType.GenerateTIF(Environment.CurrentDirectory + @"\output\" + fileName, "").ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            File.Delete(fileName);
                            return byteArray;
                        case "XLSX":
                            
                            fileName = fileType.GenerateXLSX(Environment.CurrentDirectory + @"\output\" + fileName, string.Join(" ", _faker.Rant.Reviews(" ", 10).ToArray())).ToString();
                            byteArray = null;

                            using (FileStream fs = new FileStream
                                (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {

                                byteArray = new byte[fs.Length];

                                int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                            }
                            File.Delete(fileName);
                            return byteArray;
                        default:
                            {
                                fileName = fileType.GenerateRandom(Environment.CurrentDirectory + @"\output\" + fileName).ToString();
                                byteArray = null;

                                using (FileStream fs = new FileStream
                                    (fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                                {

                                    byteArray = new byte[fs.Length];

                                    int iBytesRead = fs.Read(byteArray, 0, (int)fs.Length);
                                }
                                File.Delete(fileName);
                                return byteArray;
                                //return fileType.GenerateRandom(@"\");
                                //break;
                            }
                    }
                    

                    //return fileType.GenerateRandom(@"\");
                case DataType.Filename:
                    //var filename3 = _faker.System.FileName(fileExtension);
                   

                    return _faker.System.FileName(fileExtension);
            }
                throw new ArgumentOutOfRangeException(nameof(columnConfig.Type), columnConfig.Type,"not implemented");
        }

        private enum MinMax
        {
            Min = 0,

            Max = 1
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
