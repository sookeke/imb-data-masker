using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using DataMasker.Interfaces;
using DataMasker.Models;
using DataMasker.Runner;
using Newtonsoft.Json;
using Newtonsoft.Json.Schema;
using Newtonsoft.Json.Schema.Generation;
using Newtonsoft.Json.Serialization;
using System.ComponentModel;
using OfficeOpenXml;
using Newtonsoft.Json.Linq;
using DataMasker.DataLang;
using DataMasker.MaskingValidation;
using KellermanSoftware.CompareNetObjects;
using System.Diagnostics;
using System.Reflection;
using static DataMasker.PrintDataExtensions;
using System.Security.Principal;
using SharpCompress.Archives.Rar;
using SharpCompress.Common;
using SharpCompress.Archives;
using System.Net;
using System.Globalization;
using ChoETL;

/*
    Author: Stanley Okeke
    Company: MOTI IMB
    Title: Senior Technical Analyst IMB
    Version copies : SVN
 * */

namespace DataMasker.Examples
{
    internal class Program
    {
        #region system declarations
        #region read-only and const app config
        private static readonly string _exceptionpath = Directory.GetCurrentDirectory() + $@"\output\MaskExceptions.txt";
        private static readonly string path = Directory.GetCurrentDirectory() + $@"\output\Validation\ValidationResult.txt";
        private static string copyjsonPath;
        private static readonly string fromEmail = ConfigurationManager.AppSettings["fromEmail"];
        private static readonly string Recipients = ConfigurationManager.AppSettings["RecipientEmail"];
        private static readonly string sheetPath = ConfigurationManager.AppSettings["ExcelSheetPath"];
        private static readonly string TSchema = ConfigurationManager.AppSettings["APP_NAME"];
        private static readonly string Stype = ConfigurationManager.AppSettings["DataSourceType"];
        private static readonly string _testJson = ConfigurationManager.AppSettings["TestJson"];
        private const string ExcelSheetPath = "ExcelSheetPath"; private const string DatabaseName = "DatabaseName"; private const string WriteDML = "WriteDML";
        private const string MaskTabletoSpreadsheet = "MaskTabletoSpreadsheet"; private const string Schema = "APP_NAME"; private const string ConnectionString = "ConnectionString"; private const string ConnectionStringPrd = "ConnectionStringPrd";
        private const string MaskedCopyDatabase = "MaskedCopyDatabase"; private const string RunValidation = "RunValidation"; private const string EmailValidation = "EmailValidation"; private const string jsonMapPath = "jsonMapPath"; private const string RunValidationONLY = "RunValidationONLY";
        private const string SourceType = "DataSourceType";
        private const string RunTestJson = "RunTestJson"; private const string TestJson = "TestJson";
        private const string AutoUpdate = "AutoUpdate"; private const string CurrentVersionURL = "CurrentVersionURL"; private const string CurrentInstallerURL = "CurrentInstallerURL";

        private static readonly string exceptionPath = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
        #endregion
        private static string _nameDatabase;
        private static string _SpreadSheetPath;
        private static DataTable PrdTable = null;
        private static DataTable _dmlTable = null;
        private static DataTable MaskTable = null;
        private static int count;
        private static DataTable report = new DataTable();  
        private static List<string> _colError = new List<string>();
        private static List<KeyValuePair<string, string>> collist = new List<KeyValuePair<string, string>>();
        private static List<KeyValuePair<string, Dictionary<string, string>>> jsconfigTable = new List<KeyValuePair<string, Dictionary<string, string>>>();
        private static List<KeyValuePair<string, Dictionary<string, string>>> copyJsTable = new List<KeyValuePair<string, Dictionary<string, string>>>();
        private static List<KeyValuePair<string, Dictionary<string, string>>> _allNull = new List<KeyValuePair<string, Dictionary<string, string>>>();
        private static int rowCount;
        private static bool isBinary;
        private static readonly Dictionary<ProgressType, ProgressbarUpdate> _progressBars = new Dictionary<ProgressType, ProgressbarUpdate>();
        private static readonly Dictionary<string, object> allkey = new Dictionary<string, object>();


        public static string Jsonpath { get; private set; }
        public static string CreateDir { get; private set; }
        public static string _columnMapping { get; private set; }
        public static string DownloadMSI { get; private set; }
        public static string InstallMsiPath { get; private set; }
        public static string MaskerVersion { get; private set; }
        #endregion
        public static bool IsAdministrator()
        {
            WindowsIdentity identity = WindowsIdentity.GetCurrent();
            WindowsPrincipal principal = new WindowsPrincipal(identity);
            return principal.IsInRole(WindowsBuiltInRole.Administrator);
        }
        public static bool ExtractMSI(string destination)
        {
            try
            {
                RarArchive archive = RarArchive.Open(DownloadMSI);
                foreach (var entry in archive.Entries.Where(entry => !entry.IsDirectory))
                {
                    entry.WriteToDirectory(destination, new ExtractionOptions()
                    {
                        ExtractFullPath = true,
                        Overwrite = true
                    });
                }
                if (File.Exists(destination + @"\DataMaskerInstaller\DataMaskerInstaller.msi"))
                {
                    InstallMsiPath = destination + @"\DataMaskerInstaller\DataMaskerInstaller.msi";
                    return true;
                }
            }
            catch (Exception ex)
            {

                throw;
            }
            return false;
        }
        public static bool Download(string urlPath, string path)
        {
            try
            {
                //var https = new HttpClient();
                WebClient Clients = new WebClient
                {
                    UseDefaultCredentials = true
                };
                Clients.DownloadFile(urlPath, path + @"\DataMaskerInstaller.rar");
                if (File.Exists(path + @"\DataMaskerInstaller.rar"))
                {
                    Clients.Dispose();
                    DownloadMSI = path + @"\DataMaskerInstaller.rar";
                    return true;
                }
            }
            catch (Exception ex)
            {

                throw;
            }

            return false;
        }
        public static bool Install(string sMSIPath)
        {
            try
            {
                Console.WriteLine("Installing latest version...");
                string configfile = @"C:\Program Files\IMB\DataMasker\DataMasker.Mask.exe.config";
                File.Copy(configfile, Path.Combine(Path.GetDirectoryName(configfile)
                    , Path.GetFileName(configfile) + ".bak"), true);
                Process process = new Process();
                process.StartInfo.Arguments = string.Format("/i \"{0}\" ALLUSERS=1 /qn", sMSIPath);
                process.StartInfo.FileName = "msiexec";
                //process.StartInfo = processInfo;
                process.Start();
                process.WaitForExit();
                return true;
            }
            catch (Exception ex)
            {

                Console.WriteLine("Failed to update: {0}", ex.Message);
            }
            return false;
        }
        public static bool CheckUpdate(string urlVersion, string urlFile)
        {
            try
            {


                WebClient Client = new WebClient
                {
                    UseDefaultCredentials = true
                };
                string versionString = Client.DownloadString(urlVersion);

                Version latestVersion = new Version(versionString);

                //get my own version to compare against latest.
                Assembly assembly = Assembly.GetExecutingAssembly();
                FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
                Version myVersion = new Version(fvi.ProductVersion);
                Client.Dispose();
                if (latestVersion > myVersion)
                {
                    Console.WriteLine(string.Format("You've got version {0} of DataMasker for Windows. Would you like to update to the latest version {1} [Yes/No]?", myVersion, latestVersion));
                    var key = Console.ReadLine();
                    if (key.ToUpper() == "YES" || key.ToUpper() == "Y")
                    {
                        string mPath = @"C:\DataMaskerInstaller";
                        if (!Directory.Exists(mPath))
                        {
                            Directory.CreateDirectory(mPath);
                        }
                        if (Download(urlFile, mPath))
                        {
                            if (ExtractMSI(mPath))
                            {
                                //install
                                if (Install(InstallMsiPath))
                                {
                                    // Restart and run as admin
                                    var exeName = Process.GetCurrentProcess().MainModule.FileName;
                                    ProcessStartInfo startInfo = new ProcessStartInfo(exeName)
                                    {
                                        Verb = "runas",
                                        Arguments = "restart"
                                    };
                                    Process.Start(startInfo);
                                    Environment.Exit(1);
                                }
                            }
                        }
                    }
                   
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Update failed with the following errors: {0}", ex.Message);
                return false;
            }
            return false;
        }
        private static void Main(
            string[] args)
        {
            //var exeName1 = Process.GetCurrentProcess().MainModule.FileName;
            Assembly assembly = Assembly.GetExecutingAssembly();
            FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
            Version myVersion = new Version(fvi.ProductVersion);
            MaskerVersion = myVersion.ToString();
            Console.Title = string.Format("Data Masker v{0}",MaskerVersion) ;
            if (!IsAdministrator())
            {
                // Restart and run as admin
                var exeName = Process.GetCurrentProcess().MainModule.FileName;
                ProcessStartInfo startInfo = new ProcessStartInfo(exeName)
                {
                    Verb = "runas",
                    Arguments = "restart"
                };
                Process.Start(startInfo);
                Environment.Exit(1);
            }
            if (!CheckAppConfig())
            {
                Console.WriteLine("Program will exit: Press ENTER to exist..");
                Console.ReadLine();
                System.Environment.Exit(1);
            }
            report.Columns.Add("Table"); report.Columns.Add("Schema"); report.Columns.Add("Column"); report.Columns.Add("Hostname"); report.Columns.Add("DataSourceType") ; report.Columns.Add("TimeStamp"); report.Columns.Add("Operator"); report.Columns.Add("Row count mask"); report.Columns.Add("Row count prd"); report.Columns.Add("Result"); report.Columns.Add("Result Comment");
            //check for update
            if ((bool)allkey.Where(n => n.Key.ToUpper().Equals(AutoUpdate.ToUpper())).Select(n => n.Value).FirstOrDefault())
            {
                CheckUpdate((string)allkey.Where(n => n.Key.ToUpper().Equals(CurrentVersionURL.ToUpper())).Select(n => n.Value).FirstOrDefault(),
                    (string)allkey.Where(n => n.Key.ToUpper().Equals(CurrentInstallerURL.ToUpper())).Select(n => n.Value).FirstOrDefault());
            }
            Run();
        }
        private static void JsonConfig(string json, string excelspreadsheet)
        {
            #region Initialize system variables


            //generate list of words for comments like columns
            List<string> _comment = new List<string> { "Description", "DESCRIPTION", "TEXT", "MEMO", "describing", "Descr", "COMMENT", "comment", "NOTE", "Comment", "REMARK", "remark", "DESC" };
            List<string> _fullName = new List<string> { "full name", "AGENT_NAME", "OFFICER_NAME", "FULL_NAME", "CONTACT_NAME", "MANAGER_NAME" };
            List<string> MaskingRules = new List<string>() { "No Masking required", "Replace Value with fake data","Shuffle", "Flagged" };
            List<string> maskingoutString = new List<string>() { "char", "nchar","string","varchar", "nvarchar", "binary", "varbinary" , "character varying" };
            List<string> ScrableList = new List<string>() { "char", "nchar", "string", "varchar", "nvarchar", "binary", "varbinary", "character varying", "numeric", "decimal", "double", "int","Number"};
            List<Table> tableList = new List<Table>();
            List<string> Vehicles = new List<string> { "Manufacturer", "Vin", "Model", "Type", "Fuel" };
            DataGeneration dataGeneration = new DataGeneration
            {
                locale = "en"
            };
            Config1 config1 = new Config1
            {
                connectionString = ConfigurationManager.AppSettings["ConnectionString"],
                Databasename = ConfigurationManager.AppSettings["DatabaseName"],
                connectionStringPrd = ConfigurationManager.AppSettings["ConnectionStringPrd"],
                Hostname = ConfigurationManager.AppSettings["Hostname"]

            };
            //config1.connectionString2 = "";
            DataSource dataSource = new DataSource
            {
                config = config1,
                type = ConfigurationManager.AppSettings["DataSourceType"]
            };
            #endregion

            #region Create root json objects
            var rootObj = JsonConvert.DeserializeObject<List<RootObject>>(File.ReadAllText(json));
            var oo = ExcelToJson.FromJson(File.ReadAllText(json));
            var query = from root in rootObj
                            //where root.__invalid_name__Masking_Rule.Contains("No masking")
                        group root by root.TableName into newGroup
                        orderby newGroup.Key
                        select newGroup;
            #endregion

            #region build and map column datatype with masked column datatype
            foreach (var nameGroup in query)
            {

                //var cc = nameGroup.Where(n => !(n.MaskingRule.Contains("No masking") || n.MaskingRule.Contains("Flagged"))).Count();

                //if (nameGroup.Where(n => !(n.MaskingRule.Contains("No masking") || n.MaskingRule.Contains("Flagged"))).Count() > 0)
                //{
                Table table = new Table();
                List<Column> colList = new List<Column>();
                table.name = nameGroup.Key;
                
               

                //table.primaryKeyColumn = nameGroup.Select
                foreach (var col in nameGroup)
                {

                    table.primaryKeyColumn = col.PKconstraintName.Split(',')[0];
                    table.Schema = col.Schema;
                    table.RowCount = col.RowCount;
                    bool o = col.RetainNull.ToUpper().Equals("TRUE") ? true : false;
                    bool prview = col.Preview.ToUpper().Equals("FALSE") ? false : true;
                    bool nullString = col.RetainEmptyString.ToUpper().Equals("FALSE") ? false : true;
                    Column column = new Column
                    {
                        name = col.ColumnName,
                        retainNullValues = o,
                        RetainEmptyStringValues = nullString,
                        StringFormatPattern = "",
                        UseValue = col.UseValue.NullIfEmpty(),
                        Preview = prview

                    };
                    var rule = col.MaskingRule;

                    if (col.MaskingRule.ToUpper().Contains("NO MASKING"))
                    {
                        column.type = DataType.NoMasking.ToString();
                        if (col.DataType.ToUpper().Equals("SDO_GEOMETRY") || col.DataType.ToUpper().ToUpper().Contains("GEOMETRY"))
                        {
                            column.type = DataType.Geometry.ToString();
                        }
                        column.ignore = true;
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }                   
                    else if (col.MaskingRule.ToUpper().Contains("SHUFFLE"))
                    {
                        column.type = DataType.Shuffle.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }                
                    else if (col.MaskingRule.ToUpper().Contains("MATH"))
                    {
                        if (maskingoutString.Any(n => col.DataType.ToUpper().Contains(n.ToUpper())))
                        {
                            throw new ArgumentException("Math Operation apply only on Numeric Datatype", col.DataType + " on " + col.ColumnName);
                        }
                        column.type = DataType.math.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min;
                        if (string.IsNullOrEmpty(col.StringFormat))
                        {
                            throw new ArgumentException("Math Operation requires a stringFormat", col.StringFormat + " on " + col.ColumnName);
                        }
                        column.StringFormatPattern = col.StringFormat;
                        column.useGenderColumn = "";
                        column.ignore = true;
                        column.Operator = col.RuleReasoning;
                    }
                    else if (!MaskingRules.Any(n => col.MaskingRule.ToUpper().Contains(n.ToUpper())))
                    {
                        switch (ToEnum(col.MaskingRule, DataType.Error))
                        {
                            case DataType.None:
                                column.type = DataType.None.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = col.StringFormat;
                                column.useGenderColumn = "";
                                break;
                            case DataType.Vehicle:
                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    throw new ArgumentException("Vehicle type requires a StringFormatPattern value", nameof(col.StringFormat) + " on " + col.ColumnName);

                                }
                                else if (!Vehicles.Any(n=>n == col.StringFormat))
                                {
                                    throw new ArgumentException("Invalid Vehicle StringFormatPattern value: " + col.StringFormat.AddDoubleQuotes(), nameof(col.StringFormat) + " on " + col.ColumnName);

                                }
                                column.type = DataType.Vehicle.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = col.StringFormat;
                                column.useGenderColumn = "";
                                break;
                            case DataType.CompanyPersonName:
                                column.type = DataType.CompanyPersonName.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = col.StringFormat;
                                column.useGenderColumn = "";
                                break;
                            case DataType.MaskingOut:
                                //datatype must be string 
                                if (!maskingoutString.Any(n=>col.DataType.ToUpper().Contains(n.ToUpper())))
                                {
                                    throw new ArgumentException("MaskingOut type apply only on String dataType", col.DataType + " on " + col.ColumnName);
                                }
                                column.type = DataType.MaskingOut.ToString();                             
                                column.min = col.Min;
                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    throw new ArgumentException("MaskingOut type requires a StringFormatPattern value", nameof(col.StringFormat) + " on " + col.ColumnName);

                                }
                                column.StringFormatPattern = col.StringFormat.Split('(').FirstOrDefault(); //StringFormatpattern = MaskingRight(4) , MaskingLeft(ChunkSize), MaskigMiddle(chunkSize)
                                column.max = string.Join("", col.StringFormat.Where(char.IsDigit));
                                column.useGenderColumn = "";
                                break;
                            case DataType.Scramble:
                                //datatype must be string 
                                if (!ScrableList.Any(n => col.DataType.ToUpper().Contains(n.ToUpper())))
                                {
                                    throw new ArgumentException("Scramble type apply only on Strings and Numeric Datatype", col.DataType + " on " + col.ColumnName);
                                }
                                column.type = DataType.Scramble.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.Company:
                                column.type = DataType.Company.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "{{COMPANY.COMPANYNAME}} {{COMPANY.COMPANYSUFFIX}}";
                                column.useGenderColumn = "";
                                break;
                            case DataType.NULL:
                                break;
                            case DataType.TimeSpan:
                                column.type = DataType.TimeSpan.ToString();
                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    throw new ArgumentException("StringFormat type requires a StringFormatPattern value", nameof(col.StringFormat) + " on " + col.ColumnName);
                                }
                                column.StringFormatPattern = col.StringFormat;
                                break;
                            case DataType.PostalCode:
                                column.type = DataType.PostalCode.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = col.StringFormat;
                                column.useGenderColumn = "";
                                break;
                            case DataType.StringConcat:
                                column.type = DataType.StringConcat.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    throw new ArgumentException("StringFormat type requires a StringFormatPattern value", nameof(col.StringFormat) + " on " + col.ColumnName);
                                }
                                column.StringFormatPattern = col.StringFormat;
                                column.useGenderColumn = "";
                                break;
                            case DataType.Shuffle:
                                column.type = DataType.Shuffle.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.Ignore:
                                break;
                            case DataType.Money:
                                column.type = DataType.Money.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = "{{FINANCE.AMOUNT}}";
                                column.useGenderColumn = "";
                                break;
                            case DataType.Location:
                                break;
                            case DataType.NoMasking:
                                column.ignore = true;
                                break;
                            case DataType.math:
                                if (maskingoutString.Any(n => col.DataType.ToUpper().Contains(n.ToUpper())))
                                {
                                    throw new ArgumentException("Math Operation apply only on Numeric Datatype", col.DataType + " on " + col.ColumnName);
                                }
                                column.type = DataType.math.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = col.StringFormat;
                                column.useGenderColumn = "";
                                column.ignore = true;
                                column.Operator = col.RuleReasoning;
                                break;
                            case DataType.Shufflegeometry:
                                column.type = DataType.Shufflegeometry.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.exception:
                                break;
                            case DataType.Bogus:
                                column.type = DataType.Bogus.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    throw new ArgumentException("Bogus type requires a StringFormatPattern value", nameof(col.StringFormat) + " on " + col.ColumnName);
                                    
                                }
                                column.StringFormatPattern = col.StringFormat;
                                column.useGenderColumn = "";
                                break;
                            case DataType.RandomUsername:
                                column.type = DataType.RandomUsername.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.FirstName:
                                column.type = DataType.FirstName.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.FullName:
                                column.type = DataType.FullName.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.LastName:
                                column.type = DataType.LastName.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.DateOfBirth:
                                column.type = DataType.DateOfBirth.ToString();                    
                                if (string.IsNullOrEmpty(col.Max.ToString()) || string.IsNullOrEmpty(col.Min.ToString()))
                                {
                                    throw new ArgumentException("DateOfBirth type requires MinMax value", nameof(col.Min) + nameof(col.Max) + " on " + col.ColumnName);
                                }
                                if (col.Max.ToString().Equals("-- ::") || col.Min.ToString().Equals("-- ::"))
                                {
                                    column.ignore = true;
                                }
                                if (col.Max.ToString().Equals(col.Min.ToString()))
                                {
                                    col.Max = DateTime.Now.ToString();
                                }
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                column.RetainEmptyStringValues = false;
                                break;
                            case DataType.PickRandom:
                                column.type = DataType.PickRandom.ToString();
                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    throw new ArgumentException("PickRandom type requires a StringFormatPattern of a list of random variable seperated with a comma", col.StringFormat + " on" + col.ColumnName);

                                }
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.RandomString2:
                                column.type = DataType.RandomString2.ToString();
                                if (string.IsNullOrEmpty(col.Max.ToString()) || string.IsNullOrEmpty(col.Min.ToString()))
                                {
                                    throw new ArgumentException("RandomString2 type requires a Min and Max value", nameof(col.Min) + nameof(col.Max) + " on " + col.ColumnName);
                                }
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                                
                            case DataType.Rant:
                                column.type = DataType.Rant.ToString();
                                if (string.IsNullOrEmpty(col.Max.ToString()))
                                {
                                    throw new ArgumentException("Rant type requires a Max value",  nameof(col.Max) + " on " + col.ColumnName);
                                }
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                //column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.min = Convert.ToString(1);
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.State:
                                column.type = DataType.State.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "Canada";
                                break;
                            case DataType.City:
                                column.type = DataType.City.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "Canada";
                                break;
                            case DataType.Lorem:
                                column.type = DataType.Lorem.ToString();
                                if (string.IsNullOrEmpty(col.Max.ToString()))
                                {
                                    throw new ArgumentException("Lorem type requires a Min and Max value", nameof(col.Max) + " on " + col.ColumnName);
                                }
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.StringFormat:
                                break;
                            case DataType.FullAddress:
                                column.type = DataType.FullAddress.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "";
                                column.useGenderColumn = "Canada";
                                break;
                            case DataType.StreetAddress:
                                column.type = DataType.StreetAddress.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.SecondaryAddress:
                                column.type = DataType.SecondaryAddress.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.PhoneNumber:
                                column.type = DataType.PhoneNumber.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    column.StringFormatPattern = "##########";
                                }
                                else
                                    column.StringFormatPattern = col.StringFormat;

                                column.useGenderColumn = "";
                                break;
                            case DataType.PhoneNumberInt:
                                column.type = DataType.PhoneNumberInt.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    column.StringFormatPattern = "##########";
                                }
                                else
                                    column.StringFormatPattern = col.StringFormat;
                                column.useGenderColumn = "";
                                break;
                            case DataType.Longitude:
                                column.type = DataType.Longitude.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.Latitude:
                                column.type = DataType.Latitude.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.RandomSeason:
                                column.type = DataType.RandomSeason.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.File:
                                column.type = DataType.File.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "{{SYSTEM.FILENAME}}";
                                column.useGenderColumn = "";
                                break;
                            case DataType.Filename:                             
                                column.type = DataType.File.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                //column.StringFormatPattern = "{{ADDRESS.STREETADDRESS}} {{ADDRESS.CITY}} {{ADDRESS.STATE}}";
                                column.StringFormatPattern = "{{SYSTEM.FILENAME}}";
                                column.useGenderColumn = "";
                                break;
                            case DataType.Blob:
                                var filename = nameGroup.Where(n => n.ColumnName.Equals("FILE_NAME") || n.ColumnName.Equals("FILENAME")).Select(n => n).FirstOrDefault().ColumnName;
                                column.type = DataType.Blob.ToString();
                                column.max = col.Max.ToString();
                                column.min = col.Min.ToString();

                                column.StringFormatPattern = "";
                                if (!string.IsNullOrEmpty(filename))
                                {
                                    column.StringFormatPattern = filename;
                                }
                                column.useGenderColumn = "";
                                break;
                            case DataType.Clob:
                                column.type = DataType.Clob.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min.ToString(); ;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.RandomDec:
                                column.type = DataType.RandomDec.ToString();
                                if (string.IsNullOrEmpty(col.Max.ToString()))
                                {
                                    throw new ArgumentException("RandomDec type requires a Max value",  nameof(col.Max) + " on " + col.ColumnName);
                                }
                                if (string.IsNullOrEmpty(col.Min.ToString()))
                                {
                                    throw new ArgumentException("RandomDec type requires a Max value", nameof(col.Min) + " on " + col.ColumnName);
                                }
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min.ToString(); ;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.Geometry:
                                column.type = DataType.Geometry.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min.ToString(); ;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.RandomYear:
                                column.type = DataType.RandomYear.ToString();
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.RandomMonth:
                                column.type = DataType.RandomMonth.ToString();
                                column.max = col.Max.ToString();
                                column.min = col.Min.ToString();
                                column.StringFormatPattern = col.StringFormat;
                                column.useGenderColumn = "";
                                break;
                            case DataType.RandomInt:
                                column.type = DataType.RandomInt.ToString();
                                if (string.IsNullOrEmpty(col.Max.ToString()))
                                {
                                    throw new ArgumentException("RandomInt type requires a Max value", nameof(col.Max) + " on " + col.ColumnName);
                                }
                                if (string.IsNullOrEmpty(col.Min.ToString()))
                                {
                                    throw new ArgumentException("RandomInt type requires a Max value", nameof(col.Min) + " on " + col.ColumnName);
                                }
                                column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                                break;
                            case DataType.Computed:
                                break;
                            case DataType.RandomHexa:
                                break;
                            default:
                                column.ignore = true;   
                                break;
                        }
                    }
                    else if (col.ColumnName.ToUpper().Contains("LONGITUDE") && (col.DataType.ToUpper().Contains("NUMERIC") || col.DataType.ToUpper().Contains("DECIMAL")))
                    {
                        column.type = DataType.Longitude.ToString();
                        column.max = col.Max.ToString();
                        column.min = col.Min.ToString();
                        column.StringFormatPattern = col.Description;
                        column.Operator = "";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("LATITUDE") && (col.DataType.ToUpper().Contains("NUMERIC") || col.DataType.ToUpper().Contains("DECIMAL")))
                    {
                        column.type = DataType.Latitude.ToString();
                        column.max = col.Max.ToString();
                        column.min = col.Min.ToString();
                        column.StringFormatPattern = col.Description;
                        column.Operator = "";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("FIRST_NAME") || col.ColumnName.ToUpper().Contains("FIRSTNAME")  || col.ColumnName.ToUpper().Contains("MIDDLE_NAME"))
                    {
                        column.type = DataType.FirstName.ToString();
                        column.max = col.Max.ToString(); 
                        column.min = col.Min.ToString(); 
                        column.StringFormatPattern = "{{NAME.FIRSTNAME}}";
                        column.useGenderColumn = "";
                    }
                    else if (col.DataType.ToUpper().Equals("BLOB") || col.DataType.ToUpper().Equals("IMAGE"))
                    {
                        var filename = nameGroup.Where(n => n.ColumnName.Equals("FILE_NAME") || n.ColumnName.Equals("FILENAME")).Select(n=>n).FirstOrDefault().ColumnName;
                        column.type = DataType.Blob.ToString();
                        column.max = col.Max.ToString(); 
                        column.min = col.Min.ToString();
                       
                        column.StringFormatPattern = "";
                        if (!string.IsNullOrEmpty(filename))
                        {
                            column.StringFormatPattern = filename;
                        }
                        column.useGenderColumn = "";
                    }
                    else if (col.DataType.ToUpper().Equals("CLOB"))
                    {
                        column.type = DataType.Clob.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("CITY"))
                    {
                        column.type = DataType.City.ToString(); 
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{ADDRESS.CITY}}";
                        column.useGenderColumn = "Canada";
                    }
                    else if (col.ColumnName.ToUpper().Contains("STATE") || col.ColumnName.ToUpper().Contains("PROVINCE"))
                    {
                        column.type = DataType.State.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{ADDRESS.CITY}}";
                        column.useGenderColumn = "Canada";
                    }
                    else if (col.ColumnName.ToUpper().Contains("COUNTRY"))
                    {
                        column.type = DataType.Bogus.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{ADDRESS.COUNTRY}}";
                        column.useGenderColumn = "Canada";
                    }
                    else if (col.DataType.ToUpper().Equals("SDO_GEOMETRY") || col.DataType.ToUpper().ToUpper().Contains("GEOMETRY"))
                    {
                        column.type = DataType.Geometry.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("SURNAME") || col.ColumnName.ToUpper().Contains("LASTNAME") || col.ColumnName.ToUpper().Contains("LAST_NAME"))
                    {
                        column.type = DataType.LastName.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{NAME.LASTNAME}}";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("COMPANY_NAME") || col.ColumnName.ToUpper().Contains("ORGANIZATION_NAME"))
                    {
                        column.type = DataType.Company.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{COMPANY.COMPANYNAME}} {{COMPANY.COMPANYSUFFIX}}";
                        column.useGenderColumn = "";
                    }
                    else if (_comment.Any(n => col.ColumnName.ToUpper().Contains(n)) || col.DataType.ToUpper().Contains("CHAR") || _comment.Any(x => col.Comments.Contains(x)))
                    {
                        var size = col.DataType.ToUpper().Replace("(", " ").Replace(")", " ").Split(' ');
                        if (size.Count() > 1)
                        {
                            var sizze = size[1].ToString();
                            if (!string.IsNullOrEmpty(sizze))
                            {
                                column.type = DataType.Rant.ToString();
                                column.max = Convert.ToString(sizze);
                                column.min = Convert.ToString(1); ;
                                column.StringFormatPattern = "DESCRIPTION";
                                column.useGenderColumn = "";
                            }
                            else
                            {
                                column.type = DataType.Rant.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = Convert.ToString(1) ;
                                column.StringFormatPattern = "DESCRIPTION";
                                column.useGenderColumn = "";
                            }
                        }
                        else
                        {
                            column.type = DataType.Rant.ToString();
                            column.max = col.Max.ToString(); ;
                            column.min = Convert.ToString(1);
                            column.StringFormatPattern = "DESCRIPTION";
                            column.useGenderColumn = "";
                        }//split varchar(20 byte) and get max number

                    }
                    else if (col.DataType.ToUpper().Contains("DATE"))
                    {
                        column.type = DataType.DateOfBirth.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Equals("YEAR"))
                    {
                        column.type = DataType.RandomYear.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("PHONE_NO") || col.ColumnName.ToUpper().Contains("FAX_NO") || col.ColumnName.ToUpper().Contains("CONTRACT_NO") || col.ColumnName.ToUpper().Contains("CELL") || col.ColumnName.ToUpper().Contains("_PHONE") || col.ColumnName.ToUpper().Contains("PHONENUMBER"))
                    {
                        if (col.DataType.ToUpper().Contains("NUMBER"))
                        {
                            column.type = DataType.PhoneNumberInt.ToString();
                            column.max = col.Max.ToString(); ;
                            column.min = col.Min.ToString(); ;
                            if (string.IsNullOrEmpty(col.StringFormat))
                            {
                                column.StringFormatPattern = "##########";
                            }
                            else
                                column.StringFormatPattern = col.StringFormat;
                            column.useGenderColumn = "";
                        }
                        else
                        {
                            column.type = DataType.PhoneNumber.ToString();
                            column.max = col.Max.ToString(); ;
                            column.min = col.Min.ToString(); ;
                            if (string.IsNullOrEmpty(col.StringFormat))
                            {
                                column.StringFormatPattern = "##########";
                            }
                            else
                                column.StringFormatPattern = col.StringFormat;
                            column.useGenderColumn = "";
                        }
                    }
                    else if (col.ColumnName.ToUpper().Contains("EMAIL_ADDRESS"))
                    {
                        column.type = DataType.Bogus.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{INTERNET.EMAIL}}";
                        column.useGenderColumn = "";
                    }
                    else if (col.DataType.ToUpper().Contains("MONEY"))
                    {
                        column.type = DataType.Money.ToString();
                        column.max = col.Max.ToString();
                        column.min = col.Min.ToString();
                        column.StringFormatPattern = "{{FINANCE.AMOUNT}}";
                        //column.useGenderColumn = "Gender";
                    }
                    else if (col.ColumnName.ToUpper().Contains("POSTAL_CODE"))
                    {
                        column.type = DataType.PostalCode.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }
                    else if (col.DataType.ToUpper().Equals("CHAR(1 BYTE)"))
                    {
                        var chr = col.DataType.ToUpper().Replace("(", " ").Replace(")", " ").Split(' ');
                        if (chr.Count() > 1)
                        {
                            var charSize = chr[1].ToString();
                            if (!string.IsNullOrEmpty(charSize))
                            {
                                column.type = DataType.PickRandom.ToString();
                                column.max = Convert.ToString(charSize);
                                column.min = col.Min.ToString(); ;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                            }
                            else
                            {
                                column.type = DataType.Ignore.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min.ToString(); ;
                                column.ignore = true;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                            }
                        }
                        else
                        {
                            column.type = DataType.Ignore.ToString();
                            column.max = col.Max.ToString(); ;
                            column.min = col.Min.ToString(); ;
                            column.ignore = true;
                            column.StringFormatPattern = "";
                            column.useGenderColumn = "";
                        }
                    }
                    else if (col.ColumnName.ToUpper().Contains("ADDRESS"))
                    {
                        var size = col.DataType.ToUpper().Replace("(", " ").Replace(")", " ").Split(' ');
                        if (size.Count() > 1)
                        {
                            var sizze = size[1].ToString();
                            if (!string.IsNullOrEmpty(sizze))
                            {
                                if (col.ColumnName.ToUpper().Contains("ADDRESS") && (col.ColumnName.Contains("2") || col.ColumnName.Contains("3")))
                                {
                                    column.type = DataType.SecondaryAddress.ToString();
                                }
                                else if (col.ColumnName.ToUpper().Contains("STREET"))
                                {
                                    column.type = DataType.StreetAddress.ToString();
                                }
                                else
                                    column.type = DataType.FullAddress.ToString();

                                column.max = Convert.ToString(sizze);
                                column.min = col.Min.ToString(); ;
                                column.StringFormatPattern = "{{address.fullAddress}}";
                                column.useGenderColumn = "Canada";
                            }
                            else
                            {
                                column.type = DataType.FullAddress.ToString();
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min.ToString(); ;
                                column.StringFormatPattern = "{{address.fullAddress}}";
                                column.useGenderColumn = "Canada";
                            }
                        }
                        else
                        {
                            column.type = DataType.FullAddress.ToString();
                            column.max = col.Max.ToString(); ;
                            column.min = col.Min.ToString(); ;
                            //column.StringFormatPattern = "{{ADDRESS.STREETADDRESS}} {{ADDRESS.CITY}} {{ADDRESS.STATE}}";
                            column.StringFormatPattern = "{{address.fullAddress}}";
                            column.useGenderColumn = "Canada";
                        }
                    }
                    else if (col.ColumnName.ToUpper().Contains("USERID") || col.ColumnName.ToUpper().Contains("USERNAME"))
                    {
                        column.type = DataType.RandomUsername.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        //column.StringFormatPattern = "{{ADDRESS.STREETADDRESS}} {{ADDRESS.CITY}} {{ADDRESS.STATE}}";
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("FILE_NAME"))
                    {
                        column.type = DataType.File.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        //column.StringFormatPattern = "{{ADDRESS.STREETADDRESS}} {{ADDRESS.CITY}} {{ADDRESS.STATE}}";
                        column.StringFormatPattern = "{{SYSTEM.FILENAME}}";
                        column.useGenderColumn = "";
                    }
                    else if (_fullName.Any(n => col.ColumnName.ToUpper().Contains(n)) || _fullName.Any(x => col.Comments.Contains(x)))
                    {
                        column.type = DataType.Bogus.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        //column.StringFormatPattern = "{{ADDRESS.STREETADDRESS}} {{ADDRESS.CITY}} {{ADDRESS.STATE}}";
                        column.StringFormatPattern = "{{NAME.FULLNAME}}";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("AMOUNT") || col.ColumnName.ToUpper().Contains("AMT") || col.Comments.Contains("Amount"))
                    {
                        column.type = DataType.RandomDec.ToString();
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }                  
                    else
                    {                       
                        if (col.ColumnName.ToUpper().Equals("NAME")) //set company name
                        {
                            column.type = DataType.Bogus.ToString();
                            //column.type = col.DATA_TYPE;
                            column.StringFormatPattern = "{{COMPANY.COMPANYNAME}}";
                            column.max = col.Max.ToString(); ;
                            column.min = col.Min.ToString(); ;
                            column.useGenderColumn = "";
                        }
                        else if ((col.ColumnName.ToUpper().Contains("AUTHOR") || col.ColumnName.ToUpper().Contains("EDITOR")) && col.DataType.ToUpper().Contains("VARCHAR"))
                        {
                            column.type = DataType.FullName.ToString();
                            column.max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                            column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                            column.StringFormatPattern = "";
                            column.useGenderColumn = "";
                        }
                        else if (col.DataType.ToUpper().Contains("NUMBER"))
                        {
                            //var size = col.DataType.ToUpper().Replace("(", " ").Replace(")", " ").Split(' ')[1].Split(',')[0].ToString();

                            column.type = DataType.RandomInt.ToString();
                            column.max = col.Max.ToString(); ;
                            column.min = col.Min;
                            column.StringFormatPattern = "";
                            column.useGenderColumn = "";

                        }
                        else if (col.ColumnName.ToUpper().Equals("TOTAL_AREA")) //set company name
                        {
                            column.type = DataType.Bogus.ToString();
                            //column.type = col.DATA_TYPE;
                            column.StringFormatPattern = "";
                            column.max = col.Max.ToString(); ;
                            column.min = col.Min;
                            column.useGenderColumn = "";
                        }
                        else
                        {
                            count++;
                            collist.Add(new KeyValuePair<string, string>(col.ColumnName, col.TableName));
                            //collist.Add(col.ColumnName.ToUpper(), col.TableName);
                            column.type = DataType.Ignore.ToString();
                            //column.type = col.DATA_TYPE;
                            column.StringFormatPattern = "";
                            column.max = col.Max.ToString();
                            column.min = col.Min.ToString();
                            column.useGenderColumn = "";
                            column.ignore = true;

                        }
                    }
                    if (!col.ColumnName.Equals(table.primaryKeyColumn) && !col.MaskingRule.ToUpper().Contains("FLAGGED"))
                    {
                        colList.Add(column);
                    }
                    
                }

                // if (colList.Count > 0)
                // {
                table.columns = colList;

                tableList.Add(table);
                //}
                #region check for null type
                var nullType = table.columns.Where(x => x.type == null);
                if (nullType.Any())
                {
                    int autoNumber1 = 1;
                    foreach (var type in nullType)
                    {
                        Console.WriteLine(autoNumber1++.ToString() + ". "+ $"Column {type.name} contains invalid masking rule type");
                    }
                    Console.WriteLine("The Program will exit. Hit Enter to exit..");


                    Console.ReadLine();
                    System.Environment.Exit(1);
                }
                #endregion

            }
            #endregion

            #region Initialize Root object
            RootObject1 rootObject1 = new RootObject1
            {
                tables = tableList,

                dataSource = dataSource,
                dataGeneration = dataGeneration
          

            };
            #endregion



            //check for Tables with no primary key or any Identifier and exit if true 
            #region Check for tables without Primary Key if true exit
            var noPrimaryKey = rootObj.Where(n => n.PKconstraintName == null || n.PKconstraintName == string.Empty).GroupBy(n => n.TableName);
            //var cou = noPrimaryKey.Count();
            //primary key applied to relational database not spreadsheet         
            if (noPrimaryKey.Count() != 0 && ConfigurationManager.AppSettings[SourceType] != nameof(DataSourceType.SpreadSheet))
            {
                int autoNumber = 1;
                Console.WriteLine("Required property 'PrimaryKeyColumn' expects a value but got null. Please provide one column identifier for these tables" + "\n" +
                    "See the NullPrimaryKey.txt file in the Output folder.");
                string nullPK = "These tables has no PrimaryKey or Identifier. Provide one column identifier for these tables " + Environment.NewLine + Environment.NewLine;
                foreach (var tables in noPrimaryKey.GroupBy(n => n.Key))
                {
                    nullPK += autoNumber++.ToString() + ". " + tables.Key.ToString() + " PrimaryKey : " + "Null" + Environment.NewLine;

                    //Console.WriteLine(tables.TABLE_NAME);
                }
                File.WriteAllText(@"output\NullPrimaryKey.txt", nullPK);
                Console.WriteLine("The Program will exit. Hit Enter to exit..");



                Console.ReadLine();


                System.Environment.Exit(1);
            }
            #endregion

         

            //jsonpath = @"example-configs\jsconfigTables.json";
            if (!Directory.Exists(@"classification-configs"))
            {
                Directory.CreateDirectory(@"classification-configs");
            }
            var jsonname = Path.GetFileNameWithoutExtension(excelspreadsheet);
            Jsonpath = @"classification-configs\" + jsonname + ".json";
            string jsonresult = JsonConvert.SerializeObject(rootObject1,Formatting.Indented);
            if (!Directory.Exists(@"output\Validation"))
            {
                Directory.CreateDirectory(@"output\Validation");
            }

            #region compare original jsonconfig for datatype errors
            if (File.Exists(Jsonpath) && new FileInfo(Jsonpath).Length != 0)
            {
                var rootConfig = Config.Load(Jsonpath);
                foreach (var tabitem in rootConfig.Tables)
                {
                    foreach (var colitems in tabitem.Columns)
                    {
                        var newdic = new Dictionary<string, string> { { colitems.Name, colitems.Type.ToString() } };
                        jsconfigTable.Add(new KeyValuePair<string, Dictionary<string, string>>(tabitem.Name, newdic));

                        //(item.Name, new Dictionary<string, string>() { { items.Name, items.Type.ToString() } });
                    }
                }
                var buildConfig = JsonConvert.DeserializeObject<RootObject1>(jsonresult);
                foreach (var tabCol in buildConfig.tables)
                {
                    foreach (var col in tabCol.columns)
                    {
                        copyJsTable.Add(new KeyValuePair<string, Dictionary<string, string>>(tabCol.name, new Dictionary<string, string> { { col.name, col.type } }));
                    }
                }
                // var diff = jsconfigTable.Where(x=> x.Key != copyJsTable.Select(n=>n.Key).ToString());
                string mapped = "";
                if (copyJsTable.Count == jsconfigTable.Count)
                {
                    for (int i = 0; i < copyJsTable.Count; i++)
                    {
                        if (!jsconfigTable[i].Value.SequenceEqual(copyJsTable[i].Value))
                        {
                            mapped = string.Join(",", jsconfigTable[i].Value.Select(n => n.Value).ToArray());
                            Console.WriteLine(jsconfigTable[i].Key.ToString() + " " + string.Join(",", copyJsTable[i].Value.ToArray()) + " now mapped with: " + mapped);
                        }
                        else if (jsconfigTable[i].Value.Where(n => n.Value.Equals(DataType.Ignore.ToString())).Count() != 0)
                        {
                            var xxxx = jsconfigTable[i].Value.Where(n => n.Value.Equals(DataType.Ignore.ToString())).ToDictionary(n => n.Key, n => n.Value);
                            //exit
                            _allNull.Add(new KeyValuePair<string, Dictionary<string, string>>(string.Join("", jsconfigTable[i].Key.ToArray()).ToString(), xxxx));

                        }
                     


                    }
                    if (string.IsNullOrEmpty(mapped))
                    {
                        //replace original with new json
                        if (!Directory.Exists(@"output\Validation"))
                        {
                            Directory.CreateDirectory(@"output\Validation");
                        }

                        using (var tw = new StreamWriter(Jsonpath, false))
                        {
                            tw.WriteLine(jsonresult.ToString());
                            tw.Close();
                            Console.WriteLine("{0}{1}", "Maped Json".ToUpper() + Environment.NewLine, jsonresult);
                        }
                        //check map failures and write to file then exit for correction
                        if (count != 0)
                        {
                            string colfailed = count + " columns cannot be mapped to a masking datatype and so will be ignored during masking. Review the " + Jsonpath + " and provide mask datatype for these columns " + Environment.NewLine + Environment.NewLine + string.Join(Environment.NewLine, collist.Select(x => x.Key + " ON TABLE " + x.Value).ToArray());
                            //Console.WriteLine(colfailed);
                            Console.WriteLine(count + " columns cannot be mapped to a masking datatype and so will be ignored during masking" + Environment.NewLine + "{0}", string.Join(Environment.NewLine, collist.Select(n => n.Key + " ON TABLE " + n.Value).ToArray()));
                            Console.WriteLine("Do you wish to continue and ignore masking these columns? [yes/no]");
                            string option = Console.ReadLine();
                            if (option.ToUpper() == "YES" || option.ToUpper() == "Y")
                            {

                            }
                            else
                            {
                                Console.WriteLine("Hit Enter to exist..");
                                Console.ReadLine();
                                File.WriteAllText(@"output\failedColumn.txt", colfailed);
                                System.Environment.Exit(1);
                            }
                        }
                    }
                    if (_allNull.Count() != 0)
                    {
                        if (!Directory.Exists(@"output\Validation"))
                        {
                            Directory.CreateDirectory(@"output\Validation");
                        }
                        int notmasked = 1;
                        _colError.Add("Table_name contains column with ignore datatype Column_name :type" + Environment.NewLine + Environment.NewLine);
                        for (int i = 0; i < _allNull.Count; i++)
                        {
                            //_colError.Add("")
                            //Console.WriteLine(string.Join("", _allNull[i].Key.ToArray()) + " contains column with ignore datatype/columns" + " " + string.Join(Environment.NewLine, _allNull[i].Value.Select(n => n.Key + " :" + n.Value).ToArray()));
                            _colError.Add(notmasked++.ToString() + ". " + string.Join("", _allNull[i].Key.ToArray()) + " contains column with ignore datatype" + " " + string.Join("", _allNull[i].Value.Select(n => n.Key + " :" + n.Value).ToArray()) + Environment.NewLine);
                        }
                        string value = string.Join("", _colError.ToArray());
                        Console.Write(Environment.NewLine + "These columns have ignore datatype so will not be masked" + Environment.NewLine);
                        Console.Write(value);
                        Console.Write(Environment.NewLine);

                        Console.WriteLine("Do you want to continue and ignore masking these columns? [yes/no]");
                        string option = Console.ReadLine();
                        if (option == "yes")
                        {
                            JObject o1 = JObject.Parse(File.ReadAllText(Jsonpath));
                            Console.WriteLine(o1);
                            
                            
                            File.WriteAllText(@"output\ColumnNotMasked.txt", value);
                            
                        }
                        else
                        {
                            Console.WriteLine("Hit Enter to exist..");

                            Console.ReadLine();
                            File.WriteAllText(@"output\ColumnNotMasked.txt", value);

                            System.Environment.Exit(1);
                        }
                    }
                }
                else
                {
                    Console.WriteLine("Sequence array in both files are not equal. Check the copyJs.js and JsConfigTables.js");
                    Console.WriteLine("The program with exit....." + "\n"
                            + "Hit Enter to Exit.................");
                    Console.ReadLine();
                    System.Environment.Exit(1);
                }
            }
            else
            {
                //write json file to path
                if (!Directory.Exists(@"output\Validation"))
                {
                    Directory.CreateDirectory(@"output\Validation");
                }

                using (var tw = new StreamWriter(Jsonpath, false))
                {
                    tw.WriteLine(jsonresult.ToString());
                    tw.Close();
                    Console.WriteLine("{0}{1}", "Maped Json".ToUpper() + Environment.NewLine, jsonresult);
                }
                //check map failures and write to file then exit for correction
                if (count != 0)
                {
                    string colfailed = count + " columns cannot be mapped to a masking datatype and so will be ignored during masking. Review the " + Jsonpath +" and provide mask datatype for these columns " + Environment.NewLine + Environment.NewLine + string.Join(Environment.NewLine, collist.Select(x => x.Key + " ON TABLE " + x.Value).ToArray());
                    //Console.WriteLine(colfailed);
                    Console.WriteLine(count + " columns cannot be mapped to a masking datatype and so will be ignored during masking" + Environment.NewLine +"{0}", string.Join(Environment.NewLine, collist.Select(n => n.Key + " ON TABLE " + n.Value).ToArray()));
                    Console.WriteLine("Do you wish to continue and ignore masking these columns? [yes/no]");
                    string option = Console.ReadLine();
                    if (option.ToUpper() == "YES" || option.ToUpper() == "Y")
                    {

                    }
                    else
                    {
                        Console.WriteLine("Hit Enter to exist..");
                        Console.ReadLine();
                        File.WriteAllText(@"output\failedColumn.txt", colfailed);
                        System.Environment.Exit(1);
                    }
                }

                

            }
            #endregion

        }
        public class RootObject
        {
            [DefaultValue("")]
            [JsonProperty("TABLE_NAME", DefaultValueHandling = DefaultValueHandling.Populate)]

            public string TableName { get; set; }
            [DefaultValue("")]
            [JsonProperty("COLUMN_NAME", DefaultValueHandling = DefaultValueHandling.Populate)]
            public string ColumnName { get; set; }
           
            [DefaultValue("max")]
            [JsonProperty("ROW_COUNT", DefaultValueHandling = DefaultValueHandling.Populate)]
            public string RowCount { get; set; }
            [DefaultValue("")]
            [JsonProperty("DATA_TYPE", DefaultValueHandling = DefaultValueHandling.Populate)]
            public string DataType { get; set; }

            [JsonProperty("NULLABLE")]
            public string Nullable { get; set; }

            [JsonProperty("DATA_DEFAULT")]
            public string DataDefault { get; set; }
            [DefaultValue(0)]
            [JsonProperty("COLUMN_ID", DefaultValueHandling = DefaultValueHandling.Populate)]
            public long? ColumnId { get; set; } = 0;

            [DefaultValue("")]
            // [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            [JsonProperty("COMMENTS", DefaultValueHandling = DefaultValueHandling.Populate, NullValueHandling = NullValueHandling.Ignore)]
            public string Comments { get; set; } = "";

            [JsonProperty("Description")]
            public string Description { get; set; }

            [JsonProperty("Public")]
            public string Public { get; set; }

            [JsonProperty("Personal")]
            public string Personal { get; set; }
            [DefaultValue("")]
            [JsonProperty("PKconstraintName", DefaultValueHandling = DefaultValueHandling.Populate)]
            public string PKconstraintName { get; set; } = "";
            [JsonRequired]
            [JsonProperty("SCHEMA")]
            public string Schema { get; set; }

            [DefaultValue("TRUE")]
            [JsonProperty("Retain NULL", DefaultValueHandling = DefaultValueHandling.Populate)]
            public string RetainNull { get; set; } = "TRUE";

            [DefaultValue("FALSE")]
            [JsonProperty("RetainEmptyString", DefaultValueHandling = DefaultValueHandling.Populate)]
            public string RetainEmptyString{ get; set; } = "FALSE";

            [DefaultValue("FALSE")]
            [JsonProperty("Preview", DefaultValueHandling = DefaultValueHandling.Populate)]
            public string Preview { get; set; } = "FALSE";

            [DefaultValue("")]
            [JsonProperty("Min", DefaultValueHandling = DefaultValueHandling.Populate)]
            public string Min { get; set; }
            [DefaultValue("")]
            [JsonProperty("max", DefaultValueHandling = DefaultValueHandling.Populate)]
            public string Max { get; set; }

            [JsonProperty("Sensitive")]
            public string Sensitive { get; set; }
            [DefaultValue("")]
            [JsonProperty("Masking Rule", DefaultValueHandling = DefaultValueHandling.Populate)]
            public string MaskingRule { get; set; }

            [JsonProperty("Rule set by")]
            public string RuleSetBy { get; set; }

            [JsonProperty("Rule Reasoning")]
            public string RuleReasoning { get; set; }

            [JsonProperty("COMPLETED")]
            public string Completed { get; set; }
            [DefaultValue("")]
            [JsonProperty("StringFormat", DefaultValueHandling = DefaultValueHandling.Populate)]
            public string StringFormat { get; set; }
            [DefaultValue(null)]
            [JsonProperty("UseValue", DefaultValueHandling = DefaultValueHandling.Populate)]
            public string UseValue { get; set; }
            [JsonProperty("Conversion Consideration (NEEDS BUSINESS DISCUSSION)")]
            public string Consideration { get; set; }
        }

        private static Config LoadConfig(
            int example)
        {

            if (allkey.Where(n => n.Key.ToUpper().Equals(RunTestJson.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
            {
                
                if (!Directory.Exists(@"output\Validation"))
                {
                    Directory.CreateDirectory(@"output\Validation");
                }
                File.Create(exceptionPath).Close();
                File.Create(_exceptionpath).Close();
                File.Create(path).Close();
                return Config.Load(_testJson);
            }
            else
            {
                File.Create(exceptionPath).Close();
                File.Create(_exceptionpath).Close();
                File.Create(path).Close();
                return Config.Load(Jsonpath);
            }

            //return Config.Load($@"\\SFP.IDIR.BCGOV\U130\SOOKEKE$\Masking_sample\APP_TAP_config.json");
        }
        private static string ReadPassword()
        {
            string secured = "";
            ConsoleKeyInfo keyInfo;
           

            do
            {
                keyInfo = Console.ReadKey(true);
                // Backspace Should Not Work
                if (keyInfo.Key != ConsoleKey.Backspace)
                {
                    secured += keyInfo.KeyChar;
                    Console.Write("*");
                }
                else
                {
                    Console.Write("\b");
                }
            }
            while (keyInfo.Key != ConsoleKey.Enter);
          

           
            return secured;
        }


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
        public static void Run()
        {
                  
            try
            {                           
                if (!allkey.Where(n => n.Key.ToUpper().Equals(RunTestJson.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
                {
                    _SpreadSheetPath = ConfigurationManager.AppSettings[ExcelSheetPath];
                    copyjsonPath = ExcelToJson.ToJson(_SpreadSheetPath);
                    JsonConfig(copyjsonPath, _SpreadSheetPath);
                }
                
                Console.Title = string.Format("Data Masker v{0}",MaskerVersion);
                Config config = LoadConfig(1);               
                if (Convert.ToString(config.DataSource.Config.connectionString.ToString()).Contains("{0}"))
                {
                    Console.WriteLine("");
                    Console.WriteLine("Type your database user password for non-prod and press enter");
                    config.DataSource.Config.connectionString = string.Format(config.DataSource.Config.connectionString.ToString(), ReadPassword());
                }
                if (Convert.ToString(config.DataSource.Config.connectionStringPrd.ToString()).Contains("{0}"))
                {
                    Console.WriteLine("");
                    Console.WriteLine("Type your database user password for prod and press enter");
                    config.DataSource.Config.connectionStringPrd = string.Format(config.DataSource.Config.connectionStringPrd.ToString(), ReadPassword());
                }
               
                
                _nameDatabase = config.DataSource.Config.Databasename.ToString();
                if (string.IsNullOrEmpty(_nameDatabase)) { throw new ArgumentException("Database name cannot be null, check app.config and specify the database name", _nameDatabase); }
                IDataMasker dataMasker = new DataMasker(new DataGenerator(config.DataGeneration));
                IDataSource dataSource = DataSourceProvider.Provide(config.DataSource.Type, config.DataSource);

                //check to run validation only
                if (allkey.Where(n => n.Key.ToUpper().Equals(RunValidationONLY.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))                  
                {
                    Console.WriteLine("Data Masking Validation check has started. Program will exist after validation......................................");
                    Console.Title = string.Format("Data Masking Validation v{0}", MaskerVersion);
                    MaskValidationCheck.Verification(config.DataSource, config, sheetPath, CreateDir, _nameDatabase, exceptionPath, _columnMapping, "");
                    Console.WriteLine("Validation completed: Press ENTER to exist..");
                    Console.ReadLine();
                    System.Environment.Exit(1);
                }

                Stopwatch watch = new Stopwatch();
                watch.Start();
                #region Masking operation and Data generation
                foreach (TableConfig tableConfig in config.Tables)
                {
                    //checked if table contains blob column data type and get column that is blob
                    
                    var isblob = tableConfig.Columns.Where(x => !x.Ignore && x.Type == DataType.Blob);
                    object FileNameWithExtension = null;
                    string[] extcolumn = null;
                    IEnumerable<IDictionary<string, object>> rows = null;
                   
                    IEnumerable<IDictionary<string, object>> rawData = null;
                    File.AppendAllText(_exceptionpath, "Recorded exception for " + tableConfig.Name + ".........." + Environment.NewLine + Environment.NewLine);
                    if (config.DataSource.Type == DataSourceType.SpreadSheet)
                    {
                        //load spreadsheet to dataTable
                        Console.WriteLine(string.Format("Data Generation for {0} started....", tableConfig.Name));
                        var SheetTable = dataSource.DataTableFromCsv(config.DataSource.Config.connectionString.ToString(), tableConfig);
                        //convert DataTable to object
                        try
                        {
                            rowCount = SheetTable[tableConfig.Name].Rows.Count;
                            //rows = dataSource.CreateObject(SheetTable[config.Tables.IndexOf(tableConfig)]);
                            rows = dataSource.CreateObject(SheetTable[tableConfig.Name]);
                            rawData = dataSource.RawData(null);
                            Console.Title = string.Format("Data Masking v{0}", MaskerVersion);
                            foreach (IDictionary<string, object> row in rows)
                            {
                               
                                dataMasker.Mask(row, tableConfig, dataSource, rowCount, rawData, SheetTable[tableConfig.Name]);
                            }
                        }
                        catch (Exception ex)
                        {

                            File.WriteAllText(_exceptionpath, tableConfig.Name + " is not found in the SpreadSheet Table" + Environment.NewLine + Environment.NewLine);
                            Console.WriteLine(ex.Message);
                        }
                        try
                        {
                            //convert the object to DataTable
                            _dmlTable = dataSource.SpreadSheetTable(rows, tableConfig); MaskTable = _dmlTable;//masked table
                            //var maskedObj = dataSource.CreateObject(_dmlTable); // masked object
                            PrdTable = dataSource.SpreadSheetTable(rawData, tableConfig); //PRD table


                            //convert to DML
                            #region DML Script

                            if (allkey.Where(n => n.Key.ToUpper().Equals(WriteDML.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
                            {
                                _dmlTable.TableName = tableConfig.Name;
                                CreateDir = Directory.GetCurrentDirectory() + @"\output\" + _nameDatabase + @"\";
                                if (!Directory.Exists(CreateDir))
                                {
                                    Directory.CreateDirectory(CreateDir);
                                }
                                string writePath = CreateDir + @"\" + tableConfig.Name + ".sql";
                                var insertSQL = SqlDML.GenerateInsert(_dmlTable,null,isBinary, extcolumn, null, null, writePath, config, tableConfig);
                                if (allkey.Where(n => n.Key.ToUpper().Equals(MaskTabletoSpreadsheet.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
                                {
                                    SqlDML.DataTableToExcelSheet(_dmlTable, CreateDir + @"\" + tableConfig.Name + ".xlsx", tableConfig);
                                }
                            }
                            if (allkey.Where(n => n.Key.ToUpper().Equals(RunValidation.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true)
                                && PrdTable.Rows != null && MaskTable.Rows != null
                                && allkey.Where(n => n.Key.ToUpper().Equals(MaskedCopyDatabase.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(false))
                            {
                                Reportvalidation(PrdTable, _dmlTable, config.DataSource, tableConfig);
                            }
                                #endregion

                            
                        }
                        catch (Exception ex)
                        {
                            //string path = Directory.GetCurrentDirectory() + $@"\Output\MaskedExceptions.txt";
                            File.WriteAllText(_exceptionpath, ex.ToString() + Environment.NewLine + Environment.NewLine);
                            Console.WriteLine(ex.Message);
                        }
                    }
                    else
                    {
                        try
                        {
                            rowCount = dataSource.GetCount(tableConfig);
                            if (rowCount > 100000)
                            {
                                Console.Title = string.Format("Data Generation v{0}", MaskerVersion);
                                Console.WriteLine(Environment.NewLine);

                                int fetch = int.TryParse(ConfigurationManager.AppSettings["Fetch"].ToString(), out int f) ? f : 100000;
                                var batch = Math.Ceiling((double)rowCount / (double)fetch);
                                int offset = 0; int r = 0;
                                for (int j = 0; j < batch; j++)
                                {
                                    Console.WriteLine(string.Format("Data Generation for {0} has started from {1} to {2}....", tableConfig.Name, offset, fetch + offset));
                                    rows = dataSource.GetData(tableConfig, config, rowCount, fetch, offset); //masked
                                    rawData = dataSource.RawData(null); //unmask
                                    Console.Title = string.Format("Data Masking v{0}", MaskerVersion);
                                    Console.WriteLine(string.Format("Data Masking for {0} has started....", tableConfig.Name));


                                    offset = offset + fetch;
                                }
                            }
                            else
                            {
                                Console.Title = string.Format("Data Generation v{0}", MaskerVersion);
                                Console.WriteLine(Environment.NewLine);
                                Console.WriteLine(string.Format("Data Generation for {0} has started....", tableConfig.Name));
                                rows = dataSource.GetData(tableConfig, config, rowCount, null, null); //masked
                                rawData = dataSource.RawData(null); //unmask
                                Console.Title = string.Format("Data Masking v{0}", MaskerVersion);
                                Console.WriteLine(string.Format("Data Masking for {0} has started....", tableConfig.Name));

                                for (int i = 0; i < rowCount; i++)
                                {
                                    //var to = rows.ElementAt(i);
                                    if (isblob.Count() == 1 && rows.ElementAt(i).Select(n => n.Key).ToArray().Where(x => x.Equals(string.Join("", isblob.Select(n => n.StringFormatPattern)))).Count() > 0)
                                    {
                                        isBinary = isblob.Any();
                                        var blobLocation = @"output\" + _nameDatabase + @"\BinaryFiles\" + tableConfig.Name + @"\";
                                        if (!Directory.Exists(blobLocation))
                                        {
                                            Directory.CreateDirectory(blobLocation);
                                        }
                                        FileNameWithExtension = rows.ElementAt(i)[string.Join("", isblob.Select(n => n.StringFormatPattern))];
                                        string ex = FileNameWithExtension.ToString().Substring(FileNameWithExtension.ToString().LastIndexOf('.') + 1);
                                        dataMasker.MaskBLOB(rows.ElementAt(i), tableConfig, dataSource, tableConfig.Columns.Where(n => n.Type == DataType.Shuffle).Any() ? rawData : null, FileNameWithExtension.ToString(), ToEnum(ex, FileTypes.JPEG), blobLocation);
                                    }
                                    else
                                        dataMasker.Mask(rows.ElementAt(i), tableConfig, dataSource, rowCount, tableConfig.Columns.Where(n => n.Type == DataType.Shuffle).Any() ? rawData : null);
                                }
                            }


                            #region Create DML Script


                            _dmlTable = dataSource.SpreadSheetTable(rows, tableConfig); MaskTable = _dmlTable;//masked table      
                            #region preview
                            if (tableConfig.Columns.Where(n => !n.Ignore && n.Preview == true).Any())
                            {
                                List<string> coln = new List<string>
                                {
                                    tableConfig.PrimaryKeyColumn
                                };
                                string[] co = tableConfig.Columns.Where(n => !n.Ignore && n.Preview == true).Select(n => n.Name).Take(6).ToArray();
                                coln.AddRange(co);
                                Console.WriteLine(Environment.NewLine);
                                int.TryParse(ConfigurationManager.AppSettings["PreviewCount"].ToString(), out int previewCount);
                                Console.WriteLine("{0}: Top {1} masked data Preview for table {2}", tableConfig.Name, previewCount, tableConfig.Name);

                                MaskTable.Print(previewCount, coln.ToArray());
                            }
                            #endregion//var maskedObj = dataSource.CreateObject(_dmlTable); // masked object


                            PrdTable = dataSource.SpreadSheetTable(rawData, tableConfig); //PRD table
                            if (allkey.Where(n => n.Key.ToUpper().Equals(WriteDML.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
                            {
                                Console.WriteLine(Environment.NewLine);
                                Console.WriteLine(string.Format("Generating DML and SpreadSheet for {0}...", tableConfig.Name));
                                _dmlTable.TableName = tableConfig.Name;
                                CreateDir = Directory.GetCurrentDirectory() + @"\output\" + _nameDatabase + @"\";
                                if (!Directory.Exists(CreateDir))
                                {
                                    Directory.CreateDirectory(CreateDir);
                                }
                                string writePath = CreateDir + @"\" + tableConfig.Name + ".sql";
                                var insertSQL = SqlDML.GenerateInsert(_dmlTable, PrdTable, isBinary, extcolumn, null, null, writePath, config, tableConfig);
                                if (!string.IsNullOrEmpty(insertSQL)) { File.AppendAllText(_exceptionpath, insertSQL + Environment.NewLine); }
                                if (allkey.Where(n => n.Key.ToUpper().Equals(MaskTabletoSpreadsheet.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
                                {
                                    SqlDML.DataTableToExcelSheet(_dmlTable, CreateDir + @"\" + tableConfig.Name + ".xlsx", tableConfig);
                                }
                            }
                            if (allkey.Where(n => n.Key.ToUpper().Equals(RunValidation.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true)
                                && PrdTable.Rows != null && MaskTable.Rows != null
                                && allkey.Where(n => n.Key.ToUpper().Equals(MaskedCopyDatabase.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(false))
                            {
                                Console.WriteLine(string.Format("Running Validation for {0}....", tableConfig.Name));
                                Reportvalidation(PrdTable, _dmlTable, config.DataSource, tableConfig);
                            }
                            #endregion
                            if (allkey.Where(n => n.Key.ToUpper().Equals(MaskedCopyDatabase.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
                            {
                                Console.WriteLine("writing table " + tableConfig.Name + " on database " + _nameDatabase + "" + " .....");
                                if (tableConfig.Columns.Where(n => !n.Ignore).Any())
                                {
                                    dataSource.UpdateRows(rows, rowCount, tableConfig, config);
                                }

                            }

                        }
                        catch (Exception ex)
                        {
                            File.AppendAllText(exceptionPath, $"Recorded exception on table {tableConfig.Name}: " + ex.Message + Environment.NewLine + Environment.NewLine);
                            Console.WriteLine(ex.Message);
                        }
                    }
                    isBinary = false;
                }
                #endregion
                //write mapped table and column with type in csv file
                if (!allkey.Where(n => n.Key.ToUpper().Equals(RunTestJson.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
                {
                    var o = OutputSheet(config, copyjsonPath, _nameDatabase);
                }

                if (report.Rows.Count != 0
                    && allkey.Where(n => n.Key.ToUpper().Equals(EmailValidation.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
                {
                    watch.Stop();

                    TimeSpan timeSpan = watch.Elapsed;
                    var timeElapse = string.Format("{0}h {1}m {2}s", timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds);
                    MaskValidationCheck.Analysis(report, config.DataSource, sheetPath, _nameDatabase, CreateDir, exceptionPath, _columnMapping, timeElapse);
                }

                #region validate masking 
                if (allkey.Where(n => n.Key.ToUpper().Equals(RunValidation.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true)
                    && allkey.Where(n => n.Key.ToUpper().Equals(MaskedCopyDatabase.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true)
                    && allkey.Where(n => n.Key.ToUpper().Equals(EmailValidation.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
                {
                    Console.WriteLine("Data Masking Validation has started......................................");
                    Console.Title = string.Format("Data Masking Validation v{0}",MaskerVersion);
                    watch.Stop();

                    TimeSpan timeSpan = watch.Elapsed;
                    var timeElapse = string.Format("{0}h {1}m {2}s", timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds);

                    MaskValidationCheck.Verification(config.DataSource, config, sheetPath, CreateDir, _nameDatabase, exceptionPath, _columnMapping, timeElapse);
                }
                #endregion
            }
            catch (Exception e)
            {
                File.WriteAllText(_exceptionpath, e.Message + Environment.NewLine + Environment.NewLine);
                Console.WriteLine(e.Message);
                Console.WriteLine("Program will exit: Press ENTER to exist..");
                Console.ReadLine();
            }
          


        }
        public static bool OutputSheet(
            Config config, 
            string json, string _appname)
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("TABLE_NAME");
            dt.Columns.Add("COLUMN_NAME");
            dt.Columns.Add("MASKING RULE");
            dt.Columns.Add("MASKING RULE APPLIED");

            dt.Columns.Add("IGNORE");
            dt.Columns.Add("Format Pattern");
            dt.Columns.Add("Min - Max");
            RootObject rootObject = new RootObject();
            var rootObj = JsonConvert.DeserializeObject<List<RootObject>>(File.ReadAllText(json));
            rootObj.RemoveAll(n=>n.ColumnName == n.PKconstraintName.Split(',')[0]);
            int h = 0;
            int k = 0;
            for (int i = 0; i < config.Tables.Count; i++)
            {


                for (int j = 0; j < config.Tables[i].Columns.Count; j++)
                {



                    var minMax = ToString(config.Tables[i].Columns[j].Min) + " - " + Convert.ToString(config.Tables[i].Columns[j].Max);
                    dt.Rows.Add(config.Tables[i].Name, config.Tables[i].Columns[j].Name, rootObj[j + h].MaskingRule, config.Tables[i].Columns[j].Type, config.Tables[i].Columns[j].Ignore, config.Tables[i].Columns[j].StringFormatPattern, minMax);

                    k++;
                }
                h = k;

            }

            if (dt.Rows != null)
            {
                var csv = WriteTofile(dt, _appname, "_COLUMN_MAPPING");
                var createsheet = ToExcel(csv, _appname, _appname, "_COLUMN_MAPPING");
                if (createsheet == false)
                {
                    
                    Console.WriteLine("cannot create excel file");
                    return false;
                }
            }
            return true;
        }
        private static string ToString(object value)
        {
            if (null == value)
                return "Null";

            try
            {
                return Convert.ToString(value);
            }
            catch (Exception)
            {

                return "Null";
            }

        }
        private static string WriteTofile(DataTable textTable, string directory, string uniquekey)
        {
            StringBuilder fileContent = new StringBuilder();
            //int i = 0;
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }
            if (textTable.Columns.Count == 0)
            {
                return "";
            }
            foreach (var col in textTable.Columns)
            {
                
                fileContent.Append(col.ToString() + ",");
            }

            fileContent.Replace(",", System.Environment.NewLine, fileContent.Length - 1, 1);

            foreach (DataRow dr in textTable.Rows)
            {
               
                foreach (var column in dr.ItemArray)
                {
                    fileContent.Append("\"" + column.ToString() + "\",");
                }

                fileContent.Replace(",", System.Environment.NewLine, fileContent.Length - 1, 1);
            }

            System.IO.File.WriteAllText(directory + @"\" + directory + uniquekey + ".csv", fileContent.ToString());
            if (File.Exists(directory + @"\" + directory + uniquekey + ".csv"))
            {
                return directory + @"\" + directory + uniquekey + ".csv";
            }
            else
                return "";
            
        }

        private static bool ToExcel(
            string csvFileName, 
            string _appName, 
            string directory, 
            string uniqueKey)
        {
            string worksheetsName = _appName;
            string excelFileName = directory + @"\" + _appName + uniqueKey + ".xlsx";
            if (File.Exists(excelFileName))
            {
                File.Delete(excelFileName);
            }
            bool firstRowIsHeader = true;
            try
            {

                var format = new ExcelTextFormat
                {
                    Delimiter = ',',
                    EOL = "\r\n",
                    // DEFAULT IS "\r\n";

                    TextQualifier = '"'
                };

                using (ExcelPackage package = new ExcelPackage(new FileInfo(excelFileName)))
                {
                    ExcelWorksheet worksheet = package.Workbook.Worksheets.Add(worksheetsName);
                    worksheet.Cells["A1"].LoadFromText(new FileInfo(csvFileName), format, OfficeOpenXml.Table.TableStyles.Medium28, firstRowIsHeader);

                    package.SaveAs(new FileInfo(excelFileName));
                }

            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.Message);
            }
            if (File.Exists(excelFileName))
            {
                _columnMapping = excelFileName; 
                return true;
            }
            return false;
        }

        private static void GenerateSchema()
        {
            JSchemaGenerator generator = new JSchemaGenerator
            {
                ContractResolver = new CamelCasePropertyNamesContractResolver()
            };
            JSchema schema = generator.Generate(typeof(Config));
            generator.GenerationProviders.Add(new StringEnumGenerationProvider());
            schema.Title = "DataMasker.Config";
            StringWriter writer = new StringWriter();
            JsonTextWriter jsonTextWriter = new JsonTextWriter(writer);
            schema.WriteTo(jsonTextWriter);
            dynamic parsedJson = JsonConvert.DeserializeObject(writer.ToString());
            dynamic prettyString = JsonConvert.SerializeObject(parsedJson, Formatting.Indented);
            StreamWriter fileWriter = new StreamWriter("DataMasker.Config.schema.json");
            fileWriter.WriteLine(schema.Title);
            fileWriter.WriteLine(new string('-', schema.Title.Length));
            fileWriter.WriteLine(prettyString);
            fileWriter.Close();
        }

        public  static bool CheckAppConfig()
        {
            bool flag = false;
            List<string> allKeys = new List<string>();

            foreach (string key in ConfigurationManager.AppSettings.AllKeys)
            {
                switch (key)
                {
                    case nameof(AppConfig.APP_NAME):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.ConnectionString):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.CurrentInstallerURL):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.CurrentVersionURL):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.ConnectionStringPrd):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.DatabaseName):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.DataSourceType):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.ExcelSheetPath):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.Hostname):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.TestJson):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.WriteDML):
                        bool b = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, b);
                        break;
                    case nameof(AppConfig.MaskedCopyDatabase):
                        bool o = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, o);
                        break;
                    case nameof(AppConfig.RunValidationONLY):
                        bool vo = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, vo);
                        break;
                    case nameof(AppConfig.RunValidation):
                        bool v = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, v);
                        break;
                    case nameof(AppConfig.MaskTabletoSpreadsheet):
                        bool m = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, m);
                        break;
                    case nameof(AppConfig.EmailValidation):
                        bool e = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, e);
                        break;             
                    case nameof(AppConfig.RunTestJson):
                        bool tj = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, tj);
                        break;
                    case nameof(AppConfig.AutoUpdate):
                        bool auto = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, auto);
                        break;
                    default:
                        break;
                }
               
            }
            var missingKey = Enum.GetNames(typeof(AppConfig)).ToList().Except(allkey.Keys.ToList());
            if (allkey.Values.Where(n=>n.Equals(string.Empty)).Count() != 0)
            {
                //var xxx = allkey.Values.Where(n => n.Equals(string.Empty));
                Console.WriteLine("Referencing a null app key value: Mandatory app key value is not set in the App.config" + Environment.NewLine);
                Console.WriteLine(string.Join(Environment.NewLine, allkey.Where(n => n.Value.ToString() == string.Empty).Select(n => n.Key + " : " + n.Value + "Null").ToArray()));
                Console.Title = "Referencing a Null key";
                flag = false;
            }
            else
                flag = true;

            //check email validation and recipient
            if (allkey.Where(n => n.Key.ToUpper().Equals(EmailValidation.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
            {
                if (string.IsNullOrEmpty(fromEmail)
                    || string.IsNullOrEmpty(Recipients))
                {
                    Console.WriteLine("Sending validation email requires fromEmail AND RecipientEmail address to be set in the app.config");
                    return false;
                }
            }
            if (missingKey.Count() != 0)
            {
                Console.WriteLine("Missing App Key in the App.config file" + Environment.NewLine);
                Console.WriteLine("Keys: " + string.Join(" ", missingKey.ToArray()));
                Console.Title = "Referencing a Null key";
                flag = false;
            }
            return flag;
        }
        public enum AppConfig
        {
            ExcelSheetPath,
            DatabaseName,
            WriteDML,
            MaskTabletoSpreadsheet,
            DataSourceType,
            APP_NAME,
            ConnectionString,
            ConnectionStringPrd,
            MaskedCopyDatabase,
            RunValidation,
            RunValidationONLY,
            Hostname,
            TestJson,
            RunTestJson,
            EmailValidation,
            AutoUpdate,
            CurrentVersionURL,
            CurrentInstallerURL
        }
        private static void UpdateProgress(
            ProgressType progressType,
            int current,
            int? max = null,
            string message = null)
        {
            //if (cliOptions.NoOutput)
            //{
            //    return;
            //}

            max = max ??
                  _progressBars[progressType]
                     .ProgressBar.Max;

            _progressBars[progressType]
               .ProgressBar.Max = max.Value;

            message = message ??
                      _progressBars[progressType]
                         .LastMessage;

            _progressBars[progressType]
               .ProgressBar.Refresh(current, message);
        }
        private static void Reportvalidation(DataTable _prdTable, DataTable _maskedTable, DataSourceConfig dataSourceConfig, TableConfig tableConfig)
        {
            CompareLogic compareLogic = new CompareLogic();
            if (!Directory.Exists($@"output\Validation"))
            {
                Directory.CreateDirectory($@"output\Validation");
            }
           
            var _columndatamask = new List<object>();
            var _columndataUnmask = new List<object>();
            //string Hostname = dataSourceConfig.Config.Hostname;
            string schema = tableConfig.Schema;
            string _operator = System.DirectoryServices.AccountManagement.UserPrincipal.Current.DisplayName;

            var result = "";
            var failure = "";

            if (_prdTable.Columns.Count == 0  && tableConfig.Columns.Count() != 0)
            {
                foreach (var col in tableConfig.Columns)
                {
                    _prdTable.Columns.Add(col.Name);
                    
                }
            }
            if (_maskedTable.Columns.Count == 0 && tableConfig.Columns.Count() != 0)
            {
                foreach (var col in tableConfig.Columns)
                {
                    
                    _maskedTable.Columns.Add(col.Name);
                }
            }
            foreach (ColumnConfig dataColumn in tableConfig.Columns)
            {
              
                _columndatamask = new DataView(_maskedTable).ToTable(false, new string[] { dataColumn.Name }).AsEnumerable().Select(n => n[0]).ToList();
                _columndataUnmask = new DataView(_prdTable).ToTable(false, new string[] { dataColumn.Name }).AsEnumerable().Select(n => n[0]).ToList();


                //check for intersect
                List<string> check = new List<string>();
                int rownumber = 0;
                if (_columndatamask.Count == 0)
                {
                    check.Add("PASS");
                }

                for (int i = 0; i < _columndatamask.Count; i++)
                {
                    rownumber = i;


                    if (!_columndatamask[i].IsNullOrDbNull() && !_columndataUnmask[i].IsNullOrDbNull() && !string.IsNullOrWhiteSpace(_columndataUnmask[i].ToString()))
                    {
                        try
                        {
                            if (compareLogic.Compare(_columndatamask[i], _columndataUnmask[i]).AreEqual && dataColumn.Ignore != true)
                            {
                                check.Add("FAIL");
                                                                //match
                            }
                            else
                            {
                                check.Add("PASS");
                            }
                        }
                        catch (IndexOutOfRangeException es)
                        {
                            Console.WriteLine(es.Message);
                        }
                        catch (Exception ex)
                        {

                            Console.WriteLine(ex.Message);
                        }



                    }







                    //unmatch
                }



                if (check.Contains("FAIL"))
                {
                    result = "<font color='red'>FAIL</font>";

                    if (dataColumn.Ignore == true)
                    {
                        failure = "Masking not required";
                        result = "<font color='green'>PASS</font>";
                    }
                    else if (dataColumn.Type == DataType.Shuffle && _columndatamask.Count() == 1)
                    {
                        result = "<b><font color='red'>FAIL</font></b>";
                        failure = "row count must be > 1 for " + DataType.Shuffle.ToString();
                        //result = "<font color='red'>FAIL</font>";
                    }
                    else if (dataColumn.Type == DataType.NoMasking)
                    {
                        result = "<font color='green'>PASS</font>";
                        //result = "<b><font color='blue'>PASS</font></b>";
                        failure = "Masking not required";
                        //result = "<font color='red'>FAIL</font>";
                    }
                    else if (dataColumn.Type == DataType.Shuffle)
                    {
                        result = "<b><font color='blue'>FAIL</font></b>";
                        failure = "Cannot generate a unique shuffle value";
                        //result = "<font color='red'>FAIL</font>";
                    }
                    else if (dataColumn.Type == DataType.exception && check.Contains("PASS"))
                    {
                        failure = "<font color='red'>Applied mask with " + dataColumn.Type.ToString() + "</ font >";
                        result = "<b><font color='blue'>PASS</font></b>";
                    }
                    else if (dataColumn.Type == DataType.City && dataColumn.Ignore == false)
                    {
                        failure = "Same City found but different state or province";
                        result = "<b><font color='blue'>PASS</font></b>";
                    }
                    else
                    {
                        result = "<font color='red'>FAIL</font>";
                        failure = "<b><font color='red'>Found exact match " + dataColumn.Type.ToString() + " </font></b>";
                    }

                    Console.WriteLine(tableConfig.Name + " Failed Validation test on column " + dataColumn.Name + Environment.NewLine);
                    File.AppendAllText(path, tableConfig.Name + " Failed Validation test on column " + dataColumn.Name + Environment.NewLine);

                    report.Rows.Add(tableConfig.Name, tableConfig.Schema, dataColumn.Name, dataSourceConfig.Config.Hostname, dataSourceConfig.Type, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);

                }
                else if (check.Contains("IGNORE"))
                {
                    result = "No Validation";
                    failure = "Column not mask";
                    report.Rows.Add(tableConfig.Name, tableConfig.Schema, dataColumn.Name, dataSourceConfig.Config.Hostname, dataSourceConfig.Type, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);
                }
                else
                {
                    if (_columndatamask.Count == 0)
                    {
                        failure = "No record found";
                    }
                   else if (dataColumn.Ignore == true || dataColumn.Type == DataType.NoMasking)
                    {
                        failure = "Masking not required";
                        result = "<font color='green'>PASS</font>";
                    }
                    else
                        failure = "NULL";
                    result = "<font color='green'>PASS</font>";
                    Console.WriteLine(tableConfig.Name + " Pass Validation test on column " + dataColumn.Name);
                    File.AppendAllText(path, tableConfig.Name + " Pass Validation test on column " + dataColumn.Name + Environment.NewLine);
                    report.Rows.Add(tableConfig.Name, tableConfig.Schema, dataColumn.Name, dataSourceConfig.Config.Hostname, dataSourceConfig.Type, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);


                }

            }
            
            //return report.datar
        }
        private static DataTable DictionariesToDataTable<T>(
        IEnumerable<IDictionary<string, T>> source)
        {
            if (source == null)
            {
                return null;
            }

            var result = new DataTable();
            using (var e = source.GetEnumerator())
            {
                if (!e.MoveNext())
                {
                    return result;
                }

                if (e.Current.Keys.Count == 0)
                {
                    throw new InvalidOperationException();
                }

                var length = e.Current.Keys.Count;

                result.Columns.AddRange(
                    e.Current.Keys.Select(k => new DataColumn(k, typeof(T))).ToArray());

                do
                {
                    if (e.Current.Values.Count != length)
                    {
                        throw new InvalidOperationException();
                    }

                    result.Rows.Add(e.Current.Values);
                }
                while (e.MoveNext());

                return result;
            }
        }
        private static DataTable ConvertToDataTable(IEnumerable<IDictionary<string, object>> dict)
        {
            DataTable dt = new DataTable();

            // Add columns first
            dt.Columns.AddRange(dict.First()
                                       .Select(kvp => new DataColumn() { ColumnName = kvp.Key, DataType = System.Type.GetType("System.String") })
                                       .AsEnumerable()
                                       .ToArray()
                                       );

            // Now add the rows
            dict.SelectMany(Dict => Dict.Select(kvp => new {
                Row = dt.NewRow(),
                Kvp = kvp
            }))
                  .ToList()
                  .ForEach(rowItem => {
                      rowItem.Row[rowItem.Kvp.Key] = rowItem.Kvp.Value;
                      dt.Rows.Add(rowItem.Row);
                  }
                         );
            dt.Dump();
            return dt;
        }
        private static DataTable ToDictionary(IEnumerable<IDictionary<string, object>> list)
        {
            DataTable result = new DataTable();
            if (list.Count() == 0)
                return result;

           

            foreach (IDictionary<string, object> row in list)
            {
                foreach (KeyValuePair<string, object> entry in row)
                {
                    if (!result.Columns.Contains(entry.Key.ToString()))
                    {
                        result.Columns.Add(entry.Key);
                    }
                }
                result.Rows.Add(row.Values.ToArray());
            }

            return result;
        }
    }
    public class DictComparer : IEqualityComparer<Dictionary<string, object>>
    {
        public bool Equals(Dictionary<string, object> x, Dictionary<string, object> y)
        {
            return (x == y) || (x.Count == y.Count && !x.Except(y).Any());
        }

        public int GetHashCode(Dictionary<string, object> x)
        {
            return x.GetHashCode();
        }
    }
    public class OutputCapture : TextWriter, IDisposable
    {
        private TextWriter stdOutWriter;
        public TextWriter Captured { get; private set; }
        public override Encoding Encoding { get { return Encoding.ASCII; } }

        public OutputCapture()
        {
            this.stdOutWriter = Console.Out;
            Console.SetOut(this);
            Captured = new StringWriter();
        }

        override public void Write(string output)
        {
            // Capture the output and also send it to StdOut
            Captured.Write(output);
            stdOutWriter.Write(output);
        }

        override public void WriteLine(string output)
        {
            // Capture the output and also send it to StdOut
            Captured.WriteLine(output);
            stdOutWriter.WriteLine(output);
        }
    }
}
