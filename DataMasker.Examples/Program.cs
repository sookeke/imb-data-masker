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
using MoreLinq;
using DataMasker.MaskingValidation;
using ChoETL;
using DataMasker.DataSources;

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
        private static string _nameDatabase;
        private static string _SpreadSheetPath;
        private static DataTable PrdTable = null;
        private static DataTable MaskTable = null;
        private static int count;
        private static DataTable report = new DataTable();  
        private static List<string> _colError = new List<string>();
        private static List<KeyValuePair<string, string>> collist = new List<KeyValuePair<string, string>>();
        private static List<KeyValuePair<string, Dictionary<string, string>>> jsconfigTable = new List<KeyValuePair<string, Dictionary<string, string>>>();
        private static List<KeyValuePair<string, Dictionary<string, string>>> copyJsTable = new List<KeyValuePair<string, Dictionary<string, string>>>();
        private static List<KeyValuePair<string, Dictionary<string, string>>> _allNull = new List<KeyValuePair<string, Dictionary<string, string>>>();
        private static readonly Dictionary<ProgressType, ProgressbarUpdate> _progressBars = new Dictionary<ProgressType, ProgressbarUpdate>();
        private static string _exceptionpath = Directory.GetCurrentDirectory() + $@"\Output\MaskExceptions.txt";
        private static string copyjsonPath = ConfigurationManager.AppSettings["jsonPath"];
        private static readonly Dictionary<string, object> allkey = new Dictionary<string, object>();


        public static string jsonpath { get; private set; }
        public static string createDir { get; private set; }
        #endregion
        private static void Main(
            string[] args)
        {
            report.Columns.Add("Table"); report.Columns.Add("Column"); report.Columns.Add("Hostname"); report.Columns.Add("TimeStamp"); report.Columns.Add("Operator"); report.Columns.Add("Row count mask"); report.Columns.Add("Row count prd"); report.Columns.Add("Result"); report.Columns.Add("Result Comment");
            Example1();
        }
        private static void JsonConfig(string json)
        {
            #region Initialize system variables


            //generate list of words for comments like columns
            List<string> _comment = new List<string> { "Description", "DESCRIPTION", "TEXT", "MEMO", "describing", "Descr", "COMMENT", "comment", "NOTE", "Comment", "REMARK", "remark", "DESC" };
            List<string> _fullName = new List<string> { "full name", "AGENT_NAME", "OFFICER_NAME", "FULL_NAME", "CONTACT_NAME", "MANAGER_NAME" };
            List<Table> tableList = new List<Table>();
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
                    Column column = new Column
                    {
                        name = col.ColumnName,
                        retainNullValues = true,
                        StringFormatPattern = ""


                    };
                    var rule = col.MaskingRule;

                    if (col.MaskingRule.Contains("No masking"))
                    {
                        column.type = "NoMasking";
                        column.ignore = true;
                    }
                    else if (col.MaskingRule.Contains("Shuffle"))
                    {
                        column.type = "Shuffle";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("FIRST_NAME") || col.ColumnName.ToUpper().Contains("MIDDLE_NAME"))
                    {
                        column.type = "Bogus";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{NAME.FIRSTNAME}}";
                        column.useGenderColumn = "Gender";
                    }
                    else if (col.DataType.ToUpper().Equals("BLOB") || col.DataType.ToUpper().Equals("IMAGE"))
                    {
                        column.type = "Blob";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "Gender";
                    }
                    else if (col.DataType.ToUpper().Equals("CLOB"))
                    {
                        column.type = "Clob";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("CITY"))
                    {
                        column.type = "Bogus";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{ADDRESS.CITY}}";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("COUNTRY"))
                    {
                        column.type = "Bogus";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{ADDRESS.COUNTRY}}";
                        column.useGenderColumn = "";
                    }
                    else if (col.DataType.ToUpper().Equals("SDO_GEOMETRY") || col.DataType.ToUpper().ToUpper().Contains("GEOMETRY"))
                    {
                        column.type = "Geometry";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("SURNAME") || col.ColumnName.ToUpper().Contains("LASTNAME") || col.ColumnName.ToUpper().Contains("LAST_NAME"))
                    {
                        column.type = "Bogus";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{NAME.LASTNAME}}";
                        column.useGenderColumn = "Gender";
                    }
                    else if (col.ColumnName.ToUpper().Contains("COMPANY_NAME") || col.ColumnName.ToUpper().Contains("ORGANIZATION_NAME"))
                    {
                        column.type = "Company";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{name.lastname}} {{company.companysuffix}}";
                        column.useGenderColumn = "";
                    }
                    else if (_comment.Any(n => col.ColumnName.ToUpper().Contains(n)) || (col.DataType.ToUpper().Contains("CHAR") && Convert.ToInt32(col.DataType.ToUpper().Replace("(", " ").Replace(")", " ").Split(' ')[1]) > 1000) || _comment.Any(x => col.Comments.Contains(x)))
                    {
                        var size = col.DataType.ToUpper().Replace("(", " ").Replace(")", " ").Split(' ');
                        if (size.Count() > 1)
                        {
                            var sizze = size[1].ToString();
                            if (!string.IsNullOrEmpty(sizze))
                            {
                                column.type = "Rant";
                                column.max = Convert.ToInt32(sizze);
                                column.min = col.Min.ToString(); ;
                                column.StringFormatPattern = "DESCRIPTION";
                                column.useGenderColumn = "";
                            }
                            else
                            {
                                column.type = "Rant";
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min.ToString(); ;
                                column.StringFormatPattern = "DESCRIPTION";
                                column.useGenderColumn = "";
                            }
                        }
                        else
                        {
                            column.type = "Rant";
                            column.max = col.Max.ToString(); ;
                            column.min = col.Min.ToString(); ;
                            column.StringFormatPattern = "DESCRIPTION";
                            column.useGenderColumn = "";
                        }//split varchar(20 byte) and get max number

                    }
                    else if (col.DataType.ToUpper().Contains("DATE"))
                    {
                        column.type = "DateOfBirth";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Equals("YEAR"))
                    {
                        column.type = "RandomYear";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("PHONE_NO") || col.ColumnName.ToUpper().Contains("FAX_NO") || col.ColumnName.ToUpper().Contains("CONTRACT_NO") || col.ColumnName.ToUpper().Contains("CELL") || col.ColumnName.ToUpper().Contains("_PHONE") || col.ColumnName.ToUpper().Contains("PHONENUMBER"))
                    {
                        if (col.DataType.ToUpper().Contains("NUMBER"))
                        {
                            column.type = "PhoneNumberInt";
                            column.max = col.Max.ToString(); ;
                            column.min = col.Min.ToString(); ;
                            column.StringFormatPattern = "##########";
                            column.useGenderColumn = "";
                        }
                        else
                        {
                            column.type = "PhoneNumber";
                            column.max = col.Max.ToString(); ;
                            column.min = col.Min.ToString(); ;
                            column.StringFormatPattern = "(###)-###-####";
                            column.useGenderColumn = "";
                        }
                    }
                    else if (col.ColumnName.ToUpper().Contains("EMAIL_ADDRESS"))
                    {
                        column.type = "Bogus";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{INTERNET.EMAIL}}";
                        column.useGenderColumn = "Gender";
                    }
                    else if (col.DataType.ToUpper().Contains("MONEY"))
                    {
                        column.type = "Bogus";
                        column.max = col.Max.ToString();
                        column.min = col.Min.ToString();
                        column.StringFormatPattern = "{{FINANCE.AMOUNT}}";
                        //column.useGenderColumn = "Gender";
                    }
                    else if (col.ColumnName.ToUpper().Contains("POSTAL_CODE"))
                    {
                        column.type = "PostalCode";
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
                                column.type = "PickRandom";
                                column.max = Convert.ToInt32(charSize);
                                column.min = col.Min.ToString(); ;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "Y,N";
                            }
                            else
                            {
                                column.type = "Ignore";
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min.ToString(); ;
                                column.ignore = true;
                                column.StringFormatPattern = "";
                                column.useGenderColumn = "";
                            }
                        }
                        else
                        {
                            column.type = "Ignore";
                            column.max = col.Max.ToString(); ;
                            column.min = col.Min.ToString(); ;
                            column.ignore = true;
                            column.StringFormatPattern = "";
                            column.useGenderColumn = "";
                        }
                    }
                    else if (col.ColumnName.ToUpper().Contains("ADDRESS") || col.Comments.Contains("address"))
                    {
                        var size = col.DataType.ToUpper().Replace("(", " ").Replace(")", " ").Split(' ');
                        if (size.Count() > 1)
                        {
                            var sizze = size[1].ToString();
                            if (!string.IsNullOrEmpty(sizze))
                            {
                                column.type = "Bogus";
                                column.max = Convert.ToInt32(sizze);
                                column.min = col.Min.ToString(); ;
                                column.StringFormatPattern = "{{address.fullAddress}}";
                                column.useGenderColumn = "";
                            }
                            else
                            {
                                column.type = "Bogus";
                                column.max = col.Max.ToString(); ;
                                column.min = col.Min.ToString(); ;
                                column.StringFormatPattern = "{{address.fullAddress}}";
                                column.useGenderColumn = "";
                            }
                        }
                        else
                        {
                            column.type = "Bogus";
                            column.max = col.Max.ToString(); ;
                            column.min = col.Min.ToString(); ;
                            //column.StringFormatPattern = "{{ADDRESS.STREETADDRESS}} {{ADDRESS.CITY}} {{ADDRESS.STATE}}";
                            column.StringFormatPattern = "{{address.fullAddress}}";
                            column.useGenderColumn = "";
                        }
                    }
                    else if (col.ColumnName.ToUpper().Contains("USERID"))
                    {
                        column.type = "RandomUsername";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        //column.StringFormatPattern = "{{ADDRESS.STREETADDRESS}} {{ADDRESS.CITY}} {{ADDRESS.STATE}}";
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("FILE_NAME"))
                    {
                        column.type = "Bogus";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        //column.StringFormatPattern = "{{ADDRESS.STREETADDRESS}} {{ADDRESS.CITY}} {{ADDRESS.STATE}}";
                        column.StringFormatPattern = "{{SYSTEM.FILENAME}}";
                        column.useGenderColumn = "";
                    }
                    else if (_fullName.Any(n => col.ColumnName.ToUpper().Contains(n)) || _fullName.Any(x => col.Comments.Contains(x)))
                    {
                        column.type = "Bogus";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min.ToString(); ;
                        //column.StringFormatPattern = "{{ADDRESS.STREETADDRESS}} {{ADDRESS.CITY}} {{ADDRESS.STATE}}";
                        column.StringFormatPattern = "{{NAME.FULLNAME}}";
                        column.useGenderColumn = "Gender";
                    }
                    else if (col.ColumnName.ToUpper().Contains("AMOUNT") || col.ColumnName.ToUpper().Contains("AMT") || col.Comments.Contains("Amount"))
                    {
                        column.type = "RandomDec";
                        column.max = col.Max.ToString(); ;
                        column.min = col.Min;
                        column.StringFormatPattern = "";
                        column.useGenderColumn = "";
                    }
                    else
                    {
                        if (col.ColumnName.ToUpper().Equals("NAME")) //set company name
                        {
                            column.type = "Bogus";
                            //column.type = col.DATA_TYPE;
                            column.StringFormatPattern = "{{COMPANY.COMPANYNAME}}";
                            column.max = col.Max.ToString(); ;
                            column.min = col.Min.ToString(); ;
                            column.useGenderColumn = "";
                        }
                        else if (col.DataType.ToUpper().Contains("NUMBER"))
                        {
                            //var size = col.DataType.ToUpper().Replace("(", " ").Replace(")", " ").Split(' ')[1].Split(',')[0].ToString();

                            column.type = "RandomInt";
                            column.max = col.Max.ToString(); ;
                            column.min = col.Min;
                            column.StringFormatPattern = "";
                            column.useGenderColumn = "";

                        }
                        else if (col.ColumnName.ToUpper().Equals("TOTAL_AREA")) //set company name
                        {
                            column.type = "Bogus";
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
                            column.type = "Ignore";
                            //column.type = col.DATA_TYPE;
                            column.StringFormatPattern = "";
                            column.max = col.Max.ToString();
                            column.min = col.Min.ToString();
                            column.useGenderColumn = "";
                            column.ignore = true;

                        }
                    }
                    if (!col.ColumnName.Equals(table.primaryKeyColumn))
                    {
                        colList.Add(column);
                    }
                    
                }

                // if (colList.Count > 0)
                // {
                table.columns = colList;

                tableList.Add(table);
                //}


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
            #region Check fortables without Primary Key if true exit
            var noPrimaryKey = rootObj.Where(n => n.PKconstraintName == null || n.PKconstraintName == string.Empty).GroupBy(n => n.TableName);
            //var cou = noPrimaryKey.Count();
            //primary key applied to relational database not spreadsheet         
            if (noPrimaryKey.Count() != 0 && ConfigurationManager.AppSettings["DataSourceType"] != "SpreadSheet")
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
            jsonpath = ConfigurationManager.AppSettings["jsonMapPath"];
            string jsonresult = JsonConvert.SerializeObject(rootObject1,Formatting.Indented);


            #region compare original jsonconfig for datatype errors
            if (File.Exists(jsonpath) && new FileInfo(jsonpath).Length != 0)
            {
                var rootConfig = Config.Load(jsonpath);
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
                if (copyJsTable.Count == jsconfigTable.Count)
                {
                    for (int i = 0; i < copyJsTable.Count; i++)
                    {

                        if (!jsconfigTable[i].Value.SequenceEqual(copyJsTable[i].Value))
                        {
                            var mapped = string.Join(",", jsconfigTable[i].Value.Select(n => n.Value).ToArray());
                            Console.WriteLine(jsconfigTable[i].Key.ToString() + " " + string.Join(",", copyJsTable[i].Value.ToArray()) + " now mapped with: " + mapped);
                        }
                        else if (jsconfigTable[i].Value.Where(n => n.Value.Equals("Ignore")).Count() != 0)
                        {
                            var xxxx = jsconfigTable[i].Value.Where(n => n.Value.Equals("Ignore")).ToDictionary(n => n.Key, n => n.Value);
                            //exit
                            _allNull.Add(new KeyValuePair<string, Dictionary<string, string>>(string.Join("", jsconfigTable[i].Key.ToArray()).ToString(), xxxx));

                        }


                    }
                    if (_allNull.Count() != 0)
                    {
                        int notmasked = 1;
                        _colError.Add("These columns below will not be masked" + Environment.NewLine + Environment.NewLine);
                        for (int i = 0; i < _allNull.Count; i++)
                        {
                            //_colError.Add("")
                            //Console.WriteLine(string.Join("", _allNull[i].Key.ToArray()) + " contains column with ignore datatype/columns" + " " + string.Join(Environment.NewLine, _allNull[i].Value.Select(n => n.Key + " :" + n.Value).ToArray()));
                            _colError.Add(notmasked++.ToString() + ". " + string.Join(" ", _allNull[i].Key.ToArray()) + " contains column with ignore datatype/columns" + " " + string.Join(Environment.NewLine, _allNull[i].Value.Select(n => n.Key + " :" + n.Value).ToArray()) + Environment.NewLine);
                        }
                        string value = string.Join("", _colError.ToArray());
                        Console.Write(Environment.NewLine + "These columns have ignore datatype so will not be masked" + Environment.NewLine);
                        Console.Write(value);
                        Console.Write(Environment.NewLine);

                        Console.WriteLine("Do you want to continue and ignore masking these columns? [yes/no]");
                        string option = Console.ReadLine();
                        if (option == "yes")
                        {
                            JObject o1 = JObject.Parse(File.ReadAllText(jsonpath));
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
                using (var tw = new StreamWriter(jsonpath, false))
                {
                    tw.WriteLine(jsonresult.ToString());
                    tw.Close();
                    Console.WriteLine("{0}{1}","Maped Json".ToUpper() + Environment.NewLine, jsonresult);
                }
                //check map failures and write to file then exit for correction
                if (count != 0)
                {
                    string colfailed = count + " columns cannot be mapped to a masking datatype and so will be ignored during masking. Review the " + jsonpath +" and provide mask datatype for these columns " + Environment.NewLine + Environment.NewLine + string.Join(Environment.NewLine, collist.Select(x => x.Key + " ON TABLE " + x.Value).ToArray());
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

            [DefaultValue("")]
            [JsonProperty("Min", DefaultValueHandling = DefaultValueHandling.Populate)]
            public string Min { get; set; }
            [DefaultValue("")]
            [JsonProperty("Max", DefaultValueHandling = DefaultValueHandling.Populate)]
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
            [JsonProperty("Conversion Consideration (NEEDS BUSINESS DISCUSSION)")]
            public string Consideration { get; set; }
        }

        private static Config LoadConfig(
            int example)
        {
            return Config.Load(jsonpath);
            //return Config.Load($@"\\SFP.IDIR.BCGOV\U130\SOOKEKE$\Masking_sample\APP_GWP_config.json");
        }

        public static void Example1()
        {
            if (!CheckAppConfig())
            {
                Console.WriteLine("Program will exit: Press ENTER to exist..");
                Console.ReadLine();
                System.Environment.Exit(1);
            }
            ////{ throw new NullReferenceException("Referencing a null app key value"); }
            _nameDatabase = ConfigurationManager.AppSettings["DatabaseName"];
            _SpreadSheetPath = ConfigurationManager.AppSettings["ExcelSheetPath"];
            if (string.IsNullOrEmpty(_nameDatabase)) { throw new ArgumentException("Database name cannot be null, check app.config and specify the database name", _nameDatabase); }
            copyjsonPath = ExcelToJson.toJson(_SpreadSheetPath);
            JsonConfig(copyjsonPath);
            Config config = LoadConfig(1);
            IDataMasker dataMasker = new DataMasker(new DataGenerator(config.DataGeneration));
            IDataSource dataSource = DataSourceProvider.Provide(config.DataSource.Type, config.DataSource);
            foreach (TableConfig tableConfig in config.Tables)
            {
                //checked if table contains blob column datatype and get column that is blob
                var isblob = tableConfig.Columns.Where(x => !x.Ignore && x.Type == DataType.Blob);
                object extension = null;
                string[] extcolumn = null;
                IEnumerable<IDictionary<string, object>> rows = null;
                IEnumerable<IDictionary<string, object>> rawData = null;
                File.WriteAllText(_exceptionpath, "exception for " + tableConfig.Name + ".........." + Environment.NewLine + Environment.NewLine);
                if (config.DataSource.Type == DataSourceType.SpreadSheet)
                {
                    //load spreadsheet to dataTable
                    var SheetTable = dataSource.DataTableFromCsv(ConfigurationManager.AppSettings["ConnectionString"]);
                    //convert DataTable to object
                    rows = dataSource.CreateObject(SheetTable);
                    rawData = dataSource.CreateObject(SheetTable);
                    foreach (IDictionary<string, object> row in rows)
                    {
                        dataMasker.Mask(row, tableConfig, dataSource, SheetTable);
                    }
                    try
                    { 
                        //convert the object to DataTable
                       var _maskSpreadSheet = dataSource.SpreadSheetTable(rows, tableConfig);
                        MaskTable = _maskSpreadSheet;
                        PrdTable = dataSource.SpreadSheetTable(rawData, tableConfig);
                        if (_maskSpreadSheet.Rows.Count != 0)
                        {
                            var csvFile = writeTofile(_maskSpreadSheet, _nameDatabase, "_Masked_" + Guid.NewGuid().ToString());
                            var createsheet = toExcel(csvFile, _nameDatabase, _nameDatabase, "_Masked_" + Guid.NewGuid().ToString());
                            if (createsheet == false)
                            {
                                Console.WriteLine("cannot create excel file");
                            }
                            //convert to DML
                            #region DML Script

                            if (allkey.Where(n => n.Key.ToUpper().Equals("WRITEDML")).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
                            {
                                _maskSpreadSheet.TableName = tableConfig.Name;
                                createDir = Directory.GetCurrentDirectory() + @"\output\" + _nameDatabase + @"\";
                                if (!Directory.Exists(createDir))
                                {
                                    Directory.CreateDirectory(createDir);
                                }
                                string writePath = createDir + @"\" + tableConfig.Name + ".sql";
                                var multimedia = tableConfig.Columns.Where(n => n.Type == DataType.Blob).Select(n => n.Name);
                                if (multimedia.Count() != 0)
                                {
                                    extcolumn = new string[] { string.Join("", multimedia.ToArray()[0].ToArray()) };
                                }

                                var insertSQL = SqlDML.GenerateInsert(_maskSpreadSheet, extcolumn, null, null, writePath, config, tableConfig);
                                //MaskValidationCheck.verification(config.DataSource, config);
                            }
                            #endregion

                        }
                    }
                    catch (Exception ex)
                    {
                        //string path = Directory.GetCurrentDirectory() + $@"\Output\MaskedExceptions.txt";
                        File.WriteAllText(_exceptionpath, ex.Message + Environment.NewLine + Environment.NewLine);
                        Console.WriteLine(ex.Message);
                    }
                }
                else
                {
                    rows = dataSource.GetData(tableConfig);

                    rawData = dataSource.RawData(null);
                    //rawData = dataSource.GetData(tableConfig);
                    foreach (IDictionary<string, object> row in rows)
                    {
                        
                        if (isblob.Count() == 1 && row.Select(n => n.Key).ToArray().Where(x => x.Equals(string.Join("", isblob.Select(n => n.StringFormatPattern)))).Count() > 0)
                        {
                            extension = row[string.Join("", isblob.Select(n => n.StringFormatPattern))];
                            dataMasker.MaskBLOB(row, tableConfig, dataSource, extension.ToString(), extension.ToString().Substring(extension.ToString().LastIndexOf('.') + 1));
                        }
                        else
                        {
                            dataMasker.Mask(row, tableConfig, dataSource);
                        }
                        Console.WriteLine(extension);
                       

                        //rowIndex++;

                        //update per row, or see below,
                        //dataSource.UpdateRow(row, tableConfig);
                        //UpdateProgress(ProgressType.Masking, rowIndex);
                    }

                    //update all rows
                    Console.WriteLine("writing table " + tableConfig.Name + " on database " + _nameDatabase + "" + " .....");
                    try
                    {
                        #region Create DML Script
                        var _dmlTable = dataSource.SpreadSheetTable(rows, tableConfig);
                        MaskTable = _dmlTable;
                        PrdTable = dataSource.SpreadSheetTable(rawData, tableConfig);

                        if (allkey.Where(n => n.Key.ToUpper().Equals("WRITEDML")).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
                        {
                           
                            _dmlTable.TableName = tableConfig.Name;
                            createDir = Directory.GetCurrentDirectory() + @"\output\" + _nameDatabase + @"\";
                            if (!Directory.Exists(createDir))
                            {
                                Directory.CreateDirectory(createDir);
                            }
                            string writePath = createDir + @"\" + tableConfig.Name + ".sql";
                            var multimedia = tableConfig.Columns.Where(n => n.Type == DataType.Blob).Select(n => n.Name);
                            if (multimedia.Count() != 0)
                            {
                                extcolumn = new string[] { string.Join("", multimedia.ToArray()[0].ToArray()) };
                            }


                            var insertSQL = SqlDML.GenerateInsert(_dmlTable, extcolumn, null, null, writePath, config, tableConfig);
                            if (allkey.Where(n => n.Key.ToUpper().Equals("MaskTabletoSpreadsheet".ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
                            {
                                //remove blob type
                                //foreach (DataColumn dataColumn in _dmlTable.Columns)
                                //{
                                //    if (dataColumn.DataType == typeof(byte[])) { _dmlTable.Columns.Remove(dataColumn); }
                                   
                                //}
                                //write masked Table to SpreadSheet
                                SqlDML.DataTableToExcelSheet(_dmlTable, createDir + @"\" + tableConfig.Name + ".xlsx", tableConfig);


                            }
                        }
                        if (allkey.Where(n => n.Key.ToUpper().Equals("RunValidation".ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true)
                            && PrdTable.Rows != null && MaskTable.Rows != null
                            && allkey.Where(n => n.Key.ToUpper().Equals("MaskedCopyDatabase".ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(false))
                        {
                            reportvalidation(PrdTable, MaskTable, config.DataSource, tableConfig);
                            

                        }
                        #endregion
                        if (allkey.Where(n => n.Key.ToUpper().Equals("MaskedCopyDatabase".ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
                        {
                            dataSource.UpdateRows(rows, tableConfig);
                        }
                       
                    }
                    catch (Exception ex)
                    {
                        //string path = Directory.GetCurrentDirectory() + $@"\Output\MaskedExceptions.txt";
                        //File.WriteAllText(_exceptionpath, ex.Message + Environment.NewLine + Environment.NewLine);
                        Console.WriteLine(ex.Message);
                    }
                }

                // UpdateProgress(ProgressType.Overall, i + 1);
                // i++;


            }
            //write mapped table and column with type in csv file
            var o = OutputSheet(config, copyjsonPath, _nameDatabase);
            string sheetPath = ConfigurationManager.AppSettings["ExcelSheetPath"];
            if (report.Rows.Count != 0)
            {
                
                MaskValidationCheck.Analysis(report, config.DataSource, sheetPath, _nameDatabase, createDir);
            }

            #region validate masking 
            if (allkey.Where(n => n.Key.ToUpper().Equals("RunValidation".ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true) && allkey.Where(n => n.Key.ToUpper().Equals("MaskedCopyDatabase".ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
            {
                Console.WriteLine("Data Masking Validation started......................................");
                MaskValidationCheck.verification(config.DataSource, config, sheetPath,createDir, _nameDatabase);
            }

                
            


            #endregion


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



                    var minMax = Convert.ToString(config.Tables[i].Columns[j].Min) + " - " + Convert.ToString(config.Tables[i].Columns[j].Max);
                    dt.Rows.Add(config.Tables[i].Name, config.Tables[i].Columns[j].Name, rootObj[j + h].MaskingRule, config.Tables[i].Columns[j].Type, config.Tables[i].Columns[j].Ignore, config.Tables[i].Columns[j].StringFormatPattern, minMax);

                    k++;
                }
                h = k;

            }

            if (dt.Rows != null)
            {
                var csv = writeTofile(dt, _appname, "_MASKING_APPLIED");
                var createsheet = toExcel(csv, _appname, _appname, "_MASKING_APPLIED");
                if (createsheet == false)
                {
                    
                    Console.WriteLine("cannot create excel file");
                    return false;
                }
            }
            return true;
        }
        private static string writeTofile(DataTable textTable, string directory, string uniquekey)
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

        private static bool toExcel(
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

                var format = new ExcelTextFormat();
                format.Delimiter = ',';
                format.EOL = "\r\n";
                // DEFAULT IS "\r\n";

                format.TextQualifier = '"';

                using (ExcelPackage package = new ExcelPackage(new FileInfo(excelFileName)))
                {
                    ExcelWorksheet worksheet = package.Workbook.Worksheets.Add(worksheetsName);
                    worksheet.Cells["A1"].LoadFromText(new FileInfo(csvFileName), format, OfficeOpenXml.Table.TableStyles.Medium27, firstRowIsHeader);

                    package.SaveAs(new FileInfo(excelFileName));
                }

            }
            catch (Exception)
            {

                throw;
            }
            if (File.Exists(excelFileName))
            {
                return true;
            }
            return false;
        }

        private static void GenerateSchema()
        {
            JSchemaGenerator generator = new JSchemaGenerator();

            generator.ContractResolver = new CamelCasePropertyNamesContractResolver();
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
                    case nameof(AppConfig.jsonMapPath):
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
                    case nameof(AppConfig.RunValidation):
                        bool v = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, v);
                        break;
                    case nameof(AppConfig.MaskTabletoSpreadsheet):
                        bool m = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, m);
                        break;
                    default:
                        break;
                }

                
            }

            if (allkey.Values.Where(n=>n.Equals(string.Empty)).Count() != 0)
            {
                //var xxx = allkey.Values.Where(n => n.Equals(string.Empty));
                Console.WriteLine(new NullReferenceException("Referencing a null app key value: Mandatory app key value is not set in the App.config" + Environment.NewLine));
                Console.WriteLine(string.Join(Environment.NewLine, allkey.Where(n => n.Value.ToString() == string.Empty).Select(n => n.Key + " : " + n.Value + "Null").ToArray()));
                flag = false;
            }
            else
                flag = true;
            return flag;
        }
        public enum AppConfig
        {
            ExcelSheetPath,
            jsonMapPath,
            DatabaseName,
            WriteDML,
            MaskTabletoSpreadsheet,
            DataSourceType,
            APP_NAME,
            ConnectionString,
            ConnectionStringPrd,
            MaskedCopyDatabase,
            RunValidation,
            Hostname
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
        private static void reportvalidation(DataTable _prdTable, DataTable _maskedTable, DataSourceConfig dataSourceConfig, TableConfig tableConfig)
        {
            string path = Directory.GetCurrentDirectory() + $@"\Output\Validation\ValidationResult.txt";
            var _columndatamask = new List<object>();
            var _columndataUnmask = new List<object>();
            string Hostname = dataSourceConfig.Config.Hostname;
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
                    if (!_columndatamask[i].IsNullOrDbNull())
                    {
                        try
                        {
                            if (_columndatamask[i].Equals(_columndataUnmask[i]))
                            {
                                if (dataColumn.Type == DataType.Shuffle)
                                {
                                    var mask = _columndatamask[i];
                                    var prd = _columndataUnmask[i];
                                }
                                //else
                                //{
                                check.Add("FAIL");
                                //}


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
                    else
                    {
                        result = "<font color='red'>FAIL</font>";
                        failure = "<b><font color='red'>Found exact match "+dataColumn.Type.ToString() +" </font></b>";
                    }

                    Console.WriteLine(tableConfig.Name + " Failed Validation test on column " + dataColumn.Name + Environment.NewLine);
                    File.AppendAllText(path, tableConfig.Name + " Failed Validation test on column " + dataColumn.Name + Environment.NewLine);
                    
                    report.Rows.Add(tableConfig.Name, dataColumn.Name, Hostname, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);

                }
                else if (check.Contains("IGNORE"))
                {
                    result = "No Validation";
                    failure = "Column not mask";
                    report.Rows.Add(tableConfig.Name, dataColumn.Name, Hostname, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);
                }
                else
                {
                    if (_columndatamask.Count == 0)
                    {
                        failure = "No record found";
                    }
                    else
                        failure = "null";
                    result = "<font color='green'>PASS</font>";
                    Console.WriteLine(tableConfig.Name + " Pass Validation test on column " + dataColumn.Name);
                    File.AppendAllText(path, tableConfig.Name + " Pass Validation test on column " + dataColumn.Name + Environment.NewLine);
                    report.Rows.Add(tableConfig.Name, dataColumn.Name, Hostname, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);


                }

            }
            
            //return report.datar
        }
    }
}
