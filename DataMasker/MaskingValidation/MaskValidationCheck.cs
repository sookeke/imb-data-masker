using ChoETL;
using DataMasker.Interfaces;
using DataMasker.Models;
using KellermanSoftware.CompareNetObjects;
using Microsoft.Exchange.WebServices.Data;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;

namespace DataMasker.MaskingValidation
{
    public static class MaskValidationCheck
    {
        private static readonly string TSchema = ConfigurationManager.AppSettings["APP_NAME"];
        private static readonly string Stype = ConfigurationManager.AppSettings["DataSourceType"];
        private static readonly string _exceptionpath = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];

        public static string Jsonpath { get; private set; }
        public static string ZipName { get; private set; }

        public static string ToHTML(DataTable dt, IList ts)
        {
            if (dt.Rows.Count == 0) return ""; // enter code here

            StringBuilder builder = new StringBuilder();
            builder.Append("<html>");

            builder.Append("<body>");
            builder.Append("<table border='1px' cellpadding='5' cellspacing='0' ");
            builder.Append("style='border: solid 1px Silver; font-size: x-small;'>");
            builder.Append("<html><style>BODY{font-family: Arial; font-size: 8pt;}H1{font-size: 22px; font-family: 'Segoe UI Light','Segoe UI','Lucida Grande',Verdana,Arial,Helvetica,sans-serif;}H2{font-size: 18px; font-family: 'Segoe UI Light','Segoe UI','Lucida Grande',Verdana,Arial,Helvetica,sans-serif;}H3{font-size: 16px; font-family: 'Segoe UI Light','Segoe UI','Lucida Grande',Verdana,Arial,Helvetica,sans-serif;}TABLE{border: 1px solid black; border-collapse: collapse; font-size: 8pt;}TH{border: 1px solid #969595; background: #dddddd; padding: 5px; color: #000000;}TD{border: 1px solid #969595; padding: 5px; }td.pass{background: #B7EB83;}td.warn{background: #FFF275;}td.fail{background: #FF2626; color: #ffffff;}td.info{background: #85D4FF;}</style><body>");
            builder.Append("<tr align='left' valign='top'>");
            builder.Append(" </ body ></ html > ");
            foreach (DataColumn c in dt.Columns)
            {
                builder.Append("<td align='left' valign='top'><b>");
                builder.Append(System.Web.HttpUtility.HtmlEncode(c.ColumnName));
                builder.Append("</b></td>");
            }
            builder.Append("<br/>");
            builder.Append("<br/>");
            builder.Append("</tr>");
            foreach (DataRow r in dt.Rows)
            {
                builder.Append("<tr align='left' valign='top'>");
                foreach (DataColumn c in dt.Columns)
                {
                    builder.Append("<td align='left' valign='top'>");
                    builder.Append(r[c.ColumnName]);
                    builder.Append("</td>");
                }
                builder.Append("</tr>");
            }
            builder.Append("</table>");
            StringBuilder sb = new StringBuilder();
            builder.Append("<table>");
            foreach (var item in ts)
            {
                builder.AppendFormat("<tr><td>{0}</td></tr>", item);
            }
            builder.Append("</table>");
            builder.Append("</body>");
            builder.Append("</html>");

            return builder.ToString();
        }
        private static string GetSignature()
        {
            string final = "";
            if (File.Exists(@"Signature.htm"))
            {
                return File.ReadAllText(@"Signature.htm");
            }
            else
            {
                //string _successfulCommit = Directory.GetCurrentDirectory();
                final = "Thank you <br/>"
                    ;

            }
            return final;
        }
        public static void SendMail(string signature, string body, string database, int tcount, 
            int ccount, int pass, int fail, decimal error, 
            string _appSpreadsheet,string exceptionPath, string columnMapping)
        {
            try
            {
                string fromEmail = ConfigurationManager.AppSettings["fromEmail"];
                var toEmail = ConfigurationManager.AppSettings["RecipientEmail"].Split(';').ToList();
                var ccEmaill = ConfigurationManager.AppSettings["cCEmail"].Split(';').ToList();
                var jsonPath = Directory.GetCurrentDirectory() + @"\" + ConfigurationManager.AppSettings["jsonMapPath"];
                var TestJson = Directory.GetCurrentDirectory() + @"\" + ConfigurationManager.AppSettings["TestJson"];
                var MaskedCopyDatabase = Directory.GetCurrentDirectory() + @"\" + ConfigurationManager.AppSettings["MaskedCopyDatabase"];
                var _successfulCommit = Directory.GetCurrentDirectory() + @"\" + ConfigurationManager.AppSettings["_successfulCommit"];
                ExchangeService service = new ExchangeService
                {
                    UseDefaultCredentials = true
                };
                //service.Credentials = new NetworkCredential(user, decryptPassword);
                service.AutodiscoverUrl(fromEmail);
                EmailMessage email = new EmailMessage(service)
                {
                    Subject = "Data Masking Validation Test Report Analysis for " + database,

                    Body = new MessageBody(BodyType.HTML, "<p>This is an automated email for data masking verification and validation test report analysis for " + database + "<br />" + "</p> " +
                    body + Environment.NewLine + " " + "<br />" +
                signature +
                "<br />"
                ),
                    From = new EmailAddress(fromEmail)
                };


                //email.From = "mcs@gov.bc.ca";
                bool tj = ConfigurationManager.AppSettings["RunTestJson"].ToString().ToUpper().Equals("YES") ? true : false;
                bool maskCopy = ConfigurationManager.AppSettings["MaskedCopyDatabase"].ToString().ToUpper().Equals("YES") ? true : false;

                email.ToRecipients.AddRange(toEmail);
                email.CcRecipients.AddRange(ccEmaill);
                if (File.Exists(_appSpreadsheet)) { email.Attachments.AddFileAttachment(_appSpreadsheet); }  
                if (File.Exists(ZipName)) { email.Attachments.AddFileAttachment(ZipName); }
                if (File.Exists(exceptionPath)) { email.Attachments.AddFileAttachment(exceptionPath); }


                if (tj)
                {
                    if (File.Exists(TestJson))
                    {
                        email.Attachments.AddFileAttachment(TestJson);
                    }

                }
                else
                {
                    if (File.Exists(jsonPath))
                    {
                        email.Attachments.AddFileAttachment(jsonPath);
                    }
                    if (maskCopy && File.Exists(_successfulCommit))
                    {
                        email.Attachments.AddFileAttachment(_successfulCommit);
                    }
                    if (File.Exists(columnMapping))
                    {
                        email.Attachments.AddFileAttachment(columnMapping);
                    }
                }
                       
               
                    

                //Console.WriteLine("start to send email from IDIR to TIDIR ...");
                email.SendAndSaveCopy();


                //reed



                Console.WriteLine("email was sent successfully!");


            }



            catch (Exception ep)
            {
                File.AppendAllText(exceptionPath, "failed to send email with the following error: "+ Environment.NewLine);
                File.AppendAllText(exceptionPath, ep.Message + Environment.NewLine);
                Console.WriteLine("failed to send email with the following error:");
                Console.WriteLine(ep.Message);

            }
        }
        private static DataTable GetdataTable(string connectionString1,string schema, string table, Config config)
        {
            // This is your table to hold the result set:
            DataTable dataTable = new DataTable();
            IDataSource dataSource = DataSourceProvider.Provide(config.DataSource.Type, config.DataSource);
            switch (config.DataSource.Type)
            {
                case DataSourceType.InMemoryFake:
                    break;
                case DataSourceType.SqlServer:
                    return dataSource.GetDataTable(table,schema, connectionString1.ToString());
                case DataSourceType.OracleServer:
                    return dataSource.GetDataTable(table, schema,connectionString1.ToString());                
                case DataSourceType.SpreadSheet:
                    break;
                case DataSourceType.PostgresServer:
                    return dataSource.GetDataTable(table, schema, connectionString1.ToString());
                case DataSourceType.MySqlServer:
                    return dataSource.GetDataTable(table, schema, connectionString1.ToString());
                default:
                    break;
            }
            return dataTable;
        }
        public static void Verification( DataSourceConfig dataSourceConfig, Config config, 
            string _appSpreadsheet, string _dmlpath, string database,
            string exceptionPath, string columnMapping)
        {
            CompareLogic compareLogic = new CompareLogic();
            if (!Directory.Exists($@"Output\Validation"))
            {
                Directory.CreateDirectory($@"Output\Validation");
            }
            string path = Directory.GetCurrentDirectory() + $@"\Output\Validation\ValidationResult.txt";
            var _columndatamask = new List<object>();
            var _columndataUnmask = new List<object>();
            string Hostname = dataSourceConfig.Config.Hostname;
            string _operator = System.DirectoryServices.AccountManagement.UserPrincipal.Current.DisplayName;
            string connectionString = dataSourceConfig.Config.connectionString;
            string connectionStringPrd = dataSourceConfig.Config.connectionStringPrd;

            var result = "";
            var failure = "";
            DataTable report = new DataTable();
            //report.Columns.Add("Table"); report.Columns.Add("Column"); report.Columns.Add("Hostname"); report.Columns.Add("TimeStamp"); report.Columns.Add("Operator"); report.Columns.Add("Row count mask"); report.Columns.Add("Row count prd"); report.Columns.Add("Result"); report.Columns.Add("Result Comment");
            report.Columns.Add("Table"); report.Columns.Add("Schema"); report.Columns.Add("Column"); report.Columns.Add("Hostname"); report.Columns.Add("DataSourceType"); report.Columns.Add("TimeStamp"); report.Columns.Add("Operator"); report.Columns.Add("Row count mask"); report.Columns.Add("Row count prd"); report.Columns.Add("Result"); report.Columns.Add("Result Comment");

            foreach (TableConfig _tables in config.Tables)
            {
               

                var _dataTable = GetdataTable(connectionString, _tables.Schema, _tables.Name.ToString(), config);
                var _dataTablePrd = GetdataTable(connectionStringPrd,_tables.Schema, _tables.Name, config);
                if (_dataTable.Rows.Count == 0)
                {
                    var _norecord = _tables.Name + " No record found for validation test in this table";
                    File.AppendAllText(path, _norecord + Environment.NewLine);
                }
                foreach (var col in _tables.Columns)
                {

                    _columndatamask = new DataView(_dataTable).ToTable(false, new string[] { col.Name }).AsEnumerable().Select(n => n[0]).ToList();
                    _columndataUnmask = new DataView(_dataTablePrd).ToTable(false, new string[] { col.Name }).AsEnumerable().Select(n => n[0]).ToList();


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
                                if (compareLogic.Compare(_columndatamask[i], _columndataUnmask[i]).AreEqual && col.Ignore != true)
                                {

                                    check.Add("FAIL");

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
                                Console.WriteLine(_columndatamask[i]);
                                Console.WriteLine(_columndataUnmask[i]);
                            }



                        }



                        //unmatch
                    }



                    if (check.Contains("FAIL"))
                    {
                        result = "<font color='red'>FAIL</font>";

                        if (col.Ignore == true)
                        {
                            failure = "Masking not required";
                            result = "<font color='green'>PASS</font>";
                        }
                        else if (col.Type == DataType.Shuffle && _columndatamask.Count() == 1)
                        {
                            result = "<b><font color='red'>FAIL</font></b>";
                            failure = "row count must be > 1 for " + DataType.Shuffle.ToString();
                            //result = "<font color='red'>FAIL</font>";
                        }
                        else if (col.Type == DataType.NoMasking)
                        {
                            result = "<font color='green'>PASS</font>";
                            //result = "<b><font color='blue'>PASS</font></b>";
                            failure = "Masking not required";
                            //result = "<font color='red'>FAIL</font>";
                        }
                        else if (col.Type == DataType.Shuffle)
                        {
                           
                            if (!MatchString("Cannot generate unique shuffle value", _exceptionpath))
                            {
                                failure = "NULL";
                                result = "<font color='green'>PASS</font>";
                            }
                            else
                            {
                               
                                result = "<b><font color='blue'>FAIL</font></b>";
                                failure = "Cannot generate a unique shuffle value";
                            }
                            //result = "<font color='red'>FAIL</font>";
                        }
                        else if (col.Type == DataType.exception && check.Contains("PASS"))
                        {
                            failure = "<font color='red'>Applied mask with " + col.Type.ToString() + "</ font >";
                            result = "<b><font color='blue'>PASS</font></b>";
                        }
                        else
                        {
                            result = "<font color='red'>FAIL</font>";
                            failure = "<b><font color='red'>Found exact match " + col.Type.ToString() + " </font></b>";
                        }

                        Console.WriteLine(_tables.Name + " Failed Validation test on column " + col.Name + Environment.NewLine);
                        File.AppendAllText(path, _tables.Name + " Failed Validation test on column " + _tables.Name + Environment.NewLine);

                        report.Rows.Add(_tables.Name, TSchema, col.Name, Hostname, Stype, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);

                    }
                    else if (check.Contains("IGNORE"))
                    {
                        result = "No Validation";
                        failure = "Column not mask";
                        report.Rows.Add(_tables.Name, TSchema, col.Name, Hostname, Stype, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);
                    }
                    else
                    {
                        if (_columndatamask.Count == 0)
                        {
                            failure = "No record found";
                        }
                        else if (col.Ignore == true || col.Type == DataType.NoMasking)
                        {
                            failure = "Masking not required";
                            result = "<font color='green'>PASS</font>";
                        }
                        else
                            failure = "NULL";
                        result = "<font color='green'>PASS</font>";
                        Console.WriteLine(_tables.Name + " Pass Validation test on column " + col.Name);
                        File.AppendAllText(path, _tables.Name + " Pass Validation test on column " + col.Name + Environment.NewLine);
                        report.Rows.Add(_tables.Name, TSchema, col.Name, Hostname, Stype, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);


                    }


                }
            }
            Analysis(report, dataSourceConfig, _appSpreadsheet, database, _dmlpath,exceptionPath,columnMapping);

        }
        public static void Analysis(DataTable report, DataSourceConfig dataSourceConfig, string _appSpreadsheet, 
            string database, string _dmlPath,
            string exceptionPath, string columnMapping)
        {
            List<string> analysis = new List<string>();
            if (!string.IsNullOrEmpty(_dmlPath))
            {
                //add dml files to zip
                ZipName = Directory.GetCurrentDirectory() + "/" + database + "/" + database + "_MASKED_DML.zip";
                var dirName = Path.GetDirectoryName(ZipName);
                if (!Directory.Exists(dirName))
                {
                    Directory.CreateDirectory(dirName);
                }
                if (File.Exists(ZipName))
                {
                    Console.WriteLine(Path.GetFileName(ZipName)  + " already exist. Do you want to replace and attach. No to attach old zipfile? [yes/no]");
                    var key = Console.ReadLine();
                    if (key.ToUpper() == "YES")
                    {
                        File.Delete(ZipName);
                        ZipFile.CreateFromDirectory(_dmlPath, ZipName, CompressionLevel.Optimal, true);
                    }
                    
                }
                else
                    ZipFile.CreateFromDirectory(_dmlPath, ZipName, CompressionLevel.Optimal, true);

            }
            var tablecount = new DataView(report).ToTable(true, new string[] { "Table" }).AsEnumerable().Select(n => n[0]).ToList().Count;
            var columncount = new DataView(report).ToTable(false, new string[] { "Column" }).AsEnumerable().Select(r => r.Field<string>("Column")).ToList().Count;
            var _pass = new DataView(report).ToTable(false, new string[] { "Result" }).AsEnumerable().Select(r => r.Field<string>("Result")).Where(n => n == "<font color='green'>PASS</font>").ToList().Count;
            var _passIgnore = new DataView(report).ToTable(false, new string[] { "Result" }).AsEnumerable().Select(r => r.Field<string>("Result")).Where(n => n == "<b><font color='blue'>PASS</font></b>").ToList().Count;
            var _fail = new DataView(report).ToTable(false, new string[] { "Result" }).AsEnumerable().Select(r => r.Field<string>("Result")).Where(n => n == "<font color='red'>FAIL</font>" || n == "<b><font color='blue'>FAIL</font></b>").ToList().Count;
            var _nomasking = new DataView(report).ToTable(false, new string[] { "Result Comment" }).AsEnumerable().Select(r => r.Field<string>("Result Comment")).Where(n => n == "Masking not required").ToList().Count;
            var masked = new DataView(report).ToTable(false, new string[] { "Result Comment" }).AsEnumerable().Select(r => r.Field<string>("Result Comment")).Where(n => n != "Masking not required").ToList().Count;


            decimal top = _pass + _passIgnore;
            decimal bot = top + _fail;

            decimal Percentagecurracy = ((decimal)top / (decimal)bot) * 100;
            decimal dc = Math.Round(Percentagecurracy, 2);
            analysis.Add("Table count = " + tablecount);
            double g = (_pass - _fail) / (_pass + _fail) * 100;
            analysis.Add("Column count = " + columncount);
            analysis.Add("Total Pass = " + top);
            analysis.Add("Total Fail = " + _fail);
            analysis.Add("% Accuracy = " + dc);
            string body = ToHTML(report, analysis);
            string sig = GetSignature();

            SendMail(sig, body, database, tablecount, columncount, _pass, _fail, dc, _appSpreadsheet,exceptionPath, columnMapping);



        }
        private static bool MatchString(string search, string path)
        {
            using (var reader = new StreamReader(path))
            {
                string currentLine;
                while ((currentLine = reader.ReadLine()) != null)
                {
                    if (currentLine.Contains(search))
                    {
                        // if you do not need multiple lines and just the first one
                        // just break from the loop (break;)            
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
