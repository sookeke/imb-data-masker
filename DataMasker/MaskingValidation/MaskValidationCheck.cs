using ChoETL;
using DataMasker.Models;
using Microsoft.Exchange.WebServices.Data;
using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMasker.MaskingValidation
{
    public static class MaskValidationCheck
    {
        public static string Jsonpath { get; private set; }
        public static string ZipName { get; private set; }

        public static string ToHTML_Table(DataTable dt, IList ts)
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
            string path = System.Environment.UserName;
            string pat = @"C:\Users\" + path + @"\AppData\Roaming\Microsoft\Signatures\";
            StringBuilder st = new StringBuilder();
            string[] signature = new string[] { };
            //CreateIfMissing(pat + @"\garbage");
            string final = "";
            if (Directory.Exists(pat))
            {
                var fileHtml = Directory.GetFiles(pat, "*.htm");
                if (fileHtml.Length != 0)
                {
                    //File.ReadAllBytes("");
                    //string[] location = new string[] { fileHtml[1].ToString() };
                    signature = File.ReadAllLines(fileHtml[1]);
                    foreach (var item in signature)
                    {
                        //sig += item;
                        st.AppendLine(item);
                        //Console.WriteLine(item);

                    }

                    var foundIndexes = new List<int>();
                    string[] search = new string[] { "src" };
                    //List<int> indexes = st.ToString().AllIndexesOf("src");

                    //string[] insert = new string[] { @"\img" };
                    int xxxx = st.ToString().IndexOf(search[0]);
                    final = st.ToString();
                    //Console.WriteLine(final);

                    for (int i = st.ToString().IndexOf(search[0]); i > -1; i = st.ToString().IndexOf(search[0], i + 1))
                    {
                        // for loop end when i=-1 ('a' not found)

                        foundIndexes.Add(i);



                    }

                    for (int i = 0; i < foundIndexes.Count; i++)
                    {

                        int index = st.ToString().IndexOf(search[0], xxxx + i);

                        final = st.ToString().Insert(index + 5, pat);
                        Console.WriteLine(final);
                    }





                }
                else
                {
                    Array.Resize(ref signature, signature.Length + 1);
                    final = "Thank you <br/>";

                    Console.WriteLine(signature[0]);
                }
            }

            else
            {
                Array.Resize(ref signature, signature.Length + 1);
                final = "Thank you <br/>"
                    ;

            }
            return final;
        }
        public static void SendMail(string signature, string body, string database, int tcount, int ccount, int pass, int fail, decimal error, string _appSpreadsheet,string exceptionPath)
        {
            try
            {
                string fromEmail = ConfigurationManager.AppSettings["fromEmail"];
                var toEmail = ConfigurationManager.AppSettings["RecipientEmail"].Split(';').ToList();
                var ccEmaill = ConfigurationManager.AppSettings["cCEmail"].Split(';').ToList();
                var jsonPath = Directory.GetCurrentDirectory() + @"\" + ConfigurationManager.AppSettings["jsonMapPath"];
                ExchangeService service = new ExchangeService
                {
                    UseDefaultCredentials = true
                };
                //service.Credentials = new NetworkCredential(user, decryptPassword);
                service.AutodiscoverUrl(fromEmail);
                //service.UseDefaultCredentials = true;
                //service.Credentials = new NetworkCredential(@"idir\sookek_o", "5D16Stod@");
                EmailMessage email = new EmailMessage(service)
                {
                    Subject = "Data Masking Validation Test Report Analysis for " + database,

                    Body = new MessageBody(BodyType.HTML, "<p>This is an automated email for data masking verification and validation test report analysis for " + database + "<br />" + "</p> " +
                    body + "<br />" +
                signature +
                "<br />" +
                "</body>" +
              "</html>"),
                    From = new EmailAddress(fromEmail)
                };
                //email.From = "mcs@gov.bc.ca";


                email.ToRecipients.AddRange(toEmail);
                email.CcRecipients.AddRange(ccEmaill);
                if (File.Exists(_appSpreadsheet)) { email.Attachments.AddFileAttachment(_appSpreadsheet); }  
                if (File.Exists(ZipName)) { email.Attachments.AddFileAttachment(ZipName); }
                if (File.Exists(exceptionPath)) { email.Attachments.AddFileAttachment(exceptionPath); }
                if (File.Exists(jsonPath)){ email.Attachments.AddFileAttachment(jsonPath); }
                    

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
        private static DataTable GetdataTable(dynamic connectionString1, string table, Config config)
        {
            // This is your table to hold the result set:
            DataTable dataTable = new DataTable();
            if (config.DataSource.Type == DataSourceType.OracleServer)
            {
                using (OracleConnection oracleConnection = new OracleConnection(connectionString1))
                {
                    string squery = "Select * from " + table;
                    oracleConnection.Open();
                    
                    using (OracleDataAdapter oda = new OracleDataAdapter(squery, oracleConnection))
                    {
                        try
                        {
                            //Fill the data table with select statement's query results:
                            int recordsAffectedSubscriber = 0;

                            recordsAffectedSubscriber = oda.Fill(dataTable);

                        }
                        catch (Exception ex)
                        {

                            Console.WriteLine(ex.Message);
                        }
                    }
                }
            }
            else if (config.DataSource.Type == DataSourceType.SqlServer)
            {
                using (SqlConnection sqlConnection = new SqlConnection(connectionString1))
                {

                    string squery = "Select * from " + table;
                    sqlConnection.Open();
                    using (SqlDataAdapter adapter = new SqlDataAdapter(squery, sqlConnection))
                    {

                        try
                        {
                            //Fill the data table with select statement's query results:
                            int recordsAffectedSubscriber = 0;

                            recordsAffectedSubscriber = adapter.Fill(dataTable);

                        }
                        catch (Exception ex)
                        {

                            Console.WriteLine(ex.Message);
                        }

                    }
                }
            }
            return dataTable;
        }
        public static void Verification( DataSourceConfig dataSourceConfig, Config config, string _appSpreadsheet, string _dmlpath, string database,string exceptionPath)
        {
            string path = Directory.GetCurrentDirectory() + $@"\Output\Validation\ValidationResult.txt";
            var _columndatamask = new List<object>();
            var _columndataUnmask = new List<object>();
            string Hostname = dataSourceConfig.Config.Hostname;
            string _operator = System.DirectoryServices.AccountManagement.UserPrincipal.Current.DisplayName;

            var result = "";
            var failure = "";
            DataTable report = new DataTable();
            report.Columns.Add("Table"); report.Columns.Add("Column"); report.Columns.Add("Hostname"); report.Columns.Add("TimeStamp"); report.Columns.Add("Operator"); report.Columns.Add("Row count mask"); report.Columns.Add("Row count prd"); report.Columns.Add("Result"); report.Columns.Add("Result Comment");

            foreach (TableConfig _tables in config.Tables)
            {

                var _dataTable = GetdataTable(dataSourceConfig.Config.connectionString, _tables.Name, config);
                var _dataTable2 = GetdataTable(dataSourceConfig.Config.connectionStringPrd, _tables.Name, config);
                if (_dataTable.Rows.Count == 0)
                {
                    var _norecord = _tables.Name + " No record found for validation test in this table";
                    File.AppendAllText(path, _norecord + Environment.NewLine);
                }
                foreach (var col in _tables.Columns)
                {
                    if (col.Type == DataType.Ignore)
                    {
                        //    result = "Column not mask";
                        //    File.AppendAllText(path, _tables.Name + " Column not mask " + col.Name + Environment.NewLine);
                        //    report.Rows.Add(_tables.Name, col.Name, Hostname, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);
                        //}
                    }
                    else
                    {
                        _columndatamask = new DataView(_dataTable).ToTable(false, new string[] { col.Name }).AsEnumerable().Select(n => n[0]).ToList();
                        _columndataUnmask = new DataView(_dataTable2).ToTable(false, new string[] { col.Name }).AsEnumerable().Select(n => n[0]).ToList();

                    }
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
                                    //if (col.Ignore == true)
                                    //{
                                    //    check.Add("IGNORE");
                                    //}
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
                        if (col.Name.Contains("USERID"))
                        {
                            failure = "New user commit";
                        }
                        else if (col.Ignore == true)
                        {
                            failure = "<font color='red'>Ignore = " + col.Ignore.ToString() + "</ font >";
                            result = "<b><font color='blue'>PASS</font></b>";
                        }
                        else if (col.Type == DataType.Shuffle && _columndatamask.Count() == 1)
                        {
                            result = "<b><font color='RED'>FAIL</font></b>";
                            failure = "row count must be > 1 for " + DataType.Shuffle.ToString();
                            //result = "<font color='red'>FAIL</font>";
                        }
                        else if (col.Type == DataType.Shuffle)
                        {
                            result = "<b><font color='blue'>PASS</font></b>";
                            failure = DataType.Shuffle.ToString() + " applied";
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
                            failure = "<b><font color='red'>Found exact match</font></b>";
                        }

                        Console.WriteLine(_tables.Name + " Failed Validation test on column " + col.Name + Environment.NewLine);
                        File.AppendAllText(path, _tables.Name + " Failed Validation test on column " + col.Name + Environment.NewLine);
                        report.Rows.Add(_tables.Name, col.Name, Hostname, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);

                    }
                    else if (check.Contains("IGNORE"))
                    {
                        result = "No Validation";
                        failure = "Column not mask";
                        report.Rows.Add(_tables.Name, col.Name, Hostname, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);
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
                        Console.WriteLine(_tables.Name + " Pass Validation test on column " + col.Name);
                        File.AppendAllText(path, _tables.Name + " Pass Validation test on column " + col.Name + Environment.NewLine);
                        report.Rows.Add(_tables.Name, col.Name, Hostname, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);


                    }


                    rownumber = 0;

                }
               
               
            }
            Analysis(report, dataSourceConfig, _appSpreadsheet, database, _dmlpath,exceptionPath);

        }
        public static void Analysis(DataTable report, DataSourceConfig dataSourceConfig, string _appSpreadsheet, string database, string _dmlPath,string exceptionPath)
        {
            List<string> analysis = new List<string>();
            if (!string.IsNullOrEmpty(_dmlPath))
            {
                //add dml files to zip
                ZipName = Directory.GetCurrentDirectory() + "/" + database + "/" + database + "_MASKED_DML.zip";
                if (File.Exists(ZipName))
                {
                    Console.WriteLine(Path.GetFileName(ZipName)  + " already exist. Do you want to replace it? [yes/no]");
                    var key = Console.ReadLine();
                    if (key.ToUpper() == "YES")
                    {
                        File.Delete(ZipName);
                    }
                }
                ZipFile.CreateFromDirectory(_dmlPath, ZipName, CompressionLevel.Optimal,true);
            }
            var tablecount = new DataView(report).ToTable(true, new string[] { "Table" }).AsEnumerable().Select(n => n[0]).ToList().Count;
            var columncount = new DataView(report).ToTable(false, new string[] { "Column" }).AsEnumerable().Select(r => r.Field<string>("Column")).ToList().Count;
            var _pass = new DataView(report).ToTable(false, new string[] { "Result" }).AsEnumerable().Select(r => r.Field<string>("Result")).Where(n => n == "<font color='green'>PASS</font>").ToList().Count;
            var _passIgnore = new DataView(report).ToTable(false, new string[] { "Result" }).AsEnumerable().Select(r => r.Field<string>("Result")).Where(n => n == "<b><font color='blue'>PASS</font></b>").ToList().Count;
            var _fail = new DataView(report).ToTable(false, new string[] { "Result" }).AsEnumerable().Select(r => r.Field<string>("Result")).Where(n => n == "<font color='red'>FAIL</font>" || n == "<b><font color='blue'>FAIL</font></b>").ToList().Count;
            //var _ignore = new DataView(report).ToTable(false, new string[] { "Result" }).AsEnumerable().Select(r => r.Field<string>("Result")).Where(n => n == "<font color='red'>FAIL</font>").ToList().Count;


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
            string body = ToHTML_Table(report, analysis);
            string sig = GetSignature();

            SendMail(sig, body, database, tablecount, columncount, _pass, _fail, dc, _appSpreadsheet,exceptionPath);



        }
    }
}
