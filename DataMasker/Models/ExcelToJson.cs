using ExcelDataReader;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMasker.Models
{
   public static class ExcelToJson
    {
        public static string toJson(string szFilePath)
        {
            string jsonPath = Path.GetDirectoryName(szFilePath) + "\\" + Path.GetFileNameWithoutExtension(szFilePath) + ".json";
            using (FileStream fileStream = new FileStream(szFilePath, FileMode.Open, FileAccess.Read))
            {
                IExcelDataReader excelReader = null;
                if (Path.GetExtension(szFilePath).ToUpper() == ".XLSX")
                {
                    excelReader = ExcelReaderFactory.CreateOpenXmlReader(fileStream);
                }
                else if (Path.GetExtension(szFilePath).ToUpper() == ".XLS")
                {
                    //1. Reading from a binary Excel file ('97-2003 format; *.xls)
                     excelReader = ExcelReaderFactory.CreateBinaryReader(fileStream);
                }
                else
                    throw new ArgumentOutOfRangeException();
                if (excelReader != null)
                {
                    System.Data.DataSet result = excelReader.AsDataSet(new ExcelDataSetConfiguration()
                    {
                        ConfigureDataTable = (_) => new ExcelDataTableConfiguration()
                        {
                            UseHeaderRow = true
                        }
                    });


                    //Set to Table
                    var dataTable = result.Tables[0].AsDataView().ToTable();
                    var json = JsonConvert.SerializeObject(dataTable);
                    using (var tw = new StreamWriter(jsonPath, false))
                    {
                        tw.WriteLine(json.ToString());
                        tw.Close();
                    }
                }
            }
            if (File.Exists(jsonPath))
            {
                return jsonPath;
            }
            else
                throw new ArgumentOutOfRangeException();
        }

    }
}
