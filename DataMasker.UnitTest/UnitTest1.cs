using DataMasker;
using DataMasker.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Configuration;

namespace DataMasker.Examples.Tests
{
    [TestClass()]
    public class UnitTest1
    {
        [TestMethod()]
        public void CheckAppConfigTest()
        {
            // Program.CheckAppConfig()
            Assert.IsTrue(Program.CheckAppConfig());
        }

        [TestMethod()]
        public void LoadConfigTest()
        {
            Program.CheckAppConfig();
            var _SpreadSheetPath = ConfigurationManager.AppSettings["ExcelSheetPath"];
            var copyjsonPath = ExcelToJson.ToJson(_SpreadSheetPath);
            Program.JsonConfig(copyjsonPath, _SpreadSheetPath);
            var config = Program.LoadConfig(1);
            Assert.IsTrue(config.Tables.Count > 0);
        }
        [TestMethod]
        public void GetCount()
        {
            Program.CheckAppConfig();
            var _SpreadSheetPath = ConfigurationManager.AppSettings["ExcelSheetPath"];
            var copyjsonPath = ExcelToJson.ToJson(_SpreadSheetPath);
            Program.JsonConfig(copyjsonPath, _SpreadSheetPath);
            var config = Program.LoadConfig(1);
            var type = DataSourceProvider.Provide(config.DataSource.Type, config.DataSource);
            var data = type.GetData(config.Tables[1], config, type.GetCount(config.Tables[1]), null, null);
            Assert.IsTrue(data != null);
        }
    }

}

