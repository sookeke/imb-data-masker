using DataMasker.Interfaces;
using MimeKit;
using MsgKit;
using MsgKit.Enums;
using PdfSharp.Drawing;
using PdfSharp.Pdf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Bogus;
using System.Text;
using System.Threading.Tasks;
using MessageImportance = MsgKit.Enums.MessageImportance;
using GemBox.Spreadsheet;

namespace DataMasker
{
    public class FileType : IFileType
    {
        Faker faker = new Faker();
        public object GenerateDOCX(string path, string table)
        {
            throw new NotImplementedException();
        }

        public object GenerateHTML(string path, string table)
        {
            throw new NotImplementedException();
        }

        public object GenerateJPEG(string path, string table)
        {
            throw new NotImplementedException();
        }

        public object GenerateMSG(string path, string table)
        {
            using (var email = new Email(
          new Sender("peterpan@neverland.com", "Peter Pan"),
          new Representing("tinkerbell@neverland.com", "Tinkerbell"),
          "Hello Neverland subject"))
            {
                email.Recipients.AddTo("captainhook@neverland.com", "Captain Hook");
                email.Recipients.AddCc("crocodile@neverland.com", "The evil ticking crocodile");
                email.Subject = "Property Information Management Systems";
                email.BodyText = "Property Information Management Systems";
                email.BodyHtml = "<html><head></head><body>MOTI: Welcome to the Property Information Management System "+ Environment.NewLine + string.Join("", faker.Rant.Reviews("Product", 20).ToArray()) + "  </body></html>";
                email.Importance = MessageImportance.IMPORTANCE_HIGH;
                email.IconIndex = MessageIconIndex.ReadMail;
                //email.Attachments.Add(@"d:\crocodile.jpg");
                email.Save(Environment.CurrentDirectory + path);

                // Show the E-mail
                //System.Diagnostics.Process.Start(path);
                if (File.Exists(Environment.CurrentDirectory + path))
                {
                    return Environment.CurrentDirectory + path;
                }
                return null;
            }
        }

        public object GeneratePDF(string path, string table)
        {
            PdfDocument pdf = new PdfDocument();
            PdfPage pdfPage = pdf.AddPage();
            

            XGraphics graph = XGraphics.FromPdfPage(pdfPage);

            XFont font = new XFont("Verdana", 12, XFontStyle.Regular);
            graph.DrawString("Property Information Management System: " + Environment.NewLine  + faker.Rant.Review(), font, XBrushes.Black,
            new XRect(0, 0, pdfPage.Width.Point, pdfPage.Height.Point), XStringFormats.TopLeft
            );
            //graph.DrawString(faker.Rant.Review(), font, XBrushes.Black,
            //new XRect(0, 0, pdfPage.Width.Point, pdfPage.Height.Point), XStringFormats.TopLeft);
           // graph.DrawString()


            pdf.Save(Environment.CurrentDirectory+path);
            if (File.Exists(Environment.CurrentDirectory+path))
            {
                return Environment.CurrentDirectory+path;
            }
            return null ;
        }

        public object GenerateTIF(string path, string table)
        {
            throw new NotImplementedException();
        }

        public object GenerateRTF(string path, string table)
        {
            throw new NotImplementedException();
        }

        public object GenerateTXT(string path, string table)
        {
            //var reviews = faker.Rant.re
            File.WriteAllText(path, Environment.NewLine+ string.Join("", faker.Rant.Reviews("Product",40).ToArray()));
            if (File.Exists(path))
            {
                return path;
            }
            return null;
        }

        public object GenerateXLSX(string path, string desc)
        {
            SpreadsheetInfo.SetLicense("FREE-LIMITED-KEY");
            Faker faker = new Faker();
            var Worksheet = new ExcelFile();

            var worksheet = Worksheet.Worksheets.Add("APP_TAP");
            worksheet.Cells["A1"].Value = desc;
            Worksheet.Save(path);
            if (File.Exists(path))
            {
                return path;
            }
            return path;
        }
        public object GenerateRandom(string path)
        {
            throw new NotImplementedException();
        }
    }
}
