using Microsoft.SqlServer.Types;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity.Spatial;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Oracle.DataAccess;
using Oracle.DataAccess.Client;
using NetTopologySuite.Geometries;

namespace DataMasker
{
   public class GeographyMapper : Dapper.SqlMapper.TypeHandler<SdoGeometry>
    {
        public override void SetValue(IDbDataParameter parameter, SdoGeometry value)
        {
            parameter.Value = value == null ? (object)DBNull.Value : value.ToString();
            ((OracleParameter)parameter).UdtTypeName = "MDSYS.SDO_GEOMETRY";
            if (parameter is OracleParameter npgsqlParameter)
            {
                npgsqlParameter.OracleDbType = OracleDbType.Object;
                npgsqlParameter.UdtTypeName = "MDSYS.SDO_GEOMETRY";
                npgsqlParameter.Value = value;
            }
            else
            {
                throw new ArgumentException();
            }
        }
        public override SdoGeometry Parse(object value)
        {
            if (value is SdoGeometry geometry)
            {
                return geometry;
            }

            throw new ArgumentException();
        }
    }
}
