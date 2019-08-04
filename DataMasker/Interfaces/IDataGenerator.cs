using Bogus.DataSets;
using DataMasker.Models;
using DataMasker.DataSources;
using System.Data;

namespace DataMasker.Interfaces
{
    public interface IDataGenerator
    {
        object GetValue(
            ColumnConfig columnConfig,
            object existingValue,
            Name.Gender? gender);
        object GetValueShuffle(
            ColumnConfig columnConfig, string table, string column, IDataSource dataSources, DataTable dataTable,
            object existingValue,
            Name.Gender? gender);
        object GetBlobValue(ColumnConfig columnConfig, IDataSource dataSource, object existingValue,string filename, string FileExtension, Name.Gender? gender);
    }
}
