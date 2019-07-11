using Bogus.DataSets;
using DataMasker.Models;
using DataMasker.DataSources;

namespace DataMasker.Interfaces
{
    public interface IDataGenerator
    {
        object GetValue(
            ColumnConfig columnConfig,
            object existingValue,
            Name.Gender? gender);
        object GetValueShuffle(
            ColumnConfig columnConfig, string table, string column, IDataSource dataSources,
            object existingValue,
            Name.Gender? gender);
    }
}
