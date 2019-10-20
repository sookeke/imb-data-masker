namespace DataMasker
{
    /// <summary>
    /// 
    /// </summary>
    public enum DataType
    {
        /// <summary>
        /// The none
        /// </summary>
        None,
        CompanyPersonName,
        Company,
        NULL,
        PostalCode,
        StringConcat,
        Shuffle,
        Ignore,
        Money,
        Location,
        NoMasking,
        math,
        Shufflegeometry,
        exception,

        /// <summary>
        /// The api data type, supports {{entity.property}} e.g. {{address.FullAddress}}
        /// </summary>
        Bogus,

        RandomUsername,
        /// <summary>
        /// The first name
        /// </summary>
        FirstName,

        /// <summary>
        /// The last name
        /// </summary>
        LastName,

        /// <summary>
        /// The date of birth
        /// </summary>
        DateOfBirth,

        PickRandom,
        RandomString2,

        /// <summary>
        /// The rant
        /// </summary>
        Rant,
        State,
        City,

        /// <summary>
        /// The lorem
        /// </summary>
        Lorem,

        /// <summary>
        /// The string format
        /// </summary>
        StringFormat,

        /// <summary>
        /// The full address
        /// </summary>
        FullAddress,

        /// <summary>
        /// The phone number
        /// </summary>
        PhoneNumber,
        PhoneNumberInt,
        RandomSeason,

        File,
        Filename,
        Blob,
        Clob,
        RandomDec,
        //Polygon with same precision
        Geometry,
        RandomYear,

        RandomInt,

        /// <summary>
        /// Indicates that the column value is computed from other indicated columns
        /// </summary>
        Computed,

        RandomHexa
    }
}
