using FirebirdSql.Data.EntityFramework6;

namespace FirebirdSql.Data.EntityFramework6
{
    internal class CustomFbMigrationSqlGeneratorBehavior : BaseFbMigrationSqlGeneratorBehavior
    {
        protected override string IdentitySequenceName(string columnName, string tableName)
        {
            const string gen = "GEN_";
            string s = string.Concat(gen, tableName.ToUpperInvariant(), "_", columnName.ToUpperInvariant());
            if (s.Length >= 31)
            {
                s = MetadataHelpers.HashString(s);
            }

            return s;
        }

        protected override string CreateTriggerName(string columnName, string tableName)
        {
            var s = string.Concat(tableName.ToUpperInvariant(), "_", columnName.ToUpperInvariant(), "_BI");
            if (s.Length >= 31)
            {
                s = MetadataHelpers.HashString(s);
            }

            return s;
        }
    }
}
