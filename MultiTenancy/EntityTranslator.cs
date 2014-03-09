using System;
using System.Data;
using System.Text;
using System.Linq;
using System.Reflection;
using System.Data.Common;
using System.Data.Entity;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace MultiTenancy
{
    public enum DbVendor
    {
        SqlServer,
        PostgreSql,
        //SQLite,
        //MySQL,
        //Oracle,
        //SimpleDB,
        //DynamoDB,
        //MongoDB,
        //Redis
    }

    public class IndexAttribute : Attribute
    {
        /// <summary>
        /// Indicates if this index should enforce a unique
        /// constraint. Default is false.
        /// </summary>
        public bool Unique { get; set; }

        /// <summary>
        /// A comma seperated list of property names other
        /// than the proper on which the attribute is attached
        /// that should be included in the index.
        /// </summary>
        public string Clustered { get; set; }
    }

    public abstract partial class MultiTenantContext
    {
        protected string GetUpdateScriptForSchema( DbConnection con, string Schema, List<Type> Entities, bool Create, bool Update, bool Delete )
        {
            var sb = new StringBuilder();

            // Find the changes in the tables
            var existingTableNames = this.GetTableList( con, Schema );
            var newEntities = Entities.Where( m => !existingTableNames.Contains( GetTableName( m ) ) ).ToList();
            var newTableNames = newEntities.Select( GetTableName ).ToList();

            if( Create )
            {
                // Create the new tables
                sb.AppendLine( GetTableCreateStrings( this.Vendor, newEntities, existingTableNames ) );
            }

            if( Delete )
            {
                // Delete the tables that no longer have a corresponding entity
                var deletedTableNames = existingTableNames.Where( m => !Entities.Select( GetTableName ).Contains( m ) );
                foreach( var t in deletedTableNames )
                {
                    sb.AppendLine( GetTableDropString( t, this.Vendor ) );
                }
            }

            // Find the changes in the columns
            foreach( var entity in Entities.Where( m => !newTableNames.Contains( GetTableName( m ) ) ) )
            {
                // Get the column information from the reference schema
                var table = GetTableName( entity );

                // Get the existing column info
                var columns = this.GetColumnList( con, table, Schema );

                // Get the entity properties
                var properties = GetEntityProperties( entity ).ToList();

                if( Delete )
                {
                    // Drop the columns that are no longer part of the entity
                    foreach( var column in from c in columns where !properties.Select( GetColumnName ).Contains( c.Name ) select c.Name )
                    {
                        sb.AppendLine( GetColumnDropString( table, column, this.Vendor ) );
                    }
                }

                foreach( var property in properties )
                {
                    var name = GetColumnName( property );
                    var ec = columns.FirstOrDefault( m => m.Name.Equals( name ) );

                    if( Create && ec == null )
                    {
                        // This is a new column
                        sb.AppendLine( GetColumnAddString( entity, property, this.Vendor ) );
                    }
                    else if( Update && ec != null )
                    {
                        // String Length
                        if( ec.Type == typeof( string ) )
                        {
                            var stringLength = property.GetAttribute<StringLengthAttribute>();
                            if( ( stringLength != null && ec.Length != stringLength.MaximumLength ) || ( stringLength == null && ec.Length > 0 ) )
                            {
                                // Change the type on the column to reflect the new length
                                sb.AppendLine( GetColumnChangeTypeString( entity, property, this.Vendor ) );
                            }
                        }

                        // Nullable and Default Value
                        if( ec.Nullable && !IsColumnNullable( property ) )
                        {
                            // Add a NOT NULL constraint and default value
                            sb.AppendLine( GetColumnSetNotNullString( entity, property, this.Vendor ) );
                        }
                        else if( !ec.Nullable && IsColumnNullable( property ) )
                        {
                            // Remove the NOT NULL constraint and default value
                            sb.AppendLine( GetColumnDropNotNullString( entity, property, !string.IsNullOrEmpty( ec.Default ), this.Vendor ) );
                        }

                        // Index
                        var index = property.GetAttribute<IndexAttribute>();
                        if( string.IsNullOrEmpty( ec.Index ) && index != null )
                        {
                            // Create a new index
                            sb.AppendLine( GetIndexCreateString( entity, property, this.Vendor ) );
                        }
                        else if( !string.IsNullOrEmpty( ec.Index ) && index == null )
                        {
                            // Remove the existing index
                            sb.AppendLine( GetIndexDropString( entity, property, this.Vendor ) );
                        }
                    }
                }
            }

            return sb.ToString();
        }

        /// <summary>
        /// Returns the inner type of all DbSet properties on the given class
        /// that do not have a specific schema specified. These will be created
        /// in the tenant's schema.
        /// </summary>
        protected static List<Type> GetTenantSchemaEntityTypes( Type T )
        {
            return
            (
                // Get all properties from this class
                from p in T.GetProperties()

                // Only the properties that are a generic type of DbSet
                where p.PropertyType.IsGenericType &&
                (
                    p.PropertyType.GetGenericTypeDefinition() == typeof( DbSet<> ) ||
                    p.PropertyType.GetGenericTypeDefinition() == typeof( IDbSet<> ) ||
                    p.PropertyType.GetGenericTypeDefinition() == typeof( FilteredDbSet<> )
                )

                // Get the type used to create the generic (T in DbSet<T>)
                let e = p.PropertyType.GetGenericArguments()[0]

                // Get the TableAttribute on the class
                let t = e.GetAttribute<TableAttribute>()

                // Only property types that have the [Table( "Name" )]
                // decoration without a schema specified
                where t != null && string.IsNullOrEmpty( t.Schema )

                // Select the type of the DbSet (T as above)
                select e
            )
            .ToList();
        }

        /// <summary>
        /// Returns the inner type of all DbSet properties on the given class
        /// that do have a specific schema specified. These will be created
        /// in the default (dbo, public) schema.
        /// </summary>
        protected static List<Type> GetDefaultSchemaEntityTypes( Type T )
        {
            return
            (
                // Get all properties from this class
                from p in T.GetProperties()

                // Only the properties that are a generic type of DbSet
                where p.PropertyType.IsGenericType &&
                (
                    p.PropertyType.GetGenericTypeDefinition() == typeof( DbSet<> ) ||
                    p.PropertyType.GetGenericTypeDefinition() == typeof( IDbSet<> ) ||
                    p.PropertyType.GetGenericTypeDefinition() == typeof( FilteredDbSet<> )
                )

                // Get the type used to create the generic (T in DbSet<T>)
                let e = p.PropertyType.GetGenericArguments()[0]

                // Get the TableAttribute on the class
                let t = e.GetAttribute<TableAttribute>()

                // Only property types that have the [Table( "Name" )]
                // decoration without a schema specified
                where t != null && !string.IsNullOrEmpty( t.Schema )

                // Select the type of the DbSet (T as above)
                select e
            )
            .ToList();
        }

        /// <summary>
        /// Returns the information for columns currently in the database tables.
        /// </summary>
        protected List<EntityColumn> GetColumnDetails( IDataReader reader )
        {
            var list = new List<EntityColumn>();
            var @default = reader.GetOrdinal( "Default" );
            var length = reader.GetOrdinal( "Length" );
            var index = reader.GetOrdinal( "Index" );

            while( reader.Read() )
            {
                list.Add( new EntityColumn
                {
                    Name = reader.GetString( reader.GetOrdinal( "Name" ) ),
                    Type = GetNativeType( reader.GetString( reader.GetOrdinal( "Type" ) ) ),
                    Default = reader.IsDBNull( @default ) ? null : reader.GetString( @default ),
                    Nullable = reader.GetBoolean( reader.GetOrdinal( "Nullable" ) ),
                    Length = reader.IsDBNull( length ) ? -1 : reader.GetInt32( length ),
                    Index = reader.IsDBNull( index ) ? null : reader.GetString( index ),
                } );
            }
            return list;
        }

        /// <summary>
        /// Returns a list of PropertyInfo for the given type. Only
        /// value and string types are returned. Properties that are
        /// read only or have the NotMappedAttribute are skipped.
        /// </summary>
        /// <param name="t">The entity type for which to return the properties.</param>
        /// <returns>A list of PropertyInfo for the given entity.</returns>
        protected static IEnumerable<PropertyInfo> GetEntityProperties( Type t )
        {
            return t.GetProperties().Where( m => m.CanWrite && m.GetAttribute<NotMappedAttribute>() == null && ( m.PropertyType.IsValueType || m.PropertyType == typeof( string ) || m.PropertyType == typeof( byte[] ) ) );
        }

        /// <summary>
        /// Returns a list of table names to which the given entity
        /// type has foreign key attributes pointing to.
        /// </summary>
        /// <param name="t">The entity type for which to return the table names.</param>
        /// <returns>The list of foreign key tables pointed to by the given entity type.</returns>
        protected static IEnumerable<string> GetForeignKeysForEntity( Type t )
        {
            return
            (
                // Get all integer type properties.
                // These types are the only ones that will
                // have a useful ForeignKeyAttribute.
                from p1 in t.GetProperties()
                where p1.PropertyType == typeof( int )
                    || p1.PropertyType == typeof( int? )

                // Get the ForeignKeyAttribute on the property
                let fk = p1.GetAttribute<ForeignKeyAttribute>()
                where fk != null

                // The ForeignKeyAttribute points to another property
                // on the same type. The type of this second property
                // should contain a TableAttribute that is used in the
                // foreign key.
                let p2 = t.GetProperty( fk.Name )
                where p2 != null

                // Get the TableAttribute off the second property which
                // will have the name of the foreign table.
                let ta = p2.PropertyType.GetAttribute<TableAttribute>()
                where ta != null

                // Self-referencing foreign keys are allowed
                where !ta.Name.Equals( t.GetAttribute<TableAttribute>().Name )

                // Select the foreign table name
                select ta.Name
            )
            .ToList();
        }

        /// <summary>
        /// Generates a series of CREATE TABLE statements for the 
        /// given list of entity types. Any entities with foreign
        /// keys will appear after their corresponding foreign key
        /// table.
        /// </summary>
        /// <param name="Vendor">Determines what vendor-specific code to return.</param>
        /// <param name="Entities">An option list of entities to create. Default is all</param>
        /// <param name="ExistingTables">A list of existing table names in the schema.</param>
        /// <returns>A string containing all the CREATE TABLE statments need to create the given entities.</returns>
        protected static string GetTableCreateStrings( DbVendor Vendor, ICollection<Type> Entities, List<string> ExistingTables = null )
        {
            Type entity;

            // A list of table names that have been created
            ExistingTables = ExistingTables ?? new List<string>();

            var sb = new StringBuilder();

            // For each of the DbSet properties
            while( ( entity = Entities.FirstOrDefault() ) != null )
            {
                var name = entity.GetAttribute<TableAttribute>().Name;

                // Get the table names for any foreign keys
                var fks = GetForeignKeysForEntity( entity );

                // Check if all the required foreign tables are created
                if( fks.Except( ExistingTables ).Any() )
                {
                    // Not all foreign key tables have been created
                    // Move the entity to the end of the list
                    Entities.Remove( entity );
                    Entities.Add( entity );
                    continue;
                }

                // Create the table
                sb.AppendLine( GetTableCreateString( entity, Vendor ) + "\n" );

                // Add the table name to the "created" list
                ExistingTables.Add( name );

                // Remove the created type from the list
                Entities.Remove( entity );
            }

            return sb.ToString();
        }

        /// <summary>
        /// Returns the table name for the given entity type.
        /// </summary>
        /// <param name="T">The entity type for which to return the table name.</param>
        /// <returns>The table name for the given entity type.</returns>
        protected static string GetTableName( Type T )
        {
            var tableAttribute = T.GetAttribute<TableAttribute>();
            return tableAttribute != null ? tableAttribute.Name : null;
        }

        /// <summary>
        /// Returns the column name for the given property.
        /// </summary>
        protected static string GetColumnName( PropertyInfo p )
        {
            var name = p.Name;
            var ca = p.GetAttribute<ColumnAttribute>();
            if( ca != null )
            {
                name = ca.Name;
            }
            return name;
        }

        protected static Type GetColumnType( PropertyInfo P )
        {
            bool temp;
            return GetColumnType( P, out temp );
        }

        protected static Type GetColumnType( PropertyInfo P, out bool Nullable )
        {
            Nullable = false;
            var type = P.PropertyType;

            // If the property type is nullable, convert to the non nullable type
            if( type.IsGenericType && type.GetGenericTypeDefinition() == typeof( Nullable<> ) )
            {
                Nullable = true;
                type = type.GetGenericArguments()[0];
            }

            return type;
        }

        protected static bool IsColumnNullable( PropertyInfo P )
        {
            bool isNullable;
            var type = GetColumnType( P, out isNullable );
            var isRequired = P.GetAttribute<RequiredAttribute>() != null;

            if( type.Name.Equals( "String" ) || type.Name.Equals( "Byte[]" ) )
            {
                isNullable = !isRequired;
            }

            return isNullable;
        }

        /// <summary>
        /// Returns a CREATE SCHEMA statement for the specified vendor.
        /// </summary>
        protected static string GetSchemaCreateString( DbVendor Vendor )
        {
            switch( Vendor )
            {
                case DbVendor.SqlServer:
                    return @"IF NOT EXISTS ( SELECT * FROM sys.schemas WHERE name = '{0}' ) EXEC( 'CREATE SCHEMA [{0}]' );";

                case DbVendor.PostgreSql:
                    return @"CREATE SCHEMA IF NOT EXISTS ""{0}"";";

                default:
                    return null;
            }
        }

        /// <summary>
        /// Returns a CREATE TABLE statement for the given entity type
        /// that can be used with the given vendor.
        /// </summary>
        protected static string GetTableCreateString( Type T, DbVendor Vendor )
        {
            switch( Vendor )
            {
                case DbVendor.SqlServer:
                    return GetTableCreateStringMS( T );

                case DbVendor.PostgreSql:
                    return GetTableCreateStringPG( T );

                default:
                    return null;
            }
        }

        protected static string GetTableDropString( string Name, DbVendor Vendor )
        {
            switch( Vendor )
            {
                case DbVendor.SqlServer:
                    return @"DROP TABLE [{0}].[" + Name + @"];";

                case DbVendor.PostgreSql:
                    return @"DROP TABLE IF EXISTS ""{0}"".""" + Name + @""" CASCADE;";

                default:
                    return null;
            }
        }

        protected static string GetColumnAddString( Type T, PropertyInfo P, DbVendor Vendor )
        {
            var sb = new StringBuilder();
            var Table = GetTableName( T );
            var column = GetColumnDefinition( T, P, Vendor );

            switch( Vendor )
            {
                case DbVendor.SqlServer:
                    // Get the column definition
                    if( column != null )
                    {
                        sb.AppendLine( @"ALTER TABLE [{0}].[" + Table + @"] ADD " + column + ";" );

                        // Get the index definition
                        var index = GetIndexCreateString( T, P, Vendor );
                        if( index != null ) sb.AppendLine( index );

                        return sb.ToString();
                    }
                    break;

                case DbVendor.PostgreSql:
                    if( column != null )
                    {
                        sb.AppendLine( @"ALTER TABLE ""{0}"".""" + Table + @""" ADD COLUMN " + column + ";" );

                        // Get the index definition
                        var index = GetIndexCreateString( T, P, Vendor );
                        if( index != null ) sb.AppendLine( index );

                        return sb.ToString();
                    }
                    break;
            }

            return null;
        }

        protected static string GetColumnDropString( string Table, string Name, DbVendor Vendor )
        {
            switch( Vendor )
            {
                case DbVendor.SqlServer:
                    return @"ALTER TABLE [{0}].[" + Table + @"] DROP COLUMN [" + Name + @"];";

                case DbVendor.PostgreSql:
                    return @"ALTER TABLE ""{0}"".""" + Table + @""" DROP COLUMN IF EXISTS """ + Name + @""" CASCADE;";

                default:
                    return null;
            }
        }

        protected static string GetColumnChangeTypeString( Type T, PropertyInfo P, DbVendor Vendor )
        {
            var Table = GetTableName( T );

            switch( Vendor )
            {
                case DbVendor.SqlServer:
                    return @"ALTER TABLE [{0}].[" + Table + @"] ALTER COLUMN " + GetColumnDefinition( T, P, Vendor ) + ";";

                case DbVendor.PostgreSql:
                    return @"ALTER TABLE ""{0}"".""" + Table + @""" ALTER COLUMN """ + GetColumnName( P ) + @""" TYPE " + GetColumnType( P, Vendor ) + ";";

                default:
                    return null;
            }
        }

        protected static string GetColumnSetNotNullString( Type T, PropertyInfo P, DbVendor Vendor )
        {
            var Table = GetTableName( T );
            var Column = GetColumnName( P );
            var sb = new StringBuilder();
            var def = GetColumnDefault( T, P, Vendor );

            switch( Vendor )
            {
                case DbVendor.SqlServer:
                    if( !string.IsNullOrEmpty( def ) )
                    {
                        sb.Append( @"UPDATE [{0}].[" + Table + @"] SET [" + Column + "] = " );
                        switch( GetColumnType( P ).Name )
                        {
                            case "Boolean":
                            case "Int32":
                            case "Int64":
                            case "Double":
                                sb.Append( "0 " );
                                break;

                            case "DateTime":
                                sb.Append( "GETUTCDATE() " );
                                break;

                            default:
                                sb.Append( "'' " );
                                break;
                        }
                        sb.AppendLine( @"WHERE [" + Column + @"] IS NULL;" );
                        sb.AppendLine( @"ALTER TABLE [{0}].[" + Table + @"] ADD " + def + " FOR [" + Column + "];" );
                    }
                    sb.AppendLine( @"ALTER TABLE [{0}].[" + Table + @"] ALTER COLUMN [" + GetColumnName( P ) + "] " + GetColumnType( P, Vendor ) + " NOT NULL;" );
                    return sb.ToString();

                case DbVendor.PostgreSql:
                    if( !string.IsNullOrEmpty( def ) )
                    {
                        // Add a default value if required
                        sb.AppendLine( @"ALTER TABLE ""{0}"".""" + Table + @""" ALTER COLUMN """ + Column + @""" SET " + def + @";" );

                        // Set the default value on all null valued rows
                        sb.Append( @"UPDATE ""{0}"".""" + Table + @""" SET """ + Column + @""" = ( SELECT " );
                        switch( GetColumnType( P ).Name )
                        {
                            case "Boolean":
                                sb.Append( "CAST( column_default AS bit ) " );
                                break;

                            case "Int32":
                                sb.Append( "CAST( column_default AS integer ) " );
                                break;

                            case "Int64":
                                sb.Append( "CAST( column_default AS bigint ) " );
                                break;

                            case "Double":
                                sb.Append( "CAST( column_default AS double precision ) " );
                                break;

                            case "DateTime":
                                sb.Append( "CASE WHEN column_default LIKE '%utc%' THEN now() at time zone 'utc' ELSE now() END " );
                                break;

                            default:
                                sb.Append( "column_default " );
                                break;
                        }
                        sb.Append( "FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = '" + Table + "' AND column_name = '" + Column + "' ) " );
                        sb.AppendLine( @"WHERE """ + Column + @""" IS NULL;" );
                    }

                    // This will fail if there are rows in the table with null values in this column
                    sb.AppendLine( @"ALTER TABLE ""{0}"".""" + Table + @""" ALTER COLUMN """ + Column + @""" SET NOT NULL;" );

                    return sb.ToString();
            }

            return null;
        }

        protected static string GetColumnDropNotNullString( Type T, PropertyInfo P, bool DropDefault, DbVendor Vendor )
        {
            var sb = new StringBuilder();
            var Table = GetTableName( T );
            var Column = GetColumnName( P );

            switch( Vendor )
            {
                case DbVendor.SqlServer:
                    sb.AppendLine( @"ALTER TABLE [{0}].[" + Table + @"] ALTER COLUMN [" + Column + "] " + GetColumnType( P, Vendor ) + " NULL;" );
                    if( DropDefault )
                    {
                        sb.AppendLine( @"ALTER TABLE [{0}].[" + Table + @"] DROP CONSTRAINT [DF_{0}_" + Table + "_" + Column + "];" );
                    }
                    return sb.ToString();

                case DbVendor.PostgreSql:
                    sb.AppendLine( @"ALTER TABLE ""{0}"".""" + Table + @""" ALTER COLUMN """ + Column + @""" DROP NOT NULL;" );

                    if( DropDefault )
                    {
                        // Remove any default value
                        sb.AppendLine( @"ALTER TABLE ""{0}"".""" + Table + @""" ALTER COLUMN """ + Column + @""" DROP DEFAULT;" );
                    }

                    return sb.ToString();
            }

            return null;
        }

        /// <summary>
        /// Returns a string containing the type definition for the given
        /// entity property. This string is suitable to use in the CREATE
        /// TABLE or ALTER TABLE ... ADD COLUMN statements for the given
        /// vendor.
        /// </summary>
        protected static string GetColumnType( PropertyInfo P, DbVendor Vendor )
        {
            var type = GetColumnType( P );

            switch( Vendor )
            {
                case DbVendor.SqlServer:
                    switch( type.Name )
                    {
                        case "String":
                            var stringLength = P.GetAttribute<StringLengthAttribute>();
                            return stringLength == null || stringLength.MaximumLength <= 0 ? "NVARCHAR(MAX)" :
                                string.Format( "NVARCHAR ({0})", stringLength.MaximumLength );

                        case "Int32":
                            return !GetColumnName( P ).Equals( "ID" ) ? "INTEGER"
                                : "[INTEGER] PRIMARY KEY IDENTITY(10000,1)";

                        case "DateTime": return "DATETIME";

                        case "Int64": return "BIGINT";

                        case "Double": return "FLOAT";

                        case "Boolean": return "BIT";
                    }
                    break;

                case DbVendor.PostgreSql:
                    switch( type.Name )
                    {
                        case "String":
                            var stringLength = P.GetAttribute<StringLengthAttribute>();
                            return stringLength == null || stringLength.MaximumLength <= 0 ? "TEXT" :
                                string.Format( "CHARACTER VARYING ({0})", stringLength.MaximumLength );

                        case "Int32":
                            return GetColumnName( P ).Equals( "ID" ) ? "BIGSERIAL PRIMARY KEY" : "INTEGER";

                        case "DateTime": return "TIMESTAMP";

                        case "Int64": return "BIGINT";

                        case "Double": return "DOUBLE PRECISION";

                        case "Boolean": return "BIT";

                        case "Byte[]": return "BYTEA";
                    }
                    break;
            }

            return null;
        }

        protected static Type GetNativeType( string DbType )
        {
            switch( DbType.ToLower() )
            {
                case "text":
                case "nvarchar":
                case "character varying":
                    return typeof( string );

                case "datetime":
                case "timestamp":
                    return typeof( DateTime );

                case "int":
                case "integer":
                case "bigserial":
                    return typeof( int );

                case "bigint":
                    return typeof( long );

                case "float":
                case "double precision":
                    return typeof( double );

                case "bit":
                    return typeof( bool );

                case "bytea":
                    return typeof( byte[] );

                default:
                    return null;
            }
        }

        protected static string GetColumnDefault( Type T, PropertyInfo P, DbVendor Vendor )
        {
            var isNullable = IsColumnNullable( P );
            if( isNullable ) return null;

            var type = GetColumnType( P );
            var isRequired = P.GetAttribute<RequiredAttribute>() != null;

            var Table = GetTableName( T );
            var Column = GetColumnName( P );

            switch( Vendor )
            {
                case DbVendor.SqlServer:
                    var name = "[DF_{0}_" + Table + "_" + Column + "]";
                    switch( type.Name )
                    {
                        case "DateTime":
                            // If a DateTime property is nullable, it does not need a default value
                            // If a DateTime property is required, it should have a value supplied
                            // If a DateTime property is not nullable and not required, it should have a default value
                            return !isRequired ? "CONSTRAINT " + name + " DEFAULT(GETUTCDATE())" : string.Empty;

                        case "Int32":
                            return P.GetAttribute<ForeignKeyAttribute>() == null && !Column.Equals( "ID" ) ? "CONSTRAINT " + name + " DEFAULT(0)" : null;

                        case "Int64":
                        case "Double":
                        case "Boolean":
                            return "CONSTRAINT " + name + " DEFAULT(0)";
                    }
                    break;

                case DbVendor.PostgreSql:
                    switch( type.Name )
                    {
                        case "DateTime":
                            // If a DateTime property is nullable, it does not need a default value
                            // If a DateTime property is required, it should have a value supplied
                            // If a DateTime property is not nullable and not required, it should have a default value
                            return !isRequired ? "DEFAULT(now() at time zone 'utc')" : string.Empty;

                        case "Int32":
                            return P.GetAttribute<ForeignKeyAttribute>() == null && !Column.Equals( "ID" ) ? "DEFAULT(0)" : null;

                        case "Int64":
                        case "Double":
                            return "DEFAULT(0)";

                        case "Boolean":
                            return "DEFAULT('0'::bit)";
                    }
                    break;
            }

            return null;
        }

        protected static string GetColumnDefinition( Type T, PropertyInfo P, DbVendor Vendor )
        {
            switch( Vendor )
            {
                case DbVendor.SqlServer:
                    return ( "[" + GetColumnName( P ) + "] " + GetColumnType( P, Vendor ) + " " +
                        ( IsColumnNullable( P ) ? "NULL " : "NOT NULL " ) + GetColumnDefault( T, P, Vendor ) ).Trim();

                case DbVendor.PostgreSql:
                    return ( "\"" + GetColumnName( P ) + "\" " + GetColumnType( P, Vendor ) + " " +
                        ( IsColumnNullable( P ) ? "NULL " : "NOT NULL " ) + GetColumnDefault( T, P, Vendor ) ).Trim();
            }

            return null;
        }

        protected static string GetIndexCreateString( Type T, PropertyInfo P, DbVendor Vendor )
        {
            var indexAttribute = P.GetAttribute<IndexAttribute>();
            if( indexAttribute == null ) return null;

            switch( Vendor )
            {
                case DbVendor.SqlServer:
                    return string.Format
                    (
                        "CREATE {0} INDEX [IX_{{0}}_{1}_{2}] ON [{{0}}].[{1}] ([{2}]);",
                        indexAttribute.Unique ? "UNIQUE" : string.Empty, GetTableName( T ), GetColumnName( P )
                    );


                case DbVendor.PostgreSql:
                    return string.Format
                    (
                        "CREATE {0} INDEX \"IX_{{0}}_{1}_{2}\" ON \"{{0}}\".\"{1}\" (\"{2}\");",
                        indexAttribute.Unique ? "UNIQUE" : string.Empty, GetTableName( T ), GetColumnName( P )
                    );

                default:
                    return null;
            }
        }

        protected static string GetIndexDropString( Type T, PropertyInfo P, DbVendor Vendor )
        {
            var Table = GetTableName( T );
            var Column = GetColumnName( P );

            switch( Vendor )
            {
                case DbVendor.SqlServer:
                    return @"DROP INDEX [{0}].[" + Table + "].[IX_{0}_" + Table + "_" + Column + @"];";

                case DbVendor.PostgreSql:
                    return @"DROP INDEX IF EXISTS ""{0}"".""IX_{0}_" + Table + "_" + Column + @""";";
            }

            return null;
        }

        protected static string GetTableCreateStringMS( Type T )
        {
            // Get the table name
            var tableName = GetTableName( T );
            if( string.IsNullOrEmpty( tableName ) )
                throw new Exception( "Missing TableAttribute on table class " + T.Name );

            var indexes = new List<string>();
            var columns = new List<string>();

            // Loop through all the attributes
            foreach( var p in GetEntityProperties( T ) )
            {
                // Get the column definition
                var column = GetColumnDefinition( T, p, DbVendor.SqlServer );
                if( column == null ) continue;
                columns.Add( column );

                // Get the index definition
                var index = GetIndexCreateString( T, p, DbVendor.SqlServer );
                if( index == null ) continue;
                indexes.Add( index );
            }

            return
                "IF NOT EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[{0}].[" + tableName + "]'))\n" +
                "BEGIN\n" +
                "  CREATE TABLE [{0}].[" + tableName + "]\n" +
                "  (\n" +
                "    " + string.Join( ",\n    ", columns.ToArray() ) + "\n" +
                "  );\n" +
                "  " + string.Join( "\n  ", indexes.ToArray() ) + "\n" +
                "END;\n\n";
        }

        protected static string GetTableCreateStringPG( Type T )
        {
            // Get the table name
            var tableName = GetTableName( T );
            if( string.IsNullOrEmpty( tableName ) )
                throw new Exception( "Missing TableAttribute on table class " + T.Name );

            // This sequence is created automatically
            var sequenceName = "\"{0}\".\"" + tableName + "_ID_seq\"";

            var indexes = new List<string>();
            var columns = new List<string>();

            // Loop through all the attributes
            foreach( var p in GetEntityProperties( T ) )
            {
                // Get the column definition
                var column = GetColumnDefinition( T, p, DbVendor.PostgreSql );
                if( column == null ) continue;
                columns.Add( column );

                // Get the index definition
                var index = GetIndexCreateString( T, p, DbVendor.PostgreSql );
                if( index == null ) continue;
                indexes.Add( index );
            }

            return
                "CREATE TABLE IF NOT EXISTS \"{0}\".\"" + tableName + "\"\n" +
                "(\n" +
                "  " + string.Join( ",\n  ", columns.ToArray() ) + "\n" +
                ");\n" +
                string.Join( "\n", indexes.ToArray() ) + "\n" +
                "SELECT setval('" + sequenceName + "', 9999, true);";
        }
    }

    public class EntityColumn
    {
        public string Name { get; set; }

        public Type Type { get; set; }

        public string Default { get; set; }

        public bool Nullable { get; set; }

        public int Length { get; set; }

        public string Index { get; set; }
    }
}
