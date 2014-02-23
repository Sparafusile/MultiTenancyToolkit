using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Collections.Generic;

namespace MultiTenancy.SqlServer
{
    public abstract class SqlServerMultiSchemaContext : MultiSchemaContext
    {
        protected override string DefaultSchema { get { return "dbo"; } }
        private const string TableQuery = @"SELECT table_name AS 'Name' FROM information_schema.tables WHERE table_schema = '{0}';";
        private const string SchemaQuery = @"SELECT sys.schemas.name AS 'Name' FROM sys.all_objects LEFT JOIN sys.schemas ON sys.schemas.schema_id = sys.all_objects.schema_id WHERE sys.all_objects.type = 'u' and sys.schemas.name != 'dbo' GROUP BY sys.schemas.name ORDER BY sys.schemas.name;";
        private const string ColumnQuery = @"SELECT column_name AS 'Name', data_type AS 'Type', column_default AS 'Default', CAST( CASE is_nullable WHEN 'YES' THEN 1 ELSE 0 END AS BIT ) AS 'Nullable', character_maximum_length AS 'Length', ( SELECT TOP 1 name FROM sys.indexes WHERE name='IX_{0}_{1}_' + column_name ) AS 'Index' FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = '{1}';";

        protected SqlServerMultiSchemaContext( string connection, string schema ) : base( connection, schema, DbVendor.SqlServer )
        {
        }

        protected override List<string> GetSchemaList( DbConnection con )
        {
            var list = new List<string>();
            using( var cmd = con.CreateCommand() )
            {
                if( con.State != ConnectionState.Open ) con.Open();
                cmd.CommandText = string.Format( SchemaQuery );

                using( var reader = cmd.ExecuteReader() )
                {
                    while( reader.Read() )
                    {
                        list.Add( reader.GetString( reader.GetOrdinal( "Name" ) ) );
                    }
                }
            }
            return list;
        }

        protected override List<string> GetTableList( DbConnection con, string Schema = null )
        {
            var list = new List<string>();
            using( var cmd = con.CreateCommand() )
            {
                if( con.State != ConnectionState.Open ) con.Open();
                cmd.CommandText = string.Format( TableQuery, Schema ?? this.SchemaName );

                using( var reader = cmd.ExecuteReader() )
                {
                    while( reader.Read() )
                    {
                        list.Add( reader.GetString( reader.GetOrdinal( "Name" ) ) );
                    }
                }
            }
            return list;
        }

        protected override List<EntityColumn> GetColumnList( DbConnection con, string Table, string Schema = null )
        {
            using( var cmd = con.CreateCommand() )
            {
                cmd.CommandText = string.Format( ColumnQuery, Schema ?? this.SchemaName, Table );
                using( var reader = cmd.ExecuteReader() )
                {
                    return this.GetColumnDetails( reader );
                }
            }
        }

        public bool UpdatePublicTables()
        {
            try
            {
                // Use the connection string already established in the DBContext
                using( var con = new SqlConnection( this.Database.Connection.ConnectionString ) )
                {
                    this.UpdateDefaultSchema( con, true, true, false );
                }

                return true;
            }
            catch( Exception ex )
            {
                // Log the exception
                return false;
            }
        }

        public override bool CreateTenant()
        {
            if( string.IsNullOrEmpty( this.SchemaName ) )
                throw new Exception( "Cannot create tenant, schema name is null." );

            try
            {
                // Use the connection string already established in the DBContext
                using( var con = new SqlConnection( this.Database.Connection.ConnectionString ) )
                {
                    CreateTenantSchema( con );
                }

                return true;
            }
            catch( Exception ex )
            {
                // Log the exception
                return false;
            }
        }

        public override bool UpdateTenants( bool Create, bool Update, bool Delete )
        {
            if( string.IsNullOrEmpty( this.SchemaName ) )
                throw new Exception( "Cannot update tenants, reference schema name is null." );

            try
            {
                // Use the connection string already established in the DBContext
                using( var con = new SqlConnection( this.Database.Connection.ConnectionString ) )
                {
                    UpdateTenantSchemas( con, Create, Update, Delete );
                }

                return true;
            }
            catch( Exception ex )
            {
                // Log the exception
                return false;
            }
        }
    }
}
