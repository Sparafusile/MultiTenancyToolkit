using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;

using Npgsql;
using MultiTenancy;

namespace PostgresMultiTenantContext
{
    public abstract class PostgresMultiTenantContext : MultiTenantContext
    {
        private const string TableQuery = @"SELECT table_name AS ""Name"" FROM information_schema.tables WHERE table_schema = '{0}';";
        private const string SchemaQuery = @"SELECT schema_name AS ""Name"" FROM information_schema.schemata WHERE schema_name NOT IN ( 'pg_toast', 'pg_catalog', 'public', 'information_schema' ) AND schema_name NOT LIKE 'pg_temp_%' AND schema_name noT LIKE 'pg_toast_temp_%';";
        private const string ColumnQuery = @"SELECT column_name AS ""Name"", data_type AS ""Type"", column_default AS ""Default"", is_nullable = 'YES' AS ""Nullable"", character_maximum_length AS ""Length"", ( SELECT indexname FROM pg_indexes WHERE indexname = 'IX_' || table_schema || '_' || table_name || '_' || column_name LIMIT 1 ) AS ""Index"" FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = '{1}';";

        protected override string DefaultSchema { get { return "public"; } }

        protected PostgresMultiTenantContext( string connection, string schema ) : base( connection, schema, DbVendor.PostgreSql )
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
                using( var con = new NpgsqlConnection( this.Database.Connection.ConnectionString ) )
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
                using( var con = new NpgsqlConnection( this.Database.Connection.ConnectionString ) )
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
                using( var con = new NpgsqlConnection( this.Database.Connection.ConnectionString ) )
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