using System.Linq;
using System.Data;
using System.Data.Common;
using System.Data.Entity;
using System.Collections.Generic;

// I want to be able to:
//   Get a list of databases
//   Get a list of schemas
//   Get a list of tables
//   Get column information
//   Create a new tenant
//   Update an existing tenant

namespace MultiTenancy
{
    public abstract partial class MultiTenantContext : DbContext
    {
        protected DbVendor Vendor { get; set; }

        /// <summary>
        /// dbo for SQL Server, public for PostgreSQL, etc.
        /// </summary>
        protected string DefaultSchema { get; set; }

        protected MultiTenantContext( string connection, DbVendor Vendor )
            : base( connection )
        {
            this.Vendor = Vendor;
        }

        protected abstract List<string> GetSchemaList( DbConnection con );

        protected abstract List<string> GetTableList( DbConnection con, string Schema );

        protected abstract List<EntityColumn> GetColumnList( DbConnection con, string Table, string Schema = null );

        /// <summary>
        /// Creates the tables that are shared between all the tenants. In the
        /// case of a single-schema architecture, this method creates all the
        /// tables in the database. With a multi-schema architecture, this
        /// creates the tables located in the default schema (dbo for SQL
        /// Server or public for PostgreSQL are two examples). This method
        /// only needs to be called once and in some cases not at all.
        /// </summary>
        /// <param name="con">The database connection used to issue commands.</param>
        protected void CreateSharedTables( DbConnection con )
        {
            UpdateSharedTables( con, true, false, false );
        }

        /// <summary>
        /// Updates the tables that are shared between the tenants. These are
        /// located in the default schema (dbo for SQL Server or public for 
        /// PostgreSQL are two examples).
        /// </summary>
        /// <param name="con">The database connection used to issue commands.</param>
        /// <param name="Create">Whether new items should be created in the schema.</param>
        /// <param name="Update">Whether old items should be updated in the schema if there were changes.</param>
        /// <param name="Delete">Whether existing items should be deleted if they no longer exist in the context.</param>
        protected void UpdateSharedTables( DbConnection con, bool Create, bool Update, bool Delete )
        {
            // Generate the update script based on the current entities
            var createSql = GetSchemaCreateString( this.Vendor );
            var entities = GetDefaultSchemaEntityTypes( this.GetType() );
            var updateSql = GetUpdateScriptForSchema( con, this.DefaultSchema, entities, Create, Update, Delete );

            using( var cmd = con.CreateCommand() )
            {
                if( con.State != ConnectionState.Open ) con.Open();

                // Create the schema
                cmd.CommandText = string.Format( createSql, this.DefaultSchema );
                cmd.ExecuteNonQuery();

                // Alter the schema
                cmd.CommandText = string.Format( updateSql, this.DefaultSchema );
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Creates a single tenant in the database specified in the connection
        /// using the IDbSet entities attached to the current contex. If any
        /// entities have changed, call UpdateTenants prior to calling this
        /// method.
        /// </summary>
        /// <param name="con">The database connection used to issue commands.</param>
        /// <param name="SchemaName">The name of the database schema in which the tenant will be created.</param>
        protected void CreateTenantTables( DbConnection con, string SchemaName )
        {
            // Generate the update script based on the current entities
            var createSql = GetSchemaCreateString( this.Vendor );
            var entities = GetTenantSchemaEntityTypes( this.GetType() );
            var updateSql = GetUpdateScriptForSchema( con, SchemaName, entities, true, false, false );

            using( var cmd = con.CreateCommand() )
            {
                if( con.State != ConnectionState.Open ) con.Open();

                // Create the schema
                cmd.CommandText = string.Format( createSql, SchemaName );
                cmd.ExecuteNonQuery();

                // Alter the schema
                cmd.CommandText = string.Format( updateSql, SchemaName );
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Updates the tables and columns of all existing table for all
        /// schemas returned by GetSchemaList. For multi-database designs,
        /// this method will need to be called once for each database.
        /// This method assumes that all schemas in the database are
        /// identical.
        /// </summary>
        /// <param name="con">The database connection used to issue commands.</param>
        /// <param name="Create">Whether new items should be created in the schema.</param>
        /// <param name="Update">Whether old items should be updated in the schema if there were changes.</param>
        /// <param name="Delete">Whether existing items should be deleted if they no longer exist in the context.</param>
        protected void UpdateTenantTables( DbConnection con, bool Create, bool Update, bool Delete )
        {
            // Generate the update script based on the current entities
            var SchemaNames = this.GetSchemaList( con );
            var entities = GetTenantSchemaEntityTypes( this.GetType() );
            var updateSql = GetUpdateScriptForSchema( con, SchemaNames.First(), entities, Create, Update, Delete );

            using( var cmd = con.CreateCommand() )
            {
                // Alter the schema
                cmd.CommandText = string.Join( "\n", from schema in SchemaNames select string.Format( updateSql, schema ) );
                if( con.State != ConnectionState.Open ) con.Open();
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Returns the IDbSet property associated to this context for the given type.
        /// </summary>
        /// <typeparam name="T">The type whos IDbSet to retrieve.</typeparam>
        /// <returns>The IDbSet for the given type.</returns>
        public IDbSet<T> GetDbSetForType<T>() where T : class
        {
            var p =
                this.GetType().GetProperties()
                .Where( m => m.PropertyType.IsGenericType )
                .Where
                (
                    m => m.PropertyType.GetGenericTypeDefinition() == typeof( IDbSet<> )
                      || m.PropertyType.GetGenericTypeDefinition() == typeof( DbSet<> )
                      || m.PropertyType.GetGenericTypeDefinition() == typeof( FilteredDbSet<> )
                )
                .FirstOrDefault( m => m.PropertyType.GetGenericArguments()[0] == typeof( T ) );

            if( p == null ) return null;

            return p.GetValue( this ) as IDbSet<T>;
        }
    }
}
