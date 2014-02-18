using System;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Data.Entity;
using System.Collections.Generic;
using System.Data.Entity.Infrastructure;
using System.ComponentModel.DataAnnotations;

namespace MultiTenancy
{
    public abstract partial class MultiTenantContext : DbContext, IDbModelCacheKeyProvider
    {
        protected DbVendor Vendor { get; set; }
        protected string SchemaName { get; private set; }
        public string CacheKey { get { return this.GetType().Name + "." + this.SchemaName; } }

        static MultiTenantContext()
        {
            Database.SetInitializer<MultiTenantContext>( null );
        }

        protected MultiTenantContext( string connection, string SchemaName, DbVendor Vendor ) : base( connection )
        {
            this.SchemaName = SchemaName;
            this.Vendor = Vendor;
        }

        protected override void OnModelCreating( DbModelBuilder modelBuilder )
        {
            if( !string.IsNullOrEmpty( this.SchemaName ) )
            {
                modelBuilder.HasDefaultSchema( this.SchemaName );
            }

            base.OnModelCreating( modelBuilder );
        }

        #region Abstract
        protected abstract string DefaultSchema { get; }

        protected abstract List<string> GetSchemaList( DbConnection con );

        protected abstract List<string> GetTableList( DbConnection con, string Schema = null );

        protected abstract List<EntityColumn> GetColumnList( DbConnection con, string Table, string Schema = null );

        public abstract bool CreateTenant();

        public abstract bool UpdateTenants( bool Create, bool Update, bool Delete );
        #endregion

        protected void UpdateDefaultSchema( DbConnection con, bool Create, bool Update, bool Delete )
        {
            // Generate the update script based on the current entities
            var entities = GetDefaultSchemaEntityTypes( this.GetType() );
            var sql = GetUpdateScriptForSchema( con, this.DefaultSchema, entities, Create, Update, Delete );

            using( var cmd = con.CreateCommand() )
            {
                // Alter the default schema
                cmd.CommandText = string.Format( sql, this.DefaultSchema );
                if( con.State != ConnectionState.Open ) con.Open();
                cmd.ExecuteNonQuery();
            }
        }

        protected void CreateTenantSchema( DbConnection con )
        {
            var entities = GetTenantSchemaEntityTypes( this.GetType() );

            using( var cmd = con.CreateCommand() )
            {
                if( con.State != ConnectionState.Open ) con.Open();

                cmd.CommandText = string.Format( GetSchemaCreateString( this.Vendor ), this.SchemaName );
                cmd.ExecuteNonQuery();

                cmd.CommandText = string.Format( GetTableCreateStrings( this.Vendor, entities ), this.SchemaName );
                cmd.ExecuteNonQuery();
            }
        }

        protected void UpdateTenantSchemas( DbConnection con, bool Create, bool Update, bool Delete )
        {
            // Generate the update script based on the current entities
            var entities = GetTenantSchemaEntityTypes( this.GetType() );
            var sql = GetUpdateScriptForSchema( con, this.SchemaName, entities, Create, Update, Delete );

            using( var cmd = con.CreateCommand() )
            {
                // Alter all the schemas
                cmd.CommandText = string.Join( "\n", from schema in this.GetSchemaList( con ) select string.Format( sql, schema ) );
                if( con.State != ConnectionState.Open ) con.Open();
                cmd.ExecuteNonQuery();
            }
        }

        private string GetUpdateScriptForSchema( DbConnection con, string Schema, List<Type> Entities, bool Create, bool Update, bool Delete )
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
    }
}
