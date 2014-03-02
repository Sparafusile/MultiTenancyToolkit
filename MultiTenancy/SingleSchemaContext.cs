using System;
using System.Linq;
using System.Data.Common;
using System.Data.Entity;
using System.Linq.Expressions;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;

namespace MultiTenancy
{
    /// <summary>
    /// This DbContext-derived class is used to manage a multi tenant
    /// database where all the tenant share tables. Any IDbSet collections
    /// associated with derived contexts will be created as FilteredDbSet 
    /// collections using the TenancyCriteria expression from the constructor.
    /// Any IDbSet collection whos base type is not derived from the generic
    /// parameter used to create this contex will not be filtered.
    /// </summary>
    /// <typeparam name="TEntity">
    /// The base entity from which all other entities are derived. Must 
    /// have a column that can be used to construct tenancy criteria.
    /// </typeparam>
    public abstract class SingleSchemaContext<TEntity> : MultiTenantContext where TEntity : class
    {
        /// <param name="connection">The name of the connection string in the config or the connection string itself.</param>
        /// <param name="SchemaName">The name of the schema in which the tables reside/will be created.</param>
        /// <param name="TenancyCriteria">The LINQ expression used to filter entities by client ( eg: m => m.TenantID == 100 )</param>
        /// <param name="Vendor">The database vendor.</param>
        protected SingleSchemaContext( string connection, string SchemaName, Expression<Func<TEntity, bool>> TenancyCriteria, DbVendor Vendor ) : base( connection, Vendor )
        {
            this.DefaultSchema = SchemaName;
            this.InitDbSets( TenancyCriteria );
        }

        private void InitDbSets( Expression<Func<TEntity, bool>> filter )
        {
            // Get the generic-able FilteredDbSet type
            var filteredDbSetType = typeof( FilteredDbSet<> );

            // Get the generic-able Set method for DbContext
            var setMethod = this.GetType().GetMethods()
                .First( m => m.Name == "Set" && m.IsGenericMethod );

            // Compile the filter: Expression<Func<TEntity,bool>> to Func<TEntity,bool>
            var compiledFilter = filter.Compile();

            foreach( var p in this.GetType().GetProperties() )
            {
                if( !p.PropertyType.IsGenericType ) continue;
                if( p.PropertyType.GetGenericTypeDefinition() != typeof( IDbSet<> ) ) continue;
                var e = p.PropertyType.GetGenericArguments()[0];
                var t = e.GetAttribute<TableAttribute>();
                if( t == null ) continue;

                // Get the generic Set method for the current entity type
                var genericSetMethod = setMethod.MakeGenericMethod( e );

                // Retrieve the entity set from the DbContext
                var entitySet = genericSetMethod.Invoke( this, null );

                if( typeof( TEntity ).IsAssignableFrom( e ) )
                {
                    // This type should be filtered by the tenancy criteria
                    var genericfilteredDbSetType = filteredDbSetType.MakeGenericType( e );
                    var filteredSet = Activator.CreateInstance( genericfilteredDbSetType, entitySet, compiledFilter );
                    p.SetValue( this, filteredSet );
                }
                else
                {
                    // This type should not be filtered by the tenancy criteria
                    p.SetValue( this, entitySet );
                }
            }
        }

        protected override List<string> GetSchemaList( DbConnection con )
        {
            return new List<string>();
        }

        public abstract bool UpdateSharedTables();
    }
}
