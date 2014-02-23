using System;
using System.Linq;
using System.Data.Entity;
using System.Linq.Expressions;
using System.ComponentModel.DataAnnotations.Schema;

namespace MultiTenancy
{
    /// <summary>
    /// This DbContext-derived class is used to manage a multi tenant
    /// database where all the tenant share tables. Any IDbSet collections
    /// associated with derived contexts will be created as FilteredDbSet 
    /// collections using the TenancyCriteria expression from the constructor.
    /// Any IDbSet collection whos base type is not derived from the generic
    /// object used to create this contex will not be filtered.
    /// </summary>
    /// <typeparam name="TEntity">
    /// The base entity from which all other entities are derived. Must 
    /// have a column that can be used to construct tenancy criteria.
    /// </typeparam>
    public abstract class SingleSchemaContext<TEntity> : DbContext where TEntity : class
    {
        protected DbVendor Vendor { get; set; }
        protected string SchemaName { get; private set; }
        protected Expression<Func<TEntity, bool>> TenancyCriteria { get; set; }

        static SingleSchemaContext()
        {
            Database.SetInitializer<MultiSchemaContext>( null );
        }

        protected SingleSchemaContext( string connection, string SchemaName, Expression<Func<TEntity, bool>> TenancyCriteria, DbVendor Vendor )
            : base( connection )
        {
            this.TenancyCriteria = TenancyCriteria;
            this.SchemaName = SchemaName;
            this.Vendor = Vendor;
        }

        private void InitDbSets()
        {
            foreach( var p in this.GetType().GetProperties() )
            {
                if( !p.PropertyType.IsGenericType ) continue;
                if( p.PropertyType.GetGenericTypeDefinition() != typeof( IDbSet<> ) ) continue;
                var e = p.PropertyType.GetGenericArguments()[0];
                var t = e.GetAttribute<TableAttribute>();
                if( t == null ) continue;

                var filteredDbSetType = typeof( FilteredDbSet<> );
                if( !typeof( TEntity ).IsSubclassOf( e ) )
                {
                    // Type should be filtered by the tenancy criteria
                    var constructedType = filteredDbSetType.MakeGenericType( e );
                    p.SetValue( this, Activator.CreateInstance( constructedType, this, TenancyCriteria ) );
                }
                else
                {
                    // Type should not be filtered by the tenancy criteria
                }
            }
        }
    }
}
