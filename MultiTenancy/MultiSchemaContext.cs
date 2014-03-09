using System.Data.Entity;
using System.Data.Entity.Infrastructure;

// I need to be able to:
//   Create and update non-tenant tables
//   Restrict access to a single schema
//   Allow access to the non-tenant tables

namespace MultiTenancy
{
    public abstract class MultiSchemaContext : MultiTenantContext, IDbModelCacheKeyProvider
    {
        protected string SchemaName { get; set; }
        public string CacheKey { get { return this.GetType().Name + "." + this.SchemaName; } }

        static MultiSchemaContext()
        {
            Database.SetInitializer<MultiSchemaContext>( null );
        }

        protected MultiSchemaContext( string connection, string SchemaName, DbVendor Vendor ) : base( connection, Vendor )
        {
            this.SchemaName = SchemaName;
        }

        protected override void OnModelCreating( DbModelBuilder modelBuilder )
        {
            if( !string.IsNullOrEmpty( this.SchemaName ) )
            {
                modelBuilder.HasDefaultSchema( this.SchemaName );
            }

            base.OnModelCreating( modelBuilder );
        }

        public abstract bool CreateTenant();

        public abstract bool UpdateSharedTables();

        public abstract bool UpdateTenants( bool Create, bool Update, bool Delete );
    }
}
