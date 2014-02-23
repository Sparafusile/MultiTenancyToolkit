using System;
using System.Linq;
using System.Data.Entity;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq.Expressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using MultiTenancy;

namespace UnitTests.SingleSchemaContext
{
    #region Unit Test PgContext and Models
    public class UnitTestContext : SingleSchemaContext<BaseTable>
    {
        public IDbSet<Tenant> Tenants { get; set; }
        public IDbSet<Location> Locations { get; set; }

        public UnitTestContext( string connection, string SchemaName, Expression<Func<BaseTable, bool>> TenancyCriteria, DbVendor Vendor )
            : base( connection, SchemaName, TenancyCriteria, Vendor )
        {
        }
    }

    public abstract class BaseTable
    {
        [Key, Required, DatabaseGeneratedAttribute( DatabaseGeneratedOption.Identity )]
        public virtual int ID { get; set; }

        [Required, StringLength( 250 ), Index]
        public virtual string Name { get; set; }

        public virtual int TenantID { get; set; }

        private DateTime _CreatedDate;
        public virtual DateTime CreatedDate
        {
            get { return this._CreatedDate == default( DateTime ) ? DateTime.UtcNow : this._CreatedDate; }
            set { this._CreatedDate = value; }
        }

        private DateTime _UpdatedDate;
        public virtual DateTime UpdatedDate
        {
            get { return this._UpdatedDate == default( DateTime ) ? DateTime.UtcNow : this._UpdatedDate; }
            set { this._UpdatedDate = value; }
        }

        public virtual DateTime? DeletedDate { get; set; }
    }

    [Table( "Tenants" )]
    public class Tenant : BaseTable
    {
        [StringLength( 50 )]
        public string SubDomain { get; set; }
    }

    [Table( "Locations" )]
    public class Location : BaseTable
    {
        [StringLength( 50 )]
        public string ShortString { get; set; }

        public string LongString { get; set; }
    }
    #endregion

    [TestClass]
    public class SingleSchemaContextTest
    {
        [TestMethod]
        public void Constructor()
        {
            var db = new UnitTestContext( "SqlServerContext", "dbo", m => m.TenantID == 100, DbVendor.SqlServer );
        }
    }
}
