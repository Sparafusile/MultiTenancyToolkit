using System;
using System.Linq;
using System.Data.Common;
using System.Data.Entity;
using System.Linq.Expressions;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using MultiTenancy;

namespace UnitTests.SingleSchemaContext
{
    #region Unit Test PgContext and Models
    public class UnitTestContext : SingleSchemaContext<BaseTable>
    {
        public IDbSet<Tenant> Tenants { get; set; }
        public IDbSet<Location> Locations { get; set; }

        public UnitTestContext( string connection, Expression<Func<BaseTable, bool>> TenancyCriteria )
            : base( connection, "dbo", TenancyCriteria, DbVendor.SqlServer )
        {
        }

        protected override List<string> GetTableList( DbConnection con, string Schema )
        {
            return new List<string> { "Tenants", "Locations" };
        }

        protected override List<EntityColumn> GetColumnList( DbConnection con, string Table, string Schema = null )
        {
            throw new NotImplementedException();
        }

        public override bool UpdateSharedTables()
        {
            throw new NotImplementedException();
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
    public class Tenant
    {
        [Key, Required, DatabaseGeneratedAttribute( DatabaseGeneratedOption.Identity )]
        public virtual int ID { get; set; }

        [Required, StringLength( 250 ), Index]
        public virtual string Name { get; set; }

        [StringLength( 50 )]
        public string SubDomain { get; set; }

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
            var db = new UnitTestContext( "SqlServerContext", m => m.TenantID == 100 );

            foreach( var l in db.Locations.ToList() )
            {
                Assert.AreEqual( 100, l.TenantID );
            }
        }
    }
}
