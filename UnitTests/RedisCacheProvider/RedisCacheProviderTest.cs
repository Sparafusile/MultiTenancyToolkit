using System;
using System.Linq;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq.Expressions;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using MultiTenancy;
using MvcMultiTenancy;
using RedisProvider;
using MultiTenancy.Common;
using MultiTenancy.SqlServer;

namespace UnitTests.RedisCacheProviderTest
{
    #region Unit Test RedisProvider, MsContext, and Models
    public class UnitTestRedisCacheProvider : RedisCacheProvider
    {
        public UnitTestRedisCacheProvider() : base( "localhost", 6379, null )
        {
        }
    }

    public class UnitTestMsContext : SqlServerMultiSchemaContext
    {
        public DbSet<Tenant> Tenants { get { return this.Set<Tenant>(); } }
        public DbSet<Location> Locations { get { return this.Set<Location>(); } }
        public DbSet<Employee> Employees { get { return this.Set<Employee>(); } }
        public DbSet<Course> Course { get { return this.Set<Course>(); } }
        public DbSet<Student> Students { get { return this.Set<Student>(); } }

        public UnitTestMsContext( string connection, string schema ) : base( connection, schema )
        {
        }

        public string GetUrnFromType( Type T )
        {
            var table = T.GetAttribute<TableAttribute>();
            if( table != null )
            {
                return "urn:" + ( table.Schema ?? this.SchemaName ) + ":" + table.Name + ":{0}";
            }
            else
            {
                return "urn:" + this.SchemaName + ":" + T.Name + ":{0}";
            }
        }

        public T Get<T>( int id, ICacheProvider cache, params Expression<Func<T, object>>[] Includes ) where T : BaseTable
        {
            var includelist = Includes
                .Select( m => m.Body as MemberExpression )
                .Where( m => m != null ).Select( m => m.Member.Name )
                .OrderBy( m => m ).ToList();

            Func<T> f = () =>
            {
                var set = this.GetDbSetForType<T>();
                var includeSet = includelist.Aggregate( set.AsQueryable(), ( current, include ) => current.Include( include ) );
                return includeSet.FirstOrDefault( m => m.ID == id );
            };

            if( cache == null ) return f.Invoke();

            var key = string.Format( this.GetUrnFromType( typeof( T ) ), id );
            if( includelist.Any() )
            {
                key += ":" + string.Join( "-", includelist );
            }

            return cache.Get( key, f );
        }
    }

    public abstract class BaseTable
    {
        [Key, Required, DatabaseGeneratedAttribute( DatabaseGeneratedOption.Identity )]
        public virtual int ID { get; set; }

        [Required, StringLength( 250 ), Index]
        public virtual string Name { get; set; }

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

    [Table( "Tenants", Schema = "dbo" )]
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

        public virtual ICollection<Employee> Employees { get; set; }
    }

    [Table( "Employees" )]
    public class Employee : BaseTable
    {
        [ForeignKey( "Location" )]
        public int LocationID { get; set; }
        [ForeignKey( "LocationID" ), IgnoreDataMember]
        public Location Location { get; set; }

        [StringLength( 50 )]
        public string FirstName { get; set; }

        [Index, StringLength( 50 )]
        public string IndexMe { get; set; }

        [StringLength( 50 )]
        public string IndexNo { get; set; }
    }

    [Table( "Courses" )]
    public class Course : BaseTable
    {
        public int Level { get; set; }
    }

    [Table( "Students" )]
    public class Student : BaseTable
    {
        public DateTime StartDate { get; set; }

        public DateTime? EndDate { get; set; }

        public int gpa { get; set; }

        public int? TeacherID { get; set; }
    }
    #endregion

    [TestClass]
    public class RedisCacheProviderTest
    {
        [TestMethod]
        public void CacheTest()
        {
            int id;
            var cache = new HttpCacheProvider( new UnitTestRedisCacheProvider() );

            using( var db = new UnitTestMsContext( "SqlServerContext", "Tenant10000" ) )
            {
                using( var con = new SqlConnection( db.Database.Connection.ConnectionString ) )
                using( var cmd = con.CreateCommand() )
                {
                    if( con.State != System.Data.ConnectionState.Open ) con.Open();

                    // Delete the existing schema
                    cmd.CommandText = @"IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[Tenant10000].[Locations]')) DROP TABLE [Tenant10000].[Locations];";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = @"IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[Tenant10000].[Employees]')) DROP TABLE [Tenant10000].[Employees];";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = @"IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[Tenant10000].[Courses]')) DROP TABLE [Tenant10000].[Courses];";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = @"IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[Tenant10000].[Students]')) DROP TABLE [Tenant10000].[Students];";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = @"IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[Tenant10000].[Customs]')) DROP TABLE [Tenant10000].[Customs];";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = @"IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'Tenant10000') DROP SCHEMA [Tenant10000];";
                    cmd.ExecuteNonQuery();
                }

                Assert.IsTrue( db.CreateTenant() );

                // Create an object
                var location = new Location
                {
                    Name = "Location 1",
                    Employees = new List<Employee>
                    {
                        new Employee { Name = "Paul" },
                        new Employee { Name = "Anndi" }
                    }
                };
                db.Locations.Add( location );
                db.SaveChanges();
                id = location.ID;

                var key = string.Format( db.GetUrnFromType( typeof( Location ) ), location.ID );
                cache.Remove( key );

                var location1 = db.Get<Location>( id, cache, m => m.Employees );
                Assert.IsNotNull( location1 );
                Assert.IsNotNull( location1.Employees );
                Assert.AreEqual( location.ID, location1.ID );
                Assert.AreEqual( location.Employees.Count, location1.Employees.Count );
            }

            using( var db = new UnitTestMsContext( "SqlServerContext", "Tenant10000" ) )
            {
                var location = db.Get<Location>( id, cache, m => m.Employees );
                Assert.IsNotNull( location );
                Assert.IsNotNull( location.Employees );
                Assert.AreEqual( 2, location.Employees.Count );

                location.Name = "Location Foo";
                var employee = location.Employees.First();
                employee.Name = "Paul Hatcher";
                db.Entry( location ).State = EntityState.Modified;
                db.Entry( employee ).State = EntityState.Modified;
                db.SaveChanges();
            }
        }
    }
}
