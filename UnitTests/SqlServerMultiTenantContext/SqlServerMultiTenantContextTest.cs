using System;
using System.Linq;
using System.Data.Entity;
using System.Data.SqlClient;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using MultiTenancy;
using MultiTenancy.SqlServer;

namespace UnitTests.SqlServer
{
    #region Unit Test PgContext and Models
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
    }

    [Table( "Employees" )]
    public class Employee : BaseTable
    {
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
    public class SqlServerMultiTenantContextTest
    {
        [TestMethod]
        public void UpdatePublicTables()
        {
            var db = new UnitTestMsContext( "SqlServerContext", "Tenant10000" );

            using( var con = new SqlConnection( db.Database.Connection.ConnectionString ) )
            using( var cmd = con.CreateCommand() )
            {
                if( con.State != System.Data.ConnectionState.Open ) con.Open();

                // New entity - make sure the table is created
                cmd.CommandText = @"IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[dbo].[Tenants]')) DROP TABLE [Tenants];";
                cmd.ExecuteNonQuery();
            }

            Assert.IsTrue( db.UpdatePublicTables() );

            // Create a tenant
            db.Tenants.Add( new Tenant { Name = "Vandelay Industries", SubDomain = "vi" } );
            db.SaveChanges();

            // Assert the object was created successfully
            var t = db.Tenants.FirstOrDefault();
            Assert.IsNotNull( t );
            Assert.AreEqual( 10000, t.ID );
            Assert.AreEqual( "vi", t.SubDomain );
            Assert.AreEqual( "Vandelay Industries", t.Name );
        }

        [TestMethod]
        public void CreateTenantTables()
        {
            // Connect to the database and create a tenant 1
            var db = new UnitTestMsContext( "SqlServerContext", "Tenant10000" );

            using( var con = new SqlConnection( db.Database.Connection.ConnectionString ) )
            using( var cmd = con.CreateCommand() )
            {
                if( con.State != System.Data.ConnectionState.Open ) con.Open();

                // Delete the existing schema
                cmd.CommandText = @"IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[Tenant10000].[Locations]')) DROP TABLE [Tenant10000].[Locations];"; cmd.ExecuteNonQuery();
                cmd.CommandText = @"IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[Tenant10000].[Employees]')) DROP TABLE [Tenant10000].[Employees];"; cmd.ExecuteNonQuery();
                cmd.CommandText = @"IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[Tenant10000].[Courses]')) DROP TABLE [Tenant10000].[Courses];"; cmd.ExecuteNonQuery();
                cmd.CommandText = @"IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[Tenant10000].[Students]')) DROP TABLE [Tenant10000].[Students];"; cmd.ExecuteNonQuery();
                cmd.CommandText = @"IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'Tenant10000') DROP SCHEMA [Tenant10000];"; cmd.ExecuteNonQuery();
            }

            Assert.IsTrue( db.CreateTenant() );

            // Create an object
            db.Locations.Add( new Location { Name = "Location 1" } );
            db.SaveChanges();

            // Assert the object was created successfully
            var l = db.Locations.FirstOrDefault();
            Assert.IsNotNull( l );
            Assert.AreEqual( 10000, l.ID );
            Assert.AreEqual( "Location 1", l.Name );
        }

        [TestMethod]
        public void UpdateTenants_Individually()
        {
            const string Schema = "Tenant10000";

            // Create the tenant
            var db = new UnitTestMsContext( "SqlServerContext", Schema );

            using( var con = new SqlConnection( db.Database.Connection.ConnectionString ) )
            using( var cmd = con.CreateCommand() )
            {
                if( con.State != System.Data.ConnectionState.Open ) con.Open();

                const string sql =
                    "IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[{0}].[Locations]')) DROP TABLE [{0}].[Locations];" +
                    "IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[{0}].[Employees]')) DROP TABLE [{0}].[Employees];" +
                    "IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[{0}].[Courses]')) DROP TABLE [{0}].[Courses];" +
                    "IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[{0}].[Students]')) DROP TABLE [{0}].[Students];" +
                    "IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'{0}') DROP SCHEMA [{0}];";
                cmd.CommandText = string.Format( sql, Schema ); cmd.ExecuteNonQuery();

                Assert.IsTrue( db.CreateTenant() );

                // New entity - make sure the table is created
                cmd.CommandText = string.Format( @"IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[{0}].[Courses]')) DROP TABLE [{0}].[Courses];", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( true, false, false ) );

                cmd.CommandText = string.Format( "SELECT table_name FROM information_schema.tables WHERE table_schema = '{0}' AND table_name = 'Courses';", Schema );
                Assert.AreEqual( "Courses", cmd.ExecuteScalar() );

                cmd.CommandText = string.Format( "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Courses';", Schema );
                Assert.IsTrue( ( (int)cmd.ExecuteScalar() ) >= 5 );

                // New property - make sure the column is added
                cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Employees] DROP COLUMN [FirstName];", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( true, false, false ) );

                cmd.CommandText = string.Format( "SELECT column_name FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Employees' AND column_name = 'FirstName';", Schema );
                Assert.AreEqual( "FirstName", cmd.ExecuteScalar() );

                // Changed length of string - make sure column is updated
                cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Locations] ALTER COLUMN [ShortString] NVARCHAR(5);", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT character_maximum_length FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Locations' AND column_name = 'ShortString';", Schema );
                Assert.AreEqual( 50, cmd.ExecuteScalar() );

                cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Locations] ALTER COLUMN [LongString] NVARCHAR(50);", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT character_maximum_length FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Locations' AND column_name = 'LongString';", Schema );
                Assert.AreEqual( -1, cmd.ExecuteScalar() );

                // Made property non nullable - make sure column is non nullable and has correct default
                cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] DROP CONSTRAINT [DF_{0}_Students_StartDate];", Schema );
                cmd.ExecuteNonQuery();
                cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] ALTER COLUMN [StartDate] DATETIME NULL;", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'StartDate';", Schema );
                Assert.AreEqual( "NO", cmd.ExecuteScalar() );

                cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'StartDate';", Schema );
                Assert.IsTrue( !string.IsNullOrEmpty( cmd.ExecuteScalar().ToString() ) );

                cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] DROP CONSTRAINT [DF_{0}_Students_gpa];", Schema );
                cmd.ExecuteNonQuery();
                cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] ALTER COLUMN [gpa] INT NULL;", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'gpa';", Schema );
                Assert.AreEqual( "NO", cmd.ExecuteScalar() );

                cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'gpa';", Schema );
                Assert.AreEqual( "0", cmd.ExecuteScalar().ToString().Replace( "(", "" ).Replace( ")", "" ) );

                // Made property nullable - make sure column is nullable and no default
                cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] ALTER COLUMN [EndDate] DATETIME NOT NULL;", Schema );
                cmd.ExecuteNonQuery();
                cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] ADD CONSTRAINT [DF_{0}_Students_EndDate] DEFAULT (GETUTCDATE()) FOR [EndDate];", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'EndDate';", Schema );
                Assert.AreEqual( "YES", cmd.ExecuteScalar() );

                cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'EndDate';", Schema );
                Assert.AreEqual( DBNull.Value, cmd.ExecuteScalar() );

                cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] ALTER COLUMN [TeacherID] INT NOT NULL;", Schema );
                cmd.ExecuteNonQuery();
                cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] ADD CONSTRAINT [DF_{0}_Students_TeacherID] DEFAULT (0) FOR [TeacherID];", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'TeacherID';", Schema );
                Assert.AreEqual( "YES", cmd.ExecuteScalar() );

                cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'TeacherID';", Schema );
                Assert.AreEqual( DBNull.Value, cmd.ExecuteScalar() );

                // Added index - make sure index is placed on column
                cmd.CommandText = string.Format( @"DROP INDEX [{0}].[Employees].[IX_{0}_Employees_IndexMe];", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT name FROM sys.indexes WHERE name = 'IX_{0}_Employees_IndexMe';", Schema );
                Assert.AreEqual( string.Format( "IX_{0}_Employees_IndexMe", Schema ), cmd.ExecuteScalar() );

                // Removed index - make sure index is deleted from column
                cmd.CommandText = string.Format( @"CREATE INDEX [IX_{0}_Employees_IndexNo] ON [{0}].[Employees] ([IndexNo]);", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT name FROM sys.indexes WHERE name = 'IX_{0}_Employees_IndexNo';", Schema );
                Assert.AreEqual( null, cmd.ExecuteScalar() );

                // Removed property - make sure column is deleted
                cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Employees] ADD [DeletedColumn] VARCHAR (250) NULL;", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, false, true ) );

                cmd.CommandText = string.Format( @"SELECT column_name FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Employees' AND column_name = 'DeletedColumn';", Schema );
                Assert.AreEqual( null, cmd.ExecuteScalar() );

                // Removed entity - make sure table is deleted
                cmd.CommandText = string.Format
                (
                    @"CREATE TABLE [{0}].[DeletedTable]
                      (
                        [ID] INT PRIMARY KEY IDENTITY(10000,1) NOT NULL,
                        [Name] NVARCHAR (250) NOT NULL,
                        [CreatedDate] DATETIME DEFAULT(GETUTCDATE()) NOT NULL,
                        [UpdatedDate] DATETIME DEFAULT(GETUTCDATE()) NOT NULL,
                        [DeletedDate] DATETIME NULL
                      );
                      CREATE INDEX [IX_{0}_DeletedTable_Name] ON [{0}].[DeletedTable] ([Name]);", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, false, true ) );

                cmd.CommandText = string.Format( "SELECT table_name FROM information_schema.tables WHERE table_schema = '{0}' AND table_name = 'DeletedTable';", Schema );
                Assert.AreEqual( null, cmd.ExecuteScalar() );
            }
        }

        [TestMethod]
        public void UpdateTenants_AllTogether()
        {
            var schemaList = new[] { "Tenant10000", "Tenant10001", "Tenant10002", "Tenant10003", "Tenant10004", "Tenant10005" };

            using( var con = new SqlConnection( "Data Source=.;Initial Catalog=msTest;Persist Security Info=True;User ID=sa;Password=wwadmin;Max Pool Size=10;Min Pool Size=0;MultipleActiveResultSets=True;App=EntityFramework" ) )
            using( var cmd = con.CreateCommand() )
            {
                if( con.State != System.Data.ConnectionState.Open ) con.Open();

                foreach( var Schema in schemaList )
                {
                    const string sql =
                        "IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[{0}].[Locations]')) DROP TABLE [{0}].[Locations];" +
                        "IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[{0}].[Employees]')) DROP TABLE [{0}].[Employees];" +
                        "IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[{0}].[Courses]')) DROP TABLE [{0}].[Courses];" +
                        "IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[{0}].[Students]')) DROP TABLE [{0}].[Students];" +
                        "IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'{0}') DROP SCHEMA [{0}];";
                    cmd.CommandText = string.Format( sql, Schema ); cmd.ExecuteNonQuery();

                    // Create the tenant
                    var db = new UnitTestMsContext( "SqlServerContext", Schema );
                    Assert.IsTrue( db.CreateTenant() );

                    // New entity - make sure the table is created
                    cmd.CommandText = string.Format( @"IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[{0}].[Courses]')) DROP TABLE [{0}].[Courses];", Schema );
                    cmd.ExecuteNonQuery();

                    // New property - make sure the column is added
                    cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Employees] DROP COLUMN [FirstName];", Schema );
                    cmd.ExecuteNonQuery();

                    // Changed length of string - make sure column is updated
                    cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Locations] ALTER COLUMN [ShortString] NVARCHAR(5);", Schema );
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Locations] ALTER COLUMN [LongString] NVARCHAR(50);", Schema );
                    cmd.ExecuteNonQuery();

                    // Made property non nullable - make sure column is non nullable and has correct default
                    cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] DROP CONSTRAINT [DF_{0}_Students_StartDate];", Schema );
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] ALTER COLUMN [StartDate] DATETIME NULL;", Schema );
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] DROP CONSTRAINT [DF_{0}_Students_gpa];", Schema );
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] ALTER COLUMN [gpa] INT NULL;", Schema );
                    cmd.ExecuteNonQuery();

                    // Made property nullable - make sure column is nullable and no default
                    cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] ALTER COLUMN [EndDate] DATETIME NOT NULL;", Schema );
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] ADD CONSTRAINT [DF_{0}_Students_EndDate] DEFAULT (GETUTCDATE()) FOR [EndDate];", Schema );
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] ALTER COLUMN [TeacherID] INT NOT NULL;", Schema );
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Students] ADD CONSTRAINT [DF_{0}_Students_TeacherID] DEFAULT (0) FOR [TeacherID];", Schema );
                    cmd.ExecuteNonQuery();

                    // Added index - make sure index is placed on column
                    cmd.CommandText = string.Format( @"DROP INDEX [{0}].[Employees].[IX_{0}_Employees_IndexMe];", Schema );
                    cmd.ExecuteNonQuery();

                    // Removed index - make sure index is deleted from column
                    cmd.CommandText = string.Format( @"CREATE INDEX [IX_{0}_Employees_IndexNo] ON [{0}].[Employees] ([IndexNo]);", Schema );
                    cmd.ExecuteNonQuery();

                    // Removed property - make sure column is deleted
                    cmd.CommandText = string.Format( @"ALTER TABLE [{0}].[Employees] ADD [DeletedColumn] VARCHAR (250) NULL;", Schema );
                    cmd.ExecuteNonQuery();

                    // Removed entity - make sure table is deleted
                    cmd.CommandText = string.Format
                    (
                        @"CREATE TABLE [{0}].[DeletedTable]
                      (
                        [ID] INT PRIMARY KEY IDENTITY(10000,1) NOT NULL,
                        [Name] NVARCHAR (250) NOT NULL,
                        [CreatedDate] DATETIME DEFAULT(GETUTCDATE()) NOT NULL,
                        [UpdatedDate] DATETIME DEFAULT(GETUTCDATE()) NOT NULL,
                        [DeletedDate] DATETIME NULL
                      );
                      CREATE INDEX [IX_{0}_DeletedTable_Name] ON [{0}].[DeletedTable] ([Name]);", Schema );
                    cmd.ExecuteNonQuery();
                }

                var tempdb = new UnitTestMsContext( "SqlServerContext", schemaList[0] );
                Assert.IsTrue( tempdb.UpdateTenants( true, true, true ) );

                foreach( var Schema in schemaList )
                {
                    var db = new UnitTestMsContext( "SqlServerContext", Schema );

                    Assert.IsTrue( db.UpdateTenants( true, false, false ) );

                    cmd.CommandText = string.Format( "SELECT table_name FROM information_schema.tables WHERE table_schema = '{0}' AND table_name = 'Courses';", Schema );
                    Assert.AreEqual( "Courses", cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Courses';", Schema );
                    Assert.IsTrue( ( (int)cmd.ExecuteScalar() ) >= 5 );

                    Assert.IsTrue( db.UpdateTenants( true, false, false ) );

                    cmd.CommandText = string.Format( "SELECT column_name FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Employees' AND column_name = 'FirstName';", Schema );
                    Assert.AreEqual( "FirstName", cmd.ExecuteScalar() );

                    Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                    cmd.CommandText = string.Format( @"SELECT character_maximum_length FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Locations' AND column_name = 'ShortString';", Schema );
                    Assert.AreEqual( 50, cmd.ExecuteScalar() );

                    Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                    cmd.CommandText = string.Format( @"SELECT character_maximum_length FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Locations' AND column_name = 'LongString';", Schema );
                    Assert.AreEqual( -1, cmd.ExecuteScalar() );

                    Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                    cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'StartDate';", Schema );
                    Assert.AreEqual( "NO", cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'StartDate';", Schema );
                    Assert.IsTrue( !string.IsNullOrEmpty( cmd.ExecuteScalar().ToString() ) );

                    Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                    cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'gpa';", Schema );
                    Assert.AreEqual( "NO", cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'gpa';", Schema );
                    Assert.AreEqual( "0", cmd.ExecuteScalar().ToString().Replace( "(", "" ).Replace( ")", "" ) );

                    Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                    cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'EndDate';", Schema );
                    Assert.AreEqual( "YES", cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'EndDate';", Schema );
                    Assert.AreEqual( DBNull.Value, cmd.ExecuteScalar() );

                    Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                    cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'TeacherID';", Schema );
                    Assert.AreEqual( "YES", cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'TeacherID';", Schema );
                    Assert.AreEqual( DBNull.Value, cmd.ExecuteScalar() );

                    Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                    cmd.CommandText = string.Format( @"SELECT name FROM sys.indexes WHERE name = 'IX_{0}_Employees_IndexMe';", Schema );
                    Assert.AreEqual( string.Format( "IX_{0}_Employees_IndexMe", Schema ), cmd.ExecuteScalar() );

                    Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                    cmd.CommandText = string.Format( @"SELECT name FROM sys.indexes WHERE name = 'IX_{0}_Employees_IndexNo';", Schema );
                    Assert.AreEqual( null, cmd.ExecuteScalar() );

                    Assert.IsTrue( db.UpdateTenants( false, false, true ) );

                    cmd.CommandText = string.Format( @"SELECT column_name FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Employees' AND column_name = 'DeletedColumn';", Schema );
                    Assert.AreEqual( null, cmd.ExecuteScalar() );

                    Assert.IsTrue( db.UpdateTenants( false, false, true ) );

                    cmd.CommandText = string.Format( "SELECT table_name FROM information_schema.tables WHERE table_schema = '{0}' AND table_name = 'DeletedTable';", Schema );
                    Assert.AreEqual( null, cmd.ExecuteScalar() );
                }
            }
        }
    }
}
