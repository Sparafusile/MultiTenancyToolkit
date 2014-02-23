using System;
using System.Linq;
using System.Data.Entity;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using Npgsql;
using MultiTenancy;
using MultiTenancy.PostgreSQL;

namespace UnitTests.Postgres
{
    #region Unit Test PgContext and Models
    public class UnitTestPgContext : PostgresMultiSchemaContext
    {
        public DbSet<Tenant> Tenants { get { return this.Set<Tenant>(); } }
        public DbSet<Location> Locations { get { return this.Set<Location>(); } }
        public DbSet<Employee> Employees { get { return this.Set<Employee>(); } }
        public DbSet<Course> Course { get { return this.Set<Course>(); } }
        public DbSet<Student> Students { get { return this.Set<Student>(); } }

        public UnitTestPgContext( string connection, string schema ) : base( connection, schema )
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

    [Table( "Tenants", Schema = "public" )]
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
    public class PostgresMultiTenantContextTest
    {
        [TestMethod]
        public void UpdatePublicTables()
        {
            var db = new UnitTestPgContext( "PostgresContext", "Tenant10000" );

            using( var con = new NpgsqlConnection( db.Database.Connection.ConnectionString ) )
            using( var cmd = con.CreateCommand() )
            {
                if( con.State != System.Data.ConnectionState.Open ) con.Open();

                // New entity - make sure the table is created
                cmd.CommandText = @"DROP TABLE IF EXISTS ""Tenants"" CASCADE;";
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
            var db = new UnitTestPgContext( "PostgresContext", "Tenant10000" );
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
            var db = new UnitTestPgContext( "PostgresContext", Schema );

            using( var con = new NpgsqlConnection( db.Database.Connection.ConnectionString ) )
            using( var cmd = con.CreateCommand() )
            {
                if( con.State != System.Data.ConnectionState.Open ) con.Open();

                cmd.CommandText = string.Format( @"DROP SCHEMA IF EXISTS ""{0}"" CASCADE;", Schema );
                cmd.ExecuteNonQuery();
                Assert.IsTrue( db.CreateTenant() );

                // New entity - make sure the table is created
                cmd.CommandText = string.Format( @"DROP TABLE IF EXISTS ""{0}"".""Courses"" CASCADE;", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( true, false, false ) );

                cmd.CommandText = string.Format( "SELECT table_name FROM information_schema.tables WHERE table_schema = '{0}' AND table_name = 'Courses';", Schema );
                Assert.AreEqual( "Courses", cmd.ExecuteScalar() );

                cmd.CommandText = string.Format( "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Courses';", Schema );
                Assert.IsTrue( ( (long)cmd.ExecuteScalar() ) >= 5 );

                // New property - make sure the column is added
                cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Employees"" DROP COLUMN IF EXISTS ""FirstName"" CASCADE;", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( true, false, false ) );

                cmd.CommandText = string.Format( "SELECT column_name FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Employees' AND column_name = 'FirstName';", Schema );
                Assert.AreEqual( "FirstName", cmd.ExecuteScalar() );

                // Changed length of string - make sure column is updated
                cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Locations"" ALTER COLUMN ""ShortString"" TYPE CHARACTER VARYING (5);", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT character_maximum_length FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Locations' AND column_name = 'ShortString';", Schema );
                Assert.AreEqual( 50, cmd.ExecuteScalar() );

                cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Locations"" ALTER COLUMN ""LongString"" TYPE CHARACTER VARYING (50);", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT data_type FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Locations' AND column_name = 'LongString';", Schema );
                Assert.AreEqual( "text", cmd.ExecuteScalar() );

                // Made property non nullable - make sure column is non nullable and has correct default
                cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""StartDate"" DROP DEFAULT;", Schema );
                cmd.ExecuteNonQuery();
                cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""StartDate"" DROP NOT NULL;", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'StartDate';", Schema );
                Assert.AreEqual( "NO", cmd.ExecuteScalar() );

                cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'StartDate';", Schema );
                Assert.IsTrue( !string.IsNullOrEmpty( cmd.ExecuteScalar().ToString() ) );

                cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""gpa"" DROP DEFAULT;", Schema );
                cmd.ExecuteNonQuery();
                cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""gpa"" DROP NOT NULL;", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'gpa';", Schema );
                Assert.AreEqual( "NO", cmd.ExecuteScalar() );

                cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'gpa';", Schema );
                Assert.AreEqual( "0", cmd.ExecuteScalar() );

                // Made property nullable - make sure column is nullable and no default
                cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""EndDate"" SET NOT NULL;", Schema );
                cmd.ExecuteNonQuery();
                cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""EndDate"" SET DEFAULT timezone('utc'::text, now());", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'EndDate';", Schema );
                Assert.AreEqual( "YES", cmd.ExecuteScalar() );

                cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'EndDate';", Schema );
                Assert.AreEqual( DBNull.Value, cmd.ExecuteScalar() );

                cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""TeacherID"" SET NOT NULL;", Schema );
                cmd.ExecuteNonQuery();
                cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""TeacherID"" SET DEFAULT (0);", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'TeacherID';", Schema );
                Assert.AreEqual( "YES", cmd.ExecuteScalar() );

                cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'TeacherID';", Schema );
                Assert.AreEqual( DBNull.Value, cmd.ExecuteScalar() );

                // Added index - make sure index is placed on column
                cmd.CommandText = string.Format( @"DROP INDEX IF EXISTS ""IX_{0}_Employees_IndexMe"";", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT indexname FROM pg_indexes WHERE indexname = 'IX_{0}_Employees_IndexMe';", Schema );
                Assert.AreEqual( string.Format( "IX_{0}_Employees_IndexMe", Schema ), cmd.ExecuteScalar() );

                // Removed index - make sure index is deleted from column
                cmd.CommandText = string.Format( @"CREATE INDEX ""IX_{0}_Employees_IndexNo"" ON ""{0}"".""Employees"" (""IndexNo"");", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, true, false ) );

                cmd.CommandText = string.Format( @"SELECT indexname FROM pg_indexes WHERE indexname = 'IX_{0}_Employees_IndexNo';", Schema );
                Assert.AreEqual( null, cmd.ExecuteScalar() );

                // Removed property - make sure column is deleted
                cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Employees"" ADD COLUMN ""DeletedColumn"" CHARACTER VARYING (250) NULL;", Schema );
                cmd.ExecuteNonQuery();

                Assert.IsTrue( db.UpdateTenants( false, false, true ) );

                cmd.CommandText = string.Format( @"SELECT column_name FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Employees' AND column_name = 'DeletedColumn';", Schema );
                Assert.AreEqual( null, cmd.ExecuteScalar() );

                // Removed entity - make sure table is deleted
                cmd.CommandText = string.Format
                (
                    @"CREATE TABLE IF NOT EXISTS ""{0}"".""DeletedTable""
                      (
                        ""ID"" BIGSERIAL NOT NULL,
                        ""Name"" CHARACTER VARYING (250) NOT NULL,
                        ""CreatedDate"" TIMESTAMP NOT NULL DEFAULT(now() at time zone 'utc'),
                        ""UpdatedDate"" TIMESTAMP NOT NULL DEFAULT(now() at time zone 'utc'),
                        ""DeletedDate"" TIMESTAMP NULL
                      );
                      CREATE  INDEX ""IX_{0}_DeletedTable_Name"" ON ""{0}"".""DeletedTable"" (""Name"");
                      SELECT setval('""{0}"".""DeletedTable_ID_seq""', 9999, true);", Schema );
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

            using( var con = new NpgsqlConnection( "Server=localhost;port=5433;Database=pgTest;User Id=postgres;Password=wwadmin;" ) )
            using( var cmd = con.CreateCommand() )
            {
                if( con.State != System.Data.ConnectionState.Open ) con.Open();

                foreach( var Schema in schemaList )
                {
                    cmd.CommandText = string.Format( @"DROP SCHEMA IF EXISTS ""{0}"" CASCADE;", Schema );
                    cmd.ExecuteNonQuery();

                    // Create the tenant
                    var db = new UnitTestPgContext( "PostgresContext", Schema );
                    Assert.IsTrue( db.CreateTenant() );

                    // New entity - make sure the table is created
                    cmd.CommandText = string.Format( @"DROP TABLE IF EXISTS ""{0}"".""Courses"" CASCADE;", Schema );
                    cmd.ExecuteNonQuery();

                    // New property - make sure the column is added
                    cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Employees"" DROP COLUMN IF EXISTS ""FirstName"" CASCADE;", Schema );
                    cmd.ExecuteNonQuery();

                    // Changed length of string - make sure column is updated
                    cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Locations"" ALTER COLUMN ""ShortString"" TYPE CHARACTER VARYING (5);", Schema );
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Locations"" ALTER COLUMN ""LongString"" TYPE CHARACTER VARYING (50);", Schema );
                    cmd.ExecuteNonQuery();

                    // Made property non nullable - make sure column is non nullable and has correct default
                    cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""StartDate"" DROP DEFAULT;", Schema );
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""StartDate"" DROP NOT NULL;", Schema );
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""gpa"" DROP DEFAULT;", Schema );
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""gpa"" DROP NOT NULL;", Schema );
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = string.Format( @"INSERT INTO ""{0}"".""Students"" ( ""Name"", ""EndDate"", ""TeacherID"" ) VALUES ( 'Paul', now(), 1 );", Schema );
                    cmd.ExecuteNonQuery();

                    // Made property nullable - make sure column is nullable and no default
                    cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""EndDate"" SET NOT NULL;", Schema );
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""EndDate"" SET DEFAULT timezone('utc'::text, now());", Schema );
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""TeacherID"" SET NOT NULL;", Schema );
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Students"" ALTER COLUMN ""TeacherID"" SET DEFAULT (0);", Schema );
                    cmd.ExecuteNonQuery();

                    // Added index - make sure index is placed on column
                    cmd.CommandText = string.Format( @"DROP INDEX IF EXISTS ""IX_{0}_Employees_IndexMe"";", Schema );
                    cmd.ExecuteNonQuery();

                    // Removed index - make sure index is deleted from column
                    cmd.CommandText = string.Format( @"CREATE INDEX ""IX_{0}_Employees_IndexNo"" ON ""{0}"".""Employees"" (""IndexNo"");", Schema );
                    cmd.ExecuteNonQuery();

                    // Removed property - make sure column is deleted
                    cmd.CommandText = string.Format( @"ALTER TABLE ""{0}"".""Employees"" ADD COLUMN ""DeletedColumn"" CHARACTER VARYING (250) NULL;", Schema );
                    cmd.ExecuteNonQuery();

                    // Removed entity - make sure table is deleted
                    cmd.CommandText = string.Format
                    (
                        @"CREATE TABLE IF NOT EXISTS ""{0}"".""DeletedTable""
                          (
                            ""ID"" BIGSERIAL NOT NULL,
                            ""Name"" CHARACTER VARYING (250) NOT NULL,
                            ""CreatedDate"" TIMESTAMP NOT NULL DEFAULT(now() at time zone 'utc'),
                            ""UpdatedDate"" TIMESTAMP NOT NULL DEFAULT(now() at time zone 'utc'),
                            ""DeletedDate"" TIMESTAMP NULL
                          );
                          CREATE  INDEX ""IX_{0}_DeletedTable_Name"" ON ""{0}"".""DeletedTable"" (""Name"");
                          SELECT setval('""{0}"".""DeletedTable_ID_seq""', 9999, true);", Schema );
                    cmd.ExecuteNonQuery();
                }

                var tempdb = new UnitTestPgContext( "PostgresContext", schemaList[0] );
                Assert.IsTrue( tempdb.UpdateTenants( true, true, true ) );

                foreach( var Schema in schemaList )
                {
                    cmd.CommandText = string.Format( "SELECT table_name FROM information_schema.tables WHERE table_schema = '{0}' AND table_name = 'Courses';", Schema );
                    Assert.AreEqual( "Courses", cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Courses';", Schema );
                    Assert.IsTrue( ( (long)cmd.ExecuteScalar() ) >= 5 );

                    cmd.CommandText = string.Format( "SELECT column_name FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Employees' AND column_name = 'FirstName';", Schema );
                    Assert.AreEqual( "FirstName", cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT character_maximum_length FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Locations' AND column_name = 'ShortString';", Schema );
                    Assert.AreEqual( 50, cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT data_type FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Locations' AND column_name = 'LongString';", Schema );
                    Assert.AreEqual( "text", cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'StartDate';", Schema );
                    Assert.AreEqual( "NO", cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'StartDate';", Schema );
                    Assert.IsTrue( !string.IsNullOrEmpty( cmd.ExecuteScalar().ToString() ) );

                    cmd.CommandText = string.Format( @"SELECT ""StartDate"" FROM ""{0}"".""Students"";", Schema );
                    Assert.IsInstanceOfType( cmd.ExecuteScalar(), typeof( DateTime ) );

                    cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'gpa';", Schema );
                    Assert.AreEqual( "NO", cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'gpa';", Schema );
                    Assert.AreEqual( "0", cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT ""gpa"" FROM ""{0}"".""Students"";", Schema );
                    Assert.AreEqual( 0, cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'EndDate';", Schema );
                    Assert.AreEqual( "YES", cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'EndDate';", Schema );
                    Assert.AreEqual( DBNull.Value, cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT is_nullable FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'TeacherID';", Schema );
                    Assert.AreEqual( "YES", cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT column_default FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Students' AND column_name = 'TeacherID';", Schema );
                    Assert.AreEqual( DBNull.Value, cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT indexname FROM pg_indexes WHERE indexname = 'IX_{0}_Employees_IndexMe';", Schema );
                    Assert.AreEqual( string.Format( "IX_{0}_Employees_IndexMe", Schema ), cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT indexname FROM pg_indexes WHERE indexname = 'IX_{0}_Employees_IndexNo';", Schema );
                    Assert.AreEqual( null, cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( @"SELECT column_name FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = 'Employees' AND column_name = 'DeletedColumn';", Schema );
                    Assert.AreEqual( null, cmd.ExecuteScalar() );

                    cmd.CommandText = string.Format( "SELECT table_name FROM information_schema.tables WHERE table_schema = '{0}' AND table_name = 'DeletedTable';", Schema );
                    Assert.AreEqual( null, cmd.ExecuteScalar() );
                }
            }
        }
    }
}
