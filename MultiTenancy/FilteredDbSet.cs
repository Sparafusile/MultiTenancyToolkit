using System;
using System.Linq;
using System.Collections;
using System.Data.Entity;
using System.ComponentModel;
using System.Linq.Expressions;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Entity.Infrastructure;

namespace MultiTenancy
{
    public class FilteredDbSet<TEntity> : IDbSet<TEntity>, IOrderedQueryable<TEntity>, IOrderedQueryable, IQueryable<TEntity>, IQueryable, IEnumerable<TEntity>, IEnumerable, IListSource where TEntity : class
    {
        // http://www.agile-code.com/blog/entity-framework-code-first-applying-global-filters/

        private readonly DbSet<TEntity> Set;
        public Func<TEntity, bool> Filter { get; private set; }

        public IQueryable<TEntity> Include( string path )
        {
            return Set.Include( path ).Where( Filter ).AsQueryable();
        }

        public FilteredDbSet( DbSet<TEntity> set, Func<TEntity, bool> filter )
        {
            Set = set;
            Filter = filter;
        }

        public FilteredDbSet( DbSet<TEntity> set, Expression<Func<TEntity, bool>> filter ) : this( set, filter.Compile() )
        {
        }

        public IQueryable<TEntity> Unfiltered()
        {
            return Set;
        }

        public void ThrowIfEntityDoesNotMatchFilter( TEntity entity )
        {
            if( !Filter( entity ) ) throw new ArgumentOutOfRangeException();
        }

        public TEntity Add( TEntity entity )
        {
            ThrowIfEntityDoesNotMatchFilter( entity );
            return Set.Add( entity );
        }

        public TEntity Attach( TEntity entity )
        {
            ThrowIfEntityDoesNotMatchFilter( entity );
            return Set.Attach( entity );
        }

        public TDerivedEntity Create<TDerivedEntity>() where TDerivedEntity : class, TEntity
        {
            return Set.Create<TDerivedEntity>();
        }

        public TEntity Create()
        {
            return Set.Create();
        }

        public TEntity Find( params object[] keyValues )
        {
            var entity = Set.Find( keyValues );
            if( entity == null ) return null;
            ThrowIfEntityDoesNotMatchFilter( entity );
            return entity;
        }

        public TEntity Remove( TEntity entity )
        {
            ThrowIfEntityDoesNotMatchFilter( entity );
            return Set.Remove( entity );
        }

        public ObservableCollection<TEntity> Local
        {
            get { return Set.Local; }
        }

        IEnumerator<TEntity> IEnumerable<TEntity>.GetEnumerator()
        {
            return Set.Where( Filter ).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return Set.Where( Filter ).GetEnumerator();
        }

        Type IQueryable.ElementType
        {
            get { return typeof( TEntity ); }
        }

        Expression IQueryable.Expression
        {
            get
            {
                return Set.Where( Filter ).AsQueryable().Expression;
            }
        }

        IQueryProvider IQueryable.Provider
        {
            get
            {
                return Set.AsQueryable().Provider;
            }
        }

        bool IListSource.ContainsListCollection
        {
            get { return false; }
        }

        IList IListSource.GetList()
        {
            throw new InvalidOperationException();
        }

        public DbSqlQuery<TEntity> SqlQuery( string sql, params object[] parameters )
        {
            return Set.SqlQuery( sql, parameters );
        }
    }
}