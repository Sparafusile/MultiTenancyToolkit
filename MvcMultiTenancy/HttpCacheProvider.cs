using System;
using System.Web;
using System.Web.Caching;
using System.Collections.Generic;

using MultiTenancy.Common;

namespace MvcMultiTenancy
{
    class HttpCacheProvider : ICacheProvider
    {
        private static object Get( string key )
        {
            return HttpRuntime.Cache.Get( key );
        }

        public T Get<T>( string key )
        {
            var value = Get( key );
            return value == null ? default( T ) : (T)value;
        }

        public T Get<T>( string key, Func<T> f )
        {
            return Get( key, null, null, f );
        }

        public T Get<T>( string key, DateTime until, Func<T> f )
        {
            return Get( key, until, null, f );
        }

        public T Get<T>( string key, TimeSpan span, Func<T> f )
        {
            return Get( key, null, span, f );
        }

        private T Get<T>( string key, DateTime? until, TimeSpan? span, Func<T> f )
        {
            var value = Get( key );

            if( value == null )
            {
                value = f.Invoke();

                if( until.HasValue )
                {
                    Until( key, value, until.Value );
                }
                else if( span.HasValue )
                {
                    Sliding( key, value, span.Value );
                }
                else
                {
                    Forever( key, value );
                }
            }

            return value == null ? default( T ) : (T)value;
        }

        public bool Forever<T>( string key, T value )
        {
            return Until( key, value, DateTime.Now.AddYears( 1 ) );
        }

        public bool Until<T>( string key, T value, DateTime until )
        {
            try
            {
                HttpRuntime.Cache.Insert( key, value, null, until, Cache.NoSlidingExpiration, CacheItemPriority.Default, null );
                return true;
            }
            catch
            {
                return false;
            }
        }

        public bool Sliding<T>( string key, T value, TimeSpan span )
        {
            try
            {
                HttpRuntime.Cache.Insert( key, value, null, Cache.NoAbsoluteExpiration, span, CacheItemPriority.Default, null );
                return true;
            }
            catch
            {
                return false;
            }
        }

        public int Remove( string keyPart )
        {
            var keys = new List<string>();
            var enumerator = HttpRuntime.Cache.GetEnumerator();

            while( enumerator.MoveNext() )
            {
                if( enumerator.Key.ToString().Contains( keyPart ) )
                {
                    keys.Add( enumerator.Key.ToString() );
                }
            }

            foreach( var key in keys )
            {
                HttpRuntime.Cache.Remove( key );
            }

            return keys.Count;
        }
    }
}
