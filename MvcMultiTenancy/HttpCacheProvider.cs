using System;
using System.Web;
using System.Linq;
using System.Collections;
using System.Web.Caching;
using System.Collections.Generic;

using MultiTenancy.Common;

namespace MvcMultiTenancy
{
    public class HttpCacheProvider : ICacheProvider
    {
        private ICacheProvider Backup { get; set; }

        public HttpCacheProvider( ICacheProvider backup = null )
        {
            this.Backup = backup ?? new NullCacheProvider();
        }

        private static object Get( string key )
        {
            return HttpRuntime.Cache.Get( key );
        }

        private static bool ForeverSelf<T>( string key, T value )
        {
            return UntilSelf( key, value, DateTime.Now.AddYears( 1 ) );
        }

        private static bool UntilSelf<T>( string key, T value, DateTime until )
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

        private static bool SlidingSelf<T>( string key, T value, TimeSpan span )
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

        private T Get<T>( string key, DateTime? until, TimeSpan? span, Func<T> f )
        {
            var value = Get( key );

            if( value == null )
            {
                value = this.Backup.Get( key, f );

                if( until.HasValue )
                {
                    UntilSelf( key, value, until.Value );
                }
                else if( span.HasValue )
                {
                    SlidingSelf( key, value, span.Value );
                }
                else
                {
                    ForeverSelf( key, value );
                }
            }

            return value == null ? default( T ) : (T)value;
        }

        public bool HasValue( string key )
        {
            return HttpRuntime.Cache.Cast<DictionaryEntry>()
                .Any( m => m.Key.ToString().Equals( key ) );
        }

        public T Get<T>( string key )
        {
            return this.HasValue( key ) ? (T)HttpRuntime.Cache.Get( key ) : this.Backup.Get<T>( key );
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

        public bool Forever<T>( string key, T value )
        {
            return ForeverSelf( key, value ) && this.Backup.Forever( key, value );
        }

        public bool Until<T>( string key, T value, DateTime until )
        {
            return UntilSelf( key, value, until ) && this.Backup.Until( key, value, until );
        }

        public bool Sliding<T>( string key, T value, TimeSpan span )
        {
            return SlidingSelf( key, value, span ) && this.Backup.Sliding( key, value, span );
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

            this.Backup.Remove( keyPart );

            return keys.Count;
        }
    }
}
