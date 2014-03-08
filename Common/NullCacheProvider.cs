using System;

namespace MultiTenancy.Common
{
    public class NullCacheProvider : ICacheProvider
    {
        public bool HasValue( string key )
        {
            return false;
        }

        public T Get<T>( string key )
        {
            return default( T );
        }

        public T Get<T>( string key, Func<T> f )
        {
            return f != null ? f.Invoke() : default( T );
        }

        public T Get<T>( string key, DateTime until, Func<T> f )
        {
            return f != null ? f.Invoke() : default( T );
        }

        public T Get<T>( string key, TimeSpan span, Func<T> f )
        {
            return f != null ? f.Invoke() : default( T );
        }

        public bool Forever<T>( string key, T value )
        {
            return true;
        }

        public bool Until<T>( string key, T value, DateTime until )
        {
            return true;
        }

        public bool Sliding<T>( string key, T value, TimeSpan span )
        {
            return true;
        }

        public int Remove( string keyPart )
        {
            return 0;
        }
    }
}
