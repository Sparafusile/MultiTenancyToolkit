using System;

namespace MultiTenancy.Common
{
    public interface ICacheProvider
    {
        T Get<T>( string key );

        T Get<T>( string key, Func<T> f );

        T Get<T>( string key, DateTime until, Func<T> f );

        T Get<T>( string key, TimeSpan span, Func<T> f );

        bool Forever<T>( string key, T value );

        bool Until<T>( string key, T value, DateTime until );

        bool Sliding<T>( string key, T value, TimeSpan span );

        int Remove( string keyPart );
    }
}
