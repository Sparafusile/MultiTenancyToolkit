using System;

namespace MultiTenancy
{
    public interface ICacheProvider
    {
        T Get<T>( string key );

        T Get<T>( string key, Func<T> f );

        T Get<T>( string key, DateTime until, Func<T> f );

        T Get<T>( string key, TimeSpan span, Func<T> f );

        void Forever( string key, object value );

        void Until( string key, object value, DateTime until );

        void Sliding( string key, object value, TimeSpan span );

        int Remove( string keyPart );
    }
}
