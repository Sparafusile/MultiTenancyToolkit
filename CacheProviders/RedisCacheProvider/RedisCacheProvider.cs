using System;
using System.Configuration;
using System.Globalization;
using System.Linq;
using MultiTenancy.Common;
using ServiceStack.Redis;

namespace RedisCacheProvider
{
    public abstract class RedisCacheProvider : ICacheProvider
    {
        #region private
        private const string KeySetUrn = "urn:rediscacheprovider:keyset";
        private IRedisClientsManager ClientManager { get; set; }

        private DateTime? OutageStarted { get; set; }
        private DateTime? NextAttempt { get; set; }
        private int FailedAttempts { get; set; }

        public bool Available
        {
            get
            {
                return !NextAttempt.HasValue || NextAttempt < DateTime.UtcNow;
            }
        }

        private static string CreateRedisUrl( string Host = "localhost", int Port = 6379, string Password = null )
        {
            // password@host:port
            return ( !string.IsNullOrEmpty( Password ) ? Password + "@" : string.Empty ) + Host + ":" + Port;
        }

        private void ClearOutage()
        {
            OutageStarted = null;
            NextAttempt = null;
            FailedAttempts = 0;
        }

        private void HandleException( Exception ex )
        {
            if( !OutageStarted.HasValue )
            {
                OutageStarted = DateTime.UtcNow;
                FailedAttempts = 0;
            }

            FailedAttempts++;

            switch( FailedAttempts )
            {
                case 1:
                    NextAttempt = DateTime.UtcNow.AddMinutes( 5 );
                    break;

                case 2:
                    NextAttempt = DateTime.UtcNow.AddMinutes( 30 );
                    break;

                default:
                    NextAttempt = DateTime.Now.AddHours( 2 );
                    this.OnProlongedOutage( OutageStarted.Value, NextAttempt.Value );
                    break;
            }
        }
        #endregion

        #region Abstract
        public abstract void OnProlongedOutage( DateTime OutageStarted, DateTime NextAttempt );
        #endregion

        protected RedisCacheProvider( string Host, int Port, string Password )
        {
            try
            {
                ClientManager = new PooledRedisClientManager( CreateRedisUrl( Host, Port, Password ) );
            }
            catch
            {
                NextAttempt = DateTime.UtcNow.AddYears( 1 );
            }
        }

        public T Get<T>( string key )
        {
            if( !Available ) return default( T );

            try
            {
                using( var client = ClientManager.GetClient() )
                {
                    var value = client.Get<T>( key );

                    // Add the key to the set so we can remove them
                    // later using a wildcard search.
                    client.AddItemToSet( KeySetUrn, key );

                    ClearOutage();
                    return value;
                }
            }
            catch( Exception ex )
            {
                HandleException( ex );
                return default( T );
            }
        }

        public T Get<T>( string key, Func<T> f )
        {
            if( !Available ) return f.Invoke();

            var value = default( T );

            try
            {
                using( var client = ClientManager.GetClient() )
                {
                    if( client.ContainsKey( key ) )
                    {
                        value = client.Get<T>( key );
                    }
                    else
                    {
                        var tempF = f; f = null;
                        value = tempF.Invoke();
                        Forever( key, value );
                    }

                    ClearOutage();
                    return value;
                }
            }
            catch( Exception ex )
            {
                HandleException( ex );
                return f == null ? value : f.Invoke();
            }
        }

        public T Get<T>( string key, DateTime utcUntil, Func<T> f )
        {
            if( !Available ) return f.Invoke();

            var value = default( T );

            try
            {
                using( var client = ClientManager.GetClient() )
                {
                    if( client.ContainsKey( key ) )
                    {
                        value = client.Get<T>( key );
                    }
                    else
                    {
                        var tempF = f; f = null;
                        value = tempF.Invoke();
                        Until( key, value, utcUntil );
                    }

                    ClearOutage();
                    return value;
                }
            }
            catch( Exception ex )
            {
                HandleException( ex );
                return f == null ? value : f.Invoke();
            }
        }

        public T Get<T>( string key, TimeSpan span, Func<T> f )
        {
            throw new NotSupportedException();
        }

        public bool Forever<T>( string key, T value )
        {
            if( !Available ) return false;
            if( value == null ) return false;

            try
            {
                using( var client = ClientManager.GetClient() )
                {
                    var typedClient = client.As<T>();
                    typedClient.SetEntry( key, value );

                    // Add the key to the set so we can remove them
                    // later using a wildcard search.
                    client.AddItemToSet( KeySetUrn, key );
                }

                ClearOutage();
                return true;
            }
            catch( Exception ex )
            {
                HandleException( ex );
                return false;
            }
        }

        public bool Until<T>( string key, T value, DateTime utcUntil )
        {
            if( !Available ) return false;
            if( value == null ) return false;

            try
            {
                using( var client = ClientManager.GetClient() )
                {
                    var typedClient = client.As<T>();
                    typedClient.SetEntry( key, value, utcUntil - DateTime.UtcNow );

                    // Add the key to the set so we can remove them
                    // later using a wildcard search.
                    client.AddItemToSet( KeySetUrn, key );
                }

                ClearOutage();
                return true;
            }
            catch( Exception ex )
            {
                HandleException( ex );
                return false;
            }
        }

        public bool Sliding<T>( string key, T value, TimeSpan span )
        {
            throw new NotSupportedException();
        }

        public int Remove( string keyPart )
        {
            var count = 0;

            try
            {
                using( var client = ClientManager.GetClient() )
                {
                    foreach( var key in client.GetAllItemsFromSet( KeySetUrn ).Where( m => m.Contains( keyPart ) ) )
                    {
                        client.Remove( key );
                        client.RemoveItemFromSet( KeySetUrn, key );
                        count++;
                    }
                }
            }
            catch( Exception ex )
            {
                HandleException( ex );
            }

            return count;
        }
    }
}
