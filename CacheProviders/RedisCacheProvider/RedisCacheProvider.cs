using System;
using System.Linq;
using MultiTenancy.Common;
using ServiceStack.Redis;

namespace RedisProvider
{
    public class RedisCacheProvider : ICacheProvider
    {
        private const string KeySetUrn = "urn:rediscacheprovider:keyset";
        private IRedisClientsManager ClientManager { get; set; }

        private DateTime? OutageStarted { get; set; }
        private DateTime? NextAttempt { get; set; }
        private int FailedAttempts { get; set; }

        /// <summary>
        /// Gets if the redis server can be reached and
        /// if calls should be made to it.
        /// </summary>
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

        protected void HandleException( Exception ex )
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
                    break;
            }
        }

        private ICacheProvider Backup { get; set; }

        public RedisCacheProvider( string Host, int Port, string Password, ICacheProvider backup = null )
        {
            this.Backup = backup ?? new NullCacheProvider();

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
            if( !Available ) return this.Backup.Get<T>( key );

            try
            {
                using( var client = ClientManager.GetClient() )
                {
                    var value = client.Get<T>( key );

                    ClearOutage();
                    return value;
                }
            }
            catch( Exception ex )
            {
                HandleException( ex );
            }

            return this.Backup.Get<T>( key );
        }

        public T Get<T>( string key, Func<T> f )
        {
            if( !Available ) return this.Backup.Get( key, f );

            try
            {
                using( var client = ClientManager.GetClient() )
                {
                    T value;

                    if( this.HasKey( key ) )
                    {
                        value = client.Get<T>( key );
                    }
                    else
                    {
                        var tempF = f; f = null;
                        value = this.Backup.Get( key, tempF );
                        Forever( key, value );
                    }

                    ClearOutage();
                    return value;
                }
            }
            catch( Exception ex )
            {
                HandleException( ex );
                return this.Backup.Get( key, f );
            }
        }

        public T Get<T>( string key, DateTime until, Func<T> f )
        {
            if( !Available ) return this.Backup.Get( key, until, f );

            try
            {
                using( var client = ClientManager.GetClient() )
                {
                    T value;

                    if( client.ContainsKey( key ) )
                    {
                        value = client.Get<T>( key );
                    }
                    else
                    {
                        var tempF = f; f = null;
                        value = this.Backup.Get( key, until.ToUniversalTime(), tempF );
                        Until( key, value, until );
                    }

                    ClearOutage();
                    return value;
                }
            }
            catch( Exception ex )
            {
                HandleException( ex );
                return this.Backup.Get( key, until, f );
            }
        }

        public T Get<T>( string key, TimeSpan span, Func<T> f )
        {
            return this.Backup.Get( key, span, f );
        }

        public bool Forever<T>( string key, T value )
        {
            if( !Available ) return this.Backup.Forever( key, value );

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
            }
            catch( Exception ex )
            {
                HandleException( ex );
            }

            return this.Backup.Forever( key, value );
        }

        public bool Until<T>( string key, T value, DateTime until )
        {
            if( !Available ) return this.Backup.Until( key, value, until );

            try
            {
                using( var client = ClientManager.GetClient() )
                {
                    var typedClient = client.As<T>();
                    typedClient.SetEntry( key, value, until.ToUniversalTime() - DateTime.UtcNow );

                    // Add the key to the set so we can remove them
                    // later using a wildcard search.
                    client.AddItemToSet( KeySetUrn, key );
                }

                ClearOutage();
            }
            catch( Exception ex )
            {
                HandleException( ex );
            }

            return this.Backup.Until( key, value, until );
        }

        public bool Sliding<T>( string key, T value, TimeSpan span )
        {
            return this.Backup.Sliding( key, value, span );
        }

        public int Remove( string keyPart )
        {
            if( !Available ) return this.Backup.Remove( keyPart );

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

            this.Backup.Remove( keyPart );

            return count;
        }

        public bool HasKey( string key )
        {
            if( !Available ) return false;

            try
            {
                using( var client = ClientManager.GetClient() )
                {
                    return client.SetContainsItem( KeySetUrn, key );
                }
            }
            catch( Exception ex )
            {
                HandleException( ex );
                return false;
            }
        }
    }
}
