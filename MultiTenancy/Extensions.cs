using System;
using System.Reflection;

namespace MultiTenancy
{
    public static class Extensions
    {
        public static T GetAttribute<T>( this ICustomAttributeProvider provider ) where T : Attribute
        {
            var attributes = provider.GetCustomAttributes( typeof( T ), true );
            return attributes.Length > 0 ? attributes[0] as T : null;
        }

        public static T GetAttribute<T>( this PropertyInfo Property ) where T : Attribute
        {
            var attributes = Property.GetCustomAttributes( typeof( T ), true );
            return attributes.Length > 0 ? attributes[0] as T : null;
        }
    }
}
