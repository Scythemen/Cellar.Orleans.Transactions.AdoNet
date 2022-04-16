//
// these codes are modified from https://github.com/dotnet/orleans/blob/v3.6.2/src/AdoNet/Shared/Storage/DbConnectionFactory.cs
//
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;

namespace Cellar.Orleans.Transactions.AdoNet
{
    public enum DbConnectors
    {
        /// <summary>
        /// MySql.Data.MySqlClient, from Oracle
        /// </summary>
        MySql,
        /// <summary>
        /// MySql.Data.MySqlConnector, from https://github.com/mysql-net/MySqlConnector/
        /// </summary>
        MySqlConnector,
        /// <summary>
        /// Microsoft.Data.SqlClient
        /// </summary>
        SqlServerDotnetCore,
        /// <summary>
        /// System.Data.SqlClient
        /// </summary>
        SqlServer
    }


    internal static class DbConnectionFactory
    {
        private static readonly ConcurrentDictionary<DbConnectors, CachedFactory> factoryCache =
            new ConcurrentDictionary<DbConnectors, CachedFactory>();

        private static readonly Dictionary<DbConnectors, Tuple<string, string>> providerFactoryTypeMap =
            new Dictionary<DbConnectors, Tuple<string, string>>
            {
                { DbConnectors.MySql, new Tuple<string, string>("MySql.Data", "MySql.Data.MySqlClient.MySqlClientFactory") },
                { DbConnectors.MySqlConnector, new Tuple<string, string>("MySqlConnector", "MySqlConnector.MySqlConnectorFactory") },
                { DbConnectors.SqlServerDotnetCore, new Tuple<string, string>("Microsoft.Data.SqlClient", "Microsoft.Data.SqlClient.SqlClientFactory") },
                { DbConnectors.SqlServer,  new Tuple<string, string>("System.Data.SqlClient", "System.Data.SqlClient.SqlClientFactory") },
            };

        private static CachedFactory GetFactory(DbConnectors connector)
        {
            var providerFactoryDefinition = providerFactoryTypeMap[connector];

            List<Exception> exceptions = new List<Exception>();

            Assembly asm = null;
            try
            {
                var asmName = new AssemblyName(providerFactoryDefinition.Item1);
                asm = Assembly.Load(asmName);
            }
            catch (Exception exc)
            {
                exceptions.Add(new InvalidOperationException($"Unable to find and/or load a candidate assembly '{providerFactoryDefinition.Item1}' for '{connector}' invariant name.", exc));
            }

            if (asm == null)
            {
                exceptions.Add(new InvalidOperationException($"Can't find database provider factory with '{connector}' invariant name. Please make sure that your ADO.Net provider package library is deployed with your application."));
            }

            var providerFactoryType = asm.GetType(providerFactoryDefinition.Item2);
            if (providerFactoryType == null)
            {
                exceptions.Add(new InvalidOperationException($"Unable to load type '{providerFactoryDefinition.Item2}' for '{connector}' invariant name."));
            }

            var prop = providerFactoryType.GetFields().SingleOrDefault(p => string.Equals(p.Name, "Instance", StringComparison.OrdinalIgnoreCase) && p.IsStatic);
            if (prop == null)
            {
                exceptions.Add(new InvalidOperationException($"Invalid provider type '{providerFactoryDefinition.Item2}' for '{connector}' invariant name."));
            }

            var factory = (DbProviderFactory)prop.GetValue(null);
            return new CachedFactory(factory, providerFactoryType.Name, "", providerFactoryType.AssemblyQualifiedName);

            if (exceptions.Count > 0)
            {
                throw new AggregateException(exceptions);
            }
        }

        public static DbConnection CreateConnection(DbConnectors connector, string connectionString)
        {
            var factory = factoryCache.GetOrAdd(connector, GetFactory).Factory;
            var connection = factory.CreateConnection();

            if (connection == null)
            {
                throw new InvalidOperationException($"Database provider factory: '{connector}' did not return a connection object.");
            }

            connection.ConnectionString = connectionString;
            return connection;
        }

        private class CachedFactory
        {
            public CachedFactory(DbProviderFactory factory, string factoryName, string factoryDescription, string factoryAssemblyQualifiedNameKey)
            {
                Factory = factory;
                FactoryName = factoryName;
                FactoryDescription = factoryDescription;
                FactoryAssemblyQualifiedNameKey = factoryAssemblyQualifiedNameKey;
            }

            /// <summary>
            /// The factory to provide vendor specific functionality.
            /// </summary>
            /// <remarks>For more about <see href="http://florianreischl.blogspot.fi/2011/08/adonet-connection-pooling-internals-and.html">ConnectionPool</see>
            /// and issues with using this factory. Take these notes into account when considering robustness of Orleans!</remarks>
            public readonly DbProviderFactory Factory;

            /// <summary>
            /// The name of the loaded factory, set by a database connector vendor.
            /// </summary>
            public readonly string FactoryName;

            /// <summary>
            /// The description of the loaded factory, set by a database connector vendor.
            /// </summary>
            public readonly string FactoryDescription;

            /// <summary>
            /// The description of the loaded factory, set by a database connector vendor.
            /// </summary>
            public readonly string FactoryAssemblyQualifiedNameKey;
        }
    }
}
