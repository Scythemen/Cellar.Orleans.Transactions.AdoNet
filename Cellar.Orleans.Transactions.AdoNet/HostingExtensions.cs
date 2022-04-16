using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Hosting;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;

namespace Cellar.Orleans.Transactions.AdoNet
{
    public static class HostingExtensions
    {
        public static ISiloHostBuilder AddAdoNetTransactionalStateStorageAsDefault(
            this ISiloHostBuilder builder,
            Action<TransactionalStateStorageOptions> configureOptions = null)
        {
            return builder.AddAdoNetTransactionalStateStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME,
                configureOptions);
        }

        public static ISiloHostBuilder AddAdoNetTransactionalStateStorage(
            this ISiloHostBuilder builder,
            string name,
            Action<TransactionalStateStorageOptions> configureOptions = null)
        {
            return builder.ConfigureServices(services =>
                services.AddAdoNetTransactionalStateStorage(name, ob =>
                {
                    if (configureOptions != null) ob.Configure(configureOptions);
                }));
        }


        public static ISiloBuilder AddAdoNetTransactionalStateStorageAsDefault(
            this ISiloBuilder builder,
            Action<TransactionalStateStorageOptions> configureOptions = null)
        {
            return builder.AddAdoNetTransactionalStateStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        public static ISiloBuilder AddAdoNetTransactionalStateStorage(
            this ISiloBuilder builder,
            string name,
            Action<TransactionalStateStorageOptions> configureOptions = null)
        {
            return builder.ConfigureServices(services =>
            {
                services.AddAdoNetTransactionalStateStorage(name, ob => ob.Configure(configureOptions));
            });
        }


        private static IServiceCollection AddAdoNetTransactionalStateStorage(
            this IServiceCollection services,
            string name,
            Action<OptionsBuilder<TransactionalStateStorageOptions>> configureOptions = null)
        {
            if (string.Equals(name, ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, StringComparison.Ordinal))
            {
                name = ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME;
            }

            configureOptions?.Invoke(services.AddOptions<TransactionalStateStorageOptions>(name));

            services.TryAddSingleton<ITransactionalStateStorageFactory>(sp => sp.GetServiceByName<ITransactionalStateStorageFactory>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
            services.AddSingletonNamedService<ITransactionalStateStorageFactory>(name, TransactionalStateStorageFactory.Create);
            services.AddSingletonNamedService<ILifecycleParticipant<ISiloLifecycle>>(name, (s, n) => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredServiceByName<ITransactionalStateStorageFactory>(n));
            return services;
        }

    }
}