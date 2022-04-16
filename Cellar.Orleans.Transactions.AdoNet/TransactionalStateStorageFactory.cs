using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Transactions;
using Orleans.Transactions.Abstractions;

namespace Cellar.Orleans.Transactions.AdoNet
{
    public class TransactionalStateStorageFactory : ITransactionalStateStorageFactory, ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly string name;
        private readonly TransactionalStateStorageOptions options;
        private readonly JsonSerializerSettings jsonSerializerSettings;
        private readonly ClusterOptions clusterOptions;
        public TransactionalStateStorageFactory(
            string name,
            TransactionalStateStorageOptions options,
            ClusterOptions clusterOptions,
            ITypeResolver typeResolver,
            IGrainFactory grainFactory)
        {
            this.name = name;
            this.options = options;
            this.jsonSerializerSettings = TransactionalStateFactory.GetJsonSerializerSettings(typeResolver, grainFactory);
            this.clusterOptions = clusterOptions;
        }

        public static ITransactionalStateStorageFactory Create(IServiceProvider services, string name)
        {
            var optionsMonitor = services.GetRequiredService<IOptionsMonitor<TransactionalStateStorageOptions>>();
            var optionsMonitor2 = services.GetRequiredService<IOptions<ClusterOptions>>();
            return ActivatorUtilities.CreateInstance<TransactionalStateStorageFactory>(services, name, optionsMonitor.Get(name), optionsMonitor2.Value);
        }

        public ITransactionalStateStorage<TState> Create<TState>(
            string stateName,
            IGrainActivationContext context) where TState : class, new()
        {
            string grainKey = context.GrainInstance.GrainReference.ToShortKeyString();
            var stateId = $"{grainKey}_{this.clusterOptions.ServiceId}_{stateName}";
            stateId = SanitizePropertyName(stateId);

            var optionsMonitor = context.ActivationServices.GetRequiredService<IOptionsMonitor<TransactionalStateStorageOptions>>();
            var dbOperator = new DbOperator(optionsMonitor.Get(name));
            var logger = context.ActivationServices.GetService<ILogger<TransactionalStateStorage<TState>>>();

            return ActivatorUtilities.CreateInstance<TransactionalStateStorage<TState>>(
                context.ActivationServices, stateId, this.jsonSerializerSettings, options, dbOperator, logger);
        }

        private static string SanitizePropertyName(string key)
        {
            key = key
               .Replace('/', '_')        // Forward slash
               .Replace('\\', '_')       // Backslash
               .Replace('#', '_')        // Pound sign
               .Replace('?', '_');       // Question mark

            if (key.Length >= 255)      // the max length of stateId in database
            {
                throw new ArgumentException(string.Format("Key length {0} is too long. Key={1}", key.Length, key));
            }

            return key;
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(OptionFormattingUtilities.Name<TransactionalStateStorageFactory>(this.name), ServiceLifecycleStage.ApplicationServices, Init);
        }

        private async Task Init(CancellationToken cancellationToken)
        {
            var dbOperator = new DbOperator(this.options);
            await dbOperator.EnsureCreateTable();
        }

    }
}