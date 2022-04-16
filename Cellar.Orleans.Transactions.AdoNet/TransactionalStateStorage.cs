using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans.Transactions;
using Orleans.Transactions.Abstractions;

namespace Cellar.Orleans.Transactions.AdoNet
{
    public class TransactionalStateStorage<TState> : ITransactionalStateStorage<TState>
        where TState : class, new()
    {
        private readonly string stateId;
        private readonly TransactionalStateStorageOptions options;
        private readonly ILogger<TransactionalStateStorage<TState>> logger;
        private List<StateEntity> stateEntityList;
        private KeyEntity keyEntity;
        private readonly DbOperator dbOperator;
        private readonly JsonSerializerSettings jsonSerializerSettings;


        public TransactionalStateStorage(
            string stateId,
            JsonSerializerSettings jsonSerializerSettings,
            TransactionalStateStorageOptions options,
            DbOperator dbOperator,
            ILogger<TransactionalStateStorage<TState>> logger)
        {
            this.dbOperator = dbOperator;
            this.stateId = stateId;
            this.options = options;
            this.logger = logger;
            this.jsonSerializerSettings = jsonSerializerSettings;
        }

        public async Task<TransactionalStorageLoadResponse<TState>> Load()
        {
            keyEntity = await dbOperator.ReadKeyEntity(this.stateId).ConfigureAwait(false);
            stateEntityList = await dbOperator.ReadStateEntity(this.stateId).ConfigureAwait(false);

            if (keyEntity == null)
            {
                keyEntity = new KeyEntity()
                {
                    StateId = this.stateId,
                };
            }

            if (string.IsNullOrEmpty(keyEntity.ETag))
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug($"{stateId} Loaded v0, fresh");

                return new TransactionalStorageLoadResponse<TState>();
            }

            TState committedState;
            if (keyEntity.CommittedSequenceId == 0)
            {
                committedState = new TState();
            }
            else
            {
                if (!FindState(keyEntity.CommittedSequenceId, out var pos))
                {
                    var error = $"Storage state corrupted: no record for committed state v{keyEntity.CommittedSequenceId}";
                    logger.LogCritical($"{stateId} {error}");
                    throw new InvalidOperationException(error);
                }
                committedState = JsonConvert.DeserializeObject<TState>(stateEntityList[pos].TStateJson, this.jsonSerializerSettings);
            }

            var prepareRecordsToRecover = new List<PendingTransactionState<TState>>();
            for (int i = 0; i < stateEntityList.Count; i++)
            {
                var state = stateEntityList[i];

                // pending states for already committed transactions can be ignored
                if (state.SequenceId <= keyEntity.CommittedSequenceId)
                {
                    continue;
                }

                // upon recovery, local non-committed transactions are considered aborted
                if (state.TransactionManagerJson == null)
                    break;

                prepareRecordsToRecover.Add(new PendingTransactionState<TState>()
                {
                    SequenceId = state.SequenceId,
                    State = JsonConvert.DeserializeObject<TState>(state.TStateJson, this.jsonSerializerSettings),
                    TimeStamp = state.Timestamp,
                    TransactionId = state.TransactionId,
                    TransactionManager = JsonConvert.DeserializeObject<ParticipantId>(state.TransactionManagerJson, this.jsonSerializerSettings)
                });
            }

            // clear the state value... no longer needed, ok to GC now
            foreach (var state in stateEntityList)
            {
                state.TStateJson = null;
            }

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"{stateId} Loaded v{this.keyEntity.CommittedSequenceId} rows={string.Join(",", stateEntityList.Select(s => s.SequenceId.ToString("x16")))}");

            var meta = JsonConvert.DeserializeObject<TransactionalStateMetaData>(keyEntity.MetaDataJson, this.jsonSerializerSettings);
            return new TransactionalStorageLoadResponse<TState>(
                keyEntity.ETag,
                committedState,
                keyEntity.CommittedSequenceId,
                meta,
                prepareRecordsToRecover);

        }

        public async Task<string> Store(
            string expectedETag,
            TransactionalStateMetaData metadata,
            List<PendingTransactionState<TState>> statesToPrepare,
            long? commitUpTo,
            long? abortAfter)
        {
            if ((!string.IsNullOrWhiteSpace(keyEntity.ETag) || !string.IsNullOrWhiteSpace(expectedETag)) && keyEntity.ETag != expectedETag)
            {
                throw new ArgumentException(nameof(expectedETag), "Etag does not match");
            }

            var batchOperation = new DbBatchOperation(stateId, dbOperator, logger);

            // first, clean up aborted records
            if (abortAfter.HasValue && stateEntityList.Count != 0)
            {
                while (stateEntityList.Count > 0 && stateEntityList[stateEntityList.Count - 1].SequenceId > abortAfter)
                {
                    var en = stateEntityList[stateEntityList.Count - 1];
                    await batchOperation.Add(new TableTransactionAction(TableTransactionActionType.Delete, en));
                    stateEntityList.RemoveAt(stateEntityList.Count - 1);

                    if (logger.IsEnabled(LogLevel.Trace))
                        logger.LogTrace($"{stateId} Delete {en.TransactionId}");
                }
            }

            // second, persist non-obsolete prepare records
            var obsoleteBefore = commitUpTo.HasValue ? commitUpTo.Value : keyEntity.CommittedSequenceId;
            if (statesToPrepare != null)
            {
                foreach (var s in statesToPrepare)
                {
                    if (s.SequenceId >= obsoleteBefore)
                    {
                        if (FindState(s.SequenceId, out var pos))
                        {
                            // overwrite with new pending state
                            var existing = stateEntityList[pos];
                            existing.TransactionId = s.TransactionId;
                            existing.Timestamp = s.TimeStamp;
                            existing.TransactionManagerJson = JsonConvert.SerializeObject(s.TransactionManager, this.jsonSerializerSettings);
                            existing.TStateJson = JsonConvert.SerializeObject(s.State, this.jsonSerializerSettings);

                            await batchOperation.Add(new TableTransactionAction(TableTransactionActionType.Update, existing)).ConfigureAwait(false);

                            if (logger.IsEnabled(LogLevel.Trace))
                                logger.LogTrace($"{this.stateId} Update {existing.TransactionId}");
                        }
                        else
                        {
                            var newState = new StateEntity
                            {
                                StateId = this.stateId,
                                SequenceId = s.SequenceId,
                                TransactionId = s.TransactionId,
                                Timestamp = s.TimeStamp,
                                TransactionManagerJson = JsonConvert.SerializeObject(s.TransactionManager, this.jsonSerializerSettings),
                                TStateJson = JsonConvert.SerializeObject(s.State, this.jsonSerializerSettings)
                            };
                            await batchOperation.Add(new TableTransactionAction(TableTransactionActionType.Add, newState)).ConfigureAwait(false);
                            stateEntityList.Insert(pos, newState);

                            if (logger.IsEnabled(LogLevel.Trace))
                                logger.LogTrace($"{this.stateId} Insert {newState.TransactionId}");
                        }

                    }
                }
            }

            // third, persist metadata and commit position
            keyEntity.MetaDataJson = JsonConvert.SerializeObject(metadata, this.jsonSerializerSettings);
            if (commitUpTo.HasValue && commitUpTo.Value > keyEntity.CommittedSequenceId)
            {
                keyEntity.CommittedSequenceId = commitUpTo.Value;
            }
            if (string.IsNullOrEmpty(this.keyEntity.ETag))
            {
                keyEntity.ETag = Guid.NewGuid().ToString();
                await batchOperation.Add(new TableTransactionAction(TableTransactionActionType.Add, keyEntity)).ConfigureAwait(false);

                if (logger.IsEnabled(LogLevel.Trace))
                    logger.LogTrace($"{stateId} Insert. v{this.keyEntity.CommittedSequenceId}, {metadata.CommitRecords.Count}c");
            }
            else
            {
                await batchOperation.Add(new TableTransactionAction(TableTransactionActionType.Update, keyEntity)).ConfigureAwait(false);

                if (logger.IsEnabled(LogLevel.Trace))
                    logger.LogTrace($"{stateId} Update. v{this.keyEntity.CommittedSequenceId}, {metadata.CommitRecords.Count}c");
            }

            // fourth, remove obsolete records
            if (this.stateEntityList.Count > 0 && stateEntityList[0].SequenceId < obsoleteBefore)
            {
                FindState(obsoleteBefore, out var pos);
                for (int i = 0; i < pos; i++)
                {
                    await batchOperation.Add(new TableTransactionAction(TableTransactionActionType.Delete, stateEntityList[i])).ConfigureAwait(false);

                    if (logger.IsEnabled(LogLevel.Trace))
                        logger.LogTrace($"{this.stateId}.{stateEntityList[i].SequenceId} Delete {stateEntityList[i].TransactionId}");
                }
                stateEntityList.RemoveRange(0, pos);
            }

            await batchOperation.Flush().ConfigureAwait(false);

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"{stateId} Stored v{this.keyEntity.CommittedSequenceId} eTag={keyEntity.ETag}");

            return keyEntity.ETag;
        }

        private bool FindState(long sequenceId, out int pos)
        {
            pos = 0;
            while (pos < this.stateEntityList.Count)
            {
                switch (this.stateEntityList[pos].SequenceId.CompareTo(sequenceId))
                {
                    case 0:
                        return true;
                    case -1:
                        pos++;
                        continue;
                    case 1:
                        return false;
                }
            }
            return false;
        }

    }
}