using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cellar.Orleans.Transactions.AdoNet
{
    internal class DbBatchOperation
    {
        readonly DbOperator dbOperator;
        readonly ILogger logger;
        readonly string stateId;
        readonly int MaxBatchSize = 128;

        readonly object locker = new object();

        bool flushing = false;

        List<TableTransactionAction> actionList = new List<TableTransactionAction>();

        public DbBatchOperation(
            string stateId,
            DbOperator dbOperator,
            ILogger logger
            )
        {
            this.dbOperator = dbOperator;
            this.logger = logger;
            this.stateId = stateId;
        }

        public async ValueTask Add(TableTransactionAction operation)
        {
            if ((operation.Key != null && operation.Key.StateId != stateId) || (operation.State != null && operation.State.StateId != stateId))
            {
                throw new ArgumentException($"StateId not match.");
            }

            if (operation.Key != null && string.IsNullOrEmpty(operation.Key.ETag))
            {
                throw new ArgumentException($"{operation.Key.StateId} ETag can not be null or empty");
            }

            actionList.Add(operation);

            if (actionList.Count >= MaxBatchSize)
            {
                await Flush().ConfigureAwait(false);
            }
        }

        public async Task Flush()
        {
            if (actionList.Count < 1 || flushing)
            {
                return;
            }

            List<TableTransactionAction> buffer = null;

            lock (this.locker)
            {
                flushing = true;
                buffer = this.actionList;
                this.actionList = new List<TableTransactionAction>();
            }

            try
            {
                await dbOperator.SubmitTransactionAsync(buffer).ConfigureAwait(false);

                if (logger.IsEnabled(LogLevel.Trace))
                {
                    for (int i = 0; i < actionList.Count; i++)
                    {
                        logger.LogTrace($"{actionList[i].Key.StateId} batch-op ok     {i}");
                    }
                }

                buffer = null;
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Trace))
                {
                    for (int i = 0; i < buffer.Count; i++)
                    {
                        logger.LogTrace($"{buffer[i].Key.StateId} batch-op failed {i}");
                    }
                }

                this.logger.LogError("Transactional state store failed {Exception}.", ex);
                throw;
            }
            finally
            {
                flushing = false;
                Flush();
            }

        }

    }
}
