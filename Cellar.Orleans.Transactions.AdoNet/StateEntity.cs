using System;

namespace Cellar.Orleans.Transactions.AdoNet
{
    public class StateEntity
    {
        public string StateId { get; set; }
        public long SequenceId { get; set; }
        public string TransactionId { get; set; }
        public DateTime Timestamp { get; set; }
        public string TransactionManagerJson { get; set; }
        public string TStateJson { get; set; }

    }
}
