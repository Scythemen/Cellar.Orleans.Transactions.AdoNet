using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cellar.Orleans.Transactions.AdoNet
{
    public enum TableTransactionActionType
    {
        Add,
        Update,
        Delete,
    }

    public class TableTransactionAction
    {
        public TableTransactionAction(TableTransactionActionType action, StateEntity state)
        {
            this.ActionType = action;
            this.State = state;
        }

        public TableTransactionAction(TableTransactionActionType action, KeyEntity key)
        {
            this.ActionType = action;
            this.Key = key;
        }


        public TableTransactionActionType ActionType { get; }
        public StateEntity State { get; }
        public KeyEntity Key { get; }

    }
}
