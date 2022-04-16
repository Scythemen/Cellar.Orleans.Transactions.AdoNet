using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cellar.Orleans.Transactions.AdoNet
{
    public class KeyEntity
    {
        public string StateId { get; set; }
        public string ETag { get; set; }
        public long CommittedSequenceId { get; set; }
        public string MetaDataJson { get; set; }
    }

}
