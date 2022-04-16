
namespace Cellar.Orleans.Transactions.AdoNet
{
    public class TransactionalStateStorageOptions
    {
        /// <summary>
        /// database connector
        /// </summary>
        public DbConnectors DbConnector { get; set; } = DbConnectors.MySqlConnector;

        /// <summary>
        /// connection string
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// the table name of StateEntity in database
        /// </summary>
        public string StateEntityTableName { get; set; } = "orleanstransactionstateentity";

        /// <summary>
        /// the table name of KeyEntity in database
        /// </summary>
        public string KeyEntityTableName { get; set; } = "orleanstransactionkeyentity";
  
    }
}