using SqlKata;
using SqlKata.Compilers;
using SqlKata.Execution;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cellar.Orleans.Transactions.AdoNet
{
    public class DbOperator
    {
        readonly TransactionalStateStorageOptions options;

        public DbOperator(TransactionalStateStorageOptions options)
        {
            this.options = options;
        }

        Compiler GetCompiler()
        {
            Compiler c = null;
            switch (options.DbConnector)
            {
                case DbConnectors.MySql:
                case DbConnectors.MySqlConnector:
                    c = new MySqlCompiler();
                    break;
                case DbConnectors.SqlServer:
                case DbConnectors.SqlServerDotnetCore:
                    c = new SqlServerCompiler();
                    break;
                default:
                    break;
            }
            return c;
        }

        DbConnection CreateConnection()
        {
            var db = DbConnectionFactory.CreateConnection(this.options.DbConnector, this.options.ConnectionString);
            return db;
        }


        public void Dispose()
        {

        }


        public async Task<KeyEntity> ReadKeyEntity(string stateId)
        {
            using (var connection = CreateConnection())
            {
                var db = new QueryFactory(connection, GetCompiler());

                var key = await db.Query(options.KeyEntityTableName)
                    .Where(nameof(KeyEntity.StateId), stateId)
                    .FirstOrDefaultAsync<KeyEntity>().ConfigureAwait(false);

                return key;
            }
        }

        public async Task<List<StateEntity>> ReadStateEntity(string stateId)
        {
            var list = new List<StateEntity>();
            using (var connection = CreateConnection())
            {
                var db = new QueryFactory(connection, GetCompiler());

                var results = await db.Query(options.StateEntityTableName)
                    .Where(nameof(StateEntity.StateId), stateId)
                    .OrderBy(nameof(StateEntity.SequenceId))
                    .GetAsync<StateEntity>()
                    .ConfigureAwait(false);

                foreach (var row in results)
                {
                    list.Add(row);
                }
            }
            return list;
        }



        public async Task SubmitTransactionAsync(List<TableTransactionAction> list)
        {
            if (list == null || list.Count < 1)
            {
                return;
            }

            var cmp = GetCompiler();

            using (var cnn = CreateConnection())
            {
                cnn.Open();
                foreach (var item in list)
                {
                    var sql = this.BuildSql(item, cmp);
                    var cmd = cnn.CreateCommand();
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = sql.Sql;
                    cmd.Parameters.Clear();
                    for (int i = 0; i < sql.Bindings.Count; i++)
                    {
                        var prm = cmd.CreateParameter();
                        prm.ParameterName = "@p" + i.ToString();
                        prm.Value = sql.Bindings[i];
                        cmd.Parameters.Add(prm);
                    }
                    var affected = cmd.ExecuteNonQuery();

                    if (item.ActionType == TableTransactionActionType.Update
                        || item.ActionType == TableTransactionActionType.Add)
                    {
                        if (affected < 1)
                        {
                            var err = "Unexpected change when write to database. "
                                + Environment.NewLine
                                + $"sql: {sql.Sql}"
                                + Environment.NewLine
                                + $"values: {string.Join(',', sql.Bindings.ToArray())}";
                            throw new Exception(err);
                        }
                    }

                }
            }
        }

        SqlResult BuildSql(TableTransactionAction transaction, Compiler cmp)
        {
            SqlResult res = null;
            if (transaction.Key != null)
            {
                switch (transaction.ActionType)
                {
                    case TableTransactionActionType.Add:
                        res = cmp.Compile(new Query(options.KeyEntityTableName).AsInsert(transaction.Key));
                        break;
                    case TableTransactionActionType.Update:
                        res = cmp.Compile(new Query(options.KeyEntityTableName)
                              .Where(nameof(KeyEntity.StateId), transaction.Key.StateId)
                              .Where(nameof(KeyEntity.ETag), transaction.Key.ETag)
                              .AsUpdate(transaction.Key));
                        break;
                    case TableTransactionActionType.Delete:
                        res = cmp.Compile(new Query(options.KeyEntityTableName)
                            .Where(nameof(KeyEntity.StateId), transaction.Key.StateId).AsDelete());
                        break;
                    default:
                        break;
                }
            }

            if (transaction.State != null)
            {
                switch (transaction.ActionType)
                {
                    case TableTransactionActionType.Add:
                        res = cmp.Compile(new Query(options.StateEntityTableName).AsInsert(transaction.State));
                        break;
                    case TableTransactionActionType.Update:
                        res = cmp.Compile(new Query(options.StateEntityTableName)
                             .Where(nameof(StateEntity.StateId), transaction.State.StateId)
                             .Where(nameof(StateEntity.SequenceId), transaction.State.SequenceId)
                             .AsUpdate(transaction.State));
                        break;
                    case TableTransactionActionType.Delete:
                        res = cmp.Compile(new Query(options.StateEntityTableName)
                            .Where(nameof(StateEntity.StateId), transaction.State.StateId)
                            .Where(nameof(StateEntity.SequenceId), transaction.State.SequenceId)
                            .AsDelete());
                        break;
                    default:
                        break;
                }
            }

            return res;
        }

        public async Task EnsureCreateTable()
        {
            switch (this.options.DbConnector)
            {
                case DbConnectors.MySql:
                case DbConnectors.MySqlConnector:
                    await this.EnsureCreateTableMysql();
                    break;
                case DbConnectors.SqlServerDotnetCore:
                case DbConnectors.SqlServer:
                    await this.EnsureCreateTableSqlServer();
                    break;
                default:
                    break;
            }
        }

        private static string SanitizeTableName(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }

            key = key
               .Replace('/', '_')        // Forward slash
               .Replace('\\', '_')       // Backslash
               .Replace('#', '_')        // Pound sign
               .Replace('?', '_');       // Question mark

            return key;
        }

        private async Task EnsureCreateTableMysql()
        {
            this.options.KeyEntityTableName = SanitizeTableName(this.options.KeyEntityTableName);
            this.options.StateEntityTableName = SanitizeTableName(this.options.StateEntityTableName);

            if (this.options.KeyEntityTableName.Length > 64 || this.options.StateEntityTableName.Length > 64)
            {
                throw new ArgumentException("The table name is too long");
            }

            var sql_key_entity = $"CREATE TABLE {this.options.KeyEntityTableName} ( "
                  + $"{nameof(KeyEntity.StateId)} VARCHAR(255) NOT NULL, "
                  + $"{nameof(KeyEntity.CommittedSequenceId)} BIGINT    NOT NULL, "
                  + $"{nameof(KeyEntity.ETag)} VARCHAR(64) NOT NULL, "
                  + $"{nameof(KeyEntity.MetaDataJson)} TEXT  NOT NULL, "
                  + $"PRIMARY KEY ({nameof(KeyEntity.StateId)}) "
                  + "); ";

            var sql_state_entity = $"CREATE TABLE  { this.options.StateEntityTableName} ( "
                 + $"{nameof(StateEntity.StateId)} VARCHAR(255)    NOT NULL, "
                 + $"{nameof(StateEntity.SequenceId)} BIGINT       NOT NULL, "
                 + $"{nameof(StateEntity.TransactionManagerJson)} TEXT     NOT NULL, "
                 + $"{nameof(StateEntity.TStateJson)} TEXT     NOT NULL, "
                 + $"{nameof(StateEntity.Timestamp)} TIMESTAMP NOT NULL, "
                 + $"{nameof(StateEntity.TransactionId)} VARCHAR(64)    NOT NULL, "
                 + $"PRIMARY KEY({nameof(StateEntity.StateId)}, {nameof(StateEntity.SequenceId)}) "
                 + " ); ";

            var sql_check_exist = @"SELECT count(*) FROM information_schema.TABLES WHERE TABLE_NAME = '{0}' AND TABLE_SCHEMA in (SELECT DATABASE()); ";

            using (var connection = CreateConnection())
            {
                connection.Open();
                var cmd = connection.CreateCommand();
                cmd.CommandType = System.Data.CommandType.Text;
                cmd.CommandText = string.Format(sql_check_exist, this.options.KeyEntityTableName);
                var res = await cmd.ExecuteScalarAsync();
                if (res == null || !int.TryParse(res.ToString(), out int existed) || existed < 1)
                {
                    cmd.CommandText = sql_key_entity;
                    await cmd.ExecuteNonQueryAsync();
                }

                cmd.CommandText = string.Format(sql_check_exist, this.options.StateEntityTableName);
                res = await cmd.ExecuteScalarAsync();
                if (res == null || !int.TryParse(res.ToString(), out int existed2) || existed2 < 1)
                {
                    cmd.CommandText = sql_state_entity;
                    await cmd.ExecuteNonQueryAsync();
                }
            }

        }

        private async Task EnsureCreateTableSqlServer()
        {
            this.options.KeyEntityTableName = SanitizeTableName(this.options.KeyEntityTableName);
            this.options.StateEntityTableName = SanitizeTableName(this.options.StateEntityTableName);

            if (this.options.KeyEntityTableName.Length > 128 || this.options.StateEntityTableName.Length > 128)
            {
                throw new ArgumentException("The table name is too long");
            }

            var sql_key_entity = $"CREATE TABLE {this.options.KeyEntityTableName} ( "
                  + $"{nameof(KeyEntity.StateId)} VARCHAR(255) NOT NULL, "
                  + $"{nameof(KeyEntity.CommittedSequenceId)} BIGINT    NOT NULL, "
                  + $"{nameof(KeyEntity.ETag)} VARCHAR(64) NOT NULL, "
                  + $"{nameof(KeyEntity.MetaDataJson)} TEXT  NOT NULL, "
                  + $"PRIMARY KEY ({nameof(KeyEntity.StateId)}) "
                  + "); ";

            var sql_state_entity = $"CREATE TABLE  { this.options.StateEntityTableName} ( "
                 + $"{nameof(StateEntity.StateId)} VARCHAR(255)    NOT NULL, "
                 + $"{nameof(StateEntity.SequenceId)} BIGINT       NOT NULL, "
                 + $"{nameof(StateEntity.TransactionManagerJson)} TEXT     NOT NULL, "
                 + $"{nameof(StateEntity.TStateJson)} TEXT     NOT NULL, "
                 + $"{nameof(StateEntity.Timestamp)} DATETIME NOT NULL, "
                 + $"{nameof(StateEntity.TransactionId)} VARCHAR(64)    NOT NULL, "
                 + $"PRIMARY KEY({nameof(StateEntity.StateId)}, {nameof(StateEntity.SequenceId)}) "
                 + " ); ";

            var sql_check_exist = @"SELECT object_id FROM sys.tables WHERE name = '{0}' ; ";

            using (var connection = CreateConnection())
            {
                connection.Open();
                var cmd = connection.CreateCommand();
                cmd.CommandType = System.Data.CommandType.Text;
                cmd.CommandText = string.Format(sql_check_exist, this.options.KeyEntityTableName);
                var res = await cmd.ExecuteScalarAsync();
                if (res == null || !int.TryParse(res.ToString(), out int existed) || existed < 1)
                {
                    cmd.CommandText = sql_key_entity;
                    await cmd.ExecuteNonQueryAsync();
                }

                cmd.CommandText = string.Format(sql_check_exist, this.options.StateEntityTableName);
                res = await cmd.ExecuteScalarAsync();
                if (res == null || !int.TryParse(res.ToString(), out int existed2) || existed2 < 1)
                {
                    cmd.CommandText = sql_state_entity;
                    await cmd.ExecuteNonQueryAsync();
                }
            }

        }


    }
}
