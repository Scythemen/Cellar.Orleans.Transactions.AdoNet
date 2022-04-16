## Cellar.Orleans.Transactions.AdoNet

A transaction state storage provider in ADO.NET for [Orleans](https://github.com/dotnet/orleans). 
It provides implementations of `ITransactionalStateStorage<TState>`, `ITransactionalStateStorageFactory`, works with **MS SqlServer**, **MySql** by now. 
See more details of `Orleans Transaction`: https://dotnet.github.io/orleans/docs/grains/transactions.html

## Usage

```csharp
        siloBuilder
            .AddAdoNetTransactionalStateStorageAsDefault(cfg =>
               {
                   cfg.DbConnector = DbConnectors.SqlServer;
                   cfg.ConnectionString = sqlserverConnection;
               })
            .UseTransactions();

```

## License

This project is licensed under the MIT license.

