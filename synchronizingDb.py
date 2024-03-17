import os
from datetime import datetime, timezone
from urllib.parse import ParseResult, urlparse
import aiofiles
from dap.api import DAPClient
from dap.dap_types import Format, SnapshotQuery, IncrementalQuery, Credentials
from dap.replicator.sql import SQLReplicator
from dap.replicator.sql import SQLDrop
from dap.integration.database import DatabaseConnection

async def main():
    os.environ["DAP_API_URL"] = "url"
    os.environ["DAP_CLIENT_ID"] = "client id"
    os.environ["DAP_CLIENT_SECRET"] = "client secret"
    os.environ["DAP_CONNECTION_STRING"] = "connection string"

    credentials = Credentials.create(
        client_id=os.environ["DAP_CLIENT_ID"],
        client_secret=os.environ["DAP_CLIENT_SECRET"]
    )

    async with DAPClient(base_url=os.environ["DAP_API_URL"], credentials=credentials) as session:

        # Initialize the table in the database
        connection_string = os.environ["DAP_CONNECTION_STRING"]

        # Get the latest changes with an incremental query
        last_seen = datetime(2023, 10, 7, 0, 0, 0, tzinfo=timezone.utc)
        query = IncrementalQuery(format=Format.JSONL, mode=None, since=last_seen, until=None)
        result = await session.get_table_data(*schema, *table, query)
        del last_seen
        del query
        resources = await session.get_resources(result.objects)
        del result
        del resources

    # Replicate data to a PostgreSQL database
    db_connection = DatabaseConnection(connection_string)
    async with DAPClient() as session:
        await SQLReplicator(session, db_connection).initialize(*schema, *table)

    # Synchronize data with the PostgreSQL database
    # db_connection = DatabaseConnection(connection_string)
    async with DAPClient() as session:
        await SQLReplicator(session, db_connection).synchronize(*schema, *table)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
