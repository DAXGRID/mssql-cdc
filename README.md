# Microsoft SQL - Change Data Capture (CDC)

The MS-SQL change data capture library, integrates with MSSQL through its change data capture (CDC) system. It checks a range of specified CDC groups that each contains multiple CDC tables and streams the changesets in order based on the sequence number from the database log. The library aims only to handle the CDC integration with the datasource and therefore not the destination of the events.

## ETL Streaming VS Batch ETL

Instead of using batch ETL(extract, transform, load) based on schedule the library aims to provide a simple API to do ETL streaming of database events aimed at real-time applications.
