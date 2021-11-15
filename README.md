# MS-SQL-CDC

The MS-SQL change data capture library, integrates with MSSQL through its change data capture (CDC) system. It checks a range of specified CDC groups that each contains multiple CDC tables and streams the changesets in order based on the sequence number from the MSSQL log. The library aims only to handle the CDC integration with the datasource (MS-SQL) and therefore not the destination, this decision is made to make the library as flexible as possible.

Instead of using batch ETL(extract, transform, load) based on schedule the library aims to provide a simple API to do ETL streaming of database events aimed as real-time applications.
