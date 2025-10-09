Use `psql -h localhost -U postgres -d ogs_db` to connec to database from WSL.

Building moves database
About 7.5 hours
creates 690 million rows
37 GB postgres Docker volume

Analyzing move_counts threshold
How many rows would be left if we filtered by threshold on grouped freq sums?

 threshold | remaining_rows
-----------+----------------
         1 |      526395651
         2 |       39781810
         3 |       26093480
         5 |       17232370
        10 |       10437881
