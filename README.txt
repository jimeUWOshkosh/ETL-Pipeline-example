# ETL-Pipeline-example
Example that shows how to munge CSV, XLS, and XLSX files

All input files you wish to play with
   aaaa.csv, mongers.xls, and mongerss.xlsx
need to be in the home directory.

Yes, the employee records layout is very weak!
name, age, utf
but this an opportunity learn ETL::Pipeline

These are my ETL::Pipeline modules in lib/Local
   DelimitedTextUnicode.pm
   EEcommon001.pm
   EExlsx001.pm

Clean up db
$ rm db/up.db

Create db
$ sqlite3 db/up.db <create.sql

# Etlpipeline.pm is a modulino!!!!
$ perl Etlpipeline.pm -f mongerss.xlsx
$ perl Etlpipeline.pm -f mongers.xls
$ perl Etlpipeline.pm -f aaaa.csv

Display db
$ sqlite3 db/up.db <display.sql

If you're wonder what the db layout is
$ more db/db.layout.txt

