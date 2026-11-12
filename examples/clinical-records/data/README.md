# Data: Synthea sample, April 2020

Sixteen tables of **synthetic** patient records from the MITRE Synthea project, as published at
<https://synthetichealth.github.io/synthea-sample-data/downloads/synthea_sample_data_csv_apr2020.zip>
(`supplies.csv`, which is empty in that release, is omitted). 1,171 patients, about 470,000 rows.
They are stored as Parquet, which is about 7 MB here against 82 MB of CSV; the rows and column
types are the ones DuckDB reads from the CSVs, converted with:

```sql
COPY (SELECT * FROM read_csv_auto('patients.csv')) TO 'patients.parquet' (FORMAT parquet, COMPRESSION zstd);
```

No row describes a real person. Synthea generates every patient, name, address, identifier and
encounter from population statistics and clinical modules; the trailing digits on names
(`Gómez206`) are Synthea's own marker that a value is generated. The data is here because it
**looks** like protected health information: `patients.parquet` carries SSN, driver's license, passport,
names, birth and death dates, street address, county, ZIP and coordinates, which is ten of HIPAA's
eighteen Safe Harbor identifier categories in one table, with clinical and financial detail joined
to it. That is what a model under an access policy has to govern, and what this package shows.

Synthea is Apache 2.0 licensed; the sample data is published for unrestricted secondary use. The
schema is Synthea's CSV export, documented at
<https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary>.

`zip3_small_population.parquet` is not Synthea data. It is the list of seventeen three-digit ZIP code
areas with a population of 20,000 or fewer, as named in HHS's guidance on the Safe Harbor method
(45 CFR 164.514(b)(2)(i)(B)), which a de-identified surface must report as `000`. The policy's
Appendix B points here; the Privacy Officer maintains it against the current Census.
