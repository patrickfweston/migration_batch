# Migration Batch

A Drupal 8 module to perform CSV migrations in batches.

## Overview
When migrating large CSV files into Drupal, the migration process can occasionally run out of memory. This module temporarily divides the source CSV file into smaller files to perform the migration. 

## Usage
This module simply extends the default `drush migrate:import` command with two additional options:

### Batch Size
`--batch-size` controls how many lines the temporary CSV batch files have

#### Example
```
drush migrate:import:batch sample_migration --batch-size=100
```
When run, the source CSV file for the `sample_migration` migration will be divided into smaller files with 100 lines each. These files are stored in the private files directory at the URI of `private://migration_batch/batched_migrations` and are cleaned up after completion.

### Source File
`--source-file` allows for a different file than is defined in the migration to be used

Note: Rolling back may be affected if various source files are used with a single migration.

#### Example
```
drush migrate:import:batch sample_migration --source-file=private://migrations/test.csv
```
Runs the `sample_migration` migration with the source file in the migration's configuration replaced with the file at `private://migrations/test.csv`

## Common questions
1. How does rolling back work?

Rolling back should complete correctly. The smaller batched files add to the migration map database table just like one, larger migration file. Memory is not as scarce when rolling back.

## See also
1. Writing a script to automate rerunning migrations when out of memory: https://www.mediacurrent.com/blog/memory-management-migrations-drupal-8
1. Drupal.org issue about memory reclamation: https://www.drupal.org/node/2701335
1. Drupal.org issue about batches not being restarted completely: https://www.drupal.org/node/2701121
