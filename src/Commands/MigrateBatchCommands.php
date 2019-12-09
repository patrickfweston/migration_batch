<?php

namespace Drupal\migration_batch\Commands;

use Consolidation\SiteAlias\SiteAliasManagerAwareInterface;
use Consolidation\SiteAlias\SiteAliasManagerAwareTrait;
use Consolidation\SiteProcess\ProcessManagerAwareTrait;
use Consolidation\SiteProcess\ProcessManagerAwareInterface;
use Drupal\Core\Datetime\DateFormatter;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\File\FileSystemInterface;
use Drupal\Core\KeyValueStore\KeyValueFactoryInterface;
use Drupal\migrate\Plugin\MigrationPluginManager;
use Drupal\migrate_tools\Commands\MigrateToolsCommands;
use Drupal\migrate\Plugin\MigrationInterface;
use Symfony\Component\Process\Exception\ProcessFailedException;

/**
 * Adds a custom migration Drush command to allow import in batches.
 */
class MigrateBatchCommands extends MigrateToolsCommands implements SiteAliasManagerAwareInterface, ProcessManagerAwareInterface {

  use SiteAliasManagerAwareTrait;
  use ProcessManagerAwareTrait;

  /**
   * The base directory to store our temporary batched files.
   *
   * @var string
   */
  protected $baseDirectory = "private://migration_batch/batched_migrations/";

  /**
   * The file system interface for modifying files.
   *
   * @var \Drupal\Core\File\FileSystemInterface
   */
  protected $fileSystem;

  /**
   * Constructor that sets up dependency injection.
   *
   * @inheritDoc
   */
  public function __construct(MigrationPluginManager $migrationPluginManager, DateFormatter $dateFormatter, EntityTypeManagerInterface $entityTypeManager, KeyValueFactoryInterface $keyValue, FileSystemInterface $fileSystem) {
    parent::__construct($migrationPluginManager, $dateFormatter, $entityTypeManager, $keyValue);
    $this->fileSystem = $fileSystem;
  }

  /**
   * Execute a migration but allow the source file to be processed in pieces.
   *
   * This builds off the base import() method defined in MigrateToolsCommands
   * and is largely copy and pasted from that method. Because of the way that
   * command is structured, it is not possible to extend the method while also
   * providing our additional "source-file" and "batch-size" parameters.
   * See /migrate_tools/src/Commands/MigrateToolsCommands.php for more
   * information.
   *
   * @param string $migration_names
   *   ID of migration to import.
   * @param array $options
   *   Additional options for the command.
   *
   * @command migrate:import:batch
   *
   * @option all Process all migrations.
   * @option group A comma-separated list of migration groups to import
   * @option tag Name of the migration tag to import
   * @option limit Limit on the number of items to process in each migration
   * @option feedback Frequency of progress messages, in items processed
   * @option source-file A file path to be used for the source for this batch
   * @option batch-size The batch size for this batch
   * @option idlist Comma-separated list of IDs to import
   * @option idlist-delimiter The delimiter for records, defaults to ':'
   * @option update  In addition to processing unprocessed items from the
   *   source, update previously-imported items with the current data
   * @option force Force an operation to run, even if all dependencies are not
   *   satisfied
   * @option execute-dependencies Execute all dependent migrations first.
   *
   * @default $options []
   *
   * @usage migrate:import:batch --all
   *   Perform all migrations
   * @usage migrate:import:batch working_paper --source-file=private://migrations/working-paper-1.json
   *   Import all migrations in working_paper using a specific file
   * @usage migrate:import:batch working_paper --batch-size=100
   *   Import all migrations in working_paper but divide and process the source
   *   file in groups of 100 items
   * @usage migrate:import:batch --tag=user
   *   Import all migrations with the user tag
   * @usage migrate:import:batch --group=beer --tag=user
   *   Import all migrations in the beer group and with the user tag
   * @usage migrate:import:batch beer_term,beer_node
   *   Import new terms and nodes
   * @usage migrate:import:batch beer_user --limit=2
   *   Import no more than 2 users
   * @usage migrate:import:batch beer_user --idlist=5
   *   Import the user record with source ID 5
   * @usage migrate:import:batch beer_node_revision --idlist=1:2,2:3,3:5
   *   Import the node revision record with source IDs [1,2], [2,3], and [3,5]
   *
   * @validate-module-enabled migrate_tools
   *
   * @aliases mimb
   *
   * @throws \Exception
   *   If there are not enough parameters to the command.
   */
  public function batch($migration_names = '', array $options = [
    'all' => FALSE,
    'group' => self::REQ,
    'tag' => self::REQ,
    'limit' => self::REQ,
    'feedback' => self::REQ,
    'source-file' => self::REQ,
    'batch-size' => self::REQ,
    'idlist' => self::REQ,
    'idlist-delimiter' => ':',
    'update' => FALSE,
    'force' => FALSE,
    'execute-dependencies' => FALSE,
  ]) {
    // Define our list of options.
    $options += [
      'all' => NULL,
      'group' => NULL,
      'tag' => NULL,
      'limit' => NULL,
      'feedback' => NULL,
      'source-file' => NULL,
      'batch-size' => NULL,
      'idlist' => NULL,
      'idlist-delimiter' => ':',
      'update' => NULL,
      'force' => NULL,
      'execute-dependencies' => NULL,
    ];

    // Set the options that correspond to selecting specific migrations to run.
    $group_names = $options['group'];
    $tag_names = $options['tag'];
    $all = $options['all'];
    $additional_options = [];

    // If none of these options are set, then there are no migrations specified
    // to be run. Throw an exception.
    if (!$all && !$group_names && !$migration_names && !$tag_names) {
      throw new \Exception(dt('You must specify --all, --group, --tag or one or more migration names separated by commas'));
    }

    // Otherwise, build out an array given all of the other possible options.
    $possible_options = [
      'limit',
      'feedback',
      'source-file',
      'batch-size',
      'idlist',
      'idlist-delimiter',
      'update',
      'force',
      'execute-dependencies',
    ];
    foreach ($possible_options as $option) {
      if ($options[$option]) {
        $additional_options[$option] = $options[$option];
      }
    }

    // Get the IDs for all of the migrations that are needed to be executed.
    $migrations = $this->migrationsList($migration_names, $options);
    if (empty($migrations)) {
      $this->logger->error(dt('No migrations found.'));
    }

    // Import each migration, one at a time.
    foreach ($migrations as $group_id => $migration_list) {
      // Call our custom executeMigration method for each migration.
      array_walk(
        $migration_list,
        [$this, 'executeMigration'],
        $additional_options
      );
    }
  }

  /**
   * Executes a single migration with batching.
   *
   * @param \Drupal\migrate\Plugin\MigrationInterface $migration
   *   The migration to execute.
   * @param string $migration_id
   *   The migration ID (not used, just an artifact of array_walk()).
   * @param array $options
   *   Additional options of the command.
   *
   * @default $options []
   *
   * @throws \Exception
   *    If some migrations failed during execution.
   */
  protected function executeMigration(MigrationInterface $migration, $migration_id, array $options = []) {
    // If the source-file option is set, update the migration source
    // accordingly with our overridden value.
    if (!empty($options['source-file'])) {
      $source = $migration->getSourceConfiguration();
      $source['path'] = $options['source-file'];
      $migration->set('source', $source);
    }

    // If the batch-size property is set, create batch files for the source
    // and execute them.
    if (!empty($options['batch-size'])) {
      // First, generate the batched files necessary for our batches.
      $source_file = $migration->get('source')['path'];
      $batched_files = $this->batchSourceFile($source_file, $options['batch-size']);
      $count_batched_files = count($batched_files);

      // Now, loop over each file and execute the migration for that chunk.
      foreach ($batched_files as $index => $file) {
        // Use our batched file as the source instead of the full source file.
        $recursive_options = $options;
        $recursive_options['source-file'] = $file;
        // Remove the "batch-size" variable as it will force us into an infinite
        // recursion if it is set.
        unset($recursive_options['batch-size']);

        // Create the Drush command for the migration recursively over each
        // file.
        $selfRecord = $this->siteAliasManager()->getSelf();
        /** @var \Consolidation\SiteProcess\SiteProcess $process */
        $process = $this->processManager()->drush($selfRecord, 'migrate:import:batch', [$migration_id], $recursive_options);

        // Run the process and log the output in real time. Wrap in a try/catch
        // so that the process doesn't end abruptly. This allows for our
        // temporary batched files to still be cleaned up on failure.
        try {
          $this->logger()->notice('Starting batch ' . ($index + 1) . ' of ' . $count_batched_files . '.');

          // Run the process and log the output in realtime.
          $process->mustRun(function ($type, $buffer) {
            echo $buffer;
          });
          $process->getOutput();

          $this->logger()->notice('Finished batch ' . ($index + 1) . ' of ' . $count_batched_files . '.');
        }
        catch (ProcessFailedException $exception) {
          // Throw the exception returned.
          echo $exception->getMessage();
        }

        // Delete our temporary batched file after its been used.
        $this->fileSystem->delete($file);
      }

      return;
    }

    // Execute the migration like normal if the batching functionality is not
    // being used.
    parent::executeMigration($migration, $migration_id, $options);
  }

  /**
   * Creates batch files for a given source file.
   *
   * @param string $source_file
   *   The path to the source file.
   * @param string $batch_size
   *   The batch size to divide the files into.
   *
   * @return array
   *   An array of file paths for our batched files.
   */
  private function batchSourceFile($source_file, $batch_size) {
    // Use an iterator to read the file to preserve memory.
    $source_file_iterator = $this->getFileIterator($source_file);

    // Create a temporary, random filename for our batched files.
    $temp_file_name = substr(md5(rand()), 0, 12);

    // Initialize parameters for the first batch.
    $batch_id = 0;
    $lines_read = 0;
    $files = [];
    $file_handle = $this->createBatchedFile($files, $temp_file_name, $batch_id);

    // Log that the batching process is being started.
    $this->logger()->notice('Generating batched files for source file ' . $source_file . '.');

    // Iterate through the lines of the original source file. An iterator is
    // used to conserve memory as these files may be quite large.
    foreach ($source_file_iterator as $line) {
      // If the number of lines read is larger than the batch size, then create
      // a new batch file.
      if ($lines_read >= $batch_size) {
        // Increment our batch ID for the new file and reset how many lines have
        // been read.
        $batch_id++;
        $lines_read = 0;

        // Close the existing file and create a new one.
        fclose($file_handle);
        $file_handle = $this->createBatchedFile($files, $temp_file_name, $batch_id);
      }

      // Write a line to the file.
      fwrite($file_handle, $line);
      $lines_read++;
    }

    // Log that batching is finished.
    $this->logger()->notice('Finished generating batch files: ' . count($files) . ' created.');

    return $files;
  }

  /**
   * Creates a batched file via Drupal's unmanaged files.
   *
   * @param array $files
   *   An array of the files created. Passed by reference so we may add to it.
   * @param string $file_name
   *   The file name for the new file to be created.
   * @param string $batch_id
   *   The batch ID corresponding to this file.
   *
   * @return bool|resource
   *   The resource for the newly created file.
   */
  private function createBatchedFile(array &$files, $file_name, $batch_id) {
    // Create the directory if it does not exist.
    $this->fileSystem->prepareDirectory($this->baseDirectory, FileSystemInterface::CREATE_DIRECTORY);

    // Generate a file name for our file.
    $destination = $this->baseDirectory . $file_name . '_' . $batch_id . '.csv';

    // Create a blank file via Drupal that is not managed in the database.
    $this->fileSystem->saveData('', $destination, FileSystemInterface::EXISTS_REPLACE);

    // Get the actual path so we can use PHP to write to the file.
    $file_path = \Drupal::service('file_system')->realpath($destination);

    // Add this file to our list of batched files created so far.
    $files[] = $file_path;

    // Return the PHP file resource.
    return fopen($file_path, 'w');
  }

  /**
   * Generate an iterator for a file.
   *
   * @param string $file_path
   *   A given file path.
   *
   * @return \Generator
   *   An iterator for this file.
   */
  private function getFileIterator($file_path) {
    // Open the file.
    $file = fopen(\Drupal::service('file_system')->realpath($file_path), 'r');

    // Return a line at a time.
    while (!feof($file)) {
      yield fgets($file);
    }

    // Close the file.
    fclose($file);
  }

  /**
   * Clears out files in the batched directory.
   *
   * @command migrate:import:batch-reset
   *
   * @usage migrate:import:batch-reset
   *   Deletes all batched files.
   *
   * @aliases mimbr
   */
  public function resetBatchDirectory() {
    $this->fileSystem->deleteRecursive($this->baseDirectory);
  }

}
