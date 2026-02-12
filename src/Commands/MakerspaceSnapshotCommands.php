<?php

namespace Drupal\makerspace_snapshot\Commands;

use Drush\Commands\DrushCommands;
use Drupal\Core\Database\Connection;
use Drupal\makerspace_snapshot\SnapshotService;

class MakerspaceSnapshotCommands extends DrushCommands {

  /**
   * Database connection.
   */
  protected Connection $db;

  /**
   * Snapshot service.
   */
  protected SnapshotService $snapshotService;

  public function __construct(Connection $db, SnapshotService $snapshotService) {
    parent::__construct();
    $this->db = $db;
    $this->snapshotService = $snapshotService;
  }

  /**
   * Compute & upsert a snapshot using configured SQL.
   *
   * @command makerspace-snapshot:snapshot
   * @option snapshot-date Snapshot date (YYYY-MM-DD). Defaults to today.
   * @option snapshot-type Snapshot type (e.g., 'monthly', 'quarterly', 'annual', 'daily', 'manual').
   * @option is-test Mark this snapshot as a test snapshot.
   * @usage drush makerspace-snapshot:snapshot --snapshot-date=2025-09-30 --snapshot-type=monthly
   */
  public function snapshot(array $args, array $options = [
    'snapshot-date' => NULL,
    'snapshot-type' => NULL,
    'is-test' => FALSE,
  ]): void {
    $this->snapshotService->takeSnapshot(
      $options['snapshot-type'] ?? 'monthly',
      $options['is-test'] ?? FALSE,
      $options['snapshot-date'] ?? NULL,
      'manual_drush'
    );
  }

  /**
   * Find and optionally remove duplicate snapshot rows.
   *
   * Keeps the most recently created row for each
   * definition+snapshot_type+snapshot_date+source tuple and removes older rows.
   *
   * @command makerspace-snapshot:dedupe
   * @option source Filter by source (default automatic_cron).
   * @option snapshot-type Filter by snapshot type (default monthly).
   * @option snapshot-date Optional exact snapshot date (YYYY-MM-DD).
   * @option apply Apply deletions. Omit for dry-run.
   * @usage drush makerspace-snapshot:dedupe
   * @usage drush makerspace-snapshot:dedupe --source=automatic_cron --snapshot-type=monthly --apply
   */
  public function dedupe(array $args, array $options = [
    'source' => 'automatic_cron',
    'snapshot-type' => 'monthly',
    'snapshot-date' => NULL,
    'apply' => FALSE,
  ]): void {
    $source = (string) ($options['source'] ?? 'automatic_cron');
    $snapshotType = (string) ($options['snapshot-type'] ?? 'monthly');
    $snapshotDate = $options['snapshot-date'] ?? NULL;
    $apply = !empty($options['apply']);

    $query = $this->db->select('ms_snapshot', 's')
      ->fields('s', ['id', 'definition', 'snapshot_type', 'snapshot_date', 'source', 'created_at'])
      ->orderBy('definition', 'ASC')
      ->orderBy('snapshot_type', 'ASC')
      ->orderBy('snapshot_date', 'ASC')
      ->orderBy('source', 'ASC')
      ->orderBy('created_at', 'DESC')
      ->orderBy('id', 'DESC');

    if ($source !== '') {
      $query->condition('source', $source);
    }
    if ($snapshotType !== '') {
      $query->condition('snapshot_type', $snapshotType);
    }
    if (!empty($snapshotDate)) {
      $query->condition('snapshot_date', $snapshotDate);
    }

    $rows = $query->execute()->fetchAllAssoc('id');

    $keepByKey = [];
    $deleteIds = [];
    foreach ($rows as $row) {
      $key = implode('|', [
        (string) $row->definition,
        (string) $row->snapshot_type,
        (string) $row->snapshot_date,
        (string) $row->source,
      ]);

      if (!isset($keepByKey[$key])) {
        $keepByKey[$key] = (int) $row->id;
        continue;
      }
      $deleteIds[] = (int) $row->id;
    }

    if (empty($deleteIds)) {
      $this->output()->writeln('No duplicates found for the selected filter.');
      return;
    }

    $this->output()->writeln(sprintf(
      'Found %d duplicate snapshot rows (%d unique tuples kept).',
      count($deleteIds),
      count($keepByKey)
    ));
    $this->output()->writeln('Sample duplicate IDs: ' . implode(', ', array_slice($deleteIds, 0, 20)));

    if (!$apply) {
      $this->output()->writeln('Dry-run only. Re-run with --apply to delete duplicates.');
      return;
    }

    $factTables = [
      'ms_fact_org_snapshot',
      'ms_fact_plan_snapshot',
      'ms_fact_donation_snapshot',
      'ms_fact_donation_range_snapshot',
      'ms_fact_event_type_snapshot',
      'ms_fact_survey_snapshot',
      'ms_fact_tool_uptime_snapshot',
      'ms_fact_event_snapshot',
      'ms_fact_kpi_snapshot',
      'ms_fact_membership_type_snapshot',
      'ms_fact_membership_activity',
    ];

    $schema = $this->db->schema();
    $transaction = $this->db->startTransaction();

    try {
      foreach (array_chunk($deleteIds, 500) as $chunk) {
        foreach ($factTables as $table) {
          if (!$schema->tableExists($table)) {
            continue;
          }
          $this->db->delete($table)
            ->condition('snapshot_id', $chunk, 'IN')
            ->execute();
        }

        $this->db->delete('ms_snapshot')
          ->condition('id', $chunk, 'IN')
          ->execute();
      }

      $this->output()->writeln(sprintf('Deleted %d duplicate snapshot rows.', count($deleteIds)));
    }
    catch (\Throwable $e) {
      $transaction->rollBack();
      throw $e;
    }
  }
}
