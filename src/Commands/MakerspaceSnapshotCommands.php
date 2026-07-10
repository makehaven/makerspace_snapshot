<?php

namespace Drupal\makerspace_snapshot\Commands;

use Drush\Commands\DrushCommands;
use Drupal\Core\Database\Connection;
use Drupal\makerspace_snapshot\Service\SnapshotHealthMonitor;
use Drupal\makerspace_snapshot\SnapshotService;
use Psr\Log\LoggerInterface;

class MakerspaceSnapshotCommands extends DrushCommands {

  /**
   * Database connection.
   */
  protected Connection $db;

  /**
   * Module logger channel.
   */
  protected LoggerInterface $channelLogger;

  /**
   * Snapshot service.
   */
  protected SnapshotService $snapshotService;

  /**
   * Snapshot health monitor.
   */
  protected ?SnapshotHealthMonitor $healthMonitor;

  public function __construct(Connection $db, LoggerInterface $logger, SnapshotService $snapshotService, ?SnapshotHealthMonitor $healthMonitor = NULL) {
    parent::__construct();
    $this->db = $db;
    $this->channelLogger = $logger;
    $this->snapshotService = $snapshotService;
    $this->healthMonitor = $healthMonitor;
  }

  /**
   * Checks snapshot completeness and KPI sanity for a monthly period.
   *
   * @command makerspace-snapshot:health
   * @option period Snapshot period to check (YYYY-MM-01). Defaults to the current month.
   * @option notify Also report issues to the log and Slack (once per period/fingerprint).
   * @usage drush makerspace-snapshot:health --period=2026-07-01
   */
  public function health(array $options = ['period' => NULL, 'notify' => FALSE]): int {
    $monitor = $this->healthMonitor ?: \Drupal::service('makerspace_snapshot.health_monitor');
    $period = $options['period'] ?: date('Y-m-01');
    $issues = $options['notify'] ? $monitor->checkAndNotify($period) : $monitor->runChecks($period);
    if (!$issues) {
      $this->output()->writeln("Snapshot health OK for $period.");
      return self::EXIT_SUCCESS;
    }
    foreach ($issues as $issue) {
      $this->output()->writeln('WARNING: ' . $issue);
    }
    return self::EXIT_FAILURE;
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
   * By default keeps the most recently created row for each
   * definition+snapshot_type+snapshot_date+source tuple and removes older rows.
   *
   * With --cross-source, collapses across sources too: each
   * definition+snapshot_type+snapshot_date keeps a single row chosen by the
   * canonical source preference (automatic_cron > manual_form > manual_drush >
   * system), tie-broken on created_at then id. This is what removes a manual
   * backfill that shadows a real cron row for the same month. --cross-source
   * ignores the --source filter (it must see every source to compare them).
   *
   * @command makerspace-snapshot:dedupe
   * @option source Filter by source (default automatic_cron; ignored with --cross-source).
   * @option snapshot-type Filter by snapshot type (default monthly).
   * @option snapshot-date Optional exact snapshot date (YYYY-MM-DD).
   * @option cross-source Collapse across sources using source preference.
   * @option apply Apply deletions. Omit for dry-run.
   * @usage drush makerspace-snapshot:dedupe
   * @usage drush makerspace-snapshot:dedupe --source=automatic_cron --snapshot-type=monthly --apply
   * @usage drush makerspace-snapshot:dedupe --cross-source --snapshot-type=monthly --apply
   */
  public function dedupe(array $args, array $options = [
    'source' => 'automatic_cron',
    'snapshot-type' => 'monthly',
    'snapshot-date' => NULL,
    'cross-source' => FALSE,
    'apply' => FALSE,
  ]): void {
    $crossSource = !empty($options['cross-source']);
    $source = (string) ($options['source'] ?? 'automatic_cron');
    $snapshotType = (string) ($options['snapshot-type'] ?? 'monthly');
    $snapshotDate = $options['snapshot-date'] ?? NULL;
    $apply = !empty($options['apply']);

    $query = $this->db->select('ms_snapshot', 's')
      ->fields('s', ['id', 'definition', 'snapshot_type', 'snapshot_date', 'source', 'created_at']);

    if (!$crossSource && $source !== '') {
      $query->condition('source', $source);
    }
    if ($snapshotType !== '') {
      $query->condition('snapshot_type', $snapshotType);
    }
    if (!empty($snapshotDate)) {
      $query->condition('snapshot_date', $snapshotDate);
    }

    $rows = $query->execute()->fetchAllAssoc('id');

    // For each grouping key, keep exactly one row. When collapsing across
    // sources the winner is the most trustworthy source; otherwise rows are
    // already source-scoped and the newest one wins.
    $keepByKey = [];
    $deleteIds = [];
    foreach ($rows as $row) {
      $keyParts = [
        (string) $row->definition,
        (string) $row->snapshot_type,
        (string) $row->snapshot_date,
      ];
      if (!$crossSource) {
        $keyParts[] = (string) $row->source;
      }
      $key = implode('|', $keyParts);

      $candidate = [
        'id' => (int) $row->id,
        'rank' => $crossSource ? SnapshotService::sourceRank($row->source) : 0,
        'created_at' => (int) $row->created_at,
      ];

      $incumbent = $keepByKey[$key] ?? NULL;
      if ($incumbent === NULL) {
        $keepByKey[$key] = $candidate;
        continue;
      }

      // Candidate wins on better source, then newer created_at, then higher id.
      $candidateWins = $candidate['rank'] < $incumbent['rank']
        || ($candidate['rank'] === $incumbent['rank'] && $candidate['created_at'] > $incumbent['created_at'])
        || ($candidate['rank'] === $incumbent['rank'] && $candidate['created_at'] === $incumbent['created_at'] && $candidate['id'] > $incumbent['id']);

      if ($candidateWins) {
        $deleteIds[] = $incumbent['id'];
        $keepByKey[$key] = $candidate;
      }
      else {
        $deleteIds[] = $candidate['id'];
      }
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
      'ms_fact_survey_snapshot',
      'ms_fact_kpi_snapshot',
      'ms_fact_membership_type_snapshot',
      'ms_fact_membership_activity',
      'ms_fact_revenue_snapshot',
      'ms_fact_storage_snapshot',
      'ms_fact_certification_snapshot',
      'ms_fact_access_snapshot',
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
