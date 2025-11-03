<?php

namespace Drupal\makerspace_snapshot;

use Drupal\Core\Database\Connection;
use Psr\Log\LoggerInterface;
use Drupal\Core\Config\ConfigFactoryInterface;

/**
 * Service description.
 */
class SnapshotService {

  /**
   * The database connection.
   *
   * @var \Drupal\Core\Database\Connection
   */
  protected $database;

  /**
   * The logger.
   *
   * @var \Psr\Log\LoggerInterface
   */
  protected $logger;

  /**
   * The config factory.
   *
   * @var \Drupal\Core\Config\ConfigFactoryInterface
   */
  protected $configFactory;

  /**
   * Constructs a new SnapshotService object.
   *
   * @param \Drupal\Core\Database\Connection $database
   *   The database connection.
   * @param \Psr\Log\LoggerInterface $logger
   *   The logger.
   * @param \Drupal\Core\Config\ConfigFactoryInterface $config_factory
   *   The config factory.
   */
  public function __construct(Connection $database, LoggerInterface $logger, ConfigFactoryInterface $config_factory) {
    $this->database = $database;
    $this->logger = $logger;
    $this->configFactory = $config_factory;
  }

  public function buildDefinitions() {
    return [
      'membership_totals' => [
        'schedules' => ['monthly', 'quarterly', 'annually'],
        'headers' => ['snapshot_date', 'members_active', 'members_paused', 'members_lapsed'],
        'dataset_type' => 'membership_totals',
      ],
      'membership_activity' => [
        'schedules' => ['monthly', 'quarterly', 'annually'],
        'headers' => ['snapshot_date', 'joins', 'cancels', 'net_change'],
        'dataset_type' => 'membership_activity',
      ],
      'event_registrations' => [
        'schedules' => ['daily'],
        'headers' => ['snapshot_date', 'event_id', 'event_title', 'event_start_date', 'registration_count'],
        'dataset_type' => 'event_registrations',
      ],
      'plan_levels' => [
        'schedules' => [],
        'headers' => ['snapshot_date', 'plan_code', 'plan_label', 'count_members'],
        'dataset_type' => 'plan_levels',
      ],
    ];
  }

  /**
   * Takes a snapshot.
   *
   * @param string $snapshot_type
   *   The type of snapshot to take.
   * @param bool $is_test
   *   Whether the snapshot is a test snapshot.
   * @param string|null $snapshot_date
   *   The date of the snapshot.
   */
  public function takeSnapshot($snapshot_type, $is_test = FALSE, $snapshot_date = NULL) {
    try {
      $snapshotDate = $snapshot_date ?? (new \DateTime())->format('Y-m-d');
      $snapshotDate = (new \DateTimeImmutable($snapshotDate))->format('Y-m-01');
      $isTest = $is_test ? 1 : 0;

      $periodStart = (new \DateTimeImmutable($snapshotDate))->setTime(0,0,0)->format('Y-m-d H:i:s');
      $periodEnd   = (new \DateTimeImmutable($snapshotDate))->modify('last day of this month')->setTime(23,59,59)->format('Y-m-d H:i:s');

      $stateQuery = function(string $key): array {
        $sql = $this->configFactory->get('makerspace_snapshot.sources')->get($key);
        if (!$sql) return [];
        $rows = $this->database->query($sql)->fetchAll(\PDO::FETCH_ASSOC);
        return array_map(fn($r) => [
          'member_id' => (string) $r['member_id'],
          'plan_code' => (string) $r['plan_code'],
          'plan_label'=> (string) ($r['plan_label'] ?? $r['plan_code']),
        ], $rows);
      };

      $periodQuery = function(string $key, string $start, string $end): array {
        $sql = $this->configFactory->get('makerspace_snapshot.sources')->get($key);
        if (!$sql) return [];

        $params = [];
        if (strpos($sql, ':start') !== false && strpos($sql, ':end') !== false) {
          $params = [':start' => $start, ':end' => $end];
        }

        $rows = $this->database->query($sql, $params)->fetchAll(\PDO::FETCH_ASSOC);
        return $rows;
      };

      $active  = $stateQuery('sql_active');
      $paused  = $stateQuery('sql_paused');
      $lapsed  = $stateQuery('sql_lapsed');
      $joins   = $periodQuery('sql_joins',   $periodStart, $periodEnd);
      $cancels = $periodQuery('sql_cancels', $periodStart, $periodEnd);

      $members_active = count($active);
      $members_paused = count($paused);
      $members_lapsed = count($lapsed);
      $joins_count    = count($joins);
      $cancels_count  = count($cancels);
      $net_change     = $joins_count - $cancels_count;

      // Create snapshot metadata.
      $snapshot_id = $this->database->insert('ms_snapshot')
        ->fields([
          'definition'    => 'membership_totals',
          'snapshot_type' => $snapshot_type,
          'snapshot_date' => $snapshotDate,
          'is_test'       => $isTest,
          'created_at'    => time(),
        ])->execute();

      if (!$snapshot_id) {
        $this->logger->error("Failed to create snapshot for {$snapshotDate}");
        return;
      }

      // Insert org totals.
      $this->database->insert('ms_fact_org_snapshot')
        ->fields([
          'snapshot_id'    => $snapshot_id,
          'members_active' => $members_active,
          'members_paused' => $members_paused,
          'members_lapsed' => $members_lapsed,
          'joins'          => 0,
          'cancels'        => 0,
          'net_change'     => 0,
        ])->execute();

      // Insert membership activity.
      $this->database->insert('ms_fact_membership_activity')
        ->fields([
          'snapshot_id' => $snapshot_id,
          'joins'       => $joins_count,
          'cancels'     => $cancels_count,
          'net_change'  => $net_change,
        ])->execute();

      // Per-plan dynamic counts.
      $byPlan = [];
      foreach ($active as $r) {
        $code = $r['plan_code'];
        $label = $r['plan_label'] ?: $code;
        if (!isset($byPlan[$code])) {
          $byPlan[$code] = ['plan_code' => $code, 'plan_label' => $label, 'count_members' => 0];
        }
        $byPlan[$code]['count_members']++;
      }

      foreach ($byPlan as $plan) {
        $this->database->insert('ms_fact_plan_snapshot')
          ->fields([
            'snapshot_id'    => $snapshot_id,
            'plan_code'      => $plan['plan_code'],
            'plan_label'     => $plan['plan_label'],
            'count_members'  => $plan['count_members'],
          ])->execute();
      }

      $this->logger->info("Snapshot stored for {$snapshotDate} (ID: {$snapshot_id})");

      $this->pruneSnapshots();
    } catch (\Exception $e) {
      $this->logger->error('Error taking snapshot: @message', ['@message' => $e->getMessage()]);
    }
  }

  public function importSnapshot($definition, $schedule, $snapshot_date, $is_test, array $payload) {
    $definitions = $this->buildDefinitions();
    if (!isset($definitions[$definition])) {
      throw new \Exception("Invalid snapshot definition: {$definition}");
    }

    $snapshot_id = $this->database->insert('ms_snapshot')
      ->fields([
        'definition'    => $definition,
        'snapshot_type' => $schedule,
        'snapshot_date' => $snapshot_date,
        'is_test'       => $is_test,
        'created_at'    => time(),
      ])->execute();

    if (!$snapshot_id) {
      throw new \Exception("Failed to create snapshot for {$snapshot_date}");
    }

    switch ($definition) {
      case 'membership_totals':
        $this->importMembershipSnapshot($snapshot_id, $payload);
        break;
      case 'membership_activity':
        $this->importMembershipActivitySnapshot($snapshot_id, $payload);
        break;
      case 'event_registrations':
        $this->importEventSnapshot($snapshot_id, $payload);
        break;
    }

    return $snapshot_id;
  }

  protected function importMembershipSnapshot($snapshot_id, array $payload) {
    $this->database->insert('ms_fact_org_snapshot')
      ->fields([
        'snapshot_id'    => $snapshot_id,
        'members_active' => $payload['totals']['members_active'],
        'members_paused' => $payload['totals']['members_paused'],
        'members_lapsed' => $payload['totals']['members_lapsed'],
        'joins'          => 0,
        'cancels'        => 0,
        'net_change'     => 0,
      ])->execute();

    if (isset($payload['plans'])) {
      foreach ($payload['plans'] as $plan) {
        $this->database->insert('ms_fact_plan_snapshot')
          ->fields([
            'snapshot_id'    => $snapshot_id,
            'plan_code'      => $plan['plan_code'],
            'plan_label'     => $plan['plan_label'],
            'count_members'  => $plan['count_members'],
          ])->execute();
      }
    }
  }

  protected function importMembershipActivitySnapshot($snapshot_id, array $payload) {
    $this->database->insert('ms_fact_membership_activity')
      ->fields([
        'snapshot_id' => $snapshot_id,
        'joins'       => $payload['activity']['joins'],
        'cancels'     => $payload['activity']['cancels'],
        'net_change'  => $payload['activity']['net_change'],
      ])->execute();
  }

  protected function importEventSnapshot($snapshot_id, array $payload) {
    foreach ($payload['events'] as $event) {
      $this->database->insert('ms_fact_event_snapshot')
        ->fields([
          'snapshot_id'        => $snapshot_id,
          'event_id'           => $event['event_id'],
          'event_title'        => $event['event_title'],
          'event_start_date'   => $event['event_start_date'],
          'registration_count' => $event['registration_count'],
        ])->execute();
    }
  }

  /**
   * Prunes old snapshots.
   */
  public function pruneSnapshots() {
    $retention_window_months = $this->configFactory->get('makerspace_snapshot.settings')->get('retention_window_months');
    if (empty($retention_window_months)) {
      return;
    }

    $retention_date = (new \DateTime())->modify("-{$retention_window_months} months")->format('Y-m-d');

    $query = $this->database->select('ms_snapshot', 's');
    $query->fields('s', ['id']);
    $query->condition('snapshot_date', $retention_date, '<');
    $results = $query->execute()->fetchAll();

    if (empty($results)) {
      return;
    }

    $snapshot_ids = array_map(function ($row) {
      return $row->id;
    }, $results);

    $this->database->delete('ms_snapshot')
      ->condition('id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_org_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_plan_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->logger->info('Pruned @count snapshots older than @date.', ['@count' => count($snapshot_ids), '@date' => $retention_date]);
  }
}
