<?php

namespace Drupal\makerspace_snapshot\Commands;

use Drush\Commands\DrushCommands;
use Drupal\Core\Database\Connection;
use Symfony\Component\Console\Input\InputOption;

class MakerspaceSnapshotCommands extends DrushCommands {

  public function __construct(protected Connection $db) {
    parent::__construct();
  }

  /**
   * Compute & upsert the monthly snapshot using configured SQL.
   *
   * @command makerspace-snapshot:monthly
   * @option month Month end date (YYYY-MM-DD). Defaults to last day of previous month.
   * @usage drush makerspace-snapshot:monthly --month=2025-09-30
   */
  public function monthly(array $args = [], array $options = ['month' => InputOption::VALUE_REQUIRED]) {
    $monthEnd = $options['month'] ?? (new \DateTime('last day of previous month'))->format('Y-m-d');
    $periodStart = (new \DateTimeImmutable($monthEnd))->modify('first day of this month')->setTime(0,0,0)->format('Y-m-d H:i:s');
    $periodEnd   = (new \DateTimeImmutable($monthEnd))->setTime(23,59,59)->format('Y-m-d H:i:s');

    $stateQuery = function(string $key): array {
      $sql = \Drupal::config('makerspace_snapshot.sources')->get($key);
      if (!$sql) return [];
      $rows = \Drupal::database()->query($sql)->fetchAllAssoc(NULL, \PDO::FETCH_ASSOC);
      return array_map(fn($r) => [
        'member_id' => (string) $r['member_id'],
        'plan_code' => (string) $r['plan_code'],
        'plan_label'=> (string) ($r['plan_label'] ?? $r['plan_code']),
      ], $rows);
    };

    $periodQuery = function(string $key, string $start, string $end): array {
      $sql = \Drupal::config('makerspace_snapshot.sources')->get($key);
      if (!$sql) return [];
      $rows = \Drupal::database()->query($sql, [':start' => $start, ':end' => $end])->fetchAllAssoc(NULL, \PDO::FETCH_ASSOC);
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

    // Upsert org totals.
    $this->db->merge('ms_fact_monthly_org')
      ->key(['snapshot_month' => $monthEnd])
      ->fields([
        'members_active' => $members_active,
        'members_paused' => $members_paused,
        'members_lapsed' => $members_lapsed,
        'joins'          => $joins_count,
        'cancels'        => $cancels_count,
        'net_change'     => $net_change,
        'created_at'     => time(),
      ])->execute();

    // Per-plan dynamic counts.
    $byPlan = [];
    $bump = function(array $rows, string $bucket) use (&$byPlan) {
      foreach ($rows as $r) {
        $code = $r['plan_code'];
        $label = $r['plan_label'] ?: $code;
        if (!isset($byPlan[$code])) {
          $byPlan[$code] = ['plan_code' => $code, 'plan_label' => $label, 'active_count' => 0, 'paused_count' => 0, 'lapsed_count' => 0];
        }
        $byPlan[$code][$bucket]++;
      }
    };
    $bump($active, 'active_count');
    $bump($paused, 'paused_count');
    $bump($lapsed, 'lapsed_count');

    // Clear existing rows for this month to keep idempotent.
    $this->db->delete('ms_fact_monthly_plan_counts')
      ->condition('snapshot_month', $monthEnd)
      ->execute();

    foreach ($byPlan as $plan) {
      $this->db->insert('ms_fact_monthly_plan_counts')
        ->fields([
          'snapshot_month' => $monthEnd,
          'plan_code'      => $plan['plan_code'],
          'plan_label'     => $plan['plan_label'],
          'active_count'   => $plan['active_count'],
          'paused_count'   => $plan['paused_count'],
          'lapsed_count'   => $plan['lapsed_count'],
        ])->execute();
    }

    $this->logger()->success("Snapshot stored for {$monthEnd}");
  }
}
