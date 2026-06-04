<?php

namespace Drupal\makerspace_snapshot\Controller;

use Drupal\Core\Controller\ControllerBase;
use Symfony\Component\HttpFoundation\JsonResponse;
use Drupal\Core\Database\Connection;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\Core\Cache\CacheableJsonResponse;
use Drupal\Core\Cache\CacheableMetadata;
use Drupal\makerspace_snapshot\SnapshotService;

class SnapshotApiController extends ControllerBase {

  protected $database;

  public function __construct(Connection $database) {
    $this->database = $database;
  }

  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('database')
    );
  }

  /**
   * Returns one preferred snapshot ID per date for a definition + type.
   *
   * Collapses rows that share a snapshot_date down to a single row using the
   * canonical source preference, so charts never plot the same month twice
   * (e.g. a cron row alongside a manual backfill). Within one source/date the
   * newest row wins.
   *
   * @param string $definition
   *   The dataset definition machine name (e.g. 'membership_totals').
   * @param string $snapshot_type
   *   The snapshot cadence (e.g. 'monthly').
   *
   * @return int[]
   *   Winning snapshot IDs, one per date.
   */
  protected function preferredSnapshotIds(string $definition, string $snapshot_type): array {
    $rows = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['id', 'snapshot_date', 'source', 'created_at'])
      ->condition('s.definition', $definition)
      ->condition('s.snapshot_type', $snapshot_type)
      ->execute();

    $best = [];
    foreach ($rows as $row) {
      $date = $row->snapshot_date;
      $candidate = [
        'id' => (int) $row->id,
        'rank' => SnapshotService::sourceRank($row->source),
        'created_at' => (int) $row->created_at,
      ];
      $current = $best[$date] ?? NULL;
      if ($current === NULL
        || $candidate['rank'] < $current['rank']
        || ($candidate['rank'] === $current['rank'] && $candidate['created_at'] > $current['created_at'])
        || ($candidate['rank'] === $current['rank'] && $candidate['created_at'] === $current['created_at'] && $candidate['id'] > $current['id'])
      ) {
        $best[$date] = $candidate;
      }
    }

    return array_map(static fn(array $b): int => $b['id'], $best);
  }

  public function getOrgLevelData(string $snapshot_type): JsonResponse {
    $ids = $this->preferredSnapshotIds('membership_totals', $snapshot_type);
    $results = [];
    if ($ids) {
      $query = $this->database->select('ms_snapshot', 's');
      $query->join('ms_fact_org_snapshot', 'o', 's.id = o.snapshot_id');
      $query->fields('s', ['snapshot_date']);
      $query->fields('o', [
        'members_total', 'members_active', 'members_paused', 'members_lapsed',
        'joins', 'cancels', 'net_change',
      ]);
      $query->condition('s.id', array_values($ids), 'IN');
      $query->orderBy('s.snapshot_date', 'ASC');
      $results = $query->execute()->fetchAll(\PDO::FETCH_ASSOC);

      foreach ($results as &$row) {
        $row['date'] = $row['snapshot_date'];
        unset($row['snapshot_date']);
      }
    }

    $response = new CacheableJsonResponse($results);
    $cache_metadata = new CacheableMetadata();
    $cache_metadata->setCacheTags(['makerspace_snapshot:org']);
    $response->addCacheableDependency($cache_metadata);

    return $response;
  }

  public function getPlanLevelData(string $snapshot_type): JsonResponse {
    $ids = $this->preferredSnapshotIds('plan_levels', $snapshot_type);
    $data = [];
    if ($ids) {
      $query = $this->database->select('ms_snapshot', 's');
      $query->join('ms_fact_plan_snapshot', 'p', 's.id = p.snapshot_id');
      $query->fields('s', ['snapshot_date']);
      $query->fields('p', ['plan_code', 'plan_label', 'count_members']);
      $query->condition('s.id', array_values($ids), 'IN');
      $query->orderBy('s.snapshot_date', 'ASC');
      $results = $query->execute()->fetchAll();

      foreach ($results as $row) {
        $data[$row->snapshot_date][] = [
          'plan_code' => $row->plan_code,
          'plan_label' => $row->plan_label,
          'count_members' => (int) $row->count_members,
        ];
      }
    }

    $response = new CacheableJsonResponse($data);
    $cache_metadata = new CacheableMetadata();
    $cache_metadata->setCacheTags(['makerspace_snapshot:plan']);
    $response->addCacheableDependency($cache_metadata);

    return $response;
  }
}
