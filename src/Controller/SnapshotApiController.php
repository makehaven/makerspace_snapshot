<?php

namespace Drupal\makerspace_snapshot\Controller;

use Drupal\Core\Controller\ControllerBase;
use Symfony\Component\HttpFoundation\JsonResponse;
use Drupal\Core\Database\Connection;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\Core\Cache\CacheableJsonResponse;
use Drupal\Core\Cache\CacheableMetadata;

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

  public function getOrgLevelData(string $snapshot_type): JsonResponse {
    $query = $this->database->select('ms_snapshot', 's');
    $query->join('ms_fact_org_snapshot', 'o', 's.id = o.snapshot_id');
    $query->fields('s', ['snapshot_date']);
    $query->fields('o', ['members_total', 'members_active', 'members_paused', 'members_lapsed', 'joins', 'cancels', 'net_change']);
    $query->condition('s.snapshot_type', $snapshot_type);
    $query->orderBy('s.snapshot_date', 'ASC');
    $results = $query->execute()->fetchAll(\PDO::FETCH_ASSOC);

    foreach ($results as &$row) {
      $row['date'] = $row['snapshot_date'];
      unset($row['snapshot_date']);
    }

    $response = new CacheableJsonResponse($results);
    $cache_metadata = new CacheableMetadata();
    $cache_metadata->setCacheTags(['makerspace_snapshot:org']);
    $response->addCacheableDependency($cache_metadata);

    return $response;
  }

  public function getPlanLevelData(string $snapshot_type): JsonResponse {
    $query = $this->database->select('ms_snapshot', 's');
    $query->join('ms_fact_plan_snapshot', 'p', 's.id = p.snapshot_id');
    $query->fields('s', ['snapshot_date']);
    $query->fields('p', ['plan_code', 'plan_label', 'count_members']);
    $query->condition('s.snapshot_type', $snapshot_type);
    $query->orderBy('s.snapshot_date', 'ASC');
    $results = $query->execute()->fetchAll();

    $data = [];
    foreach ($results as $row) {
      $data[$row->snapshot_date][] = [
        'plan_code' => $row->plan_code,
        'plan_label' => $row->plan_label,
        'count_members' => (int) $row->count_members,
      ];
    }

    $response = new CacheableJsonResponse($data);
    $cache_metadata = new CacheableMetadata();
    $cache_metadata->setCacheTags(['makerspace_snapshot:plan']);
    $response->addCacheableDependency($cache_metadata);

    return $response;
  }
}
