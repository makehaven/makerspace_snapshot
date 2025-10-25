<?php

namespace Drupal\makerspace_snapshot\Controller;

use Drupal\Core\Controller\ControllerBase;
use Symfony\Component\HttpFoundation\JsonResponse;
use Drupal\Core\Database\Connection;
use Symfony\Component\DependencyInjection\ContainerInterface;

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
    $query->fields('s', ['snapshot_date', 'snapshot_type']);
    $query->fields('o');
    $query->condition('s.snapshot_type', $snapshot_type);
    $query->orderBy('s.snapshot_date', 'DESC');
    $results = $query->execute()->fetchAllAssoc('snapshot_date');

    return new JsonResponse($results);
  }

  public function getPlanLevelData(string $snapshot_type): JsonResponse {
    $query = $this->database->select('ms_snapshot', 's');
    $query->join('ms_fact_plan_snapshot', 'p', 's.id = p.snapshot_id');
    $query->fields('s', ['snapshot_date', 'snapshot_type']);
    $query->fields('p');
    $query->condition('s.snapshot_type', $snapshot_type);
    $query->orderBy('s.snapshot_date', 'DESC');
    $results = $query->execute()->fetchAll();

    $data = [];
    foreach ($results as $row) {
      $data[$row->snapshot_date][] = (array) $row;
    }

    return new JsonResponse($data);
  }
}
