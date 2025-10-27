<?php

namespace Drupal\makerspace_snapshot\Controller;

use Drupal\Core\Controller\ControllerBase;
use Drupal\Core\Database\Connection;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\HttpFoundation\StreamedResponse;

class SnapshotDownloadController extends ControllerBase {

  protected $database;

  public function __construct(Connection $database) {
    $this->database = $database;
  }

  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('database')
    );
  }

  public function downloadOrgLevelData($snapshot_id) {
    $response = new StreamedResponse(function() use ($snapshot_id) {
      $handle = fopen('php://output', 'r+');

      $header = ['snapshot_date', 'members_active', 'members_paused', 'members_lapsed', 'joins', 'cancels', 'net_change'];
      fputcsv($handle, $header);

      $query = $this->database->select('ms_snapshot', 's');
      $query->join('ms_fact_org_snapshot', 'o', 's.id = o.snapshot_id');
      $query->fields('s', ['snapshot_date']);
      $query->fields('o', ['members_active', 'members_paused', 'members_lapsed', 'joins', 'cancels', 'net_change']);
      $query->condition('s.id', $snapshot_id);
      $results = $query->execute()->fetchAll(\PDO::FETCH_ASSOC);

      foreach ($results as $row) {
        fputcsv($handle, $row);
      }

      fclose($handle);
    });

    $response->headers->set('Content-Type', 'text/csv');
    $response->headers->set('Content-Disposition', 'attachment; filename="org_level_snapshot.csv"');

    return $response;
  }

  public function downloadPlanLevelData($snapshot_id) {
    $response = new StreamedResponse(function() use ($snapshot_id) {
        $handle = fopen('php://output', 'r+');

        $header = ['snapshot_date', 'plan_code', 'plan_label', 'count_members'];
        fputcsv($handle, $header);

        $query = $this->database->select('ms_snapshot', 's');
        $query->join('ms_fact_plan_snapshot', 'p', 's.id = p.snapshot_id');
        $query->fields('s', ['snapshot_date']);
        $query->fields('p', ['plan_code', 'plan_label', 'count_members']);
        $query->condition('s.id', $snapshot_id);
        $results = $query->execute()->fetchAll(\PDO::FETCH_ASSOC);

        foreach ($results as $row) {
            fputcsv($handle, $row);
        }

        fclose($handle);
    });

    $response->headers->set('Content-Type', 'text/csv');
    $response->headers->set('Content-Disposition', 'attachment; filename="plan_level_snapshot.csv"');

    return $response;
  }
}
