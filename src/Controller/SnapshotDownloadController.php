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
    $filename = $this->buildFilename($snapshot_id, 'org_level_snapshot.csv', 'membership_totals');

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
    $response->headers->set('Content-Disposition', sprintf('attachment; filename="%s"', $filename));

    return $response;
  }

  public function downloadPlanLevelData($snapshot_id) {
    $filename = $this->buildFilename($snapshot_id, 'plan_level_snapshot.csv', 'plan_levels');

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
    $response->headers->set('Content-Disposition', sprintf('attachment; filename="%s"', $filename));

    return $response;
  }

  public function downloadMembershipActivityData($snapshot_id) {
    $filename = $this->buildFilename($snapshot_id, 'membership_activity_snapshot.csv', 'membership_activity');

    $response = new StreamedResponse(function() use ($snapshot_id) {
      $handle = fopen('php://output', 'r+');

      $header = ['snapshot_date', 'joins', 'cancels', 'net_change'];
      fputcsv($handle, $header);

      $query = $this->database->select('ms_snapshot', 's');
      $query->join('ms_fact_membership_activity', 'a', 's.id = a.snapshot_id');
      $query->fields('s', ['snapshot_date']);
      $query->fields('a', ['joins', 'cancels', 'net_change']);
      $query->condition('s.id', $snapshot_id);
      $results = $query->execute()->fetchAll(\PDO::FETCH_ASSOC);

      foreach ($results as $row) {
        fputcsv($handle, $row);
      }

      fclose($handle);
    });

    $response->headers->set('Content-Type', 'text/csv');
    $response->headers->set('Content-Disposition', sprintf('attachment; filename="%s"', $filename));

    return $response;
  }

  /**
   * Builds a filename for the snapshot download.
   */
  protected function buildFilename($snapshot_id, string $fallback, ?string $expected_definition = NULL): string {
    $snapshot = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['snapshot_type', 'snapshot_date', 'definition'])
      ->condition('s.id', $snapshot_id)
      ->execute()
      ->fetchAssoc();

    if (!$snapshot) {
      return $fallback;
    }

    // If we expect a specific definition, prefer it, otherwise fall back to
    // whatever the snapshot record stores.
    $definition = $expected_definition ?: ($snapshot['definition'] ?? '');
    if (!$definition) {
      $definition = 'snapshot';
    }

    $snapshot_type = $snapshot['snapshot_type'] ?? '';
    $snapshot_date = $snapshot['snapshot_date'] ?? '';

    try {
      if ($snapshot_date) {
        $snapshot_date = (new \DateTimeImmutable($snapshot_date))->format('Y-m-d');
      }
    }
    catch (\Exception $e) {
      // Leave the date as-is if parsing fails.
    }

    $parts = array_filter([
      $definition,
      $snapshot_type,
      $snapshot_date,
    ]);

    if (empty($parts)) {
      return $fallback;
    }

    $base = strtolower(implode('-', $parts));
    $base = preg_replace('/[^a-z0-9\-]+/', '-', $base ?? '');
    $base = trim(preg_replace('/-+/', '-', $base), '-');

    if ($base === '') {
      $base = pathinfo($fallback, PATHINFO_FILENAME);
    }

    return $base . '.csv';
  }

}
