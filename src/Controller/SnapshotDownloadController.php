<?php

namespace Drupal\makerspace_snapshot\Controller;

use Drupal\Core\Controller\ControllerBase;
use Drupal\Core\Database\Connection;
use Drupal\makerspace_snapshot\SnapshotService;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\HttpFoundation\StreamedResponse;

class SnapshotDownloadController extends ControllerBase {

  protected $database;
  protected SnapshotService $snapshotService;

  public function __construct(Connection $database, SnapshotService $snapshot_service) {
    $this->database = $database;
    $this->snapshotService = $snapshot_service;
  }

  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('database'),
      $container->get('makerspace_snapshot.snapshot_service')
    );
  }

  public function downloadOrgLevelData($snapshot_id) {
    $filename = $this->buildFilename($snapshot_id, 'org_level_snapshot.csv', 'membership_totals');

    $response = new StreamedResponse(function() use ($snapshot_id) {
      $handle = fopen('php://output', 'r+');

      $header = ['snapshot_date', 'members_total', 'members_active', 'members_paused', 'members_lapsed', 'joins', 'cancels', 'net_change'];
      fputcsv($handle, $header);

      $query = $this->database->select('ms_snapshot', 's');
      $query->join('ms_fact_org_snapshot', 'o', 's.id = o.snapshot_id');
      $query->fields('s', ['snapshot_date']);
      $query->fields('o', ['members_total', 'members_active', 'members_paused', 'members_lapsed', 'joins', 'cancels', 'net_change']);
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

  public function downloadMembershipTypesData($snapshot_id) {
    $snapshot = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['snapshot_type', 'snapshot_date'])
      ->condition('id', $snapshot_id)
      ->execute()
      ->fetchAssoc();

    if (!$snapshot) {
      throw $this->createNotFoundException();
    }

    $exportData = $this->snapshotService->getSnapshotExportData($snapshot['snapshot_type'] ?? '', $snapshot['snapshot_date'] ?? '');
    if (empty($exportData['membership_types'])) {
      throw $this->createNotFoundException();
    }

    $dataset = $exportData['membership_types'];
    $header = $dataset['header'];
    $labels = $dataset['labels'] ?? [];
    foreach ($header as $index => $column) {
      if (strpos($column, 'membership_type_') === 0) {
        $tid = (int) substr($column, strlen('membership_type_'));
        $label = $labels[$tid] ?? ('Membership Type ' . $tid);
        $header[$index] = sprintf('%s (%s, TID %d)', $column, $label, $tid);
      }
    }

    $rows = $dataset['rows'];
    $filename = $this->buildFilename($snapshot_id, 'membership_types_snapshot.csv', 'membership_types');

    $response = new StreamedResponse(function() use ($header, $rows) {
      $handle = fopen('php://output', 'r+');
      fputcsv($handle, $header);
      foreach ($rows as $row) {
        fputcsv($handle, $row);
      }
      fclose($handle);
    });

    $response->headers->set('Content-Type', 'text/csv');
    $response->headers->set('Content-Disposition', sprintf('attachment; filename="%s"', $filename));

    return $response;
  }

  public function exportSnapshotPackage(string $snapshot_type, string $snapshot_date) {
    $exportData = $this->snapshotService->getSnapshotExportData($snapshot_type, $snapshot_date);
    if (empty($exportData)) {
      throw $this->createNotFoundException();
    }

    if (!class_exists(\ZipArchive::class)) {
      throw new \RuntimeException('ZIP extension is required to export snapshot packages.');
    }

    $filename = sprintf('snapshot_export_%s_%s.zip', $snapshot_type ?: 'period', str_replace('-', '', $snapshot_date));

    $response = new StreamedResponse(function () use ($exportData) {
      $tmpFile = tempnam(sys_get_temp_dir(), 'snapshot_export_');
      if ($tmpFile === FALSE) {
        throw new \RuntimeException('Unable to create temporary file for export.');
      }
      $zip = new \ZipArchive();
      if ($zip->open($tmpFile, \ZipArchive::CREATE | \ZipArchive::OVERWRITE) !== TRUE) {
        throw new \RuntimeException('Unable to create export archive.');
      }

      foreach ($exportData as $dataset => $info) {
        $handle = fopen('php://temp', 'w+');
        fputcsv($handle, $info['header']);
        foreach ($info['rows'] as $row) {
          fputcsv($handle, $row);
        }
        rewind($handle);
        $zip->addFromString($info['filename'], stream_get_contents($handle));
        fclose($handle);
      }

      $zip->close();
      readfile($tmpFile);
      unlink($tmpFile);
    });

    $response->headers->set('Content-Type', 'application/zip');
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
