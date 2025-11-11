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
    return $this->streamSnapshotDefinition((int) $snapshot_id, 'membership_totals', 'membership_totals.csv');
  }

  public function downloadPlanLevelData($snapshot_id) {
    return $this->streamSnapshotDefinition((int) $snapshot_id, 'plan_levels', 'plan_levels.csv');
  }

  public function downloadMembershipTypesData($snapshot_id) {
    return $this->streamSnapshotDefinition((int) $snapshot_id, 'membership_types', 'membership_types.csv');
  }

  public function downloadMembershipTypeJoinsData($snapshot_id) {
    return $this->streamSnapshotDefinition((int) $snapshot_id, 'membership_type_joins', 'membership_type_joins.csv');
  }

  public function downloadMembershipTypeCancelsData($snapshot_id) {
    return $this->streamSnapshotDefinition((int) $snapshot_id, 'membership_type_cancels', 'membership_type_cancels.csv');
  }

  public function downloadEventRegistrationsData($snapshot_id) {
    return $this->streamSnapshotDefinition((int) $snapshot_id, 'event_registrations', 'event_registrations.csv');
  }

  public function downloadDonationMetricsData($snapshot_id) {
    return $this->streamSnapshotDefinition((int) $snapshot_id, 'donation_metrics', 'donation_metrics.csv');
  }

  public function downloadDonationRangeData($snapshot_id) {
    return $this->streamSnapshotDefinition((int) $snapshot_id, 'donation_range_metrics', 'donation_range_metrics.csv');
  }

  public function downloadEventTypeMetricsData($snapshot_id) {
    return $this->streamSnapshotDefinition((int) $snapshot_id, 'event_type_metrics', 'event_type_metrics.csv');
  }

  public function downloadEventTypeCountsData($snapshot_id) {
    return $this->streamSnapshotDefinition((int) $snapshot_id, 'event_type_counts', 'event_type_counts.csv');
  }

  public function downloadEventTypeRegistrationsData($snapshot_id) {
    return $this->streamSnapshotDefinition((int) $snapshot_id, 'event_type_registrations', 'event_type_registrations.csv');
  }

  public function downloadEventTypeRevenueData($snapshot_id) {
    return $this->streamSnapshotDefinition((int) $snapshot_id, 'event_type_revenue', 'event_type_revenue.csv');
  }

  public function downloadSurveyMetricsData($snapshot_id) {
    return $this->streamSnapshotDefinition((int) $snapshot_id, 'survey_metrics', 'survey_metrics.csv');
  }

  public function downloadToolAvailabilityData($snapshot_id) {
    return $this->streamSnapshotDefinition((int) $snapshot_id, 'tool_availability', 'tool_availability.csv');
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
        $label_row = $info['label_row'] ?? [];
        if ($this->hasLabelRowContent($label_row)) {
          fputcsv($handle, $label_row);
        }
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
   * Streams a CSV for the requested snapshot definition.
   */
  protected function streamSnapshotDefinition(int $snapshot_id, string $definition, string $fallback_filename): StreamedResponse {
    $snapshot = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['snapshot_type', 'snapshot_date', 'definition'])
      ->condition('id', $snapshot_id)
      ->execute()
      ->fetchAssoc();

    if (!$snapshot) {
      throw $this->createNotFoundException();
    }

    if (!empty($snapshot['definition']) && $snapshot['definition'] !== $definition) {
      throw $this->createNotFoundException();
    }

    $snapshot_type = (string) ($snapshot['snapshot_type'] ?? '');
    $snapshot_date = (string) ($snapshot['snapshot_date'] ?? '');

    $exportData = $this->snapshotService->getSnapshotExportData($snapshot_type, $snapshot_date);
    $dataset = $exportData[$definition] ?? NULL;
    if (empty($dataset)) {
      throw $this->createNotFoundException();
    }

    $header = $dataset['header'] ?? [];
    $label_row = $dataset['label_row'] ?? [];
    $rows = $dataset['rows'] ?? [];
    $filename = $this->buildFilename($snapshot_id, $dataset['filename'] ?? $fallback_filename, $definition);

    $response = new StreamedResponse(function () use ($header, $label_row, $rows) {
      $handle = fopen('php://output', 'w');
      if ($this->hasLabelRowContent($label_row)) {
        fputcsv($handle, $label_row);
      }
      if (!empty($header)) {
        fputcsv($handle, $header);
      }
      foreach ($rows as $row) {
        $values = is_array($row) ? array_values($row) : [$row];
        if (!empty($header) && count($values) !== count($header)) {
          $values = array_pad($values, count($header), '');
        }
        fputcsv($handle, $values);
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

  /**
   * Determines if a label row contains at least one non-empty value.
   */
  protected function hasLabelRowContent(array $row): bool {
    foreach ($row as $value) {
      if ($value !== '' && $value !== NULL) {
        return TRUE;
      }
    }
    return FALSE;
  }

}
