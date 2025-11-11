<?php

namespace Drupal\makerspace_snapshot\Controller;

use Drupal\Core\Controller\ControllerBase;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\makerspace_snapshot\SnapshotService;
use Symfony\Component\HttpFoundation\StreamedResponse;

/**
 * Controller for downloading snapshot CSV templates.
 */
class SnapshotTemplateController extends ControllerBase {

  /**
   * The snapshot service.
   *
   * @var \Drupal\makerspace_snapshot\SnapshotService
   */
  protected $snapshotService;

  /**
   * Constructs a new SnapshotTemplateController object.
   *
   * @param \Drupal\makerspace_snapshot\SnapshotService $snapshotService
   *   The snapshot service.
   */
  public function __construct(SnapshotService $snapshotService) {
    $this->snapshotService = $snapshotService;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('makerspace_snapshot.snapshot_service')
    );
  }

  /**
   * Downloads a CSV template for a given snapshot definition.
   *
   * @param string $definition
   *   The snapshot definition.
   *
   * @return \Symfony\Component\HttpFoundation\StreamedResponse
   *   The CSV file response.
   */
  public function downloadTemplate($definition) {
    $definitions = $this->snapshotService->buildDefinitions();
    if (!isset($definitions[$definition])) {
      throw new \Symfony\Component\HttpKernel\Exception\NotFoundHttpException();
    }

    $headers = $definitions[$definition]['headers'];
    $label_row = $this->snapshotService->getDatasetLabelRow($definition, $headers);
    $example_data = $this->getExampleData($definition);

    $response = new StreamedResponse(function () use ($headers, $label_row, $example_data) {
      $handle = fopen('php://output', 'w');
      fputcsv($handle, $headers);
      if (!empty($label_row)) {
        fputcsv($handle, $label_row);
      }
      if ($example_data) {
        fputcsv($handle, $example_data);
      }
      fclose($handle);
    });

    $response->headers->set('Content-Type', 'text/csv');
    $response->headers->set('Content-Disposition', 'attachment; filename="' . $definition . '_template.csv"');

    return $response;
  }

  protected function getExampleData($definition) {
    switch ($definition) {
      case 'membership_totals':
        return ['2025-01-01', 100, 10, 5, 110];
      case 'event_registrations':
        return ['2025-01-01', 123, 'Intro to Woodworking', '2025-01-15', 12];
      case 'plan_levels':
        $definitions = $this->snapshotService->buildDefinitions();
        $headers = $definitions['plan_levels']['headers'] ?? ['snapshot_date'];
        $example = [];
        foreach ($headers as $header) {
          $example[] = $header === 'snapshot_date' ? '2025-01-01' : 0;
        }
        return $example;
      case 'membership_types':
        $definitions = $this->snapshotService->buildDefinitions();
        $headers = $definitions['membership_types']['headers'] ?? ['snapshot_date', 'members_total'];
        $example = [];
        foreach ($headers as $header) {
          if ($header === 'snapshot_date') {
            $example[] = '2025-01-01';
          }
          elseif ($header === 'members_total') {
            $example[] = 110;
          }
          else {
            $example[] = 0;
          }
        }
        return $example;
      case 'membership_type_joins':
      case 'membership_type_cancels':
        $definitions = $this->snapshotService->buildDefinitions();
        $headers = $definitions[$definition]['headers'] ?? ['snapshot_date'];
        $example = [];
        foreach ($headers as $header) {
          if ($header === 'snapshot_date') {
            $example[] = '2025-01-01';
          }
          elseif ($header === 'joins_total') {
            $example[] = 8;
          }
          elseif ($header === 'cancels_total') {
            $example[] = 2;
          }
          else {
            $example[] = 0;
          }
        }
        return $example;
      case 'event_type_metrics':
        return ['2025-01-01', 2025, 1, 1, 5, 'Workshops', 3, 150, 4200.75, 28.01];
      case 'donation_metrics':
        return [
          '2025-01-01',
          2025,
          1,
          18,
          20,
          32,
          6,
          26,
          5,
          27,
          4,
          8200.50,
          2500.00,
          5700.50,
        ];
      case 'donation_range_metrics':
        return [
          '2025-01-01',
          2025,
          1,
          1,
          'under_100',
          'Under $100',
          0,
          99.99,
          12,
          15,
          850.00,
        ];
      case 'event_type_counts':
      case 'event_type_registrations':
      case 'event_type_revenue':
        $definitions = $this->snapshotService->buildDefinitions();
        $headers = $definitions[$definition]['headers'] ?? ['snapshot_date'];
        $example = [];
        foreach ($headers as $header) {
          if ($header === 'snapshot_date') {
            $example[] = '2025-01-01';
          }
          else {
            $example[] = $definition === 'event_type_revenue' ? 0.0 : 0;
          }
        }
        return $example;
      case 'survey_metrics':
        return [
          '2025-01-01',
          250,
          85.5,
          40.0,
          92.3,
          88.1,
          86.4,
          79.2,
          83.5,
          91.0,
          87.6,
          90.2,
        ];
      default:
        return NULL;
    }
  }

}
