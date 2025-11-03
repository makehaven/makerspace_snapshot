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

    $response = new StreamedResponse(function () use ($headers) {
      $handle = fopen('php://output', 'w');
      fputcsv($handle, $headers);
      fclose($handle);
    });

    $response->headers->set('Content-Type', 'text/csv');
    $response->headers->set('Content-Disposition', 'attachment; filename="' . $definition . '_template.csv"');

    return $response;
  }

}
