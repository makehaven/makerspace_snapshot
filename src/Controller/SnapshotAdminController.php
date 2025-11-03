<?php

namespace Drupal\makerspace_snapshot\Controller;

use Drupal\Core\Controller\ControllerBase;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\makerspace_snapshot\SnapshotService;
use Symfony\Component\HttpFoundation\JsonResponse;

/**
 * Controller for snapshot admin operations.
 */
class SnapshotAdminController extends ControllerBase {

  /**
   * The snapshot service.
   *
   * @var \Drupal\makerspace_snapshot\SnapshotService
   */
  protected $snapshotService;

  /**
   * Constructs a new SnapshotAdminController object.
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
   * Deletes a snapshot via AJAX.
   *
   * @param int $snapshot_id
   *   The ID of the snapshot to delete.
   *
   * @return \Symfony\Component\HttpFoundation\JsonResponse
   *   A JSON response indicating success or failure.
   */
  public function deleteSnapshotAjax($snapshot_id) {
    try {
      $this->snapshotService->deleteSnapshot($snapshot_id);
      return new JsonResponse([
        'success' => TRUE,
        'message' => $this->t('Snapshot with ID %id has been deleted.', ['%id' => $snapshot_id]),
      ]);
    } catch (\Exception $e) {
      return new JsonResponse([
        'success' => FALSE,
        'message' => $this->t('An error occurred while deleting the snapshot.'),
      ], 500);
    }
  }

}
