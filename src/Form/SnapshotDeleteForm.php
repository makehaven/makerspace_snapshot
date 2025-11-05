<?php

namespace Drupal\makerspace_snapshot\Form;

use Drupal\Core\Form\ConfirmFormBase;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Url;
use Drupal\makerspace_snapshot\SnapshotService;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Confirmation form for deleting a snapshot.
 */
class SnapshotDeleteForm extends ConfirmFormBase {

  /**
   * The snapshot service.
   *
   * @var \Drupal\makerspace_snapshot\SnapshotService
   */
  protected SnapshotService $snapshotService;

  /**
   * The snapshot ID being deleted.
   *
   * @var int
   */
  protected int $snapshotId;

  /**
   * Query parameters to restore after deletion.
   *
   * @var array
   */
  protected array $query;

  /**
   * Constructs the form.
   */
  public function __construct(SnapshotService $snapshot_service) {
    $this->snapshotService = $snapshot_service;
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
   * {@inheritdoc}
   */
  public function getFormId() {
    return 'makerspace_snapshot_delete_form';
  }

  /**
   * {@inheritdoc}
   */
  public function getQuestion() {
    return $this->t('Are you sure you want to delete snapshot ID %id?', ['%id' => $this->snapshotId]);
  }

  /**
   * {@inheritdoc}
   */
  public function getCancelUrl() {
    return Url::fromRoute('makerspace_snapshot.snapshots', [], ['query' => $this->query]);
  }

  /**
   * {@inheritdoc}
   */
  public function getConfirmText() {
    return $this->t('Delete');
  }

  /**
   * {@inheritdoc}
   */
  public function getCancelText() {
    return $this->t('Cancel');
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state, $snapshot_id = NULL) {
    $this->snapshotId = (int) $snapshot_id;
    $this->query = $this->getRequest()->query->all();
    return parent::buildForm($form, $form_state);
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    try {
      $this->snapshotService->deleteSnapshot($this->snapshotId);
      $this->messenger()->addStatus($this->t('Snapshot with ID %id has been deleted.', ['%id' => $this->snapshotId]));
    }
    catch (\Exception $e) {
      $this->messenger()->addError($this->t('An error occurred while deleting snapshot ID %id.', ['%id' => $this->snapshotId]));
      $this->logger('makerspace_snapshot')->error('Snapshot deletion failed for ID @id: @message', ['@id' => $this->snapshotId, '@message' => $e->getMessage()]);
    }

    $form_state->setRedirect('makerspace_snapshot.snapshots', [], ['query' => $this->query]);
  }

}
