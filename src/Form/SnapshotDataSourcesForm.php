<?php

namespace Drupal\makerspace_snapshot\Form;

use Drupal\Core\Form\FormStateInterface;

/**
 * Landing page showing snapshot dataset information.
 */
class SnapshotDataSourcesForm extends SnapshotAdminBaseForm {

  /**
   * {@inheritdoc}
   */
  public function getFormId() {
    return 'makerspace_snapshot_data_sources';
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    $form['intro'] = [
      '#markup' => '<p>' . $this->t('Review the datasets available to the Makerspace Snapshot system along with their default schedules, acquisition modes, and underlying SQL (when available).') . '</p>',
    ];

    $form['dataset_information'] = [
      '#type' => 'container',
      '#attributes' => ['class' => ['snapshot-dataset-information']],
    ];
    $form['dataset_information'] += $this->buildDatasetInformation();

    return $form;
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    // This form is informational only.
  }

}
