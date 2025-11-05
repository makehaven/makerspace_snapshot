<?php

namespace Drupal\makerspace_snapshot\Form;

use Drupal\Core\Form\FormStateInterface;

/**
 * Filter form for the snapshot listing.
 */
class SnapshotListFilterForm extends SnapshotAdminBaseForm {

  /**
   * {@inheritdoc}
   */
  public function getFormId() {
    return 'makerspace_snapshot_list_filters';
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state, array $options = []) {
    $type_options = $options['type_options'] ?? [];
    $definition_options = $options['definition_options'] ?? [];
    $date_options = $options['date_options'] ?? [];
    $current_filters = $options['current_filters'] ?? [];

    $form['#method'] = 'get';
    $form['#token'] = FALSE;
    $form['#attributes']['class'][] = 'snapshot-filter-form';

    $form['type'] = [
      '#type' => 'select',
      '#title' => $this->t('Type'),
      '#options' => $type_options,
      '#default_value' => $current_filters['type'] ?? '',
    ];

    $form['definition'] = [
      '#type' => 'select',
      '#title' => $this->t('Definition'),
      '#options' => $definition_options,
      '#default_value' => $current_filters['definition'] ?? '',
    ];

    $form['date'] = [
      '#type' => 'select',
      '#title' => $this->t('Snapshot Date'),
      '#options' => $date_options,
      '#default_value' => $current_filters['date'] ?? '',
    ];

    $form['actions'] = [
      '#type' => 'actions',
    ];

    $form['actions']['apply'] = [
      '#type' => 'submit',
      '#value' => $this->t('Apply filters'),
      '#button_type' => 'primary',
    ];

    $form['actions']['reset'] = [
      '#type' => 'submit',
      '#value' => $this->t('Reset'),
      '#limit_validation_errors' => [],
      '#submit' => ['::submitReset'],
    ];

    return $form;
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    $filters = $this->collectFilters($form_state);
    $form_state->setRedirect('makerspace_snapshot.snapshots', [], ['query' => array_filter($filters)]);
  }

  /**
   * Handles reset submissions.
   */
  public function submitReset(array &$form, FormStateInterface $form_state) {
    $form_state->setRedirect('makerspace_snapshot.snapshots');
  }

  /**
   * Extracts the filters from form state.
   */
  protected function collectFilters(FormStateInterface $form_state): array {
    $values = $form_state->getValues();
    return [
      'type' => isset($values['type']) ? (string) $values['type'] : '',
      'definition' => isset($values['definition']) ? (string) $values['definition'] : '',
      'date' => isset($values['date']) ? (string) $values['date'] : '',
    ];
  }

}
