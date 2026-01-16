<?php

namespace Drupal\makerspace_snapshot\Form;

use Drupal\Core\Form\FormStateInterface;

/**
 * Form for triggering manual snapshots.
 */
class ManualSnapshotForm extends SnapshotAdminBaseForm {

  /**
   * {@inheritdoc}
   */
  public function getFormId() {
    return 'makerspace_snapshot_manual';
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    $form['description'] = [
      '#markup' => '<p>' . $this->t('Use this page to trigger snapshots immediately. For dataset details and SQL references, visit the <em>Snapshot Data Sources</em> tab.') . '</p>',
    ];

    $snapshot_type_options = [
      'monthly' => $this->t('Monthly'),
      'quarterly' => $this->t('Quarterly'),
      'annually' => $this->t('Annually'),
      'daily' => $this->t('Daily'),
    ];

    $manual_definition_options = $this->getManualSnapshotDefinitionOptions();

    $form['snapshot_type'] = [
      '#type' => 'select',
      '#title' => $this->t('Snapshot Type'),
      '#options' => $snapshot_type_options,
      '#default_value' => 'monthly',
      '#description' => $this->t('Monthly snapshots are always recorded as the 1st of the current month, even if run mid-month. This standardizes reporting periods.'),
    ];

    $form['snapshot_definitions'] = [
      '#type' => 'checkboxes',
      '#title' => $this->t('Definitions'),
      '#options' => $manual_definition_options,
      '#default_value' => [],
      '#description' => $this->t('Select the datasets to refresh. Leave all unchecked if you only intend to snapshot specific definitions.'),
    ];

    $form['actions'] = [
      '#type' => 'actions',
    ];

    $form['actions']['submit'] = [
      '#type' => 'submit',
      '#value' => $this->t('Take Snapshot Now'),
      '#name' => 'manual_snapshot_take',
    ];

    return $form;
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    $trigger = $form_state->getTriggeringElement();
    if (($trigger['#name'] ?? '') !== 'manual_snapshot_take') {
      return;
    }

    $snapshot_type = $form_state->getValue('snapshot_type');
    $is_test = $form_state->getValue('is_test');
    $definitions_input = $form_state->getValue('snapshot_definitions') ?? [];
    $available_definition_options = $this->getManualSnapshotDefinitionOptions();
    $available_definition_keys = array_keys($available_definition_options);
    $selected_definitions = [];

    if (is_array($definitions_input)) {
      foreach ($definitions_input as $key => $value) {
        if (!in_array($key, $available_definition_keys, TRUE)) {
          continue;
        }
        if ($value !== 0 && $value !== '0' && $value !== NULL && $value !== '') {
          $selected_definitions[] = $key;
        }
      }
    }

    if (empty($selected_definitions)) {
      $selected_definitions = $available_definition_keys;
    }

    $this->snapshotService->takeSnapshot($snapshot_type, $is_test, NULL, 'manual_form', $selected_definitions);

    $this->messenger()->deleteAll();

    $definition_labels = array_map(function ($definition) use ($available_definition_options) {
      return $available_definition_options[$definition] ?? $definition;
    }, $selected_definitions);

    $this->messenger()->addMessage($this->t('Snapshot of type %type has been taken for %definitions.', [
      '%type' => $snapshot_type,
      '%definitions' => implode(', ', $definition_labels),
    ]));
  }

}
