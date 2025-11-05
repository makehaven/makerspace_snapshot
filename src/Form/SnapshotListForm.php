<?php

namespace Drupal\makerspace_snapshot\Form;

use Drupal\Core\Ajax\AjaxResponse;
use Drupal\Core\Ajax\HtmlCommand;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Url;

/**
 * Form displaying existing snapshots with filter controls.
 */
class SnapshotListForm extends SnapshotAdminBaseForm {

  /**
   * {@inheritdoc}
   */
  public function getFormId() {
    return 'makerspace_snapshot_list';
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    $active_filters = $this->getActiveFilters($form_state);

    $snapshot_type_options = [
      '' => $this->t('- All types -'),
      'monthly' => $this->t('Monthly'),
      'quarterly' => $this->t('Quarterly'),
      'annually' => $this->t('Annually'),
      'daily' => $this->t('Daily'),
    ];

    $definition_options = ['' => $this->t('- All definitions -')];
    $known_definitions = $this->snapshotService->buildDefinitions();
    foreach ($known_definitions as $definition_key => $info) {
      $definition_options[$definition_key] = $this->formatDefinitionLabel($definition_key);
    }

    $date_options = [
      '' => $this->t('- All dates -'),
    ];

    $has_snapshot_table = $this->database->schema()->tableExists('ms_snapshot');
    if ($has_snapshot_table) {
      $period_values = $this->database->select('ms_snapshot', 'p')
        ->fields('p', ['snapshot_date'])
        ->distinct()
        ->orderBy('snapshot_date', 'DESC')
        ->execute()
        ->fetchCol();

      $type_values = $this->database->select('ms_snapshot', 't')
        ->fields('t', ['snapshot_type'])
        ->distinct()
        ->execute()
        ->fetchCol();

      $definition_values = $this->database->select('ms_snapshot', 'd')
        ->fields('d', ['definition'])
        ->distinct()
        ->execute()
        ->fetchCol();

      foreach ($period_values as $value) {
        if (!$value) {
          continue;
        }
        try {
          $label = (new \DateTimeImmutable($value))->format('F Y');
        }
        catch (\Exception $e) {
          $label = $value;
        }
        $date_options[$value] = $label;
      }

      foreach ($type_values as $type_value) {
        if ($type_value === '' || isset($snapshot_type_options[$type_value])) {
          continue;
        }
        $snapshot_type_options[$type_value] = $this->formatTypeLabel($type_value);
      }

      foreach ($definition_values as $definition_value) {
        if ($definition_value === '' || isset($definition_options[$definition_value])) {
          continue;
        }
        $definition_options[$definition_value] = $this->formatDefinitionLabel($definition_value);
      }
    }

    if (!empty($active_filters['type']) && !isset($snapshot_type_options[$active_filters['type']])) {
      $active_filters['type'] = '';
    }
    if (!empty($active_filters['date']) && !isset($date_options[$active_filters['date']])) {
      try {
        $period_label = (new \DateTimeImmutable($active_filters['date']))->format('F Y');
      }
      catch (\Exception $e) {
        $period_label = $active_filters['date'];
      }
      $date_options[$active_filters['date']] = $period_label;
    }
    if (!empty($active_filters['definition']) && !isset($definition_options[$active_filters['definition']])) {
      $active_filters['definition'] = '';
    }

    $form_state->setValue('snapshot_filters', $active_filters);

    $form['description'] = [
      '#markup' => '<p>' . $this->t('Review existing snapshots, download exports, and delete entries as needed.') . '</p>',
    ];

    $form['filters'] = [
      '#type' => 'container',
      '#attributes' => ['class' => ['snapshot-filter-controls']],
      '#tree' => TRUE,
    ];

    $form['filters']['filter_type'] = [
      '#type' => 'select',
      '#title' => $this->t('Type'),
      '#options' => $snapshot_type_options,
      '#default_value' => $active_filters['type'],
      '#parents' => ['snapshot_filters', 'type'],
      '#attributes' => ['class' => ['snapshot-filter-type']],
    ];

    $form['filters']['filter_definition'] = [
      '#type' => 'select',
      '#title' => $this->t('Definition'),
      '#options' => $definition_options,
      '#default_value' => $active_filters['definition'],
      '#parents' => ['snapshot_filters', 'definition'],
      '#attributes' => ['class' => ['snapshot-filter-definition']],
    ];

    $form['filters']['filter_date'] = [
      '#type' => 'select',
      '#title' => $this->t('Snapshot Date'),
      '#options' => $date_options,
      '#default_value' => $active_filters['date'],
      '#parents' => ['snapshot_filters', 'date'],
      '#attributes' => ['class' => ['snapshot-filter-date']],
    ];

    $form['filters']['actions'] = [
      '#type' => 'actions',
    ];

    $form['filters']['actions']['apply'] = [
      '#type' => 'submit',
      '#value' => $this->t('Apply filters'),
      '#name' => 'snapshot_filter_apply',
      '#limit_validation_errors' => [],
      '#button_type' => 'primary',
      '#submit' => ['::submitApplyFilters'],
    ];

    $form['filters']['actions']['reset'] = [
      '#type' => 'submit',
      '#value' => $this->t('Reset'),
      '#name' => 'snapshot_filter_reset',
      '#limit_validation_errors' => [],
      '#submit' => ['::submitResetFilters'],
    ];

    $rows = [];
    if ($has_snapshot_table) {
      $query = $this->database->select('ms_snapshot', 's')
        ->fields('s')
        ->orderBy('snapshot_date', 'DESC')
        ->orderBy('created_at', 'DESC');

      if (!empty($active_filters['type'])) {
        $query->condition('snapshot_type', $active_filters['type']);
      }
      if (!empty($active_filters['date'])) {
        $query->condition('snapshot_date', $active_filters['date']);
      }
      if (!empty($active_filters['definition'])) {
        $query->condition('definition', $active_filters['definition']);
      }

      $snapshots = $query->execute();

      foreach ($snapshots as $snapshot) {
        $download_links = [];
        $definition_key = $snapshot->definition ?? 'membership_totals';

        switch ($definition_key) {
          case 'membership_totals':
            $download_links['csv'] = [
              'title' => $this->t('Download CSV'),
              'url' => Url::fromRoute('makerspace_snapshot.download.org_level', ['snapshot_id' => $snapshot->id]),
            ];
            break;

          case 'plan_levels':
            $download_links['csv'] = [
              'title' => $this->t('Download CSV'),
              'url' => Url::fromRoute('makerspace_snapshot.download.plan_level', ['snapshot_id' => $snapshot->id]),
            ];
            break;

          case 'membership_activity':
            $download_links['csv'] = [
              'title' => $this->t('Download CSV'),
              'url' => Url::fromRoute('makerspace_snapshot.download.membership_activity', ['snapshot_id' => $snapshot->id]),
            ];
            break;
        }

        $operations = [
          '#type' => 'container',
          '#attributes' => ['class' => ['snapshot-operations']],
        ];

        if (!empty($download_links)) {
          $operations['download'] = [
            '#type' => 'dropbutton',
            '#links' => $download_links,
          ];
        }

        $operations['delete'] = [
          '#type' => 'submit',
          '#value' => $this->t('Delete'),
          '#name' => 'delete_snapshot_' . $snapshot->id,
          '#limit_validation_errors' => [],
          '#submit' => ['::submitDeleteSnapshot'],
          '#ajax' => [
            'callback' => '::ajaxDeleteSnapshot',
            'wrapper' => 'makerspace-snapshot-table-wrapper',
          ],
          '#attributes' => [
            'class' => ['snapshot-delete-button'],
            'data-snapshot-id' => $snapshot->id,
          ],
          '#snapshot_id' => $snapshot->id,
        ];

        $rows[$snapshot->id] = [
          'snapshot_id' => $snapshot->id,
          'definition' => $this->formatDefinitionLabel($snapshot->definition ?? 'membership_totals'),
          'snapshot_type' => $snapshot->snapshot_type,
          'snapshot_date' => $snapshot->snapshot_date,
          'source' => $this->formatSourceLabel($snapshot->source ?? ''),
          'created_at' => date('Y-m-d H:i:s', $snapshot->created_at),
          'operations' => [
            'data' => $operations,
          ],
          '#attributes' => ['data-snapshot-id' => $snapshot->id],
        ];
      }
    }

    $form['table_wrapper'] = [
      '#type' => 'container',
      '#attributes' => ['id' => 'makerspace-snapshot-table-wrapper'],
    ];

    $form['table_wrapper']['table'] = [
      '#type' => 'table',
      '#header' => [
        'snapshot_id' => $this->t('ID'),
        'definition' => $this->t('Definition'),
        'snapshot_type' => $this->t('Type'),
        'snapshot_date' => $this->t('Date'),
        'source' => $this->t('Source'),
        'created_at' => $this->t('Created'),
        'operations' => $this->t('Operations'),
      ],
      '#rows' => $rows,
      '#empty' => $this->t('No snapshots found.'),
    ];

    return $form;
  }

  /**
   * Applies filter selections.
   */
  public function submitApplyFilters(array &$form, FormStateInterface $form_state): void {
    $filters = $this->normalizeFilters($form_state->getValue('snapshot_filters') ?? []);
    $this->setActiveFilters($form_state, $filters);
    $form_state->setRedirect('makerspace_snapshot.snapshots', [], ['query' => array_filter($filters)]);
  }

  /**
   * Resets filters to defaults.
   */
  public function submitResetFilters(array &$form, FormStateInterface $form_state): void {
    $this->setActiveFilters($form_state, []);
    $form_state->setValue('snapshot_filters', []);
    $form_state->setRedirect('makerspace_snapshot.snapshots');
  }

  /**
   * Deletes a snapshot from the table.
   */
  public function submitDeleteSnapshot(array &$form, FormStateInterface $form_state): void {
    $trigger = $form_state->getTriggeringElement();
    $snapshot_id = (int) ($trigger['#snapshot_id'] ?? 0);
    if (!$snapshot_id) {
      return;
    }

    try {
      $this->snapshotService->deleteSnapshot($snapshot_id);
      $this->messenger()->addStatus($this->t('Snapshot with ID %id has been deleted.', ['%id' => $snapshot_id]));
    }
    catch (\Exception $e) {
      $this->logger('makerspace_snapshot')->error('Snapshot deletion failed for ID @id: @message', ['@id' => $snapshot_id, '@message' => $e->getMessage()]);
      $this->messenger()->addError($this->t('An error occurred while deleting the snapshot.'));
    }

    if (!isset($trigger['#ajax'])) {
      $form_state->setRedirect('makerspace_snapshot.snapshots', [], ['query' => array_filter($this->getActiveFilters($form_state))]);
      return;
    }

    $form_state->setRebuild(TRUE);
  }

  /**
   * Ajax callback for snapshot deletions.
   */
  public function ajaxDeleteSnapshot(array &$form, FormStateInterface $form_state): AjaxResponse {
    $response = new AjaxResponse();
    $renderer = \Drupal::service('renderer');
    $table_markup = $renderer->renderRoot($form['table_wrapper']);
    $response->addCommand(new HtmlCommand('#makerspace-snapshot-table-wrapper', $table_markup));

    $messages = $this->messenger()->deleteAll();
    if (!empty($messages)) {
      $render_messages = $renderer->renderRoot([
        '#theme' => 'status_messages',
        '#message_list' => $messages,
        '#status_headings' => [
          'status' => $this->t('Status message'),
          'warning' => $this->t('Warning message'),
          'error' => $this->t('Error message'),
        ],
      ]);
      $response->addCommand(new HtmlCommand('#drupal-messages', $render_messages));
    }

    return $response;
  }

  /**
   * Retrieves active filters from state or request.
   */
  protected function getActiveFilters(FormStateInterface $form_state): array {
    if ($form_state->has('active_filters')) {
      return $this->normalizeFilters($form_state->get('active_filters'));
    }

    $filters = $this->getRequestFilters();
    $this->setActiveFilters($form_state, $filters);
    return $filters;
  }

  /**
   * Retrieves current filters from the request.
   */
  protected function getRequestFilters(): array {
    $request = \Drupal::request();
    $filters = [
      'type' => (string) $request->query->get('type', ''),
      'definition' => (string) $request->query->get('definition', ''),
      'date' => (string) $request->query->get('date', ''),
    ];
    return $this->normalizeFilters($filters);
  }

  /**
   * Stores active filters in form state.
   */
  protected function setActiveFilters(FormStateInterface $form_state, array $filters): void {
    $normalized = $this->normalizeFilters($filters);
    $form_state->set('active_filters', $normalized);
    $form_state->setValue('snapshot_filters', $normalized);
  }

  /**
   * Normalises filter arrays to expected keys.
   */
  protected function normalizeFilters(array $filters): array {
    return [
      'type' => isset($filters['type']) ? (string) $filters['type'] : '',
      'definition' => isset($filters['definition']) ? (string) $filters['definition'] : '',
      'date' => isset($filters['date']) ? (string) $filters['date'] : '',
    ];
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    // No default submit handler is required.
  }

}
