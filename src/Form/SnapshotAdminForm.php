<?php

namespace Drupal\makerspace_snapshot\Form;

use Drupal\Core\Form\ConfigFormBase;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Database\Connection;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\makerspace_snapshot\SnapshotService;
use Drupal\Core\Config\ConfigFactoryInterface;
use Drupal\Core\Ajax\AjaxResponse;
use Drupal\Core\Ajax\HtmlCommand;
use Drupal\Core\Url;
use Drupal\Component\Utility\Html;
use Drupal\file\Entity\File;

/**
 * Defines a form that configures makerspace_snapshot settings.
 */
class SnapshotAdminForm extends ConfigFormBase {

  /**
   * The database connection.
   *
   * @var \Drupal\Core\Database\Connection
   */
  protected $database;

  /**
   * The snapshot service.
   *
   * @var \Drupal\makerspace_snapshot\SnapshotService
   */
  protected $snapshotService;

  /**
   * Constructs a new SnapshotAdminForm object.
   *
   * @param \Drupal\Core\Database\Connection $database
   *   The database connection.
   */
  public function __construct(Connection $database, SnapshotService $snapshotService, ConfigFactoryInterface $config_factory) {
    parent::__construct($config_factory);
    $this->database = $database;
    $this->snapshotService = $snapshotService;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('database'),
      $container->get('makerspace_snapshot.snapshot_service'),
      $container->get('config.factory')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function getFormId() {
    return 'makerspace_snapshot_admin';
  }

  /**
   * {@inheritdoc}
   */
  protected function getEditableConfigNames() {
    return [
      'makerspace_snapshot.settings',
      'makerspace_snapshot.org_metrics',
      'makerspace_snapshot.plan_levels',
    ];
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    $request = \Drupal::request();
    $active_filters = $this->getActiveFilters($form_state);
    $requested_tab = $this->getActiveTab($form_state);
    if ($requested_tab === '') {
      $requested_tab = (string) $request->query->get('tab', '');
      $this->setActiveTab($form_state, $requested_tab);
    }

    $snapshot_type_form_options = [
      'monthly' => $this->t('Monthly'),
      'quarterly' => $this->t('Quarterly'),
      'annually' => $this->t('Annually'),
      'daily' => $this->t('Daily'),
    ];

    $snapshot_type_filter_options = ['' => $this->t('- All types -')] + $snapshot_type_form_options;

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
        if ($type_value === '' || isset($snapshot_type_filter_options[$type_value])) {
          continue;
        }
        $snapshot_type_filter_options[$type_value] = $this->formatTypeLabel($type_value);
      }

      foreach ($definition_values as $definition_value) {
        if ($definition_value === '' || isset($definition_options[$definition_value])) {
          continue;
        }
        $definition_options[$definition_value] = $this->formatDefinitionLabel($definition_value);
      }
    }

    if (!empty($active_filters['type']) && !isset($snapshot_type_filter_options[$active_filters['type']])) {
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

    $this->setActiveFilters($form_state, $active_filters);

    if ($requested_tab === '' && array_filter($active_filters)) {
      $requested_tab = 'existing_snapshots';
    }
    if ($requested_tab === '') {
      $requested_tab = 'manual_snapshot';
    }
    $default_tab_id = 'edit-' . str_replace('_', '-', $requested_tab);

    $form['description'] = [
      '#markup' => '<p>' . $this->t('Use this page to configure and trigger snapshots. Reference the <strong>Snapshot Data Sources</strong> tab for schedules, acquisition methods, and SQL (when available) for each dataset.') . '</p>',
    ];

    $form['tabs'] = [
      '#type' => 'vertical_tabs',
      '#default_tab' => $default_tab_id,
    ];

    $form['manual_snapshot'] = [
      '#type' => 'details',
      '#title' => $this->t('Manual Snapshot'),
      '#group' => 'tabs',
    ];

    $manual_definition_options = $this->getManualSnapshotDefinitionOptions();
    $default_definition_selection = array_keys($manual_definition_options);

    $form['manual_snapshot']['snapshot_type'] = [
      '#type' => 'select',
      '#title' => $this->t('Snapshot Type'),
      '#options' => $snapshot_type_form_options,
      '#default_value' => 'monthly',
    ];

    $form['manual_snapshot']['snapshot_definitions'] = [
      '#type' => 'checkboxes',
      '#title' => $this->t('Definitions'),
      '#options' => $manual_definition_options,
      '#default_value' => $default_definition_selection,
      '#description' => $this->t('Select the data sets to refresh. Leave all checked to snapshot every available definition.'),
    ];


    $form['manual_snapshot']['submit'] = [
      '#type' => 'submit',
      '#value' => $this->t('Take Snapshot Now'),
      '#name' => 'manual_snapshot_take',
      '#submit' => ['::submitManualSnapshot'],
    ];

    $form['import_snapshot'] = [
      '#type' => 'details',
      '#title' => $this->t('Import Snapshot'),
      '#group' => 'tabs',
    ];

    $form['import_snapshot']['description'] = [
        '#markup' => '<p>' . $this->t('Import historical snapshot data from CSV files. You can import data for multiple dates in a single file. Dates will be normalized to the first of the month. Empty numeric values are acceptable and will be treated as 0.') . '</p>',
    ];

    $import_schedule_options = $snapshot_type_form_options + ['specific' => $this->t('Specific date')];

    $form['import_snapshot']['import_schedule'] = [
      '#type' => 'select',
      '#title' => $this->t('Snapshot Schedule'),
      '#options' => $import_schedule_options,
      '#default_value' => 'monthly',
    ];


    $form['import_snapshot']['membership_totals_csv'] = [
      '#type' => 'managed_file',
      '#title' => $this->t('Membership Totals CSV'),
      '#upload_validators' => ['file_validate_extensions' => ['csv']],
      '#description' => $this->t('<a href="@url">Download Template</a>', ['@url' => Url::fromRoute('makerspace_snapshot.download_template', ['definition' => 'membership_totals'])->toString()]),
    ];

    $form['import_snapshot']['membership_activity_csv'] = [
      '#type' => 'managed_file',
      '#title' => $this->t('Membership Activity CSV'),
      '#upload_validators' => ['file_validate_extensions' => ['csv']],
      '#description' => $this->t('<a href="@url">Download Template</a>', ['@url' => Url::fromRoute('makerspace_snapshot.download_template', ['definition' => 'membership_activity'])->toString()]),
    ];

    $form['import_snapshot']['event_registrations_csv'] = [
      '#type' => 'managed_file',
      '#title' => $this->t('Event Registrations CSV'),
      '#upload_validators' => ['file_validate_extensions' => ['csv']],
      '#description' => $this->t('<a href="@url">Download Template</a>', ['@url' => Url::fromRoute('makerspace_snapshot.download_template', ['definition' => 'event_registrations'])->toString()]),
    ];

    $form['import_snapshot']['plan_csv'] = [
      '#type' => 'managed_file',
      '#title' => $this->t('Plan CSV'),
      '#upload_validators' => ['file_validate_extensions' => ['csv']],
      '#description' => $this->t('<a href="@url">Download Template</a>', ['@url' => Url::fromRoute('makerspace_snapshot.download_template', ['definition' => 'plan_levels'])->toString()]),
    ];

    $form['import_snapshot']['submit'] = [
      '#type' => 'submit',
      '#value' => $this->t('Import Snapshot'),
      '#submit' => ['::submitImportSnapshot'],
      '#validate' => ['::validateImportSnapshot'],
    ];

    $form['existing_snapshots'] = [
      '#type' => 'details',
      '#title' => $this->t('Existing Snapshots'),
      '#group' => 'tabs',
    ];

    $header = [
      'snapshot_id' => $this->t('ID'),
      'definition' => $this->t('Definition'),
      'snapshot_type' => $this->t('Type'),
      'snapshot_date' => $this->t('Date'),
      'source' => $this->t('Source'),
      'created_at' => $this->t('Created'),
      'operations' => $this->t('Operations'),
    ];

    $form['existing_snapshots']['filters'] = [
      '#type' => 'container',
      '#attributes' => ['class' => ['snapshot-filter-controls']],
      '#tree' => TRUE,
    ];

    $form['existing_snapshots']['filters']['filter_type'] = [
      '#type' => 'select',
      '#title' => $this->t('Type'),
      '#options' => $snapshot_type_filter_options,
      '#default_value' => $active_filters['type'],
      '#parents' => ['snapshot_filters', 'type'],
      '#attributes' => ['class' => ['snapshot-filter-type']],
    ];

    $form['existing_snapshots']['filters']['filter_definition'] = [
      '#type' => 'select',
      '#title' => $this->t('Definition'),
      '#options' => $definition_options,
      '#default_value' => $active_filters['definition'],
      '#parents' => ['snapshot_filters', 'definition'],
      '#attributes' => ['class' => ['snapshot-filter-definition']],
    ];

    $form['existing_snapshots']['filters']['filter_date'] = [
      '#type' => 'select',
      '#title' => $this->t('Snapshot Date'),
      '#options' => $date_options,
      '#default_value' => $active_filters['date'],
      '#parents' => ['snapshot_filters', 'date'],
      '#attributes' => ['class' => ['snapshot-filter-date']],
    ];

    $form['existing_snapshots']['filters']['actions'] = [
      '#type' => 'actions',
    ];

    $form['existing_snapshots']['filters']['actions']['apply'] = [
      '#type' => 'submit',
      '#value' => $this->t('Apply filters'),
      '#name' => 'snapshot_filter_apply',
      '#limit_validation_errors' => [],
      '#button_type' => 'primary',
      '#submit' => ['::submitApplyFilters'],
    ];

    $form['existing_snapshots']['filters']['actions']['reset'] = [
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
        switch ($snapshot->definition) {
          case 'membership_totals':
            $download_links['org'] = [
              'title' => $this->t('Download Membership Totals CSV'),
              'url' => Url::fromRoute('makerspace_snapshot.download.org_level', ['snapshot_id' => $snapshot->id]),
            ];
            break;

          case 'plan_levels':
            $download_links['plan'] = [
              'title' => $this->t('Download Plan Levels CSV'),
              'url' => Url::fromRoute('makerspace_snapshot.download.plan_level', ['snapshot_id' => $snapshot->id]),
            ];
            break;

          case 'membership_activity':
            $download_links['activity'] = [
              'title' => $this->t('Download Membership Activity CSV'),
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
          '#attributes' => [
            'class' => ['snapshot-delete-button'],
            'data-snapshot-id' => $snapshot->id,
          ],
          '#snapshot_id' => $snapshot->id,
          '#submit' => ['::submitDeleteSnapshot'],
          '#ajax' => [
            'callback' => '::ajaxDeleteSnapshot',
            'event' => 'click',
            'progress' => ['type' => 'throbber', 'message' => $this->t('Removing snapshot...')],
          ],
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

    $form['existing_snapshots']['table_wrapper'] = [
      '#type' => 'container',
      '#attributes' => ['id' => 'makerspace-snapshot-table-wrapper'],
    ];

    $form['existing_snapshots']['table_wrapper']['table'] = [
      '#type' => 'table',
      '#header' => $header,
      '#rows' => $rows,
      '#empty' => $this->t('No snapshots found.'),
    ];

    $form['query_information'] = [
      '#type' => 'details',
      '#title' => $this->t('Snapshot Data Sources'),
      '#group' => 'tabs',
    ];

    $form['query_information']['intro'] = [
      '#markup' => '<p>' . $this->t('Each section lists the source of truth for the dataset, how it is populated, the default schedules, and—when applicable—the SQL queries executed during automated runs.') . '</p>',
    ];

    $sourceQueries = $this->snapshotService->getSourceQueries();
    $datasetMap = $this->snapshotService->getDatasetSourceMap();
    $definitionMetadata = $this->snapshotService->buildDefinitions();

    foreach ($datasetMap as $dataset_key => $dataset) {
      $dataset_section = [
        '#type' => 'details',
        '#title' => isset($definitionMetadata[$dataset_key]['label'])
          ? $this->t($definitionMetadata[$dataset_key]['label'])
          : $dataset['label'],
        '#open' => FALSE,
      ];

      if (!empty($dataset['description'])) {
        $dataset_section['description'] = [
          '#markup' => '<p>' . Html::escape($dataset['description']) . '</p>',
        ];
      }

      $definitionInfo = $definitionMetadata[$dataset_key] ?? [];
      $metaItems = [];
      if (!empty($definitionInfo['data_source'])) {
        $metaItems[] = $this->t('Primary source: @source', ['@source' => $this->formatDataSourceLabel($definitionInfo['data_source'])]);
      }
      if (!empty($definitionInfo['acquisition'])) {
        $metaItems[] = $this->t('Acquisition: @mode', ['@mode' => $this->formatAcquisitionLabel($definitionInfo['acquisition'])]);
      }
      if (!empty($definitionInfo['schedules']) && is_array($definitionInfo['schedules'])) {
        $metaItems[] = $this->t('Default schedules: @list', ['@list' => $this->formatScheduleList($definitionInfo['schedules'])]);
      }

      if ($metaItems) {
        $dataset_section['metadata'] = [
          '#theme' => 'item_list',
          '#items' => $metaItems,
          '#attributes' => ['class' => ['snapshot-dataset-meta']],
        ];
      }

      $acquisitionMode = $definitionInfo['acquisition'] ?? 'automated';

      if ($acquisitionMode !== 'automated') {
        $dataset_section['placeholder'] = [
          '#markup' => '<p><em>' . $this->getAcquisitionPlaceholderMessage($acquisitionMode) . '</em></p>',
        ];
      }
      elseif (!empty($dataset['queries'])) {
        $dataset_section['queries'] = [
          '#type' => 'container',
          '#attributes' => ['class' => ['snapshot-dataset-queries']],
        ];

        foreach ($dataset['queries'] as $query_key) {
          if (!isset($sourceQueries[$query_key])) {
            continue;
          }
          $query_info = $sourceQueries[$query_key];
          $dataset_section['queries'][$query_key] = [
            '#type' => 'details',
            '#title' => $query_info['label'],
            '#open' => FALSE,
          ];

          if (!empty($query_info['description'])) {
            $dataset_section['queries'][$query_key]['description'] = [
              '#markup' => '<p>' . Html::escape($query_info['description']) . '</p>',
            ];
          }

          $dataset_section['queries'][$query_key]['sql'] = [
            '#type' => 'item',
            '#title' => $this->t('SQL'),
            '#markup' => '<pre><code>' . Html::escape(trim($query_info['sql'])) . '</code></pre>',
          ];
        }
      }
      elseif ($acquisitionMode === 'automated') {
        $dataset_section['placeholder'] = [
          '#markup' => '<p><em>' . $this->t('No automated SQL source has been implemented for this dataset yet.') . '</em></p>',
        ];
      }

      $form['query_information']['dataset_' . $dataset_key] = $dataset_section;
    }

    return parent::buildForm($form, $form_state);
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    // All actions are handled by dedicated submit handlers.
  }

  /**
   * Handles manual snapshot submissions.
   */
  public function submitManualSnapshot(array &$form, FormStateInterface $form_state): void {
    $snapshotType = $form_state->getValue('snapshot_type');
    $isTest = $form_state->getValue('is_test');
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

    $this->snapshotService->takeSnapshot($snapshotType, $isTest, NULL, 'manual_form', $selected_definitions);

    $this->messenger()->deleteAll();

    $definition_labels = array_map(function ($definition) use ($available_definition_options) {
      return $available_definition_options[$definition] ?? $definition;
    }, $selected_definitions);

    $this->messenger()->addMessage($this->t('Snapshot of type %type has been taken for %definitions.', [
      '%type' => $snapshotType,
      '%definitions' => implode(', ', $definition_labels),
    ]));
    $this->setActiveTab($form_state, 'manual_snapshot');
    $filters = $this->getActiveFilters($form_state);
    $query = array_filter($filters);
    $query['tab'] = 'manual_snapshot';
    $form_state->setRedirect('makerspace_snapshot.admin', [], ['query' => $query]);
  }

  public function submitApplyFilters(array &$form, FormStateInterface $form_state): void {
    $filters = $this->normalizeFilters($form_state->getValue('snapshot_filters') ?? []);
    $this->setActiveFilters($form_state, $filters);
    $this->setActiveTab($form_state, 'existing_snapshots');
    $form_state->setRebuild(TRUE);
  }

  public function submitResetFilters(array &$form, FormStateInterface $form_state): void {
    $this->setActiveFilters($form_state, []);
    $this->setActiveTab($form_state, 'existing_snapshots');
    $form_state->setRedirect('makerspace_snapshot.admin', [], ['query' => ['tab' => 'existing_snapshots']]);
  }

  public function submitDeleteSnapshot(array &$form, FormStateInterface $form_state): void {
    $trigger = $form_state->getTriggeringElement();
    $snapshot_id = (int) ($trigger['#snapshot_id'] ?? 0);
    if (!$snapshot_id) {
      return;
    }

    try {
      $this->snapshotService->deleteSnapshot($snapshot_id);
      if (isset($trigger['#ajax'])) {
        $this->messenger()->deleteAll();
      }
      $this->messenger()->addStatus($this->t('Snapshot with ID %id has been deleted.', ['%id' => $snapshot_id]));
    }
    catch (\Exception $e) {
      $this->logger('makerspace_snapshot')->error('Snapshot deletion failed for ID @id: @message', ['@id' => $snapshot_id, '@message' => $e->getMessage()]);
      $this->messenger()->addError($this->t('An error occurred while deleting the snapshot.'));
    }

    $this->setActiveTab($form_state, 'existing_snapshots');

    if (isset($trigger['#ajax'])) {
      $form_state->setRebuild(TRUE);
    }
    else {
      $filters = $this->getActiveFilters($form_state);
      $query = array_filter($filters);
      $query['tab'] = 'existing_snapshots';
      $form_state->setRedirect('makerspace_snapshot.admin', [], ['query' => $query]);
    }
  }

  public function ajaxDeleteSnapshot(array &$form, FormStateInterface $form_state): AjaxResponse {
    $response = new AjaxResponse();
    $trigger = $form_state->getTriggeringElement();
    $snapshot_id = (int) ($trigger['#snapshot_id'] ?? 0);

    $response->addCommand(new HtmlCommand('#makerspace-snapshot-table-wrapper', $form['existing_snapshots']['table_wrapper']));

    $messages = $this->messenger()->all();
    $this->messenger()->deleteAll();
    $render_messages = [
      '#theme' => 'status_messages',
      '#message_list' => $messages,
      '#status_headings' => [
        'status' => $this->t('Status message'),
        'warning' => $this->t('Warning message'),
        'error' => $this->t('Error message'),
      ],
    ];
    $response->addCommand(new HtmlCommand('#drupal-messages', $render_messages));

    return $response;
  }

  /**
   * Builds a human-friendly label for a snapshot definition.
   */
  protected function formatDefinitionLabel(string $definition): string {
    $definitions = $this->snapshotService->buildDefinitions();
    if (isset($definitions[$definition]['label'])) {
      return (string) $this->t($definitions[$definition]['label']);
    }
    $label = str_replace('_', ' ', $definition);
    return ucwords($label);
  }

  /**
   * Builds a human-friendly label for a snapshot type.
   */
  protected function formatTypeLabel(string $type): string {
    $label = str_replace('_', ' ', $type);
    return ucwords($label);
  }

  /**
   * Builds a human-friendly label for a snapshot source.
   */
  protected function formatSourceLabel(string $source): string {
    $source = $source ?: 'system';
    $map = [
      'manual_form' => $this->t('Manual (Admin)'),
      'manual_import' => $this->t('Manual (Import)'),
      'manual_drush' => $this->t('Manual (Drush)'),
      'automatic_cron' => $this->t('Automatic (Cron)'),
      'system' => $this->t('System'),
    ];

    if (isset($map[$source])) {
      return $map[$source];
    }

    $label = str_replace('_', ' ', $source);
    return ucwords($label);
  }

  /**
   * Returns the list of definitions supported by manual snapshots.
   *
   * @return string[]
   *   An associative array keyed by definition machine name with human labels.
   */
  protected function getManualSnapshotDefinitionOptions(): array {
    $definitions = [];
    foreach ($this->snapshotService->buildDefinitions() as $definition_key => $info) {
      $acquisition = $info['acquisition'] ?? 'automated';
      if ($acquisition !== 'automated') {
        continue;
      }
      $definitions[$definition_key] = $this->formatDefinitionLabel($definition_key);
    }
    return $definitions;
  }

  /**
   * Formats a human-readable data source label.
   */
  protected function formatDataSourceLabel(string $source): string {
    switch (strtolower($source)) {
      case 'drupal sql':
        return (string) $this->t('Drupal SQL');
      case 'civicrm sql':
        return (string) $this->t('CiviCRM SQL');
      case 'manual import':
        return (string) $this->t('Manual Import');
      case 'external api':
        return (string) $this->t('External API');
      default:
        return (string) $this->t('@source', ['@source' => $source]);
    }
  }

  /**
   * Formats acquisition mode labels.
   */
  protected function formatAcquisitionLabel(string $acquisition): string {
    switch (strtolower($acquisition)) {
      case 'automated':
        return (string) $this->t('Automated');
      case 'import':
        return (string) $this->t('Manual Import');
      case 'placeholder':
        return (string) $this->t('Planned Integration');
      default:
        return (string) $this->t('@mode', ['@mode' => $acquisition]);
    }
  }

  /**
   * Formats schedule arrays into a comma-separated list.
   */
  protected function formatScheduleList(array $schedules): string {
    if (empty($schedules)) {
      return (string) $this->t('None');
    }
    $labels = array_map(function ($schedule) {
      return $this->formatTypeLabel((string) $schedule);
    }, $schedules);
    return implode(', ', $labels);
  }

  /**
   * Provides contextual messaging when SQL is unavailable.
   */
  protected function getAcquisitionPlaceholderMessage(string $acquisition): string {
    switch (strtolower($acquisition)) {
      case 'import':
        return (string) $this->t('This dataset is populated via manual import, so no SQL preview is available.');
      case 'placeholder':
        return (string) $this->t('This dataset is planned for a future integration. Data collection will begin once the source is wired.');
      default:
        return (string) $this->t('No automated SQL source has been implemented for this dataset yet.');
    }
  }

  protected function normalizeFilters(array $filters): array {
    return [
      'type' => isset($filters['type']) ? (string) $filters['type'] : '',
      'definition' => isset($filters['definition']) ? (string) $filters['definition'] : '',
      'date' => isset($filters['date']) ? (string) $filters['date'] : '',
    ];
  }

  protected function getActiveFilters(FormStateInterface $form_state): array {
    if ($form_state->has('active_filters')) {
      return $this->normalizeFilters($form_state->get('active_filters'));
    }

    $request = \Drupal::request();
    $filters = [
      'type' => (string) $request->query->get('type', ''),
      'definition' => (string) $request->query->get('definition', ''),
      'date' => (string) $request->query->get('date', ''),
    ];

    $filters = $this->normalizeFilters($filters);
    $form_state->set('active_filters', $filters);
    return $filters;
  }

  protected function setActiveFilters(FormStateInterface $form_state, array $filters): void {
    $form_state->set('active_filters', $this->normalizeFilters($filters));
  }

  protected function getActiveTab(FormStateInterface $form_state): string {
    if ($form_state->has('active_tab')) {
      return (string) $form_state->get('active_tab');
    }

    $request = \Drupal::request();
    $tab = (string) $request->query->get('tab', '');
    $form_state->set('active_tab', $tab);
    return $tab;
  }

  protected function setActiveTab(FormStateInterface $form_state, string $tab): void {
    $form_state->set('active_tab', $tab);
  }

  public function submitImportSnapshot(array &$form, FormStateInterface $form_state) {
    $import_data = $form_state->get('import_snapshot_data');
    foreach ($import_data as $date => $date_data) {
      foreach ($date_data as $definition => $data) {
        $this->snapshotService->importSnapshot(
          $definition,
          $form_state->getValue('import_schedule'),
          $date,
          $data
        );
      }
    }
    $this->messenger()->addMessage($this->t('The snapshot(s) have been imported.'));
    $this->cleanupUploadedFiles($form_state);
  }

  public function validateImportSnapshot(array &$form, FormStateInterface $form_state) {
    $import_data = [];
    $all_dates = [];

    $normalize_row = function (&$row) use ($form_state) {
        // Normalize date.
        try {
            $row['snapshot_date'] = (new \DateTimeImmutable($row['snapshot_date']))->format('Y-m-01');
        } catch (\Exception $e) {
            $form_state->setErrorByName('membership_totals_csv', $this->t('Invalid date format found in CSV: @date', ['@date' => $row['snapshot_date']]));
            return;
        }

        // Normalize numeric fields.
        foreach ($row as $key => &$value) {
            if ($key !== 'snapshot_date' && $key !== 'plan_code' && $key !== 'plan_label' && $key !== 'event_title' && $key !== 'event_start_date') {
                if ($value === '' || !is_numeric($value)) {
                    $value = 0;
                }
            }
        }
    };

    // Membership Totals
    if ($file_id = $form_state->getValue(['membership_totals_csv', 0])) {
      $data = $this->extractCsvData($file_id, ['snapshot_date', 'members_active', 'members_paused', 'members_lapsed']);
      foreach ($data as $row) {
        $normalize_row($row);
        $import_data[$row['snapshot_date']]['membership_totals']['totals'] = $row;
        $all_dates[] = $row['snapshot_date'];
      }
    }

    // Membership Activity
    if ($file_id = $form_state->getValue(['membership_activity_csv', 0])) {
      $data = $this->extractCsvData($file_id, ['snapshot_date', 'joins', 'cancels', 'net_change']);
      foreach ($data as $row) {
        $normalize_row($row);
        $import_data[$row['snapshot_date']]['membership_activity']['activity'] = $row;
        $all_dates[] = $row['snapshot_date'];
      }
    }

    // Plan Data
    if ($file_id = $form_state->getValue(['plan_csv', 0])) {
      $data = $this->extractCsvData($file_id, ['snapshot_date', 'plan_code', 'plan_label', 'count_members']);
      foreach ($data as $row) {
        $normalize_row($row);
        $import_data[$row['snapshot_date']]['membership_totals']['plans'][] = $row;
      }
    }

    // Event Registrations
    if ($file_id = $form_state->getValue(['event_registrations_csv', 0])) {
        $data = $this->extractCsvData($file_id, ['snapshot_date', 'event_id', 'event_title', 'event_start_date', 'registration_count']);
        foreach ($data as $row) {
            $normalize_row($row);
        }
        $event_dates = array_unique(array_column($data, 'snapshot_date'));
        if (count($event_dates) > 1) {
            $form_state->setErrorByName('event_registrations_csv', $this->t('The event registrations CSV can only contain data for a single date.'));
        }
        $import_data[$event_dates[0]]['event_registrations']['events'] = $data;
        $all_dates[] = $event_dates[0];
    }

    // Check for existing snapshots.
    $all_dates = array_unique($all_dates);
    $query = $this->database->select('ms_snapshot', 's');
    $query->fields('s', ['id', 'snapshot_date']);
    $query->condition('snapshot_date', $all_dates, 'IN');
    $results = $query->execute()->fetchAllAssoc('snapshot_date');

    foreach ($import_data as $date => &$date_data) {
      if (isset($results[$date])) {
        foreach ($date_data as &$payload) {
          $payload['snapshot_id'] = $results[$date]->id;
        }
      }
    }

    $form_state->set('import_snapshot_data', $import_data);
  }

  protected function extractCsvData($file_id, array $expected_headers) {
    $file = File::load($file_id);
    $file_path = $file->getFileUri();
    return $this->parseCsvFile($file_path, $expected_headers);
  }

  protected function parseCsvFile($file_path, array $expected_headers) {
    $handle = fopen($file_path, 'r');
    if (!$handle) {
      throw new \Exception("Could not open the file: {$file_path}");
    }

    // Strip UTF-8 BOM.
    if (fgets($handle, 4) !== "\xef\xbb\xbf") {
      rewind($handle);
    }

    $header = fgetcsv($handle);
    if ($header !== $expected_headers) {
      throw new \Exception('The CSV file has an invalid header.');
    }

    $data = [];
    while (($row = fgetcsv($handle)) !== FALSE) {
      if (!array_filter($row)) {
        continue;
      }
      $data[] = array_combine($header, $row);
    }

    fclose($handle);
    return $data;
  }

  protected function cleanupUploadedFiles(FormStateInterface $form_state) {
      $file_inputs = [
          'membership_totals_csv',
          'membership_activity_csv',
          'event_registrations_csv',
          'plan_csv',
      ];

      foreach ($file_inputs as $input) {
          if ($file_id = $form_state->getValue([$input, 0])) {
              $file = File::load($file_id);
              if ($file) {
                  $file->delete();
              }
          }
      }
  }
}
