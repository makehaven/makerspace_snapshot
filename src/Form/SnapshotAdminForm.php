<?php

namespace Drupal\makerspace_snapshot\Form;

use Drupal\Core\Form\ConfigFormBase;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Database\Connection;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\makerspace_snapshot\SnapshotService;
use Drupal\Core\Config\ConfigFactoryInterface;
use Drupal\Core\Url;
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
      'makerspace_snapshot.sources',
      'makerspace_snapshot.org_metrics',
      'makerspace_snapshot.plan_levels',
    ];
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    $form['description'] = [
      '#markup' => '<p>This page allows you to configure and manually trigger snapshots of your website data. You can edit the <a href="/admin/config/makerspace/snapshot/sql">Snapshot SQL Queries</a> on a dedicated configuration page.</p>',
    ];

    $form['tabs'] = [
      '#type' => 'vertical_tabs',
      '#default_tab' => 'edit-manual-snapshot',
    ];

    $form['manual_snapshot'] = [
      '#type' => 'details',
      '#title' => $this->t('Manual Snapshot'),
      '#group' => 'tabs',
    ];

    $form['manual_snapshot']['snapshot_type'] = [
      '#type' => 'select',
      '#title' => $this->t('Snapshot Type'),
      '#options' => [
        'monthly' => $this->t('Monthly'),
        'quarterly' => $this->t('Quarterly'),
        'annually' => $this->t('Annually'),
        'daily' => $this->t('Daily'),
        'manual' => $this->t('Manual'),
      ],
      '#default_value' => 'manual',
    ];


    $form['manual_snapshot']['submit'] = [
      '#type' => 'submit',
      '#value' => $this->t('Take Snapshot Now'),
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

    $form['import_snapshot']['import_schedule'] = [
      '#type' => 'select',
      '#title' => $this->t('Snapshot Schedule'),
      '#options' => [
        'monthly' => $this->t('Monthly'),
        'quarterly' => $this->t('Quarterly'),
        'annually' => $this->t('Annually'),
        'daily' => $this->t('Daily'),
        'manual' => $this->t('Manual'),
      ],
      '#default_value' => 'manual',
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
      'created_at' => $this->t('Created'),
      'operations' => $this->t('Operations'),
    ];

    $rows = [];
    if ($this->database->schema()->tableExists('ms_snapshot')) {
      $snapshots = $this->database->select('ms_snapshot', 's')
        ->fields('s')
        ->orderBy('created_at', 'DESC')
        ->execute();

      foreach ($snapshots as $snapshot) {
        $rows[$snapshot->id] = [
          'snapshot_id' => $snapshot->id,
          'definition' => $snapshot->definition ?? 'membership_totals',
          'snapshot_type' => $snapshot->snapshot_type,
          'snapshot_date' => $snapshot->snapshot_date,
          'created_at' => date('Y-m-d H:i:s', $snapshot->created_at),
          'operations' => [
            'data' => [
              '#type' => 'container',
              'download_links' => [
                '#type' => 'dropbutton',
                '#links' => [
                  'download_org_csv' => [
                    'title' => $this->t('Download Org CSV'),
                    'url' => Url::fromRoute('makerspace_snapshot.download.org_level', ['snapshot_id' => $snapshot->id]),
                  ],
                  'download_plan_csv' => [
                    'title' => $this->t('Download Plan CSV'),
                    'url' => Url::fromRoute('makerspace_snapshot.download.plan_level', ['snapshot_id' => $snapshot->id]),
                  ],
                ],
              ],
              'delete' => [
                '#type' => 'submit',
                '#value' => $this->t('Delete'),
                '#name' => 'delete_' . $snapshot->id,
                '#submit' => ['::submitDeleteSnapshot'],
                '#limit_validation_errors' => [],
                '#snapshot_id' => $snapshot->id,
                '#attributes' => ['data-snapshot-id' => $snapshot->id],
              ],
            ]
          ],
        ];
      }
    }

    $form['existing_snapshots']['table'] = [
      '#type' => 'table',
      '#header' => $header,
      '#rows' => $rows,
      '#empty' => $this->t('No snapshots found.'),
    ];

    return parent::buildForm($form, $form_state);
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    // This is handled by the custom submit handlers.
  }

  public function submitManualSnapshot(array &$form, FormStateInterface $form_state) {
    $snapshotType = $form_state->getValue('snapshot_type');
    $isTest = $form_state->getValue('is_test');

    $this->snapshotService->takeSnapshot($snapshotType, $isTest);
    $this->messenger()->addMessage($this->t('Snapshot of type %type has been taken.', ['%type' => $snapshotType]));
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

  public function submitDeleteSnapshot(array &$form, FormStateInterface $form_state) {
    $triggering_element = $form_state->getTriggeringElement();
    $snapshot_id = $triggering_element['#snapshot_id'] ?? NULL;

    if (!$snapshot_id && isset($triggering_element['#name'])) {
      if (preg_match('/^delete[_-](\d+)$/', $triggering_element['#name'], $matches)) {
        $snapshot_id = $matches[1];
      }
    }

    if (!$snapshot_id) {
      $this->messenger()->addError($this->t('Unable to determine which snapshot to delete.'));
      return;
    }

    if ($snapshot_id) {
        $this->database->delete('ms_snapshot')
            ->condition('id', $snapshot_id)
            ->execute();
        $this->database->delete('ms_fact_org_snapshot')
            ->condition('snapshot_id', $snapshot_id)
            ->execute();
        $this->database->delete('ms_fact_plan_snapshot')
            ->condition('snapshot_id', $snapshot_id)
            ->execute();
        $this->database->delete('ms_fact_membership_activity')
            ->condition('snapshot_id', $snapshot_id)
            ->execute();
        $this->database->delete('ms_fact_event_snapshot')
            ->condition('snapshot_id', $snapshot_id)
            ->execute();
        $this->messenger()->addMessage($this->t('Snapshot with ID %id has been deleted.', ['%id' => $snapshot_id]));
        $form_state->setRebuild(TRUE);
    }
  }
}
