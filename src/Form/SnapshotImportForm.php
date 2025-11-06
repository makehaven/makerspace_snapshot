<?php

namespace Drupal\makerspace_snapshot\Form;

use Drupal\Component\Utility\Html;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Link;
use Drupal\Core\Url;
use Drupal\file\Entity\File;

/**
 * Form for importing snapshot data from CSV files.
 */
class SnapshotImportForm extends SnapshotAdminBaseForm {

  /**
   * {@inheritdoc}
   */
  public function getFormId() {
    return 'makerspace_snapshot_import';
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    $form['description'] = [
      '#markup' => '<p>' . $this->t('Import historical snapshot data from CSV files. Multiple dates can be included per upload; each date will be normalized to the first of that month.') . '</p>',
    ];

    $snapshot_type_options = [
      'monthly' => $this->t('Monthly'),
      'quarterly' => $this->t('Quarterly'),
      'annually' => $this->t('Annually'),
      'daily' => $this->t('Daily'),
      'specific' => $this->t('Specific date'),
    ];

    $form['import_schedule'] = [
      '#type' => 'select',
      '#title' => $this->t('Snapshot Schedule'),
      '#options' => $snapshot_type_options,
      '#default_value' => 'monthly',
    ];

    $definitions = $this->snapshotService->buildDefinitions();
    uasort($definitions, function (array $a, array $b) {
      return strcasecmp($a['label'] ?? '', $b['label'] ?? '');
    });

    $dataset_map = $this->snapshotService->getDatasetSourceMap();

    $form['datasets'] = [
      '#type' => 'container',
      '#attributes' => ['class' => ['snapshot-import-datasets']],
    ];

    foreach ($definitions as $definition => $info) {
      $field_name = $this->getDatasetFieldName($definition);
      $label = $this->formatDefinitionLabel($definition);
      $dataset_description = $dataset_map[$definition]['description'] ?? '';
      $template_link = Link::fromTextAndUrl($this->t('Download template'), Url::fromRoute('makerspace_snapshot.download_template', ['definition' => $definition]))->toString();
      $expected_headers = implode(', ', $info['headers'] ?? []);

      $form['datasets'][$definition] = [
        '#type' => 'details',
        '#title' => $label,
        '#open' => FALSE,
      ];

      if (!empty($dataset_description)) {
        $form['datasets'][$definition]['summary'] = [
          '#markup' => '<p>' . Html::escape($dataset_description) . '</p>',
        ];
      }

      $form['datasets'][$definition][$field_name] = [
        '#type' => 'managed_file',
        '#title' => $this->t('@label CSV', ['@label' => $label]),
        '#upload_validators' => ['file_validate_extensions' => ['csv']],
        '#description' => $template_link . '<br/>' . $this->t('Expected headers: @headers', ['@headers' => $expected_headers]),
      ];
    }

    $form['actions'] = [
      '#type' => 'actions',
    ];
    $form['actions']['submit'] = [
      '#type' => 'submit',
      '#value' => $this->t('Import Snapshot'),
    ];

    return $form;
  }

  /**
   * {@inheritdoc}
   */
  public function validateForm(array &$form, FormStateInterface $form_state) {
    $import_data = [];
    $all_dates = [];

    foreach ($this->getDatasetUploadFieldMap() as $definition => $field_name) {
      $file_id = $form_state->getValue([$field_name, 0]);
      if (!$file_id) {
        continue;
      }

      try {
        $rows = $this->readDatasetCsv($definition, $file_id);
      }
      catch (\Exception $e) {
        $form_state->setErrorByName($field_name, $this->t('The @label CSV could not be processed: @message', [
          '@label' => $this->formatDefinitionLabel($definition),
          '@message' => $e->getMessage(),
        ]));
        continue;
      }

      $this->processDatasetRows($definition, $field_name, $rows, $import_data, $all_dates, $form_state);
    }

    if ($form_state->getErrors()) {
      return;
    }

    $all_dates = array_unique($all_dates);
    if (!empty($all_dates)) {
      $query = $this->database->select('ms_snapshot', 's')
        ->fields('s', ['id', 'definition', 'snapshot_date'])
        ->condition('snapshot_date', $all_dates, 'IN');
      $existing = $query->execute()->fetchAll();
      $existing_map = [];
      foreach ($existing as $row) {
        $existing_map[$row->snapshot_date][$row->definition] = $row->id;
      }

      foreach ($import_data as $date => &$date_payloads) {
        foreach ($date_payloads as $definition => &$payload) {
          if (isset($existing_map[$date][$definition])) {
            $payload['snapshot_id'] = $existing_map[$date][$definition];
          }
        }
      }
    }

    $form_state->set('import_snapshot_data', $import_data);
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    $import_data = $form_state->get('import_snapshot_data') ?? [];
    if (empty($import_data)) {
      $this->messenger()->addWarning($this->t('No import data was processed.'));
      return;
    }

    foreach ($import_data as $date => $datasets) {
      foreach ($datasets as $definition => $payload) {
        $this->snapshotService->importSnapshot(
          $definition,
          $form_state->getValue('import_schedule'),
          $date,
          $payload
        );
      }
    }

    $this->messenger()->addMessage($this->t('The snapshot data has been imported.'));
    $this->cleanupUploadedFiles($form_state);
  }

  /**
   * Builds a mapping of dataset definitions to upload field names.
   */
  protected function getDatasetUploadFieldMap(): array {
    $map = [];
    foreach ($this->snapshotService->buildDefinitions() as $definition => $info) {
      $map[$definition] = $this->getDatasetFieldName($definition);
    }
    return $map;
  }

  /**
   * Returns the field name for a dataset upload element.
   */
  protected function getDatasetFieldName(string $definition): string {
    return $definition . '_csv';
  }

  /**
   * Reads CSV rows for a given dataset.
   *
   * @throws \RuntimeException
   */
  protected function readDatasetCsv(string $definition, $file_id): array {
    $definitions = $this->snapshotService->buildDefinitions();
    $headers = $definitions[$definition]['headers'] ?? [];

    if ($definition === 'membership_totals') {
      try {
        return $this->extractCsvData($file_id, $headers);
      }
      catch (\Exception $e) {
        $fallback = ['snapshot_date', 'members_active', 'members_paused', 'members_lapsed'];
        return $this->extractCsvData($file_id, $fallback);
      }
    }

    if ($definition === 'membership_types' && empty($headers)) {
      $headers = ['snapshot_date', 'members_total'];
    }

    if (empty($headers)) {
      throw new \RuntimeException('Dataset headers are not defined.');
    }

    return $this->extractCsvData($file_id, $headers);
  }

  /**
   * Processes CSV rows into snapshot payloads.
   */
  protected function processDatasetRows(string $definition, string $field_name, array $rows, array &$import_data, array &$all_dates, FormStateInterface $form_state): void {
    switch ($definition) {
      case 'membership_totals':
        foreach ($rows as $row) {
          $normalized_date = $this->normalizeSnapshotDate($row['snapshot_date'], $field_name, $form_state);
          if ($normalized_date === NULL) {
            continue;
          }
          $members_active = (int) ($row['members_active'] ?? 0);
          $members_paused = (int) ($row['members_paused'] ?? 0);
          $members_lapsed = (int) ($row['members_lapsed'] ?? 0);
          $members_total = isset($row['members_total']) && $row['members_total'] !== ''
            ? (int) $row['members_total']
            : ($members_active + $members_paused);

          $import_data[$normalized_date]['membership_totals']['totals'] = [
            'members_active' => $members_active,
            'members_paused' => $members_paused,
            'members_lapsed' => $members_lapsed,
            'members_total' => $members_total,
          ];
          $all_dates[] = $normalized_date;
        }
        break;

      case 'plan_levels':
        foreach ($rows as $row) {
          $normalized_date = $this->normalizeSnapshotDate($row['snapshot_date'], $field_name, $form_state);
          if ($normalized_date === NULL) {
            continue;
          }
          $import_data[$normalized_date]['plan_levels']['plans'][] = [
            'plan_code' => (string) ($row['plan_code'] ?? ''),
            'plan_label' => (string) ($row['plan_label'] ?? ($row['plan_code'] ?? '')),
            'count_members' => (int) ($row['count_members'] ?? 0),
          ];
          $all_dates[] = $normalized_date;
        }
        break;

      case 'membership_activity':
        foreach ($rows as $row) {
          $normalized_date = $this->normalizeSnapshotDate($row['snapshot_date'], $field_name, $form_state);
          if ($normalized_date === NULL) {
            continue;
          }
          $import_data[$normalized_date]['membership_activity']['activity'] = [
            'joins' => (int) ($row['joins'] ?? 0),
            'cancels' => (int) ($row['cancels'] ?? 0),
            'net_change' => (int) ($row['net_change'] ?? 0),
          ];
          $all_dates[] = $normalized_date;
        }
        break;

      case 'event_registrations':
        $date_tracker = [];
        foreach ($rows as $row) {
          $normalized_date = $this->normalizeSnapshotDate($row['snapshot_date'], $field_name, $form_state);
          if ($normalized_date === NULL) {
            continue;
          }
          $date_tracker[$normalized_date] = TRUE;
          $import_data[$normalized_date]['event_registrations']['events'][] = [
            'event_id' => (int) ($row['event_id'] ?? 0),
            'event_title' => (string) ($row['event_title'] ?? ''),
            'event_start_date' => (string) ($row['event_start_date'] ?? ''),
            'registration_count' => (int) ($row['registration_count'] ?? 0),
          ];
          $all_dates[] = $normalized_date;
        }
        if (count($date_tracker) > 1) {
          $form_state->setErrorByName($field_name, $this->t('The event registrations CSV can only contain data for a single date.'));
        }
        break;

      case 'membership_types':
        foreach ($rows as $row) {
          $normalized_date = $this->normalizeSnapshotDate($row['snapshot_date'], $field_name, $form_state);
          if ($normalized_date === NULL) {
            continue;
          }
          $counts = [];
          foreach ($row as $key => $value) {
            if (strpos($key, 'membership_type_') === 0) {
              $tid = (int) substr($key, strlen('membership_type_'));
              $counts[$tid] = (int) $value;
            }
          }
          $import_data[$normalized_date]['membership_types']['types'] = [
            'members_total' => (int) ($row['members_total'] ?? array_sum($counts)),
            'counts' => $counts,
          ];
          $all_dates[] = $normalized_date;
        }
        break;

      case 'donation_metrics':
        foreach ($rows as $row) {
          $normalized_date = $this->normalizeSnapshotDate($row['snapshot_date'], $field_name, $form_state);
          if ($normalized_date === NULL) {
            continue;
          }
          $import_data[$normalized_date]['donation_metrics']['metrics'] = [
            'period_year' => (int) ($row['period_year'] ?? 0),
            'period_month' => (int) ($row['period_month'] ?? 0),
            'donors_count' => (int) ($row['donors_count'] ?? 0),
            'ytd_unique_donors' => (int) ($row['ytd_unique_donors'] ?? 0),
            'contributions_count' => (int) ($row['contributions_count'] ?? 0),
            'recurring_contributions_count' => (int) ($row['recurring_contributions_count'] ?? 0),
            'onetime_contributions_count' => (int) ($row['onetime_contributions_count'] ?? 0),
            'recurring_donors_count' => (int) ($row['recurring_donors_count'] ?? 0),
            'onetime_donors_count' => (int) ($row['onetime_donors_count'] ?? 0),
            'total_amount' => round((float) ($row['total_amount'] ?? 0), 2),
            'recurring_amount' => round((float) ($row['recurring_amount'] ?? 0), 2),
            'onetime_amount' => round((float) ($row['onetime_amount'] ?? 0), 2),
          ];
          $all_dates[] = $normalized_date;
        }
        break;

      case 'event_type_metrics':
        foreach ($rows as $row) {
          $normalized_date = $this->normalizeSnapshotDate($row['snapshot_date'], $field_name, $form_state);
          if ($normalized_date === NULL) {
            continue;
          }
          $import_data[$normalized_date]['event_type_metrics']['event_types'][] = [
            'period_year' => (int) ($row['period_year'] ?? 0),
            'period_quarter' => (int) ($row['period_quarter'] ?? 0),
            'period_month' => (int) ($row['period_month'] ?? 0),
            'event_type_id' => ($row['event_type_id'] ?? '') === '' ? NULL : (int) $row['event_type_id'],
            'event_type_label' => (string) ($row['event_type_label'] ?? 'Unknown'),
            'participant_count' => (int) ($row['participant_count'] ?? 0),
            'total_amount' => round((float) ($row['total_amount'] ?? 0), 2),
            'average_ticket' => round((float) ($row['average_ticket'] ?? 0), 2),
          ];
          $all_dates[] = $normalized_date;
        }
        break;

      case 'survey_metrics':
        foreach ($rows as $row) {
          $normalized_date = $this->normalizeSnapshotDate($row['snapshot_date'], $field_name, $form_state);
          if ($normalized_date === NULL) {
            continue;
          }
          $import_data[$normalized_date]['survey_metrics']['metrics'] = [
            'period_year' => (int) ($row['period_year'] ?? 0),
            'period_month' => (int) ($row['period_month'] ?? 0),
            'period_day' => (int) ($row['period_day'] ?? 0),
            'timeframe_label' => (string) ($row['timeframe_label'] ?? ''),
            'respondents_count' => (int) ($row['respondents_count'] ?? 0),
            'likely_recommend' => round((float) ($row['likely_recommend'] ?? 0), 2),
            'net_promoter_score' => round((float) ($row['net_promoter_score'] ?? 0), 2),
            'satisfaction_rating' => round((float) ($row['satisfaction_rating'] ?? 0), 2),
            'equipment_score' => round((float) ($row['equipment_score'] ?? 0), 2),
            'learning_resources_score' => round((float) ($row['learning_resources_score'] ?? 0), 2),
            'member_events_score' => round((float) ($row['member_events_score'] ?? 0), 2),
            'paid_workshops_score' => round((float) ($row['paid_workshops_score'] ?? 0), 2),
            'facility_score' => round((float) ($row['facility_score'] ?? 0), 2),
            'community_score' => round((float) ($row['community_score'] ?? 0), 2),
            'vibe_score' => round((float) ($row['vibe_score'] ?? 0), 2),
          ];
          $all_dates[] = $normalized_date;
        }
        break;

      case 'tool_availability':
        foreach ($rows as $row) {
          $normalized_date = $this->normalizeSnapshotDate($row['snapshot_date'], $field_name, $form_state);
          if ($normalized_date === NULL) {
            continue;
          }
          $import_data[$normalized_date]['tool_availability']['metrics'] = [
            'period_year' => (int) ($row['period_year'] ?? 0),
            'period_month' => (int) ($row['period_month'] ?? 0),
            'period_day' => (int) ($row['period_day'] ?? 0),
            'total_tools' => (int) ($row['total_tools'] ?? 0),
            'available_tools' => (int) ($row['available_tools'] ?? 0),
            'down_tools' => (int) ($row['down_tools'] ?? 0),
            'maintenance_tools' => (int) ($row['maintenance_tools'] ?? 0),
            'unknown_tools' => (int) ($row['unknown_tools'] ?? 0),
            'availability_percent' => round((float) ($row['availability_percent'] ?? 0), 2),
          ];
          $all_dates[] = $normalized_date;
        }
        break;

      default:
        $form_state->setErrorByName($field_name, $this->t('Import handling for @definition is not configured.', ['@definition' => $definition]));
        break;
    }
  }

  /**
   * Normalizes a snapshot date value to Y-m-01.
   */
  protected function normalizeSnapshotDate($value, string $field_name, FormStateInterface $form_state): ?string {
    try {
      return (new \DateTimeImmutable((string) $value))->format('Y-m-01');
    }
    catch (\Exception $e) {
      $form_state->setErrorByName($field_name, $this->t('Invalid date found in the CSV: @date', ['@date' => $value]));
      return NULL;
    }
  }

  /**
   * Reads CSV data from the managed file storage.
   */
  protected function extractCsvData($file_id, array $expected_headers) {
    $file = File::load($file_id);
    if (!$file) {
      throw new \RuntimeException('Uploaded file could not be loaded.');
    }
    $file_path = $file->getFileUri();
    return $this->parseCsvFile($file_path, $expected_headers);
  }

  /**
   * Parses a CSV file.
   */
  protected function parseCsvFile($file_path, array $expected_headers) {
    $handle = fopen($file_path, 'r');
    if (!$handle) {
      throw new \RuntimeException("Could not open the file: {$file_path}");
    }

    if (fgets($handle, 4) !== "\xef\xbb\xbf") {
      rewind($handle);
    }

    $header = fgetcsv($handle);
    if ($header !== $expected_headers) {
      fclose($handle);
      throw new \RuntimeException('The CSV file has an invalid header.');
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

  /**
   * Deletes uploaded files after processing.
   */
  protected function cleanupUploadedFiles(FormStateInterface $form_state) {
    foreach ($this->getDatasetUploadFieldMap() as $field_name) {
      if ($file_id = $form_state->getValue([$field_name, 0])) {
        if ($file = File::load($file_id)) {
          $file->delete();
        }
      }
    }
  }

}
