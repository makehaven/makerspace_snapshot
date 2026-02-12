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
      $acquisition = $info['acquisition'] ?? 'automated';
      if ($acquisition === 'derived') {
        continue;
      }
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
        '#upload_validators' => [
          'FileExtension' => ['extensions' => 'csv'],
        ],
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
        ->condition('snapshot_date', $all_dates, 'IN')
        ->condition('source', 'manual_import');
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
      $acquisition = $info['acquisition'] ?? 'automated';
      if ($acquisition === 'derived') {
        continue;
      }
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

    if ($definition === 'plan_levels') {
      return $this->readPlanLevelsCsv($file_id, $headers);
    }

    if (empty($headers)) {
      throw new \RuntimeException('Dataset headers are not defined.');
    }

    return $this->extractCsvData($file_id, $headers);
  }

  /**
   * Reads plan level CSV data while tolerating evolving headers.
   */
  protected function readPlanLevelsCsv($file_id, array $expected_headers): array {
    $file = File::load($file_id);
    if (!$file) {
      throw new \RuntimeException('Uploaded file could not be loaded.');
    }

    $handle = fopen($file->getFileUri(), 'r');
    if (!$handle) {
      throw new \RuntimeException('Could not open the uploaded CSV.');
    }

    if (fgets($handle, 4) !== "\xef\xbb\xbf") {
      rewind($handle);
    }

    $header = fgetcsv($handle);
    if ($header === FALSE || empty($header)) {
      fclose($handle);
      throw new \RuntimeException('The CSV file is missing a header row.');
    }

    $label_row_consumed = FALSE;
    $pending_rows = [];

    if (!$this->planHeaderMatchesExpected($header, $expected_headers)) {
      $maybe_header = fgetcsv($handle);
      if ($maybe_header === FALSE) {
        fclose($handle);
        throw new \RuntimeException('The CSV file is missing a header row.');
      }
      if ($this->planHeaderMatchesExpected($maybe_header, $expected_headers)) {
        $label_row_consumed = TRUE;
        $header = $maybe_header;
      }
      else {
        $pending_rows[] = $maybe_header;
      }
    }

    $first_column = strtolower(str_replace(' ', '_', trim((string) ($header[0] ?? ''))));
    if ($first_column !== 'snapshot_date') {
      fclose($handle);
      throw new \RuntimeException('The first column must be snapshot_date.');
    }

    if (!$label_row_consumed) {
      $peek = fgetcsv($handle);
      if ($peek !== FALSE && $this->looksLikeLabelRow($peek)) {
        $label_row_consumed = TRUE;
      }
      elseif ($peek !== FALSE) {
        $pending_rows[] = $peek;
      }
    }

    $merged_header = $header;
    foreach ($expected_headers as $expected) {
      if (!in_array($expected, $merged_header, TRUE)) {
        $merged_header[] = $expected;
      }
    }

    $row_length = count($merged_header);
    $data = [];
    while (TRUE) {
      if (!empty($pending_rows)) {
        $row = array_shift($pending_rows);
      }
      else {
        $row = fgetcsv($handle);
      }
      if ($row === FALSE) {
        break;
      }
      if (!array_filter($row)) {
        continue;
      }
      if (!$label_row_consumed && $this->looksLikeLabelRow($row)) {
        $label_row_consumed = TRUE;
        continue;
      }
      if (count($row) < $row_length) {
        $row = array_pad($row, $row_length, '');
      }
      elseif (count($row) > $row_length) {
        $row = array_slice($row, 0, $row_length);
      }
      $data[] = array_combine($merged_header, $row);
    }

    fclose($handle);
    return $data;
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
          $joins = (int) ($row['joins'] ?? 0);
          $cancels = (int) ($row['cancels'] ?? 0);
          $net_change = isset($row['net_change']) && $row['net_change'] !== ''
            ? (int) $row['net_change']
            : ($joins - $cancels);

          $import_data[$normalized_date]['membership_totals']['totals'] = [
            'members_active' => $members_active,
            'members_paused' => $members_paused,
            'members_lapsed' => $members_lapsed,
            'members_total' => $members_total,
            'joins' => $joins,
            'cancels' => $cancels,
            'net_change' => $net_change,
          ];
          $all_dates[] = $normalized_date;
        }
        break;

      case 'plan_levels':
        $plan_definitions = $this->snapshotService->getPlanLevelDefinitions();
        foreach ($rows as $row) {
          $normalized_date = $this->normalizeSnapshotDate($row['snapshot_date'], $field_name, $form_state);
          if ($normalized_date === NULL) {
            continue;
          }

          // Legacy row-per-plan imports.
          if (isset($row['plan_code']) && array_key_exists('count_members', $row)) {
            $plan_code = (string) ($row['plan_code'] ?? '');
            if ($plan_code === '') {
              continue;
            }
            if (!isset($plan_definitions[$plan_code])) {
              $this->snapshotService->registerPlanLevelDefinition($plan_code, (string) ($row['plan_label'] ?? $plan_code));
              $plan_definitions = $this->snapshotService->getPlanLevelDefinitions();
            }
            $import_data[$normalized_date]['plan_levels']['plans'][] = [
              'plan_code' => $plan_code,
              'plan_label' => (string) ($row['plan_label'] ?? ($plan_definitions[$plan_code]['label'] ?? $plan_code)),
              'count_members' => (int) ($row['count_members'] ?? 0),
            ];
            $all_dates[] = $normalized_date;
            continue;
          }

          $plans = [];
          foreach ($row as $column => $value) {
            if ($column === 'snapshot_date') {
              continue;
            }
            $plan_code = (string) $column;
            if ($plan_code === '') {
              continue;
            }
            if (!isset($plan_definitions[$plan_code])) {
              $this->snapshotService->registerPlanLevelDefinition($plan_code, $plan_code);
              $plan_definitions = $this->snapshotService->getPlanLevelDefinitions();
            }
            $plans[] = [
              'plan_code' => $plan_code,
              'plan_label' => $plan_definitions[$plan_code]['label'] ?? $plan_code,
              'count_members' => (int) ($value ?? 0),
            ];
          }
          $import_data[$normalized_date]['plan_levels']['plans'] = $plans;
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
      case 'membership_type_joins':
      case 'membership_type_cancels':
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
          $totalKey = 'members_total';
          if ($definition === 'membership_type_joins') {
            $totalKey = 'joins_total';
          }
          elseif ($definition === 'membership_type_cancels') {
            $totalKey = 'cancels_total';
          }
          $totalValue = isset($row[$totalKey]) && $row[$totalKey] !== ''
            ? (int) $row[$totalKey]
            : array_sum($counts);
          $import_data[$normalized_date][$definition]['types'] = [
            'members_total' => $totalValue,
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
            'first_time_donors_count' => (int) ($row['first_time_donors_count'] ?? 0),
            'total_amount' => round((float) ($row['total_amount'] ?? 0), 2),
            'recurring_amount' => round((float) ($row['recurring_amount'] ?? 0), 2),
            'onetime_amount' => round((float) ($row['onetime_amount'] ?? 0), 2),
          ];
          $all_dates[] = $normalized_date;
        }
        break;

      case 'donation_range_metrics':
        foreach ($rows as $row) {
          $normalized_date = $this->normalizeSnapshotDate($row['snapshot_date'], $field_name, $form_state);
          if ($normalized_date === NULL) {
            continue;
          }
          $maxAmount = $row['max_amount'] ?? NULL;
          $import_data[$normalized_date]['donation_range_metrics']['ranges'][] = [
            'period_year' => (int) ($row['period_year'] ?? 0),
            'period_month' => (int) ($row['period_month'] ?? 0),
            'is_year_to_date' => (int) ($row['is_year_to_date'] ?? 1),
            'range_key' => (string) ($row['range_key'] ?? ''),
            'range_label' => (string) ($row['range_label'] ?? ''),
            'min_amount' => round((float) ($row['min_amount'] ?? 0), 2),
            'max_amount' => ($maxAmount === '' || $maxAmount === NULL) ? NULL : round((float) $maxAmount, 2),
            'donors_count' => (int) ($row['donors_count'] ?? 0),
            'contributions_count' => (int) ($row['contributions_count'] ?? 0),
            'total_amount' => round((float) ($row['total_amount'] ?? 0), 2),
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
            'events_count' => (int) ($row['events_count'] ?? 0),
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
            'snapshot_date' => $normalized_date,
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
  protected function extractCsvData($file_id, array $expected_headers, bool $allow_label_row = TRUE) {
    $file = File::load($file_id);
    if (!$file) {
      throw new \RuntimeException('Uploaded file could not be loaded.');
    }
    $file_path = $file->getFileUri();
    return $this->parseCsvFile($file_path, $expected_headers, $allow_label_row);
  }

  /**
   * Parses a CSV file.
   */
  protected function parseCsvFile($file_path, array $expected_headers, bool $allow_label_row = TRUE) {
    $handle = fopen($file_path, 'r');
    if (!$handle) {
      throw new \RuntimeException("Could not open the file: {$file_path}");
    }

    if (fgets($handle, 4) !== "\xef\xbb\xbf") {
      rewind($handle);
    }

    $header = fgetcsv($handle);
    if ($header === FALSE) {
      fclose($handle);
      throw new \RuntimeException('The CSV file is missing a header row.');
    }

    if ($header !== $expected_headers) {
      if ($allow_label_row) {
        $maybe_header = fgetcsv($handle);
        if ($maybe_header === $expected_headers) {
          $header = $maybe_header;
        }
        else {
          fclose($handle);
          throw new \RuntimeException('The CSV file has an invalid header.');
        }
      }
      else {
        fclose($handle);
        throw new \RuntimeException('The CSV file has an invalid header.');
      }
    }

    $label_row_consumed = FALSE;

    if ($allow_label_row) {
      $position = ftell($handle);
      $peek = fgetcsv($handle);
      if ($peek !== FALSE && $this->looksLikeLabelRow($peek)) {
        $label_row_consumed = TRUE;
      }
      else {
        if ($peek !== FALSE) {
          fseek($handle, $position);
        }
      }
    }

    $data = [];
    while (($row = fgetcsv($handle)) !== FALSE) {
      if (!array_filter($row)) {
        continue;
      }
      if ($allow_label_row && $this->looksLikeLabelRow($row) && !$label_row_consumed) {
        $label_row_consumed = TRUE;
        continue;
      }
      if (count($row) !== count($header)) {
        $row = array_pad($row, count($header), '');
      }
      $data[] = array_combine($header, $row);
    }

    fclose($handle);
    return $data;
  }

  /**
   * Determines whether the provided CSV row should be treated as a label row.
   */
  protected function looksLikeLabelRow(array $row): bool {
    if (empty($row)) {
      return FALSE;
    }
    $raw = trim((string) ($row[0] ?? ''));
    if ($raw === '') {
      return FALSE;
    }
    $normalized = strtolower(preg_replace('/[\s_]+/', ' ', $raw));
    if ($normalized !== 'snapshot date') {
      return FALSE;
    }
    return strtolower($raw) !== 'snapshot_date';
  }

  /**
   * Determines if a plan-level header contains all expected machine columns.
   */
  protected function planHeaderMatchesExpected(array $header, array $expected_headers): bool {
    if (empty($header)) {
      return FALSE;
    }
    $first_column = strtolower(str_replace(' ', '_', trim((string) ($header[0] ?? ''))));
    if ($first_column !== 'snapshot_date') {
      return FALSE;
    }
    foreach ($expected_headers as $expected) {
      if ($expected === 'snapshot_date') {
        continue;
      }
      if (!in_array($expected, $header, TRUE)) {
        return FALSE;
      }
    }
    return TRUE;
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
