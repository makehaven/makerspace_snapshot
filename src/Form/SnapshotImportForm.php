<?php

namespace Drupal\makerspace_snapshot\Form;

use Drupal\Core\Form\FormStateInterface;
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
      '#markup' => '<p>' . $this->t('Import historical snapshot data from CSV files. You can import data for multiple dates in a single file. Dates will be normalized to the first of the month. Empty numeric values are acceptable and will be treated as 0.') . '</p>',
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

    $form['membership_totals_csv'] = [
      '#type' => 'managed_file',
      '#title' => $this->t('Membership Totals CSV'),
      '#upload_validators' => ['file_validate_extensions' => ['csv']],
      '#description' => $this->t('<a href="@url">Download Template</a>', ['@url' => Url::fromRoute('makerspace_snapshot.download_template', ['definition' => 'membership_totals'])->toString()]),
    ];

    $form['membership_activity_csv'] = [
      '#type' => 'managed_file',
      '#title' => $this->t('Membership Activity CSV'),
      '#upload_validators' => ['file_validate_extensions' => ['csv']],
      '#description' => $this->t('<a href="@url">Download Template</a>', ['@url' => Url::fromRoute('makerspace_snapshot.download_template', ['definition' => 'membership_activity'])->toString()]),
    ];

    $form['event_registrations_csv'] = [
      '#type' => 'managed_file',
      '#title' => $this->t('Event Registrations CSV'),
      '#upload_validators' => ['file_validate_extensions' => ['csv']],
      '#description' => $this->t('<a href="@url">Download Template</a>', ['@url' => Url::fromRoute('makerspace_snapshot.download_template', ['definition' => 'event_registrations'])->toString()]),
    ];

    $form['plan_csv'] = [
      '#type' => 'managed_file',
      '#title' => $this->t('Plan CSV'),
      '#upload_validators' => ['file_validate_extensions' => ['csv']],
      '#description' => $this->t('<a href="@url">Download Template</a>', ['@url' => Url::fromRoute('makerspace_snapshot.download_template', ['definition' => 'plan_levels'])->toString()]),
    ];

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

    $normalize_row = function (&$row) use ($form_state) {
      try {
        $row['snapshot_date'] = (new \DateTimeImmutable($row['snapshot_date']))->format('Y-m-01');
      }
      catch (\Exception $e) {
        $form_state->setErrorByName('membership_totals_csv', $this->t('Invalid date format found in CSV: @date', ['@date' => $row['snapshot_date']]));
        return;
      }

      foreach ($row as $key => &$value) {
        if (in_array($key, ['snapshot_date', 'plan_code', 'plan_label', 'event_title', 'event_start_date'], TRUE)) {
          continue;
        }
        if ($value === '' || !is_numeric($value)) {
          $value = 0;
        }
      }
    };

    if ($file_id = $form_state->getValue(['membership_totals_csv', 0])) {
      $data = $this->extractCsvData($file_id, ['snapshot_date', 'members_active', 'members_paused', 'members_lapsed']);
      foreach ($data as $row) {
        $normalize_row($row);
        $import_data[$row['snapshot_date']]['membership_totals']['totals'] = $row;
        $all_dates[] = $row['snapshot_date'];
      }
    }

    if ($file_id = $form_state->getValue(['membership_activity_csv', 0])) {
      $data = $this->extractCsvData($file_id, ['snapshot_date', 'joins', 'cancels', 'net_change']);
      foreach ($data as $row) {
        $normalize_row($row);
        $import_data[$row['snapshot_date']]['membership_activity']['activity'] = $row;
        $all_dates[] = $row['snapshot_date'];
      }
    }

    if ($file_id = $form_state->getValue(['plan_csv', 0])) {
      $data = $this->extractCsvData($file_id, ['snapshot_date', 'plan_code', 'plan_label', 'count_members']);
      foreach ($data as $row) {
        $normalize_row($row);
        $import_data[$row['snapshot_date']]['membership_totals']['plans'][] = $row;
      }
    }

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

    $all_dates = array_unique($all_dates);
    if (!empty($all_dates)) {
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
