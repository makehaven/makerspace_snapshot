<?php

namespace Drupal\makerspace_snapshot\Form;

use Drupal\Core\Form\FormStateInterface;

/**
 * Provides a centralized interface for downloading snapshot data.
 */
class SnapshotDownloadCenterForm extends SnapshotAdminBaseForm {

  /**
   * {@inheritdoc}
   */
  public function getFormId() {
    return 'makerspace_snapshot_download_center';
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    $form['intro'] = [
      '#markup' => '<p>' . $this->t('Download historical snapshot data as CSV files or export complete snapshot packages for archival and analysis.') . '</p>',
    ];

    $historical_routes = $this->getHistoricalDownloadRouteMap();
    if (!empty($historical_routes)) {
      $datasets = $this->snapshotService->buildDefinitions();
      uasort($datasets, function (array $a, array $b) {
        return strcasecmp($a['label'] ?? '', $b['label'] ?? '');
      });

      $form['historical'] = [
        '#type' => 'details',
        '#title' => $this->t('Historical dataset downloads'),
        '#open' => TRUE,
      ];

      foreach (array_keys($historical_routes) as $definition) {
        $links = $this->buildHistoricalDownloadLinks($definition);
        if (empty($links)) {
          continue;
        }
        $form['historical'][$definition] = [
          '#type' => 'item',
          '#title' => $this->formatDefinitionLabel($definition),
          '#markup' => implode(' | ', $links),
          '#attributes' => ['class' => ['snapshot-download-option']],
        ];
      }
    }
    else {
      $form['historical_message'] = [
        '#markup' => '<p>' . $this->t('No historical dataset downloads are currently available.') . '</p>',
      ];
    }

    $export_options = $this->snapshotService->getSnapshotExportOptions();
    if (!empty($export_options)) {
      $export_options = ['' => $this->t('- Select snapshot -')] + $export_options;
    }
    else {
      $export_options = ['' => $this->t('- No snapshots available -')];
    }

    $form['snapshot_package'] = [
      '#type' => 'details',
      '#title' => $this->t('Export snapshot package'),
      '#open' => !empty($export_options) && count($export_options) > 1,
    ];

    $form['snapshot_package']['export_selection'] = [
      '#type' => 'select',
      '#title' => $this->t('Snapshot period'),
      '#options' => $export_options,
      '#empty_value' => '',
      '#description' => $this->t('Choose a snapshot period to download all datasets as a ZIP archive.'),
    ];

    $form['snapshot_package']['actions'] = [
      '#type' => 'actions',
    ];
    $form['snapshot_package']['actions']['download'] = [
      '#type' => 'submit',
      '#value' => $this->t('Download snapshot package'),
      '#submit' => ['::submitDownloadPackage'],
      '#limit_validation_errors' => [['snapshot_package', 'export_selection']],
      '#disabled' => count($export_options) <= 1,
    ];

    return $form;
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    // No-op. Specific submit handlers handle redirects.
  }

  /**
   * Submit handler for exporting a snapshot package.
   */
  public function submitDownloadPackage(array &$form, FormStateInterface $form_state) {
    $selection = (string) $form_state->getValue(['snapshot_package', 'export_selection']);
    if ($selection === '') {
      $form_state->setErrorByName('snapshot_package][export_selection', $this->t('Select a snapshot period to export.'));
      return;
    }

    if (strpos($selection, '|') === FALSE) {
      $form_state->setErrorByName('snapshot_package][export_selection', $this->t('Invalid snapshot selection.'));
      return;
    }

    [$snapshot_type, $snapshot_date] = explode('|', $selection, 2);
    $form_state->setRedirect('makerspace_snapshot.export_snapshot_package', [
      'snapshot_type' => $snapshot_type,
      'snapshot_date' => $snapshot_date,
    ]);
  }

}
