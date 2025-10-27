<?php

namespace Drupal\makerspace_snapshot\Form;

use Drupal\Core\Form\ConfigFormBase;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Database\Connection;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\makerspace_snapshot\SnapshotService;
use Drupal\Core\Config\ConfigFactoryInterface;

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

    $form['config_preview'] = [
      '#type' => 'details',
      '#title' => $this->t('Loaded Configurations'),
      '#open' => TRUE,
    ];

    $settings = $this->config('makerspace_snapshot.settings');
    $enabled_types = $settings->get('enabled_snapshot_types');
    if (!is_array($enabled_types)) {
        $enabled_types = $enabled_types ? [$enabled_types] : [];
    }
    $form['config_preview']['settings'] = [
      '#type' => 'item',
      '#title' => $this->t('Settings'),
      '#markup' => 'Interval: ' . $settings->get('interval') . '<br>' .
                   'Enabled Types: ' . implode(', ', $enabled_types) . '<br>' .
                   'Retention (months): ' . $settings->get('retention_window_months'),
    ];

    $metrics = $this->config('makerspace_snapshot.org_metrics')->get('metrics');
    $metric_list = '<ul>';
    if (is_array($metrics)) {
      foreach ($metrics as $metric) {
        $metric_list .= '<li>' . $metric['label'] . ' (' . $metric['id'] . ')</li>';
      }
    }
    $metric_list .= '</ul>';
    $form['config_preview']['org_metrics'] = [
      '#type' => 'item',
      '#title' => $this->t('Org Metrics'),
      '#markup' => $metric_list,
    ];

    $plans = $this->config('makerspace_snapshot.plan_levels')->get('plan_levels');
    $plan_list = '<ul>';
    if (empty($plans)) {
      $plan_list = '<p>No plan levels configured.</p>';
    } else {
      foreach ($plans as $plan) {
        $plan_list .= '<li>' . $plan['label'] . ' (' . $plan['code'] . ')</li>';
      }
      $plan_list .= '</ul>';
    }
    $form['config_preview']['plan_levels'] = [
      '#type' => 'item',
      '#title' => $this->t('Plan Levels'),
      '#markup' => $plan_list,
    ];

    $form['manual_snapshot'] = [
      '#type' => 'details',
      '#title' => $this->t('Manual Snapshot'),
      '#open' => TRUE,
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

    $form['manual_snapshot']['is_test'] = [
      '#type' => 'checkbox',
      '#title' => $this->t('Is this a test snapshot?'),
      '#description' => $this->t('Test snapshots can be deleted later.'),
    ];

    $form['manual_snapshot']['submit'] = [
      '#type' => 'submit',
      '#value' => $this->t('Take Snapshot Now'),
      '#submit' => ['::submitManualSnapshot'],
    ];

    $form['existing_snapshots'] = [
      '#type' => 'details',
      '#title' => $this->t('Existing Snapshots'),
      '#open' => TRUE,
    ];

    $header = [
      'snapshot_id' => $this->t('ID'),
      'snapshot_type' => $this->t('Type'),
      'snapshot_date' => $this->t('Date'),
      'is_test' => $this->t('Is Test?'),
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
          'snapshot_type' => $snapshot->snapshot_type,
          'snapshot_date' => $snapshot->snapshot_date,
          'is_test' => $snapshot->is_test ? $this->t('Yes') : $this->t('No'),
          'created_at' => date('Y-m-d H:i:s', $snapshot->created_at),
          'operations' => [
            '#type' => 'submit',
            '#value' => $this->t('Delete'),
            '#submit' => ['::submitDeleteSnapshot'],
            '#attributes' => ['snapshot-id' => $snapshot->id],
            '#access' => (bool) $snapshot->is_test,
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
    $config = $this->config('makerspace_snapshot.sources');
    $values = $form_state->getValue('snapshot_sources');
    if (is_array($values)) {
        foreach ($values as $key => $value) {
            $config->set($key, $value);
        }
    }
    $config->save();

    parent::submitForm($form, $form_state);
  }

  /**
   * Custom submit handler for the manual snapshot button.
   */
    public function submitManualSnapshot(array &$form, FormStateInterface $form_state) {
        $snapshotType = $form_state->getValue('snapshot_type');
        $isTest = $form_state->getValue('is_test');

        $this->snapshotService->takeSnapshot($snapshotType, $isTest);
        $this->messenger()->addMessage($this->t('Snapshot of type %type has been taken.', ['%type' => $snapshotType]));
    }

  /**
   * Custom submit handler for deleting a snapshot.
   */
    public function submitDeleteSnapshot(array &$form, FormStateInterface $form_state) {
        $snapshot_id = $form_state->getTriggeringElement()['#attributes']['snapshot-id'];

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
            $this->messenger()->addMessage($this->t('Snapshot with ID %id has been deleted.', ['%id' => $snapshot_id]));
        }
    }
}
