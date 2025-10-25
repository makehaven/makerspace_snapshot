<?php

namespace Drupal\makerspace_snapshot\Form;

use Drupal\Core\Form\ConfigFormBase;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Database\Connection;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\Core\Queue\QueueFactory;
use Drupal\Core\Queue\QueueWorkerManagerInterface;
use Drupal\Core\Queue\SuspendQueueException;

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
   * Constructs a new SnapshotAdminForm object.
   *
   * @param \Drupal\Core\Database\Connection $database
   *   The database connection.
   */
  public function __construct(Connection $database) {
    $this->database = $database;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('database')
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
    return ['makerspace_snapshot.settings', 'makerspace_snapshot.sources'];
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    $form['description'] = [
      '#markup' => '<p>This page allows you to configure and manually trigger snapshots of your website data.</p>',
    ];

    $form['snapshot_interval'] = [
      '#type' => 'select',
      '#title' => $this->t('Snapshot Interval'),
      '#options' => [
        'monthly' => $this->t('Monthly'),
        'quarterly' => $this->t('Quarterly'),
        'annually' => $this->t('Annually'),
        'daily' => $this->t('Daily'),
      ],
      '#default_value' => $this->config('makerspace_snapshot.settings')->get('interval'),
      '#description' => $this->t('Select the interval at which snapshots should be automatically taken.'),
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

    $form['snapshot_sources'] = [
      '#type' => 'details',
      '#title' => $this->t('Snapshot SQL Queries'),
      '#open' => FALSE,
    ];

    $sources = $this->config('makerspace_snapshot.sources')->get();
    foreach ($sources as $key => $sql) {
      $form['snapshot_sources'][$key] = [
        '#type' => 'textarea',
        '#title' => $key,
        '#default_value' => $sql,
        '#rows' => 10,
      ];
    }

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
    $this->config('makerspace_snapshot.settings')
      ->set('interval', $form_state->getValue('snapshot_interval'))
      ->save();

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

        $batch = [
          'title' => $this->t('Taking snapshot...'),
          'operations' => [
            ['makerspace_snapshot_take_snapshot', [$snapshotType, $isTest]],
          ],
          'finished' => 'makerspace_snapshot_take_snapshot_finished',
        ];

        batch_set($batch);
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
