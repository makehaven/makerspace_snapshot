<?php

namespace Drupal\makerspace_snapshot\Form;

use Drupal\Core\Form\ConfigFormBase;
use Drupal\Core\Form\FormStateInterface;

class SnapshotSourceConfigForm extends ConfigFormBase {

  protected function getEditableConfigNames() {
    return ['makerspace_snapshot.sources'];
  }

  public function getFormId() {
    return 'makerspace_snapshot_sources_form';
  }

  public function buildForm(array $form, FormStateInterface $form_state) {
    $c = $this->config('makerspace_snapshot.sources');

    $form['help'] = [
      '#type' => 'markup',
      '#markup' => '<p>Provide SQL that returns required columns using your data model. State queries must return: member_id, plan_code, plan_label. Event queries must return: member_id, occurred_at. Period placeholders: :start, :end</p>',
    ];

    $textarea = function($title, $key, $placeholder) use ($c) {
      return [
        '#type' => 'textarea',
        '#title' => $title,
        '#default_value' => $c->get($key) ?: '',
        '#description' => $placeholder,
        '#rows' => 8,
      ];
    };

    $form['sql_active']  = $textarea('SQL: Active members',  'sql_active',  'SELECT member_id, plan_code, plan_label FROM your_view_active_members;');
    $form['sql_paused']  = $textarea('SQL: Paused members',  'sql_paused',  'SELECT member_id, plan_code, plan_label FROM your_view_paused_members;');
    $form['sql_lapsed']  = $textarea('SQL: Lapsed members',  'sql_lapsed',  'SELECT member_id, plan_code, plan_label FROM your_view_lapsed_members;');
    $form['sql_joins']   = $textarea('SQL: Joins in period', 'sql_joins',   'SELECT member_id, occurred_at FROM your_view_joins WHERE occurred_at BETWEEN :start AND :end;');
    $form['sql_cancels'] = $textarea('SQL: Cancels in period','sql_cancels','SELECT member_id, occurred_at FROM your_view_cancels WHERE occurred_at BETWEEN :start AND :end;');

    return parent::buildForm($form, $form_state);
  }

  public function submitForm(array &$form, FormStateInterface $form_state) {
    $this->configFactory()->getEditable('makerspace_snapshot.sources')
      ->set('sql_active',  $form_state->getValue('sql_active'))
      ->set('sql_paused',  $form_state->getValue('sql_paused'))
      ->set('sql_lapsed',  $form_state->getValue('sql_lapsed'))
      ->set('sql_joins',   $form_state->getValue('sql_joins'))
      ->set('sql_cancels', $form_state->getValue('sql_cancels'))
      ->save();
    parent::submitForm($form, $form_state);
  }
}
