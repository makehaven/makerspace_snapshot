<?php

namespace Drupal\makerspace_snapshot\Controller;

use Drupal\Core\Controller\ControllerBase;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;

class SnapshotApiController extends ControllerBase {

  public function monthly(string $ym): JsonResponse {
    $month = (new \DateTimeImmutable($ym . '-01'))->modify('last day of this month')->format('Y-m-d');
    $db = \Drupal::database();

    $org = $db->select('ms_fact_monthly_org', 'o')
      ->fields('o')
      ->condition('snapshot_month', $month)
      ->execute()
      ->fetchAssoc() ?: [];

    $plans = $db->select('ms_fact_monthly_plan_counts', 'p')
      ->fields('p')
      ->condition('snapshot_month', $month)
      ->execute()
      ->fetchAllAssoc('plan_code');

    return new JsonResponse([
      'month' => $month,
      'org' => $org,
      'plans' => array_values(array_map(fn($r) => (array) $r, $plans)),
    ]);
  }

  public function compare(Request $request): JsonResponse {
    $fromYm = $request->query->get('from');
    $toYm   = $request->query->get('to');
    if (!$fromYm || !$toYm) {
      return new JsonResponse(['error' => 'Params required: ?from=YYYY-MM&to=YYYY-MM'], 400);
    }

    $toDate   = (new \DateTimeImmutable($toYm . '-01'))->modify('last day of this month')->format('Y-m-d');
    $fromDate = (new \DateTimeImmutable($fromYm . '-01'))->modify('last day of this month')->format('Y-m-d');

    $db = \Drupal::database();

    $org = $db->select('ms_fact_monthly_org', 'o')
      ->fields('o')
      ->condition('snapshot_month', [$fromDate, $toDate], 'IN')
      ->execute()
      ->fetchAllAssoc('snapshot_month');

    $plans = $db->select('ms_fact_monthly_plan_counts', 'p')
      ->fields('p')
      ->condition('snapshot_month', [$fromDate, $toDate], 'IN')
      ->execute()
      ->fetchAll();

    return new JsonResponse([
      'from' => $fromDate,
      'to'   => $toDate,
      'org'  => array_map(fn($r) => (array) $r, $org),
      'plans'=> array_map(fn($r) => (array) $r, $plans),
    ]);
  }
}
