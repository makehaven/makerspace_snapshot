<?php

/**
 * @file
 * Manual database fix script for the Makerspace Snapshot module.
 *
 * This script is intended to be run from the browser.
 * It manually applies database updates that may have failed during the
 * standard /update.php process.
 *
 * Usage:
 * 1. Log in to your Drupal site as the administrator (user ID 1).
 * 2. Navigate to this script in your browser. The path will be something like:
 *    /modules/custom/makerspace_snapshot/MANUAL_DB_FIX.php
 * 3. After running the script, delete this file for security reasons.
 */

use Drupal\Core\DrupalKernel;
use Symfony\Component\HttpFoundation\Request;

// Find the Drupal root.
$dir = __DIR__;
$drupal_root = '';
while (strlen($dir) > 1) {
  if (file_exists($dir . '/autoload.php') && (file_exists($dir . '/core/lib/Drupal.php') || file_exists($dir . '/web/core/lib/Drupal.php'))) {
    $drupal_root = $dir;
    break;
  }
  $dir = dirname($dir);
}

if (!$drupal_root) {
  die('Error: Could not find Drupal root.');
}

chdir($drupal_root);

$autoloader = require_once 'autoload.php';

// Create a minimal request.
$request = Request::createFromGlobals();

// Bootstrap Drupal.
$kernel = DrupalKernel::createFromRequest($request, $autoloader, 'prod');
$kernel->boot();
$kernel->preHandle($request);

// Check for user 1.
if (\Drupal::currentUser()->id() != 1) {
    die('Access denied. You must be logged in as the superuser (user ID 1) to run this script.');
}

$connection = \Drupal::database();
$schema = $connection->schema();

// Manually include the .install file to get the schema definition.
$module_path = \Drupal::service('extension.list.module')->getPath('makerspace_snapshot');
include_once $drupal_root . '/' . $module_path . '/makerspace_snapshot.install';

$new_schema = makerspace_snapshot_schema();

echo "<h1>Starting manual database fixes for Makerspace Snapshot module...</h1>";

// --- Action from makerspace_snapshot_update_10004 ---
echo "<h2>Running update 10004...</h2>";
if (!$schema->tableExists('ms_fact_membership_activity')) {
  $schema->createTable('ms_fact_membership_activity', $new_schema['ms_fact_membership_activity']);
  echo "<p>  - Created table: ms_fact_membership_activity</p>";
} else {
  echo "<p>  - Table already exists: ms_fact_membership_activity</p>";
}

if (!$schema->tableExists('ms_fact_event_snapshot')) {
  $schema->createTable('ms_fact_event_snapshot', $new_schema['ms_fact_event_snapshot']);
  echo "<p>  - Created table: ms_fact_event_snapshot</p>";
} else {
  echo "<p>  - Table already exists: ms_fact_event_snapshot</p>";
}

if ($schema->tableExists('ms_snapshot') && !$schema->fieldExists('ms_snapshot', 'definition')) {
  $schema->addField('ms_snapshot', 'definition', [
    'type' => 'varchar',
    'length' => 255,
    'not null' => TRUE,
    'default' => 'membership_totals',
  ]);
  echo "<p>  - Added column 'definition' to 'ms_snapshot' table.</p>";
} else {
  echo "<p>  - Column 'definition' already exists or 'ms_snapshot' table not found.</p>";
}

// --- Action from makerspace_snapshot_update_10006 ---
echo "<h2>Running update 10006...</h2>";
if ($schema->tableExists('ms_snapshot') && $schema->fieldExists('ms_snapshot', 'is_test')) {
  $schema->dropField('ms_snapshot', 'is_test');
  echo "<p>  - Dropped column 'is_test' from 'ms_snapshot' table.</p>";
} else {
  echo "<p>  - Column 'is_test' does not exist or 'ms_snapshot' table not found.</p>";
}

// Manually mark the updates as complete.
\Drupal::keyValue('system.schema')->set('makerspace_snapshot', 10006);
echo "<h2>Marked all makerspace_snapshot updates as complete.</h2>";

echo "<h2>Manual database fix script complete.</h2>";
echo "<p>You should now run the cache clear tool at <a href='/makerspace-snapshot/safe-cache-clear'>/makerspace-snapshot/safe-cache-clear</a></p>";
echo "<p><b>Please delete this script now for security reasons.</b></p>";
